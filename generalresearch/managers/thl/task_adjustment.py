import logging
from datetime import datetime, timezone
from decimal import Decimal
from functools import cached_property
from typing import Optional

from generalresearch.managers import parse_order_by
from generalresearch.managers.base import (
    PostgresManager,
)
from generalresearch.managers.thl.ledger_manager.thl_ledger import (
    ThlLedgerManager,
)
from generalresearch.managers.thl.session import SessionManager
from generalresearch.managers.thl.wall import WallManager
from generalresearch.models.thl.definitions import (
    WallAdjustedStatus,
    Status,
)
from generalresearch.models.thl.session import (
    _check_adjusted_status_wall_consistent,
)
from generalresearch.models.thl.task_adjustment import TaskAdjustmentEvent


class TaskAdjustmentManager(PostgresManager):

    @cached_property
    def wall_manager(self):
        return WallManager(pg_config=self.pg_config)

    @cached_property
    def session_manager(self):
        return SessionManager(pg_config=self.pg_config)

    def filter_by_wall_uuid(
        self,
        wall_uuid,
        page: int = 1,
        size: int = 100,
        order_by: Optional[str] = "-created",
    ):
        params = {"wall_uuid": wall_uuid}
        order_by_str = parse_order_by(order_by)
        paginated_filter_str = "LIMIT %(limit)s OFFSET %(offset)s"
        params["offset"] = (page - 1) * size
        params["limit"] = size
        res = self.pg_config.execute_sql_query(
            f"""
            SELECT
                uuid,
                adjusted_status,
                ext_status_code,
                amount,
                alerted,
                created,
                user_id,
                wall_uuid,
                started,
                source,
                survey_id
            FROM thl_taskadjustment
            WHERE wall_uuid = %(wall_uuid)s 
            {order_by_str}
            {paginated_filter_str};""",
            params=params,
        )
        return [TaskAdjustmentEvent.model_validate(x) for x in res]

    def create_task_adjustment_event(self, event: TaskAdjustmentEvent):
        # Only insert a new record into thl_taskadjustment if the status for this wall_uuid
        #   is different from the last one. Don't need the same thing twice
        res = self.filter_by_wall_uuid(
            wall_uuid=event.wall_uuid, page=1, size=1, order_by="-created"
        )

        if res and event.adjusted_status == res[0].adjusted_status:
            # We already have this and it's the same change. Still call the wall_manager.adjust_status
            #   and ledger code b/c 1) it also won't do the same thing twice, and 2) we could be out of sync
            #   so check anyway.
            return res[0]

        self.pg_config.execute_write(
            """
        INSERT INTO thl_taskadjustment
        (uuid, adjusted_status, ext_status_code, amount, alerted,
        created, user_id, wall_uuid, started, source, survey_id)
        VALUES (%(uuid)s, %(adjusted_status)s, %(ext_status_code)s, %(amount)s, %(alerted)s,
        %(created)s, %(user_id)s, %(wall_uuid)s, %(started)s, %(source)s, %(survey_id)s)
        """,
            params=event.model_dump(mode="json"),
        )
        return event

    def handle_single_recon(
        self,
        ledger_manager: ThlLedgerManager,
        wall_uuid: str,
        adjusted_status: WallAdjustedStatus,
        alert_time: Optional[datetime] = None,
        ext_status_code: Optional[str] = None,
        adjusted_cpi: Optional[Decimal] = None,
    ):
        """
        We just got an adjustment notification from a marketplace.

        See note on TaskAdjustmentEvent.adjusted_status.
        These fields (specifically adjusted_status and adjusted_cpi) are CHANGES/DELTAS
            as just communicated by the marketplace, not what the Wall's final adjusted_* will be.
        """
        alert_time = alert_time or datetime.now(tz=timezone.utc)
        assert alert_time.tzinfo == timezone.utc

        wall = self.wall_manager.get_from_uuid(wall_uuid)
        session = self.session_manager.get_from_id(wall.session_id)
        user = session.user
        user.prefetch_product(self.pg_config)

        if adjusted_status == WallAdjustedStatus.ADJUSTED_TO_FAIL:
            amount_usd = wall.cpi * -1
            adjusted_cpi = 0
        elif adjusted_status == WallAdjustedStatus.ADJUSTED_TO_COMPLETE:
            amount_usd = wall.cpi
            adjusted_cpi = wall.cpi
        elif adjusted_status == WallAdjustedStatus.CPI_ADJUSTMENT:
            amount_usd = adjusted_cpi
        elif adjusted_status == WallAdjustedStatus.CONFIRMED_COMPLETE:
            amount_usd = None
        else:
            raise ValueError

        # If the wall event is a complete -> fail -> complete, we are going to
        #   receive an adjusted_status.adjust_to_complete, but internally,
        #   this is going to set the adjusted_status to None (b/c it was already a complete)
        if (
            wall.status == Status.COMPLETE
            and adjusted_status == WallAdjustedStatus.ADJUSTED_TO_COMPLETE
        ):
            new_adjusted_status = None
            new_adjusted_cpi = None
        elif (
            wall.status != Status.COMPLETE
            and adjusted_status == WallAdjustedStatus.ADJUSTED_TO_FAIL
        ):
            new_adjusted_status = None
            new_adjusted_cpi = None
        else:
            new_adjusted_status = adjusted_status
            new_adjusted_cpi = adjusted_cpi

        # Validate that this event's transition is allowed
        try:
            _check_adjusted_status_wall_consistent(
                status=wall.status,
                cpi=wall.cpi,
                adjusted_status=wall.adjusted_status,
                adjusted_cpi=wall.adjusted_cpi,
                new_adjusted_status=new_adjusted_status,
                new_adjusted_cpi=new_adjusted_cpi,
            )
        except AssertionError as e:
            logging.warning(e)
            return None

        event = TaskAdjustmentEvent(
            adjusted_status=adjusted_status,
            alerted=alert_time,
            amount=amount_usd,
            wall_uuid=wall_uuid,
            started=wall.started,
            source=wall.source,
            survey_id=wall.survey_id,
            user_id=user.user_id,
            ext_status_code=ext_status_code,
        )

        self.create_task_adjustment_event(event=event)
        self.wall_manager.adjust_status(
            wall,
            adjusted_status=new_adjusted_status,
            adjusted_cpi=new_adjusted_cpi,
            adjusted_timestamp=alert_time,
        )
        ledger_manager.create_tx_task_adjustment(wall, user=user, created=alert_time)
        session.wall_events = self.wall_manager.get_wall_events(session.id)
        self.session_manager.adjust_status(session)
        ledger_manager.create_tx_bp_adjustment(session, created=alert_time)
