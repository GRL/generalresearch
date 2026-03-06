import logging
from random import randint

import pytest
from datetime import datetime, timezone, timedelta
from decimal import Decimal

from generalresearch.models import Source
from generalresearch.models.thl.definitions import (
    Status,
    StatusCode1,
    WallAdjustedStatus,
)


@pytest.fixture()
def session_complete(session_with_tx_factory, user):
    return session_with_tx_factory(
        user=user, final_status=Status.COMPLETE, wall_req_cpi=Decimal("1.23")
    )


@pytest.fixture()
def session_complete_with_wallet(session_with_tx_factory, user_with_wallet):
    return session_with_tx_factory(
        user=user_with_wallet,
        final_status=Status.COMPLETE,
        wall_req_cpi=Decimal("1.23"),
    )


@pytest.fixture()
def session_fail(user, session_manager, wall_manager):
    session = session_manager.create_dummy(
        started=datetime.now(timezone.utc), user=user
    )
    wall1 = wall_manager.create_dummy(
        session_id=session.id,
        user_id=user.user_id,
        source=Source.DYNATA,
        req_survey_id="72723",
        req_cpi=Decimal("3.22"),
        started=datetime.now(timezone.utc),
    )
    wall_manager.finish(
        wall=wall1,
        status=Status.FAIL,
        status_code_1=StatusCode1.PS_FAIL,
        finished=wall1.started + timedelta(seconds=randint(a=60 * 2, b=60 * 10)),
    )
    session.wall_events.append(wall1)
    return session


class TestHandleRecons:

    def test_complete_to_recon(
        self,
        session_complete,
        thl_lm,
        task_adjustment_manager,
        wall_manager,
        session_manager,
        caplog,
    ):
        print(wall_manager.pg_config.dsn)
        mid = session_complete.uuid
        wall_uuid = session_complete.wall_events[-1].uuid
        s = session_complete
        ledger_manager = thl_lm

        revenue_account = ledger_manager.get_account_task_complete_revenue()
        current_amount = ledger_manager.get_account_filtered_balance(
            revenue_account, "thl_wall", wall_uuid
        )
        assert (
            current_amount == 123
        ), "this is the amount of revenue from this task complete"

        bp_wallet_account = ledger_manager.get_account_or_create_bp_wallet(
            s.user.product
        )
        current_bp_payout = ledger_manager.get_account_filtered_balance(
            bp_wallet_account, "thl_session", mid
        )
        assert current_bp_payout == 117, "this is the amount paid to the BP"

        # Do the work here !! ----v
        task_adjustment_manager.handle_single_recon(
            ledger_manager=thl_lm,
            wall_uuid=wall_uuid,
            adjusted_status=WallAdjustedStatus.ADJUSTED_TO_FAIL,
        )
        assert (
            len(task_adjustment_manager.filter_by_wall_uuid(wall_uuid=wall_uuid)) == 1
        )

        current_amount = ledger_manager.get_account_filtered_balance(
            revenue_account, "thl_wall", wall_uuid
        )
        assert current_amount == 0, "after recon, it should be zeroed"
        current_bp_payout = ledger_manager.get_account_filtered_balance(
            bp_wallet_account, "thl_session", mid
        )
        assert current_bp_payout == 0, "this is the amount paid to the BP"
        commission_account = ledger_manager.get_account_or_create_bp_commission(
            s.user.product
        )
        assert ledger_manager.get_account_balance(commission_account) == 0

        # Now, say we get the exact same *adjust to incomplete* msg again. It should do nothing!
        adjusted_timestamp = datetime.now(tz=timezone.utc)
        wall = wall_manager.get_from_uuid(wall_uuid=wall_uuid)
        with pytest.raises(match=" is already "):
            wall_manager.adjust_status(
                wall,
                adjusted_status=WallAdjustedStatus.ADJUSTED_TO_FAIL,
                adjusted_timestamp=adjusted_timestamp,
                adjusted_cpi=Decimal(0),
            )

        session = session_manager.get_from_id(wall.session_id)
        user = session.user
        with caplog.at_level(logging.INFO):
            ledger_manager.create_tx_task_adjustment(
                wall, user=user, created=adjusted_timestamp
            )
        assert "No transactions needed" in caplog.text

        session.wall_events = wall_manager.get_wall_events(session.id)
        session.user.prefetch_product(wall_manager.pg_config)

        with caplog.at_level(logging.INFO, logger="Wall"):
            session_manager.adjust_status(session)
        assert "is already f" in caplog.text or "is already Status.FAIL" in caplog.text

        with caplog.at_level(logging.INFO, logger="LedgerManager"):
            ledger_manager.create_tx_bp_adjustment(session, created=adjusted_timestamp)
        assert "No transactions needed" in caplog.text

        current_amount = ledger_manager.get_account_filtered_balance(
            revenue_account, "thl_wall", wall_uuid
        )
        assert current_amount == 0, "after recon, it should be zeroed"
        current_bp_payout = ledger_manager.get_account_filtered_balance(
            bp_wallet_account, "thl_session", mid
        )
        assert current_bp_payout == 0, "this is the amount paid to the BP"

        # And if we get an adj to fail, and handle it, it should do nothing at all
        task_adjustment_manager.handle_single_recon(
            ledger_manager=thl_lm,
            wall_uuid=wall_uuid,
            adjusted_status=WallAdjustedStatus.ADJUSTED_TO_FAIL,
        )
        assert (
            len(task_adjustment_manager.filter_by_wall_uuid(wall_uuid=wall_uuid)) == 1
        )
        current_amount = ledger_manager.get_account_filtered_balance(
            revenue_account, "thl_wall", wall_uuid
        )
        assert current_amount == 0, "after recon, it should be zeroed"

    def test_fail_to_complete(self, session_fail, thl_lm, task_adjustment_manager):
        s = session_fail
        mid = session_fail.uuid
        wall_uuid = session_fail.wall_events[-1].uuid
        ledger_manager = thl_lm

        revenue_account = ledger_manager.get_account_task_complete_revenue()
        current_amount = ledger_manager.get_account_filtered_balance(
            revenue_account, "thl_wall", mid
        )
        assert (
            current_amount == 0
        ), "this is the amount of revenue from this task complete"

        bp_wallet_account = ledger_manager.get_account_or_create_bp_wallet(
            s.user.product
        )
        current_bp_payout = ledger_manager.get_account_filtered_balance(
            bp_wallet_account, "thl_session", mid
        )
        assert current_bp_payout == 0, "this is the amount paid to the BP"

        task_adjustment_manager.handle_single_recon(
            ledger_manager=thl_lm,
            wall_uuid=wall_uuid,
            adjusted_status=WallAdjustedStatus.ADJUSTED_TO_COMPLETE,
        )

        current_amount = ledger_manager.get_account_filtered_balance(
            revenue_account, "thl_wall", wall_uuid
        )
        assert current_amount == 322, "after recon, we should be paid"
        current_bp_payout = ledger_manager.get_account_filtered_balance(
            bp_wallet_account, "thl_session", mid
        )
        assert current_bp_payout == 306, "this is the amount paid to the BP"

        # Now reverse it back to fail
        task_adjustment_manager.handle_single_recon(
            ledger_manager=thl_lm,
            wall_uuid=wall_uuid,
            adjusted_status=WallAdjustedStatus.ADJUSTED_TO_FAIL,
        )

        current_amount = ledger_manager.get_account_filtered_balance(
            revenue_account, "thl_wall", wall_uuid
        )
        assert current_amount == 0
        current_bp_payout = ledger_manager.get_account_filtered_balance(
            bp_wallet_account, "thl_session", mid
        )
        assert current_bp_payout == 0

        commission_account = ledger_manager.get_account_or_create_bp_commission(
            s.user.product
        )
        assert ledger_manager.get_account_balance(commission_account) == 0

    def test_complete_already_complete(
        self, session_complete, thl_lm, task_adjustment_manager
    ):
        s = session_complete
        mid = session_complete.uuid
        wall_uuid = session_complete.wall_events[-1].uuid
        ledger_manager = thl_lm

        for _ in range(4):
            # just run it 4 times to make sure nothing happens 4 times
            task_adjustment_manager.handle_single_recon(
                ledger_manager=thl_lm,
                wall_uuid=wall_uuid,
                adjusted_status=WallAdjustedStatus.ADJUSTED_TO_COMPLETE,
            )

        revenue_account = ledger_manager.get_account_task_complete_revenue()
        bp_wallet_account = ledger_manager.get_account_or_create_bp_wallet(
            s.user.product
        )
        commission_account = ledger_manager.get_account_or_create_bp_commission(
            s.user.product
        )
        current_amount = ledger_manager.get_account_filtered_balance(
            revenue_account, "thl_wall", wall_uuid
        )
        assert current_amount == 123
        assert ledger_manager.get_account_balance(commission_account) == 6

        current_bp_payout = ledger_manager.get_account_filtered_balance(
            bp_wallet_account, "thl_session", mid
        )
        assert current_bp_payout == 117

    def test_incomplete_already_incomplete(
        self, session_fail, thl_lm, task_adjustment_manager
    ):
        s = session_fail
        mid = session_fail.uuid
        wall_uuid = session_fail.wall_events[-1].uuid
        ledger_manager = thl_lm

        for _ in range(4):
            task_adjustment_manager.handle_single_recon(
                ledger_manager=thl_lm,
                wall_uuid=wall_uuid,
                adjusted_status=WallAdjustedStatus.ADJUSTED_TO_FAIL,
            )

        revenue_account = ledger_manager.get_account_task_complete_revenue()
        bp_wallet_account = ledger_manager.get_account_or_create_bp_wallet(
            s.user.product
        )
        commission_account = ledger_manager.get_account_or_create_bp_commission(
            s.user.product
        )
        current_amount = ledger_manager.get_account_filtered_balance(
            revenue_account, "thl_wall", mid
        )
        assert current_amount == 0
        assert ledger_manager.get_account_balance(commission_account) == 0

        current_bp_payout = ledger_manager.get_account_filtered_balance(
            bp_wallet_account, "thl_session", mid
        )
        assert current_bp_payout == 0

    def test_complete_to_recon_user_wallet(
        self,
        session_complete_with_wallet,
        user_with_wallet,
        thl_lm,
        task_adjustment_manager,
    ):
        s = session_complete_with_wallet
        mid = s.uuid
        wall_uuid = s.wall_events[-1].uuid
        ledger_manager = thl_lm

        revenue_account = ledger_manager.get_account_task_complete_revenue()
        amount = ledger_manager.get_account_filtered_balance(
            revenue_account, "thl_wall", wall_uuid
        )
        assert amount == 123, "this is the amount of revenue from this task complete"

        bp_wallet_account = ledger_manager.get_account_or_create_bp_wallet(
            s.user.product
        )
        user_wallet_account = ledger_manager.get_account_or_create_user_wallet(s.user)
        commission_account = ledger_manager.get_account_or_create_bp_commission(
            s.user.product
        )
        amount = ledger_manager.get_account_filtered_balance(
            bp_wallet_account, "thl_session", mid
        )
        assert amount == 70, "this is the amount paid to the BP"
        amount = ledger_manager.get_account_filtered_balance(
            user_wallet_account, "thl_session", mid
        )
        assert amount == 47, "this is the amount paid to the user"
        assert (
            ledger_manager.get_account_balance(commission_account) == 6
        ), "earned commission"

        task_adjustment_manager.handle_single_recon(
            ledger_manager=thl_lm,
            wall_uuid=wall_uuid,
            adjusted_status=WallAdjustedStatus.ADJUSTED_TO_FAIL,
        )

        amount = ledger_manager.get_account_filtered_balance(
            revenue_account, "thl_wall", wall_uuid
        )
        assert amount == 0
        amount = ledger_manager.get_account_filtered_balance(
            bp_wallet_account, "thl_session", mid
        )
        assert amount == 0
        amount = ledger_manager.get_account_filtered_balance(
            user_wallet_account, "thl_session", mid
        )
        assert amount == 0
        assert (
            ledger_manager.get_account_balance(commission_account) == 0
        ), "earned commission"
