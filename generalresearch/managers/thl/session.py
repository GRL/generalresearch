from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Collection, Dict, List, Optional, Tuple
from uuid import UUID, uuid4

from faker import Faker
from psycopg import sql
from pydantic import NonNegativeInt, PositiveInt

from generalresearch.managers import parse_order_by
from generalresearch.managers.base import (
    Permission,
    PostgresManager,
)
from generalresearch.managers.thl.product import ProductManager
from generalresearch.models import DeviceType
from generalresearch.models.custom_types import UUIDStr
from generalresearch.models.legacy.bucket import Bucket
from generalresearch.models.thl.definitions import (
    SessionStatusCode2,
    Status,
    StatusCode1,
)
from generalresearch.models.thl.session import (
    Session,
    Wall,
)
from generalresearch.models.thl.task_status import (
    TasksStatusResponse,
    TaskStatusResponse,
)
from generalresearch.models.thl.user import User

fake = Faker()


class SessionManager(PostgresManager):
    # I'm assuming the SessionManager will ALWAYS be passed a SqlHelper via
    #   thl_web_rw_db b/c the UPDATE operations... the SELECT operations
    #   will also be done with thl_web_rw_db bc of potential ReadReplica
    #   latency issues.

    def create(
        self,
        started: datetime,
        user: User,
        country_iso: Optional[str] = None,
        device_type: Optional[DeviceType] = None,
        ip: Optional[str] = None,
        bucket: Optional[Bucket] = None,
        url_metadata: Optional[Dict[str, str]] = None,
        uuid_id: Optional[str] = None,
    ) -> Session:
        """Creates a Session. Prefer to use this rather than instantiating the
        model directly, because we're explicitly defining here which keys
        should be set and which won't get set until later.
        """
        if uuid_id is None:
            uuid_id = uuid4().hex

        session = Session(
            uuid=uuid_id,
            started=started,
            user=user,
            country_iso=country_iso,
            device_type=device_type,
            ip=ip,
            clicked_bucket=bucket,
            url_metadata=url_metadata,
        )

        d = session.model_dump_mysql()
        query = sql.SQL(
            """
        INSERT INTO thl_session (
            uuid, user_id, started, loi_min, loi_max, 
            user_payout_min, user_payout_max, country_iso, 
            device_type, ip, url_metadata
        ) VALUES (
            %(uuid)s, %(user_id)s, %(started)s, %(loi_min)s, %(loi_max)s, 
            %(user_payout_min)s, %(user_payout_max)s, %(country_iso)s, 
            %(device_type)s, %(ip)s, %(url_metadata_json)s
        ) RETURNING id;
        """
        )
        with self.pg_config.make_connection() as conn:
            with conn.cursor() as c:
                c.execute(query=query, params=d)
                session.id = c.fetchone()["id"]  # type: ignore
            conn.commit()
        return session

    def create_dummy(
        self,
        # -- Create Dummy "optional" -- #
        started: Optional[datetime] = None,
        user: Optional[User] = None,
        # -- Optional -- #
        country_iso: Optional[str] = None,
        device_type: Optional[DeviceType] = None,
        ip: Optional[str] = None,
        bucket: Optional[Bucket] = None,
        url_metadata: Optional[Dict[str, str]] = None,
        uuid_id: Optional[str] = None,
    ) -> Session:
        """To be used in tests, where we don't care about certain fields"""
        started = started or fake.date_time_between(
            start_date=datetime(year=1900, month=1, day=1),
            end_date=datetime(year=2000, month=1, day=1),
            tzinfo=timezone.utc,
        )
        user = user or User(
            user_id=fake.random_int(min=1, max=2_147_483_648), uuid=uuid4().hex
        )

        return self.create(
            started=started,
            user=user,
            country_iso=country_iso,
            device_type=device_type,
            ip=ip,
            bucket=bucket,
            url_metadata=url_metadata,
            uuid_id=uuid_id,
        )

    def get_from_uuid(self, session_uuid: UUIDStr) -> Session:
        query = """
        SELECT  
            s.id AS session_id, 
            s.uuid AS session_uuid, 
            s.user_id, s.started, s.finished, s.loi_min, s.loi_max,
            s.user_payout_min, s.user_payout_max, s.country_iso, s.device_type, 
            s.ip, s.status, s.status_code_1, s.status_code_2, s.payout, 
            s.user_payout, s.adjusted_status, s.adjusted_payout, 
            s.adjusted_user_payout, s.adjusted_timestamp, s.url_metadata::jsonb,
            u.product_id, u.product_user_id, u.uuid AS user_uuid
        FROM thl_session AS s
        LEFT JOIN thl_user AS u
            ON s.user_id = u.id
        WHERE s.uuid = %(session_uuid)s
        LIMIT 2
        """
        res = self.pg_config.execute_sql_query(
            query=query, params={"session_uuid": session_uuid}
        )
        assert len(res) == 1
        return self.session_from_mysql(res[0])

    def get_from_id(self, session_id: int) -> Session:
        query = """
        SELECT  
            s.id AS session_id, 
            s.uuid AS session_uuid, 
            s.user_id, s.started, s.finished, s.loi_min, s.loi_max,
            s.user_payout_min, s.user_payout_max, s.country_iso, s.device_type, 
            s.ip, s.status, s.status_code_1, s.status_code_2, s.payout, 
            s.user_payout, s.adjusted_status, s.adjusted_payout, 
            s.adjusted_user_payout, s.adjusted_timestamp, s.url_metadata::jsonb,
            u.product_id, u.product_user_id, u.uuid AS user_uuid
        FROM thl_session AS s
        LEFT JOIN thl_user AS u
            ON s.user_id = u.id
        WHERE s.id = %(session_id)s
        LIMIT 2
        """
        res = self.pg_config.execute_sql_query(
            query=query, params={"session_id": session_id}
        )
        assert len(res) == 1
        return self.session_from_mysql(res[0])

    def session_from_mysql(self, d: Dict) -> Session:
        d["id"] = d.pop("session_id")
        d["uuid"] = UUID(d.pop("session_uuid")).hex
        d["user"] = User(
            product_id=UUID(d.pop("product_id")).hex,
            product_user_id=d.pop("product_user_id"),
            uuid=UUID(d.pop("user_uuid")).hex,
            user_id=d.pop("user_id"),
        )

        d["loi_min"] = (
            timedelta(seconds=d["loi_min"]) if d["loi_min"] is not None else None
        )
        d["loi_max"] = (
            timedelta(seconds=d["loi_max"]) if d["loi_max"] is not None else None
        )
        bucket_keys = [
            "loi_min",
            "loi_max",
            "user_payout_min",
            "user_payout_max",
        ]
        if all(d.get(k) is None for k in bucket_keys):
            d["clicked_bucket"] = None
        else:
            d["clicked_bucket"] = Bucket(
                loi_min=d.get("loi_min"),
                loi_max=d.get("loi_max"),
                user_payout_min=d.get("user_payout_min"),
                user_payout_max=d.get("user_payout_max"),
            )
        for k in bucket_keys:
            d.pop(k, None)
        if d["url_metadata"] is not None:
            d["url_metadata"] = {k: str(v) for k, v in d["url_metadata"].items()}
        return Session.model_validate(d)

    def finish_with_status(
        self,
        session: Session,
        finished: Optional[datetime] = None,
        status: Optional[Status] = None,
        status_code_1: Optional[StatusCode1] = None,
        status_code_2: Optional[SessionStatusCode2] = None,
        payout: Optional[Decimal] = None,
        user_payout: Optional[Decimal] = None,
    ) -> Session:
        # We have to update all the fields at once, or else we'll get
        # validation errors. There doesn't seem to be a clean way of doing this.
        # model_copy with update doesn't trigger the validators, so we
        # re-run model_validate after
        finished = finished if finished else datetime.now(tz=timezone.utc)
        session.update(
            **{
                "status": status,
                "status_code_1": status_code_1,
                "status_code_2": status_code_2,
                "finished": finished,
                "payout": payout,
                "user_payout": user_payout,
            }
        )
        d = session.model_dump_mysql()
        self.pg_config.execute_write(
            query="""
                UPDATE thl_session
                SET status = %(status)s, status_code_1 = %(status_code_1)s,
                    status_code_2 = %(status_code_2)s, finished = %(finished)s,
                    payout = %(payout)s, user_payout = %(user_payout)s
                WHERE id = %(id)s;
            """,
            params=d,
        )
        return session

    def adjust_status(self, session: Session) -> None:
        assert session.user.product, "prefetch product"
        modified = session.adjust_status()
        if not modified:
            return None

        d = {
            "adjusted_status": (
                session.adjusted_status.value if session.adjusted_status else None
            ),
            "adjusted_timestamp": session.adjusted_timestamp,
            # These are Decimals which is why we str() them
            "adjusted_payout": (
                str(session.adjusted_payout)
                if session.adjusted_payout is not None
                else None
            ),
            "adjusted_user_payout": (
                str(session.adjusted_user_payout)
                if session.adjusted_user_payout is not None
                else None
            ),
            "uuid": session.uuid,
        }

        self.pg_config.execute_write(
            query="""
                UPDATE thl_session
                SET adjusted_status = %(adjusted_status)s, 
                    adjusted_timestamp = %(adjusted_timestamp)s,
                    adjusted_payout = %(adjusted_payout)s, 
                    adjusted_user_payout = %(adjusted_user_payout)s
                WHERE uuid = %(uuid)s;
            """,
            params=d,
        )

        return None

    def filter_paginated(
        self,
        user_id: Optional[PositiveInt] = None,
        session_uuids: Optional[List[UUIDStr]] = None,
        product_uuids: Optional[List[UUIDStr]] = None,
        started_after: Optional[datetime] = None,
        started_before: Optional[datetime] = None,
        status: Optional[Status] = None,
        adjusted_after: Optional[datetime] = None,
        adjusted_before: Optional[datetime] = None,
        page: int = 1,
        size: int = 100,
        order_by: Optional[str] = "-started",
    ) -> Tuple[List[Session], int]:
        """
        Sessions are filtered using user, product_uuids, started_after, &
            started_before (if set).
          - started_after is optional, default = beginning of time
          - started_before is optional, default = now

        If page/size are passed, return only that page of the filtered (by
        account_uuid and optionally time) items. Returns (list of items, total
        (after filtering)).

         :param user_id: Return sessions from this User. Cannot pass both user_id and product_uuids
         :param product_uuids: Return sessions from these products. Cannot pass both user_id and product_uuids
         :param started_after: Filter to include this range. Default: beginning of time
         :param started_before: Filter to include this range. Default: now
         :param status: Filter for sessions with this status.
         :param adjusted_after: Filter for sessions adjusted after this timestamp.
         :param adjusted_before: Filter for sessions adjusted before this timestamp. If either adjusted_after
            or adjusted_before is not None, then only adjusted sessions will be returned.
         :param page: page starts at 1
         :param size: size of page, default (if page is not None) = 100. (1<=page<=100)
         :param order_by: Required for pagination. Uses django-rest-framework ordering syntax,
            e.g. '-created,tag' for (created desc, tag asc)
        """
        filter_str, params = self.make_filter_str(
            user_id=user_id,
            session_uuids=session_uuids,
            product_uuids=product_uuids,
            started_after=started_after,
            started_before=started_before,
            status=status,
            adjusted_after=adjusted_after,
            adjusted_before=adjusted_before,
        )

        if page is not None:
            assert type(page) is int
            assert page >= 1, "page starts at 1"
            size = size if size is not None else 100
            assert type(size) is int
            assert 1 <= size <= 100
            params["offset"] = (page - 1) * size
            params["limit"] = size
            paginated_filter_str = "LIMIT %(limit)s OFFSET %(offset)s"
            total = self.filter_count(
                user_id=user_id,
                session_uuids=session_uuids,
                product_uuids=product_uuids,
                started_after=started_after,
                started_before=started_before,
                status=status,
                adjusted_before=adjusted_before,
                adjusted_after=adjusted_after,
            )
        else:
            paginated_filter_str = ""
            # Don't need to do a count if we aren't paginating
            total = None

        order_by_str = parse_order_by(order_by)
        query = f"""
        SELECT
            s.id AS session_id, s.uuid AS session_uuid, 
            s.user_id, s.started, s.finished, s.loi_min, s.loi_max,
            s.user_payout_min, s.user_payout_max, s.country_iso, s.device_type, 
            s.ip, s.status, s.status_code_1, s.status_code_2, s.payout, 
            s.user_payout, s.adjusted_status, s.adjusted_payout, 
            s.adjusted_user_payout, s.adjusted_timestamp, s.url_metadata::jsonb,
            u.product_id, u.product_user_id, u.uuid AS user_uuid,
            COALESCE(walls.walls_json, '[]'::jsonb) AS walls_json
        FROM thl_session s
        
        JOIN thl_user u
            ON s.user_id = u.id
        
        LEFT JOIN LATERAL (
            SELECT jsonb_agg(
                    jsonb_build_object(
                        'uuid', w.uuid,
                        'started', w.started::timestamptz,
                        'finished', w.finished::timestamptz,
                        'source', w.source,
                        'survey_id', w.survey_id,
                        'req_survey_id', w.req_survey_id,
                        'cpi', w.cpi,
                        'req_cpi', w.req_cpi,
                        'buyer_id', w.buyer_id,
                        'status', w.status,
                        'status_code_1', w.status_code_1,
                        'status_code_2', w.status_code_2,
                        'ext_status_code_1', w.ext_status_code_1,
                        'ext_status_code_2', w.ext_status_code_2,
                        'ext_status_code_3', w.ext_status_code_3,
                        'adjusted_timestamp', w.adjusted_timestamp::timestamptz,
                        'adjusted_status', w.adjusted_status,
                        'adjusted_cpi', w.adjusted_cpi,
                        'report_notes', w.report_notes,
                        'report_value', w.report_value
                        )
                    ) AS walls_json
                FROM thl_wall w
                WHERE w.session_id = s.id
            ) walls ON TRUE

        {filter_str}            
        {order_by_str}
        {paginated_filter_str}
        """
        res = self.pg_config.execute_sql_query(
            query=query,
            params=params,
        )
        if total is None:
            total = len(res)

        return (
            self.session_from_mysql_rows_json(res),
            total,
        )

    def session_from_mysql_rows_json(
        self,
        rows: Collection[Dict],
    ) -> List[Session]:
        """Columns: thl_session.*, thl_user.*, walls_json
        - walls_json: list of objects, containing keys: thl_wall.*
        """
        sessions = []
        for row in rows:
            walls = [
                Wall(
                    uuid=UUID(w["uuid"]).hex,
                    started=datetime.fromisoformat(w["started"]),
                    finished=(
                        datetime.fromisoformat(w["finished"]) if w["finished"] else None
                    ),
                    source=w["source"],
                    survey_id=w["survey_id"],
                    buyer_id=w["buyer_id"],
                    status=w["status"],
                    status_code_1=w["status_code_1"],
                    status_code_2=w["status_code_2"],
                    ext_status_code_1=w["ext_status_code_1"],
                    ext_status_code_2=w["ext_status_code_2"],
                    ext_status_code_3=w["ext_status_code_3"],
                    adjusted_cpi=(
                        Decimal(w["adjusted_cpi"]).quantize(Decimal("0.01"))
                        if w["adjusted_cpi"] is not None
                        else None
                    ),
                    adjusted_status=w["adjusted_status"],
                    adjusted_timestamp=(
                        datetime.fromisoformat(w["adjusted_timestamp"])
                        if w["adjusted_timestamp"]
                        else None
                    ),
                    report_notes=w["report_notes"],
                    report_value=w["report_value"],
                    req_survey_id=w["req_survey_id"],
                    req_cpi=Decimal(w["req_cpi"]).quantize(Decimal("0.01")),
                    cpi=Decimal(w["cpi"]).quantize(Decimal("0.01")),
                    session_id=row["session_id"],
                    user_id=row["user_id"],
                )
                for w in row["walls_json"]
            ]
            walls = sorted(walls, key=lambda x: x.started)
            row.pop("walls_json")
            s = self.session_from_mysql(row)
            s.wall_events = walls
            sessions.append(s)
        return sessions

    @staticmethod
    def make_filter_str(
        user_id: Optional[int] = None,
        session_uuids: Optional[List[UUIDStr]] = None,
        product_uuids: Optional[List[UUIDStr]] = None,
        started_after: Optional[datetime] = None,
        started_before: Optional[datetime] = None,
        status: Optional[Status] = None,
        adjusted_after: Optional[datetime] = None,
        adjusted_before: Optional[datetime] = None,
        extra_filters: Optional[str] = None,
    ) -> Tuple[str, Dict[str, Any]]:
        filters = []
        params = {}

        if started_before or started_after:
            started_after = started_after or datetime(2017, 1, 1, tzinfo=timezone.utc)
            started_before = started_before or datetime.now(tz=timezone.utc)
            assert (
                started_after.tzinfo == timezone.utc
            ), "started_after must be tz-aware as UTC"
            assert (
                started_before.tzinfo == timezone.utc
            ), "started_before must be tz-aware as UTC"
            assert (
                started_after < started_before
            ), "started_after must be before started_before"
            filters.append("started BETWEEN %(started_after)s AND %(started_before)s")
            params["started_after"] = started_after
            params["started_before"] = started_before

        if adjusted_before or adjusted_after:
            adjusted_after = adjusted_after or datetime(2017, 1, 1, tzinfo=timezone.utc)
            adjusted_before = adjusted_before or datetime.now(tz=timezone.utc)
            assert (
                adjusted_after.tzinfo == timezone.utc
            ), "adjusted_after must be tz-aware as UTC"
            assert (
                adjusted_before.tzinfo == timezone.utc
            ), "adjusted_before must be tz-aware as UTC"
            assert (
                adjusted_after < adjusted_before
            ), "adjusted_after must be before adjusted_before"
            filters.append(
                "adjusted_timestamp BETWEEN %(adjusted_after)s AND %(adjusted_before)s"
            )
            params["adjusted_after"] = adjusted_after
            params["adjusted_before"] = adjusted_before

        if user_id:
            assert product_uuids is None
            filters.append("user_id = %(user_id)s")
            params["user_id"] = user_id

        if product_uuids:
            assert user_id is None
            filters.append("product_id = ANY(%(product_uuids)s)")
            params["product_uuids"] = product_uuids

        if session_uuids:
            filters.append("s.uuid = ANY(%(session_uuids)s)")
            params["session_uuids"] = session_uuids

        if status:
            # We need to include the cases where status is NULL as ABANDON. We'll handle the distinction
            #   between TIMEOUT (no status, older than 90 min) and UNKNOWN (no status, newer than 90 min) later.
            params["status"] = status.value
            filters.append(f"COALESCE(status, 'a') = %(status)s")

        if extra_filters:
            filters.append(extra_filters)

        filter_str = "WHERE " + " AND ".join(filters) if filters else ""
        return filter_str, params

    def filter(
        self,
        started_since: Optional[datetime] = None,
        started_between: Optional[Tuple[datetime, datetime]] = None,
        user: Optional[User] = None,
        product_uuids: Optional[List[UUIDStr]] = None,
        team_uuids: Optional[List[UUIDStr]] = None,
        business_uuids: Optional[List[UUIDStr]] = None,
        order_by: str = "-started",
        limit: Optional[int] = None,
    ) -> List[Session]:
        # to be deprecated ...

        if team_uuids:
            raise NotImplementedError("Cannot filter by Teams (yet)")

        if business_uuids:
            raise NotImplementedError("Cannot filter by Businesses (yet)")

        if started_since and started_between:
            raise ValueError()
        started_after = None
        started_before = None
        if started_since:
            started_after = started_since
        if started_between:
            started_after, started_before = started_between

        return self.filter_paginated(
            user_id=user.user_id if user is not None else None,
            product_uuids=product_uuids,
            started_after=started_after,
            started_before=started_before,
            size=limit or 100,
            order_by=order_by,
        )[0]

    def filter_count(
        self,
        user_id: Optional[int] = None,
        session_uuids: Optional[List[UUIDStr]] = None,
        product_uuids: Optional[List[UUIDStr]] = None,
        started_after: Optional[datetime] = None,
        started_before: Optional[datetime] = None,
        status: Optional[Status] = None,
        adjusted_after: Optional[datetime] = None,
        adjusted_before: Optional[datetime] = None,
        extra_filters: Optional[str] = None,
    ) -> NonNegativeInt:
        filter_str, params = self.make_filter_str(
            user_id=user_id,
            session_uuids=session_uuids,
            product_uuids=product_uuids,
            started_after=started_after,
            started_before=started_before,
            status=status,
            adjusted_after=adjusted_after,
            adjusted_before=adjusted_before,
            extra_filters=extra_filters,
        )

        res = self.pg_config.execute_sql_query(
            query=f"""
            SELECT COUNT(1) AS cnt
            FROM thl_session AS s
            JOIN thl_user AS u 
                ON s.user_id = u.id
            {filter_str}
            """,
            params=params,
        )
        return res[0]["cnt"] if res else 0

    def get_task_status_response(
        self, session_uuid: UUIDStr
    ) -> Optional[TaskStatusResponse]:
        res, total = self.filter_paginated(session_uuids=[session_uuid])
        if total == 0:
            return None
        session = res[0]
        PM = ProductManager(pg_config=self.pg_config, permissions=[Permission.READ])
        product = PM.get_by_uuid(product_uuid=session.user.product_id)
        return TaskStatusResponse.from_session(session=session, product=product)

    def get_tasks_status_response(
        self,
        product_uuid: UUIDStr,
        user_id: Optional[int] = None,
        started_after: Optional[datetime] = None,
        started_before: Optional[datetime] = None,
        status: Optional[Status] = None,
        adjusted_after: Optional[datetime] = None,
        adjusted_before: Optional[datetime] = None,
        page: int = 1,
        size: int = 100,
        order_by: Optional[str] = "-started",
    ) -> Optional[TasksStatusResponse]:
        PM = ProductManager(pg_config=self.pg_config, permissions=[Permission.READ])
        product = PM.get_by_uuid(product_uuid=product_uuid)

        # This is for filtering. If we're not filtering by user, then add the product_id filter
        product_uuids = [product_uuid] if user_id is None else None
        res, total = self.filter_paginated(
            user_id=user_id,
            product_uuids=product_uuids,
            started_after=started_after,
            started_before=started_before,
            status=status,
            adjusted_after=adjusted_after,
            adjusted_before=adjusted_before,
            page=page,
            size=size,
            order_by=order_by,
        )
        tsrs = [
            TaskStatusResponse.from_session(session=session, product=product)
            for session in res
        ]

        return TasksStatusResponse.model_validate(
            {"tasks_status": tsrs, "page": page, "size": size, "total": total}
        )
