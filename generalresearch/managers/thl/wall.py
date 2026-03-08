import logging
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from decimal import ROUND_DOWN, Decimal
from functools import cached_property
from random import choice as rchoice
from typing import Collection, List, Optional
from uuid import uuid4

from faker import Faker
from psycopg import sql
from psycopg.rows import dict_row
from pydantic import AwareDatetime, PositiveInt

from generalresearch.managers import parse_order_by
from generalresearch.managers.base import (
    Permission,
    PostgresManager,
    PostgresManagerWithRedis,
)
from generalresearch.models import Source
from generalresearch.models.custom_types import SurveyKey, UUIDStr
from generalresearch.models.thl.definitions import (
    ReportValue,
    Status,
    StatusCode1,
    WallAdjustedStatus,
    WallStatusCode2,
)
from generalresearch.models.thl.ledger import OrderBy
from generalresearch.models.thl.session import (
    Wall,
    WallAttempt,
    check_adjusted_status_wall_consistent,
)
from generalresearch.models.thl.survey.model import TaskActivity
from generalresearch.pg_helper import PostgresConfig

logger = logging.getLogger("WallManager")
fake = Faker()


class WallManager(PostgresManager):
    def __init__(
        self,
        pg_config: PostgresConfig,
        permissions: Optional[Collection[Permission]] = None,
    ):
        assert pg_config.row_factory == dict_row
        super().__init__(pg_config=pg_config, permissions=permissions)

    def create(
        self,
        session_id: int,
        user_id: int,
        started: datetime,
        source: Source,
        req_survey_id: str,
        req_cpi: Decimal,
        buyer_id: Optional[str] = None,
        uuid_id: Optional[str] = None,
    ) -> Wall:
        """
        Creates a Wall event. Prefer to use this rather than instantiating
        the model directly, because we're explicitly defining here which keys
        should be set and which won't get set until later.
        """
        if uuid_id is None:
            uuid_id = uuid4().hex

        wall = Wall(
            session_id=session_id,
            user_id=user_id,
            uuid=uuid_id,
            started=started,
            source=source,
            buyer_id=buyer_id,
            req_survey_id=req_survey_id,
            req_cpi=req_cpi,
        )
        d = wall.model_dump_mysql()
        query = """
        INSERT INTO thl_wall (
            uuid, started, source, buyer_id, req_survey_id,
            req_cpi, survey_id, cpi, session_id
        ) VALUES (
            %(uuid)s, %(started)s, %(source)s, 
            %(buyer_id)s, %(req_survey_id)s, %(req_cpi)s,
            %(survey_id)s, %(cpi)s, %(session_id)s
        );
        """
        self.pg_config.execute_write(query=query, params=d)
        return wall

    def create_dummy(
        self,
        session_id: Optional[int] = None,
        user_id: Optional[int] = None,
        started: Optional[datetime] = None,
        source: Optional[Source] = None,
        req_survey_id: Optional[str] = None,
        req_cpi: Optional[Decimal] = None,
        buyer_id: Optional[str] = None,
        uuid_id: Optional[str] = None,
    ):
        """To be used in tests, where we don't care about certain fields"""

        user_id = user_id or fake.random_int(min=1, max=2_147_483_648)
        started = started or fake.date_time_between(
            start_date=datetime(year=1900, month=1, day=1, tzinfo=timezone.utc),
            end_date=datetime.now(tz=timezone.utc),
            tzinfo=timezone.utc,
        )

        if session_id is None:
            from generalresearch.managers.thl.session import SessionManager

            session = SessionManager(pg_config=self.pg_config).create_dummy(
                started=started
            )
            session_id = session.id

        source = source or rchoice(list(Source))
        req_survey_id = req_survey_id or uuid4().hex
        req_cpi = req_cpi or Decimal(fake.random_int(min=1, max=150) / 100).quantize(
            Decimal(".01"), rounding=ROUND_DOWN
        )

        return self.create(
            session_id=session_id,
            user_id=user_id,
            started=started,
            source=source,
            req_survey_id=req_survey_id,
            req_cpi=req_cpi,
            buyer_id=buyer_id,
            uuid_id=uuid_id,
        )

    def get_from_uuid(self, wall_uuid: UUIDStr) -> Wall:
        query = """
        SELECT
            tw.uuid, tw.source, tw.buyer_id, tw.survey_id, 
            tw.req_survey_id, tw.cpi, tw.req_cpi, tw.started,
            tw.finished, tw.status, tw.status_code_1, 
            tw.status_code_2, tw.ext_status_code_1, 
            tw.ext_status_code_2, tw.ext_status_code_3, 
            tw.report_value, tw.report_notes, tw.adjusted_status, 
            tw.adjusted_cpi, tw.adjusted_timestamp, tw.session_id,
            ts.user_id
        FROM thl_wall AS tw
        JOIN thl_session AS ts 
            ON tw.session_id = ts.id
        WHERE tw.uuid = %(wall_uuid)s
        LIMIT 2;
        """
        res = self.pg_config.execute_sql_query(query, params={"wall_uuid": wall_uuid})
        assert len(res) == 1, f"Expected 1 result, got {len(res)}"
        return Wall.model_validate(res[0])

    def get_from_uuid_if_exists(self, wall_uuid: UUIDStr) -> Optional[Wall]:
        try:
            return self.get_from_uuid(wall_uuid=wall_uuid)
        except AssertionError:
            return None

    def finish(
        self,
        wall: Wall,
        status: Status,
        status_code_1: StatusCode1,
        finished: datetime,
        ext_status_code_1: Optional[str] = None,
        ext_status_code_2: Optional[str] = None,
        ext_status_code_3: Optional[str] = None,
        status_code_2: Optional[WallStatusCode2] = None,
        survey_id: Optional[str] = None,
        cpi: Optional[Decimal] = None,
    ) -> None:
        """This wall event is finished. This would be called if/when we get a
        callback for this wall event. Some other code is responsible for
        translating external status codes to grl statuses
        """
        wall.finish(
            status=status,
            status_code_1=status_code_1,
            status_code_2=status_code_2,
            ext_status_code_1=ext_status_code_1,
            ext_status_code_2=ext_status_code_2,
            ext_status_code_3=ext_status_code_3,
            finished=finished,
            survey_id=survey_id,
            cpi=cpi,
        )
        d = {
            "status": status,
            "status_code_1": status_code_1.value,
            "status_code_2": status_code_2.value if status_code_2 else None,
            "finished": finished,
            "ext_status_code_1": ext_status_code_1,
            "ext_status_code_2": ext_status_code_2,
            "ext_status_code_3": ext_status_code_3,
            "uuid": wall.uuid,
        }
        extra = []
        if survey_id is not None:
            extra.append("survey_id = %(survey_id)s")
            d["survey_id"] = survey_id
        if cpi is not None:
            extra.append("cpi = %(cpi)s")
            d["cpi"] = str(cpi)
        extra_str = "," + ", ".join(extra) if extra else ""

        query = f"""
        UPDATE thl_wall
        SET status=%(status)s, status_code_1=%(status_code_1)s,
            status_code_2=%(status_code_2)s, finished=%(finished)s,
            ext_status_code_1=%(ext_status_code_1)s,
            ext_status_code_2=%(ext_status_code_2)s,
            ext_status_code_3=%(ext_status_code_3)s
            {extra_str}
        WHERE uuid = %(uuid)s;
        """

        with self.pg_config.make_connection() as conn:
            with conn.cursor() as c:
                c.execute(query, params=d)
                assert c.rowcount == 1
            conn.commit()

        return None

    def get_wall_events(
        self,
        session_id: Optional[PositiveInt] = None,
        session_ids: Optional[List[PositiveInt]] = None,
        order_by: OrderBy = OrderBy.ASC,
    ) -> List[Wall]:

        if session_id is not None and session_ids is not None:
            raise ValueError("Cannot provide both session_id and session_ids")

        if session_id is None and session_ids is None:
            raise ValueError("Must provide either session_id or session_ids")

        ids = session_ids if session_ids is not None else [session_id]

        if len(ids) > 500:
            raise ValueError("Cannot look up more than 500 Sessions at once.")

        query = f"""
        SELECT 
            tw.uuid, tw.source, tw.buyer_id, tw.survey_id,
            tw.req_survey_id, tw.cpi, tw.req_cpi, tw.started,
            tw.finished, tw.status, tw.status_code_1,
            tw.status_code_2, tw.ext_status_code_1,
            tw.ext_status_code_2, tw.ext_status_code_3,
            tw.report_value, tw.report_notes, tw.adjusted_status,
            tw.adjusted_cpi, tw.adjusted_timestamp, tw.session_id,
            ts.user_id
        FROM thl_wall AS tw
        JOIN thl_session AS ts
            ON tw.session_id = ts.id
        WHERE tw.session_id = ANY(%s)
        ORDER BY tw.started {order_by.value} 
        """
        res = self.pg_config.execute_sql_query(query=query, params=[ids])
        return [Wall.model_validate(d) for d in res]

    def adjust_status(
        self,
        wall: Wall,
        adjusted_timestamp: AwareDatetime,
        adjusted_status: Optional[WallAdjustedStatus] = None,
        adjusted_cpi: Optional[Decimal] = None,
    ) -> None:
        assert wall.status, "Wall must have an existing Status"

        # Be generous here, and if adjusted_status is adj to fail and
        #   adjusted_cpi is None, set it to 0
        if (
            adjusted_status == WallAdjustedStatus.ADJUSTED_TO_FAIL
            and adjusted_cpi is None
        ):
            adjusted_cpi = 0
        elif (
            adjusted_status == WallAdjustedStatus.ADJUSTED_TO_COMPLETE
            and adjusted_cpi is None
        ):
            adjusted_cpi = wall.cpi

        allowed, msg = check_adjusted_status_wall_consistent(
            status=wall.status,
            cpi=wall.cpi,
            adjusted_status=wall.adjusted_status,
            adjusted_cpi=wall.adjusted_cpi,
            new_adjusted_status=adjusted_status,
            new_adjusted_cpi=adjusted_cpi,
        )

        if not allowed:
            raise ValueError(msg)

        wall.update(
            adjusted_status=adjusted_status,
            adjusted_cpi=adjusted_cpi,
            adjusted_timestamp=adjusted_timestamp,
        )
        d = {
            "adjusted_status": (
                wall.adjusted_status.value if wall.adjusted_status else None
            ),
            "adjusted_timestamp": adjusted_timestamp,
            "adjusted_cpi": (
                str(wall.adjusted_cpi) if wall.adjusted_cpi is not None else None
            ),
            "uuid": wall.uuid,
        }

        query = sql.SQL(
            """
        UPDATE thl_wall
        SET adjusted_status = %(adjusted_status)s, 
            adjusted_timestamp = %(adjusted_timestamp)s,
            adjusted_cpi = %(adjusted_cpi)s
        WHERE uuid = %(uuid)s;
        """
        )

        with self.pg_config.make_connection() as conn:
            with conn.cursor() as c:
                c.execute(query=query, params=d)
                assert c.rowcount == 1
            conn.commit()

        return None

    def report(
        self,
        wall: Wall,
        report_value: ReportValue,
        report_notes: Optional[str] = None,
        report_timestamp: Optional[AwareDatetime] = None,
    ) -> None:
        wall.report(
            report_value=report_value,
            report_notes=report_notes,
            report_timestamp=report_timestamp,
        )
        params = {
            "uuid": wall.uuid,
            "report_value": report_value.value,
            "status": wall.status.value,
            "finished": wall.finished,
            "report_notes": report_notes,
        }
        query = sql.SQL(
            """
        UPDATE thl_wall
        SET report_value = %(report_value)s, 
            report_notes = %(report_notes)s,
            status = %(status)s, 
            finished = %(finished)s
        WHERE uuid = %(uuid)s;
        """
        )
        with self.pg_config.make_connection() as conn:
            with conn.cursor() as c:
                c.execute(query=query, params=params)
                assert c.rowcount == 1
            conn.commit()
        return None

    def filter_count_attempted_live(self, user_id: int) -> int:
        """
        Get the number of surveys this user has attempted that
        are still currently live. This can be shown as port of
        a "progress bar" for eligible, live, surveys they've
        already attempted.
        """
        query = """
        SELECT
            COUNT(1) as cnt
        FROM thl_wall w
        JOIN thl_session s ON w.session_id = s.id
        JOIN marketplace_survey ms ON
            ms.source = w.source AND
            ms.survey_id = w.req_survey_id AND
            ms.is_live
        WHERE user_id = %(user_id)s
        AND w.source != 'g'
        """
        params = {"user_id": user_id}
        res = self.pg_config.execute_sql_query(
            query=query,
            params=params,
        )
        return res[0]["cnt"]  # type: ignore[union-attr]

    def filter_wall_attempts_paginated(
        self,
        user_id: int,
        started_after: Optional[datetime] = None,
        started_before: Optional[datetime] = None,
        page: int = 1,
        size: int = 100,
        order_by: Optional[str] = "-started",
    ) -> List[WallAttempt]:
        """
        Returns WallAttempt
        """
        filters = []
        params = {}
        filters.append("user_id = %(user_id)s")
        params["user_id"] = user_id
        default_started = datetime.now(tz=timezone.utc) - timedelta(days=90)
        started_after = started_after or default_started
        started_before = started_before or datetime.now(tz=timezone.utc)
        assert (
            started_before.tzinfo == timezone.utc
        ), "started_before must be tz-aware as UTC"
        assert (
            started_after < started_before
        ), "started_after must be before started_before"
        # Don't use BETWEEN b/c we want exclusive started_after here
        filters.append(
            "(w.started > %(started_after)s AND w.started <= %(started_before)s)"
        )
        params["started_after"] = started_after
        params["started_before"] = started_before

        filter_str = "WHERE " + " AND ".join(filters) if filters else ""

        assert page >= 1, "page starts at 1"
        assert 1 <= size <= 500
        params["offset"] = (page - 1) * size
        params["limit"] = size
        paginated_filter_str = "LIMIT %(limit)s OFFSET %(offset)s"

        order_by_str = parse_order_by(order_by)
        query = f"""
        SELECT
            w.req_survey_id,
            w.started::timestamptz,
            w.source,
            w.uuid::uuid,
            s.user_id
        FROM thl_wall w
        JOIN thl_session s on w.session_id = s.id

        {filter_str}            
        {order_by_str}
        {paginated_filter_str}
        """
        res = self.pg_config.execute_sql_query(
            query=query,
            params=params,
        )
        return [WallAttempt.model_validate(x) for x in res]

    def filter_wall_attempts(
        self,
        user_id: int,
        started_after: Optional[datetime] = None,
        started_before: Optional[datetime] = None,
        order_by: Optional[str] = "-started",
    ) -> List[WallAttempt]:
        started_before = started_before or datetime.now(tz=timezone.utc)
        res = []
        page = 1
        while True:
            chunk = self.filter_wall_attempts_paginated(
                user_id=user_id,
                started_after=started_after,
                started_before=started_before,
                order_by=order_by,
                page=page,
                size=250,
            )
            res.extend(chunk)
            if not chunk:
                break
            page += 1

        return res

    def get_survey_activities(
        self, survey_keys: Collection[SurveyKey], product_id: Optional[str] = None
    ) -> List[TaskActivity]:
        query_base = """
        row_stats AS (
            SELECT
                source, survey_id,
                count(*) FILTER (WHERE effective_status IS NULL) AS in_progress_count,
                max(started) AS last_entrance,
                max(finished) FILTER (WHERE effective_status = 'c') AS last_complete
            FROM classified
            GROUP BY source, survey_id
        ),
        status_agg AS (
            SELECT
                source, survey_id,
                jsonb_object_agg(effective_status, cnt) AS status_counts
            FROM (
                SELECT source, survey_id, effective_status, count(*) AS cnt
                FROM classified
                WHERE effective_status IS NOT NULL
                GROUP BY source, survey_id, effective_status
            ) s
            GROUP BY source, survey_id
        ),
        status_code_1_agg AS (
            SELECT
                source, survey_id,
                jsonb_object_agg(status_code_1, cnt) AS status_code_1_counts
            FROM (
                SELECT source, survey_id, status_code_1, count(*) AS cnt
                FROM classified
                WHERE status_code_1 IS NOT NULL
                GROUP BY source, survey_id, status_code_1
            ) sc
            GROUP BY source, survey_id
        )
        SELECT
            rs.source,
            rs.survey_id,
            rs.in_progress_count,
            rs.last_entrance,
            rs.last_complete,
            COALESCE(sa.status_counts, '{}'::jsonb) as status_counts,
            COALESCE(sc1.status_code_1_counts, '{}'::jsonb) as status_code_1_counts
        FROM row_stats rs
        LEFT JOIN status_agg sa
               ON sa.source = rs.source
              AND sa.survey_id = rs.survey_id
        LEFT JOIN status_code_1_agg sc1
               ON sc1.source = rs.source
              AND sc1.survey_id = rs.survey_id
        ORDER BY rs.source, rs.survey_id;
        """

        params = dict()
        filters = []

        # Instead of doing a big IN with a big set of tuples, since we know
        #   we only have N possible sources, we just split by that and do e.g.:
        #      ( (source = 'x' and survey_id IN ('1', '2') ) OR
        #        (source = 'y' and survey_id IN ('3', '4') ) ... )
        sk_filters = []
        survey_source_ids = defaultdict(set)
        for sk in survey_keys:
            source, survey_id = sk.split(":")
            survey_source_ids[Source(source).value].add(survey_id)
        for source, survey_ids in survey_source_ids.items():
            sk_filters.append(
                f"(source = '{source}' AND survey_id = ANY(%(survey_ids_{source})s))"
            )
            params[f"survey_ids_{source}"] = list(survey_ids)
        # Make sure this is wrapped in parentheses!
        filters.append(f"({' OR '.join(sk_filters)})")

        product_query_join = ""
        if product_id is not None:
            product_query_join = """
            JOIN thl_session ON w.session_id = thl_session.id
            JOIN thl_user ON thl_user.id = thl_session.user_id"""
            filters.append("product_id = %(product_id)s")
            params["product_id"] = product_id

        filter_str = "WHERE " + " AND ".join(filters) if filters else ""
        query_filter = f"""
        WITH classified AS (
            SELECT
                CASE WHEN w.status IS NULL AND now() - w.started >= interval '90 minutes'
                    THEN 't' ELSE w.status
                    END AS effective_status,
                w.status_code_1,
                w.started,
                w.finished,
                w.source,
                w.survey_id
            FROM thl_wall w {product_query_join}
            {filter_str}
        ),
        """
        query = query_filter + query_base
        res = self.pg_config.execute_sql_query(query, params)
        return [TaskActivity.model_validate(x) for x in res]


class WallCacheManager(PostgresManagerWithRedis):

    @cached_property
    def wall_manager(self):
        return WallManager(pg_config=self.pg_config)

    def get_cache_key_(self, user_id: int) -> str:
        assert type(user_id) is int, "user_id must be int"
        return f"{self.cache_prefix}:generate_attempts:{user_id}"

    def get_flag_key_(self, user_id: int) -> str:
        assert type(user_id) is int, "user_id must be int"
        return f"{self.cache_prefix}:generate_attempts:flag:{user_id}"

    def is_flag_set(self, user_id: int) -> bool:
        # This flag gets set if a new wall event is created. Whenever we
        #   update the cache we'll delete the flag.
        assert type(user_id) is int, "user_id must be int"
        return bool(self.redis_client.get(self.get_flag_key_(user_id=user_id)))

    def set_flag(self, user_id: int) -> None:
        # Upon a wall entrance, set this, so we know we have to refresh the cache
        assert type(user_id) is int, "user_id must be int"
        self.redis_client.set(self.get_flag_key_(user_id=user_id), 1, ex=60 * 60 * 24)

    def clear_flag(self, user_id: int) -> None:
        assert type(user_id) is int, "user_id must be int"
        self.redis_client.delete(self.get_flag_key_(user_id=user_id))

    def get_attempts_redis_(self, user_id: int) -> List[WallAttempt]:
        redis_key = self.get_cache_key_(user_id=user_id)
        # Returns a list even if there is nothing set
        res = self.redis_client.lrange(redis_key, 0, 5000)
        attempts = [WallAttempt.model_validate_json(x) for x in res]
        return attempts

    def update_attempts_redis_(self, attempts: List[WallAttempt], user_id: int) -> None:
        if not attempts:
            return None

        redis_key = self.get_cache_key_(user_id=user_id)
        # Make sure attempts is ordered, so the most recent is last
        # "LPUSH mylist a b c will result into a list containing c as first element,
        # b as second element and a as third element"
        attempts = sorted(attempts, key=lambda x: x.started)
        json_res = [attempt.model_dump_json() for attempt in attempts]
        res = self.redis_client.lpush(redis_key, *json_res)
        self.redis_client.expire(redis_key, time=60 * 60 * 24)

        # So this doesn't grow forever, keep only the most recent 5k
        self.redis_client.ltrim(redis_key, 0, 4999)
        return None

    def get_attempts(self, user_id: PositiveInt) -> List[WallAttempt]:
        """
        This is used in the GetOpportunityIDs call to get a list of surveys
        (& surveygroups) which should be excluded for this user. We don't
        need to know the status or if they finished the survey, just they
        entered it, so we don't need to fetch 90 min backfills. The
        WallAttempts are stored in a Redis List, ordered most-recent
        in index 0.
        """
        assert type(user_id) is int, "user_id must be int"

        wall_modified = self.is_flag_set(user_id=user_id)
        if not wall_modified:
            return self.get_attempts_redis_(user_id=user_id)

        # Attempt to get the most recent wall attempt
        redis_key = self.get_cache_key_(user_id=user_id)
        res: Optional[str] = self.redis_client.lindex(redis_key, 0)  # type: ignore[assignment]
        if res is None:
            # Nothing in the cache, query for all from db
            attempts = self.wall_manager.filter_wall_attempts(user_id=user_id)
            self.update_attempts_redis_(attempts=attempts, user_id=user_id)
            self.clear_flag(user_id=user_id)
            return attempts

        # See if there is anything after the latest cached wall event we have
        w = WallAttempt.model_validate_json(res)
        new_attempts = self.wall_manager.filter_wall_attempts(
            user_id=user_id, started_after=w.started
        )
        self.update_attempts_redis_(attempts=new_attempts, user_id=user_id)
        self.clear_flag(user_id=user_id)
        return self.get_attempts_redis_(user_id=user_id)
