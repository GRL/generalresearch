from __future__ import annotations

from collections.abc import Collection
from datetime import datetime
from typing import TYPE_CHECKING, Any

from psycopg import sql
from pydantic import NonNegativeInt, PositiveInt

from generalresearch.grliq.models.events import PointerMove, TimingData
from generalresearch.grliq.models.forensic_data import (
    COLUMN_ONLY_FIELDS,
    GrlIqData,
)
from generalresearch.grliq.models.forensic_result import (
    GrlIqCheckerResults,
    GrlIqForensicCategoryResult,
    Phase,
)
from generalresearch.models.custom_types import UUIDStr

if TYPE_CHECKING:
    from generalresearch.models.thl.user import User
    from generalresearch.models.thl.user_ref import UserRef
    from generalresearch.pg_helper import PostgresConfig


class GrlIqDataManager:
    def __init__(self, postgres_config: PostgresConfig):
        self.postgres_config = postgres_config

    def create(
        self,
        iq_data: GrlIqData,
        result_data: GrlIqCheckerResults | None = None,
        category_result: GrlIqForensicCategoryResult | None = None,
        fraud_score: int | None = None,
        is_attempt_allowed: bool | None = None,
    ) -> GrlIqData:

        data = iq_data.model_dump_sql(
            exclude={
                "events",
                "mouse_events",
                "timing_data",
                "results",
                "category_result",
            }
        )

        data["result_data"] = None
        if result_data:
            data["result_data"] = result_data.model_dump_json(
                exclude_none=True, exclude={"is_complete"}
            )
            iq_data.results = result_data

        data["category_result"] = None
        if category_result:
            data["category_result"] = category_result.model_dump_json()
            iq_data.category_result = category_result

        data["fingerprint"] = iq_data.fingerprint
        data["fraud_score"] = fraud_score
        data["is_attempt_allowed"] = is_attempt_allowed

        query = sql.SQL("""
         INSERT INTO grliq_forensicdata
            (uuid, session_uuid, created_at, product_id, product_user_id,
            business_id, team_id,
            country_iso, client_ip, ua_browser_family, ua_browser_version,
            ua_os_family, ua_os_version, ua_device_family, ua_device_brand,
            ua_device_model, ua_hash, data, phase,
            fingerprint, fraud_score, is_attempt_allowed,
            result_data, category_result)
         VALUES 
            (%(uuid)s, %(session_uuid)s, %(created_at)s, %(product_id)s, %(product_user_id)s,
            %(business_id)s, %(team_id)s,
            %(country_iso)s, %(client_ip)s, %(ua_browser_family)s, %(ua_browser_version)s,
            %(ua_os_family)s, %(ua_os_version)s, %(ua_device_family)s, %(ua_device_brand)s,
            %(ua_device_model)s, %(ua_hash)s, %(data)s, %(phase)s,
            %(fingerprint)s, %(fraud_score)s, %(is_attempt_allowed)s,
            %(result_data)s, %(category_result)s)
         RETURNING id
         """)

        with self.postgres_config.make_connection() as conn, conn.cursor() as c:
            c.execute(query, data)
            pk = c.fetchone()["id"]  # type: ignore
            conn.commit()

        iq_data.id = pk

        return iq_data

    def set_results(
        self,
        uuid: UUIDStr,
        result_data: GrlIqCheckerResults,
        category_result: GrlIqForensicCategoryResult,
        fingerprint: str | None = None,
        fraud_score: int | None = None,
        is_attempt_allowed: bool | None = None,
    ) -> None:
        data = {"uuid": uuid}
        data["result_data"] = result_data.model_dump_json(exclude_none=True)
        data["category_result"] = category_result.model_dump_json()
        data["fingerprint"] = fingerprint
        data["fraud_score"] = fraud_score
        data["is_attempt_allowed"] = is_attempt_allowed

        query = sql.SQL("""
          UPDATE grliq_forensicdata
          SET result_data = %(result_data)s,
          category_result = %(category_result)s,
          fingerprint = %(fingerprint)s,
          fraud_score = %(fraud_score)s,
          is_attempt_allowed = %(is_attempt_allowed)s
          WHERE uuid = %(uuid)s
          """)
        with self.postgres_config.make_connection() as conn, conn.cursor() as c:
            c.execute(query, data)
            if c.rowcount != 1:
                raise ValueError(
                    f"Expected 1 row to be updated, but {c.rowcount} rows were affected."
                )
            conn.commit()

    def update_fingerprint(self, iq_data: GrlIqData) -> None:
        # We should only run this if we modified the fingerprint algorithm
        if "fingerprint" in iq_data.__dict__:
            # make sure it's not cached
            del iq_data.__dict__["fingerprint"]
        data = {"uuid": iq_data.uuid, "fingerprint": iq_data.fingerprint}
        query = sql.SQL("""
         UPDATE grliq_forensicdata 
         SET fingerprint = %(fingerprint)s
         WHERE uuid = %(uuid)s
         """)
        with self.postgres_config.make_connection() as conn, conn.cursor() as c:
            c.execute(query, data)
            if c.rowcount != 1:
                raise ValueError(
                    f"Expected 1 row to be updated, but {c.rowcount} rows were affected."
                )
            conn.commit()

    def update_data(self, iq_data: GrlIqData) -> None:
        # We should only run this if we structured new fields and want to
        # back-populate them in the db
        data = {"id": iq_data.id, "data": iq_data.model_dump_sql()["data"]}
        query = sql.SQL("""
         UPDATE grliq_forensicdata 
         SET data = %(data)s
         WHERE id = %(id)s
         """)
        with self.postgres_config.make_connection() as conn, conn.cursor() as c:
            c.execute(query, data)
            if c.rowcount != 1:
                raise ValueError(
                    f"Expected 1 row to be updated, but {c.rowcount} rows were affected."
                )
            conn.commit()

    def get_data_if_exists(
        self, forensic_uuid: UUIDStr, load_events: bool = False
    ) -> GrlIqData | None:
        try:
            return self.get_data(forensic_uuid=forensic_uuid, load_events=load_events)
        except AssertionError:
            return None

    def get_data(
        self,
        forensic_id: PositiveInt | None = None,
        forensic_uuid: UUIDStr | None = None,
        load_events: bool = False,
    ) -> GrlIqData:
        from generalresearch.grliq.managers.forensic_events import (
            GrlIqEventManager,
        )

        assert any([forensic_id, forensic_uuid]), "Must provide a Forensic ID or UUID"

        if load_events:
            # Gets the forensicevents where the 1) session_uuid matches the
            # forensic items' session, 2) event_start is closest to the
            # created_at for this forensic item, and within 1 minute.

            query = sql.SQL("""
                SELECT d.id, d.data, d.team_id, d.business_id,
                    e.events, e.mouse_events, t.timing_data
                FROM grliq_forensicdata d
                -- Closest event_start within 1 minute
                LEFT JOIN LATERAL (
                    SELECT events, mouse_events, timing_data
                    FROM grliq_forensicevents e
                    WHERE e.session_uuid = d.session_uuid
                        AND ABS(EXTRACT(EPOCH FROM (e.event_start - d.created_at))) <= 60
                    ORDER BY ABS(EXTRACT(EPOCH FROM (e.event_start - d.created_at))) ASC
                    LIMIT 1
                ) e ON true
                -- Most recent timing_data by id
                LEFT JOIN LATERAL (
                    SELECT timing_data
                    FROM grliq_forensicevents e2
                    WHERE e2.session_uuid = d.session_uuid
                    ORDER BY e2.id DESC
                    LIMIT 1
                ) t ON true
            """)

        else:
            query = sql.SQL("""
                SELECT d.id, d.data, d.team_id, d.business_id
                FROM grliq_forensicdata d
            """)

        if forensic_id is not None:
            column_name = "id"
            param_value = forensic_id
        else:
            column_name = "uuid"
            param_value = forensic_uuid

        where_clause = sql.SQL(" WHERE {} = %s").format(
            sql.Identifier("d", column_name)
        )
        limit_clause = sql.SQL(" LIMIT 1")
        q1 = sql.Composed([query, where_clause, limit_clause])

        with self.postgres_config.make_connection() as conn, conn.cursor() as c:
            c.execute(query=q1, params=(param_value,))
            x = c.fetchone()

        assert x is not None, f"GrlIqDataManager.get_data({forensic_uuid=}) not found"

        self.temporary_add_missing_fields(x["data"])
        self.add_column_only_fields(x)
        x["data"]["id"] = x["id"]
        d = GrlIqData.model_validate(x["data"])

        if load_events:
            d.events = x["events"] if x["events"] is not None else []
            d.pointer_move_events = (
                [PointerMove.from_dict(e) for e in x["mouse_events"]]
                if x["mouse_events"] is not None
                else []
            )
            d.timing_data = (
                TimingData.model_validate(x["timing_data"])
                if x["timing_data"] is not None
                else None
            )
            d.mouse_events = (
                GrlIqEventManager.process_mouse_events(
                    events=d.events or [],
                    pointer_moves=d.pointer_move_events or [],
                )
                if d.events is not None
                else []
            )
            d.keyboard_events = (
                GrlIqEventManager.process_keyboard_events(events=d.events)
                if d.events is not None
                else []
            )

        return d

    def filter_timing_data(
        self,
        created_between: tuple[datetime, datetime],
        limit: int | None = None,
        offset: int | None = None,
    ) -> list[dict[str, Any]]:

        # TODO! created_between used to be marked as Optional, but it would
        #   break the query. Evaluate it's use to determine best behavior.

        limit_str = f"LIMIT {limit}" if limit is not None else ""
        offset_str = f"OFFSET {offset}" if offset is not None else ""

        params = {
            "created_after": created_between[0],
            "created_before": created_between[1],
        }

        query = f"""
        SELECT
            d.id, d.session_uuid, d.client_ip, d.country_iso,
            d.created_at, d.product_id, d.product_user_id,
            d.fraud_score, e.timing_data, d.phase
        FROM grliq_forensicdata d
        JOIN LATERAL (
            SELECT timing_data
            FROM grliq_forensicevents e
            WHERE e.session_uuid = d.session_uuid AND e.timing_data IS NOT NULL
            ORDER BY e.id DESC
            LIMIT 1
        ) e ON TRUE
        WHERE d.created_at BETWEEN %(created_after)s AND %(created_before)s
        {limit_str} {offset_str};
        """

        with self.postgres_config.make_connection() as conn, conn.cursor() as c:
            c.execute(query, params)
            res: list[dict[str, Any]] = c.fetchall()  # type: ignore

        for x in res:
            x["timing_data"] = TimingData.model_validate(x["timing_data"])

        return res

    def get_unique_user_count_by_fingerprint(
        self,
        product_id: UUIDStr,
        fingerprint: str,
        product_user_id_not: str,
    ) -> NonNegativeInt:

        # This is used for filtering for other forensic posts with a certain
        #   fingerprint, in this product_id, but NOT for this user.

        query = sql.SQL("""
        SELECT COUNT(DISTINCT product_user_id) as user_count
        FROM grliq_forensicdata d
        WHERE product_id = %(product_id)s
            AND fingerprint = %(fingerprint)s
            AND product_user_id != %(product_user_id)s
            AND created_at > NOW() - INTERVAL '30 DAYS'
        """)
        params = {
            "product_id": product_id,
            "fingerprint": fingerprint,
            "product_user_id": product_user_id_not,
        }
        # print(query)
        with self.postgres_config.make_connection() as conn, conn.cursor() as c:
            c.execute(query, params)
            user_count = c.fetchone()["user_count"]  # type: ignore

        return int(user_count)

    def filter_data(
        self,
        session_uuid: str | None = None,
        fingerprint: str | None = None,
        fingerprints: Collection[str] | None = None,
        product_id: str | None = None,
        product_ids: Collection[str] | None = None,
        team_ids: Collection[str] | None = None,
        business_ids: Collection[str] | None = None,
        uuids: Collection[str] | None = None,
        created_after: datetime | None = None,
        created_before: datetime | None = None,
        created_between: tuple[datetime, datetime] | None = None,
        user: User | UserRef | None = None,
        users: Collection[User | UserRef] | None = None,
        phase: Phase | None = None,
        order_by: str = "created_at DESC",
        limit: int | None = None,
        offset: int | None = None,
    ) -> list[GrlIqData]:

        res = self.filter(
            select_str="d.id, d.data, d.team_id, d.business_id",
            session_uuid=session_uuid,
            fingerprint=fingerprint,
            fingerprints=fingerprints,
            product_id=product_id,
            product_ids=product_ids,
            team_ids=team_ids,
            business_ids=business_ids,
            created_after=created_after,
            created_before=created_before,
            created_between=created_between,
            uuids=uuids,
            user=user,
            users=users,
            phase=phase,
            order_by=order_by,
            limit=limit,
            offset=offset,
        )
        return [x["data"] for x in res]

    def filter_results(
        self,
        session_uuid: str | None = None,
        uuid: str | None = None,
        product_ids: Collection[str] | None = None,
        product_id: str | None = None,
        team_ids: Collection[str] | None = None,
        business_ids: Collection[str] | None = None,
        created_after: datetime | None = None,
        created_before: datetime | None = None,
        created_between: tuple[datetime, datetime] | None = None,
        user: User | UserRef | None = None,
        limit: int | None = None,
        offset: int | None = None,
        order_by: str = "created_at DESC",
    ) -> list[GrlIqCheckerResults]:
        select_str = (
            "id, session_uuid, product_id, product_user_id, created_at, result_data"
        )
        res = self.filter(
            select_str=select_str,
            session_uuid=session_uuid,
            uuids=[uuid] if uuid else None,
            product_ids=product_ids,
            product_id=product_id,
            team_ids=team_ids,
            business_ids=business_ids,
            created_after=created_after,
            created_before=created_before,
            created_between=created_between,
            user=user,
            limit=limit,
            offset=offset,
            order_by=order_by,
        )
        for x in res:
            x["result_data"] = (
                GrlIqCheckerResults.model_validate(x["result_data"])
                if x["result_data"]
                else None
            )
        return [x["result_data"] for x in res]

    def filter_category_results(
        self,
        session_uuid: str | None = None,
        uuid: str | None = None,
        product_id: str | None = None,
        product_ids: Collection[str] | None = None,
        team_ids: Collection[str] | None = None,
        business_ids: Collection[str] | None = None,
        created_after: datetime | None = None,
        created_before: datetime | None = None,
        created_between: tuple[datetime, datetime] | None = None,
        user: User | UserRef | None = None,
        order_by: str = "created_at DESC",
        limit: int | None = None,
        offset: int | None = None,
    ) -> list[GrlIqForensicCategoryResult]:
        select_str = (
            "id, session_uuid, product_id, product_user_id, created_at, category_result"
        )
        res = self.filter(
            select_str=select_str,
            session_uuid=session_uuid,
            uuids=[uuid] if uuid else None,
            product_id=product_id,
            product_ids=product_ids,
            team_ids=team_ids,
            business_ids=business_ids,
            created_after=created_after,
            created_before=created_before,
            created_between=created_between,
            user=user,
            order_by=order_by,
            limit=limit,
            offset=offset,
        )

        return [x["category_result"] for x in res]

    @staticmethod
    def make_filter_str(
        session_uuid: str | None = None,
        fingerprint: str | None = None,
        fingerprints: Collection[str] | None = None,
        uuids: Collection[str] | None = None,
        product_id: str | None = None,
        product_ids: Collection[str] | None = None,
        team_ids: Collection[str] | None = None,
        business_ids: Collection[str] | None = None,
        created_after: datetime | None = None,
        created_before: datetime | None = None,
        created_between: tuple[datetime, datetime] | None = None,
        user: User | UserRef | None = None,
        users: Collection[User | UserRef] | None = None,
        phase: Phase | None = None,
    ) -> tuple[str, dict[str, Any]]:

        filters = []
        params: dict[str, Any] = {}

        if session_uuid:
            params["session_uuid"] = session_uuid
            filters.append("d.session_uuid = %(session_uuid)s")

        if fingerprint:
            params["fingerprint"] = fingerprint
            filters.append("d.fingerprint = %(fingerprint)s")

        if fingerprints:
            params["fingerprints"] = list(set(fingerprints))
            filters.append("d.fingerprint = ANY(%(fingerprints)s)")

        if product_ids and len(product_ids) == 1:
            product_id = next(iter(product_ids))
            product_ids = None

        if team_ids or business_ids:
            assert not (team_ids and business_ids), (
                "team_ids and business_ids are mutually exclusive"
            )
            assert (
                users is None
                and user is None
                and product_id is None
                and product_ids is None
            ), (
                "team_ids/business_ids are mutually exclusive with "
                "user, users, product_id, and product_ids"
            )
            if team_ids:
                params["team_ids"] = list(set(team_ids))
                filters.append("d.team_id = ANY(%(team_ids)s::UUID[])")
            else:
                assert business_ids is not None
                params["business_ids"] = list(set(business_ids))
                filters.append("d.business_id = ANY(%(business_ids)s::UUID[])")

        if product_ids:
            assert users is None and user is None and product_id is None, (
                "user, users, product_id, and product_ids are mutually exclusive"
            )
            params["product_ids"] = list(set(product_ids))
            filters.append("d.product_id = ANY(%(product_ids)s::UUID[])")

        if product_id:
            assert users is None and user is None and product_ids is None, (
                "user, users, product_id, and product_ids are mutually exclusive"
            )
            params["product_id"] = product_id
            filters.append("d.product_id = %(product_id)s")

        if uuids:
            params["uuids"] = uuids
            filters.append("d.uuid = ANY(%(uuids)s)")

        if created_after:
            params["created_after"] = created_after
            filters.append(
                "d.created_at >= %(created_after)s::timestamp with time zone"
            )

        if created_before:
            params["created_before"] = created_before
            filters.append(
                "d.created_at < %(created_before)s::timestamp with time zone"
            )

        if created_between:
            assert created_after is None, (
                "Cannot pass both created_after and created_between"
            )
            assert created_before is None, (
                "Cannot pass both created_before and created_between"
            )
            params["created_after"] = created_between[0]
            params["created_before"] = created_between[1]
            filters.append(
                "d.created_at BETWEEN %(created_after)s::timestamptz AND %(created_before)s::timestamptz"
            )

        if user:
            assert product_ids is None and users is None, (
                "user, users, and product_ids are mutually exclusive"
            )
            params["product_id"] = user.product_id
            params["product_user_id"] = user.product_user_id
            filters.append(
                "(d.product_id = %(product_id)s AND d.product_user_id = %(product_user_id)s)"
            )

        if users:
            assert product_ids is None and user is None, (
                "user, users, and product_ids are mutually exclusive"
            )
            user_args = ", ".join(
                [f"(%(bp_{i})s, %(bpuid_{i})s)" for i in range(len(users))]
            )
            filters.append(f"(d.product_id, d.product_user_id) IN ({user_args})")
            for i, _user in enumerate(users):
                params[f"bp_{i}"] = _user.product_id
                params[f"bpuid_{i}"] = _user.product_user_id

        if phase:
            params["phase"] = phase.value
            filters.append("d.phase = %(phase)s")

        filter_str = " AND ".join(filters)
        filter_str = "WHERE " + filter_str if filter_str else ""
        return filter_str, params

    def filter_count(
        self,
        session_uuid: str | None = None,
        fingerprint: str | None = None,
        fingerprints: Collection[str] | None = None,
        uuids: Collection[str] | None = None,
        product_id: str | None = None,
        product_ids: Collection[str] | None = None,
        team_ids: Collection[str] | None = None,
        business_ids: Collection[str] | None = None,
        created_after: datetime | None = None,
        created_before: datetime | None = None,
        created_between: tuple[datetime, datetime] | None = None,
        user: User | UserRef | None = None,
        users: Collection[User | UserRef] | None = None,
        phase: Phase | None = None,
    ) -> NonNegativeInt:

        filter_str, params = self.make_filter_str(
            session_uuid=session_uuid,
            fingerprint=fingerprint,
            fingerprints=fingerprints,
            uuids=uuids,
            product_id=product_id,
            product_ids=product_ids,
            team_ids=team_ids,
            business_ids=business_ids,
            created_after=created_after,
            created_before=created_before,
            created_between=created_between,
            user=user,
            users=users,
            phase=phase,
        )

        # If the only filter is on product, team, or business, use the counts
        #   table. Falsy checks match make_filter_str, which ignores falsy
        #   filters. make_filter_str has already checked that at most one of
        #   product_id(s) / team_ids / business_ids was passed.
        other_filters = any(
            (
                session_uuid,
                fingerprint,
                fingerprints,
                uuids,
                created_after,
                created_before,
                created_between,
                user,
                users,
                phase,
            )
        )
        if not other_filters:
            if product_id:
                return self.count_from_product_counts("product_id", [product_id])
            if product_ids:
                return self.count_from_product_counts("product_id", product_ids)
            if business_ids:
                return self.count_from_product_counts("business_id", business_ids)
            if team_ids:
                return self.count_from_product_counts("team_id", team_ids)

        query = f"""
        SELECT COUNT(1) AS c
        FROM grliq_forensicdata d
        {filter_str}
        """
        with self.postgres_config.make_connection() as conn, conn.cursor() as c:
            c.execute(query=query, params=params)
            res = c.fetchone()
        return int(res["c"])

    def count_from_product_counts(
        self, column: str, ids: Collection[str]
    ) -> NonNegativeInt:
        """
        Total rows for these products / teams / businesses, from the
        trigger-maintained grliq_forensicdata_product_counts table (one row
        per product). A product with no row there has no forensic data, so
        it counts as 0.
        """
        assert column in {"product_id", "team_id", "business_id"}
        query = f"""
            SELECT COALESCE(SUM(count), 0) AS c
            FROM grliq_forensicdata_product_counts
            WHERE {column} = ANY(%(ids)s::UUID[])
        """
        with self.postgres_config.make_connection() as conn, conn.cursor() as c:
            c.execute(query=query, params={"ids": list(set(ids))})
            res = c.fetchone()
        return int(res["c"])

    def filter(
        self,
        select_str: str,
        session_uuid: str | None = None,
        fingerprint: str | None = None,
        fingerprints: Collection[str] | None = None,
        uuids: Collection[str] | None = None,
        product_id: str | None = None,
        product_ids: Collection[str] | None = None,
        team_ids: Collection[str] | None = None,
        business_ids: Collection[str] | None = None,
        created_after: datetime | None = None,
        created_before: datetime | None = None,
        created_between: tuple[datetime, datetime] | None = None,
        user: User | UserRef | None = None,
        users: Collection[User | UserRef] | None = None,
        phase: Phase | None = None,
        order_by: str = "created_at DESC",
        limit: int | None = None,
        offset: int | None = None,
    ) -> list[dict[str, Any]]:
        """
        Accepts lots of optional filters.
        """
        if not limit:
            limit = 5_000

        if not offset:
            offset = 0

        if product_ids:
            # It doesn't use the (product_id, created_at) index with multiple product_ids
            assert offset == 0, (
                "Cannot paginate using product_ids, use product_id instead"
            )

        # When filtering by business or team, that "drives" the query: we do a
        #   (business_id|team_id, created_at) index range scan per id (see
        #   filter_by_driver_query), so it's not passed to make_filter_str.
        assert not (team_ids and business_ids), (
            "team_ids and business_ids are mutually exclusive"
        )
        driver_col: str | None = None
        driver_ids: Collection[str] | None = None
        if business_ids:
            driver_col, driver_ids, business_ids = "business_id", business_ids, None
        elif team_ids:
            driver_col, driver_ids, team_ids = "team_id", team_ids, None

        if driver_col:
            assert (
                users is None
                and user is None
                and product_id is None
                and product_ids is None
            ), (
                "team_ids/business_ids are mutually exclusive with "
                "user, users, product_id, and product_ids"
            )

        filter_str, params = self.make_filter_str(
            session_uuid=session_uuid,
            fingerprint=fingerprint,
            fingerprints=fingerprints,
            uuids=uuids,
            product_id=product_id,
            product_ids=product_ids,
            team_ids=team_ids,
            business_ids=business_ids,
            created_after=created_after,
            created_before=created_before,
            created_between=created_between,
            user=user,
            users=users,
            phase=phase,
        )

        if driver_col:
            assert driver_ids is not None
            query, params = self.filter_by_driver_query(
                select_str=select_str,
                driver_col=driver_col,
                driver_ids=driver_ids,
                filter_str=filter_str,
                params=params,
                order_by=order_by,
                limit=limit,
                offset=offset,
            )
        else:
            query = f"""
                SELECT {select_str}
                FROM grliq_forensicdata d
                {filter_str}
                ORDER BY {order_by}
                LIMIT {limit}
                OFFSET {offset}
            """
        # print(query)
        with self.postgres_config.make_connection() as conn, conn.cursor() as c:
            c.execute(query=query, params=params)
            res: list[dict[str, Any]] = c.fetchall()  # type: ignore

        for x in res:
            if "data" in x:
                self.temporary_add_missing_fields(x["data"])
                self.add_column_only_fields(x)
                x["data"]["id"] = x["id"]
                x["data"] = GrlIqData.model_validate(x["data"]) if x["data"] else None

            if "result_data" in x:
                if x["result_data"]:
                    x["result_data"].pop("is_complete", None)
                x["result_data"] = (
                    GrlIqCheckerResults.model_validate(x["result_data"])
                    if x["result_data"]
                    else None
                )

            if "category_result" in x:
                x["category_result"] = (
                    GrlIqForensicCategoryResult.model_validate(x["category_result"])
                    if x["category_result"]
                    else None
                )

        return res

    @staticmethod
    def filter_by_driver_query(
        select_str: str,
        driver_col: str,
        driver_ids: Collection[str],
        filter_str: str,
        params: dict[str, Any],
        order_by: str,
        limit: int,
        offset: int,
    ) -> tuple[str, dict[str, Any]]:
        """
        `d.business_id = ANY(...) ORDER BY created_at LIMIT n` can't return
        rows in created_at order from the (business_id, created_at) index
        when there are multiple ids, so Postgres reads and sorts every
        matching row. Instead, for each id, take the top (limit + offset)
        rows with an index range scan, then merge those and take the page.

        Only (id, created_at) are selected in the per-id scans, so the large
        columns (data, result_data, ...) are only read for the final page.
        """
        assert driver_col in {"business_id", "team_id"}

        parts = order_by.split()
        assert (
            parts and parts[0] in {"created_at", "d.created_at"} and len(parts) <= 2
        ), f"order_by must be on created_at when filtering by {driver_col}"
        direction = parts[1].upper() if len(parts) == 2 else "ASC"
        assert direction in {"ASC", "DESC"}, f"invalid order_by: {order_by}"

        extra_filters = filter_str.removeprefix("WHERE ").strip()
        extra_filters = f"AND {extra_filters}" if extra_filters else ""

        params = {**params, "_driver_ids": list(set(driver_ids))}

        # The CTE's columns are renamed so they can't be ambiguous with
        #   unqualified columns in select_str (e.g. "id, created_at, ...")
        query = f"""
            WITH page AS (
                SELECT x.id AS _page_id, x.created_at AS _page_created_at
                FROM unnest(%(_driver_ids)s::UUID[]) AS drv(id)
                CROSS JOIN LATERAL (
                    SELECT d.id, d.created_at
                    FROM grliq_forensicdata d
                    WHERE d.{driver_col} = drv.id
                    {extra_filters}
                    ORDER BY d.created_at {direction}
                    LIMIT {limit + offset}
                ) x
                ORDER BY x.created_at {direction}
                LIMIT {limit}
                OFFSET {offset}
            )
            SELECT {select_str}
            FROM page
            JOIN grliq_forensicdata d ON d.id = page._page_id
            ORDER BY page._page_created_at {direction}
        """
        return query, params

    @staticmethod
    def add_column_only_fields(x: dict[str, Any]) -> None:
        # These fields aren't stored in the data json, so copy them in from
        #   their columns (if they were selected)
        if not x.get("data"):
            return
        for k in COLUMN_ONLY_FIELDS:
            if k in x:
                x["data"][k] = x[k]

    @staticmethod
    def temporary_add_missing_fields(d: dict[str, Any]) -> None:
        # The following fields were added recently, and so we must give them
        #   a value or old db rows won't be parseable. Once logs are backfilled
        #   then this can be removed
        field_default = {
            "audio_codecs": None,
            "video_codecs": None,
            "color_gamut": "2",
            "prefers_contrast": "0",
            "prefers_reduced_motion": False,
            "dynamic_range": False,
            "inverted_colors": False,
            "forced_colors": False,
            "prefers_color_scheme": False,
        }
        for k, v in field_default.items():
            if k not in d:
                d[k] = v

        # We made a mistake once and saved the grliq data object with the
        #  events fields set. Make sure they are not set here. We load them
        # from the events table, not here!
        d.pop("events", None)
        d.pop("pointer_move_events", None)
        d.pop("mouse_events", None)
        d.pop("keyboard_events", None)
        d.pop("timing_data", None)
