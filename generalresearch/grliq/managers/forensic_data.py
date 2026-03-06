from datetime import datetime, timezone
from typing import Optional, List, Collection, Dict, Tuple, Any
from uuid import uuid4

from psycopg import sql
from pydantic import PositiveInt, NonNegativeInt

from generalresearch.grliq.managers import DUMMY_GRLIQ_DATA
from generalresearch.grliq.models.events import PointerMove, TimingData
from generalresearch.grliq.models.forensic_data import GrlIqData
from generalresearch.grliq.models.forensic_result import (
    GrlIqForensicCategoryResult,
    GrlIqCheckerResults,
    Phase,
)
from generalresearch.models.custom_types import UUIDStr
from generalresearch.models.thl.user import User
from generalresearch.pg_helper import PostgresConfig


class GrlIqDataManager:

    def __init__(self, postgres_config: PostgresConfig):
        self.postgres_config = postgres_config

    def create_dummy(
        self,
        is_attempt_allowed: True,
        product_id: Optional[str] = None,
        product_user_id: Optional[str] = None,
        uuid: Optional[str] = None,
        mid: Optional[str] = None,
        created_at: Optional[datetime] = None,
    ) -> GrlIqData:
        """
        Creates a dummy record in the db with a GrlIqData (data), GrlIqCheckerResults (result_data),
            and GrlIqForensicCategoryResult (category_results)
        :param is_attempt_allowed: Whether the attempt is allowed.
        :param product_id: product_id of user
        :param product_user_id:  product_user_id of user
        :param uuid: uuid for the grliq data record
        :param mid: the thl_session:uuid / mid for the attempt.
        :return:
        """
        import copy

        res: GrlIqData = copy.deepcopy(DUMMY_GRLIQ_DATA[int(is_attempt_allowed)])

        product_id = product_id or uuid4().hex
        product_user_id = product_user_id or uuid4().hex
        uuid = uuid or uuid4().hex
        mid = mid or uuid4().hex
        created_at = created_at or datetime.now(tz=timezone.utc)

        res["data"].product_id = product_id
        res["data"].product_user_id = product_user_id
        res["data"].uuid = uuid
        res["data"].mid = mid
        res["data"].created_at = created_at
        res["result_data"].uuid = uuid
        res["category_result"].uuid = uuid

        return self.create(
            iq_data=res["data"],
            result_data=res["result_data"],
            category_result=res["category_result"],
            fraud_score=res["category_result"].fraud_score,
            is_attempt_allowed=res["category_result"].is_attempt_allowed(),
        )

    def create(
        self,
        iq_data: GrlIqData,
        result_data: Optional[GrlIqCheckerResults] = None,
        category_result: Optional[GrlIqForensicCategoryResult] = None,
        fraud_score: Optional[int] = None,
        is_attempt_allowed: Optional[bool] = None,
    ) -> GrlIqData:

        data = iq_data.model_dump_sql(exclude={"events", "mouse_events", "timing_data"})

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

        query = sql.SQL(
            """
         INSERT INTO grliq_forensicdata
            (uuid, session_uuid, created_at, product_id, product_user_id,
            country_iso, client_ip, ua_browser_family, ua_browser_version,
            ua_os_family, ua_os_version, ua_device_family, ua_device_brand,
            ua_device_model, ua_hash, data, phase,
            fingerprint, fraud_score, is_attempt_allowed,
            result_data, category_result)
         VALUES 
            (%(uuid)s, %(session_uuid)s, %(created_at)s, %(product_id)s, %(product_user_id)s,
            %(country_iso)s, %(client_ip)s, %(ua_browser_family)s, %(ua_browser_version)s,
            %(ua_os_family)s, %(ua_os_version)s, %(ua_device_family)s, %(ua_device_brand)s,
            %(ua_device_model)s, %(ua_hash)s, %(data)s, %(phase)s,
            %(fingerprint)s, %(fraud_score)s, %(is_attempt_allowed)s,
            %(result_data)s, %(category_result)s)
         RETURNING id
         """
        )

        with self.postgres_config.make_connection() as conn:
            with conn.cursor() as c:
                c.execute(query, data)
                pk = c.fetchone()["id"]
                conn.commit()

        iq_data.id = pk

        return iq_data

    def set_results(
        self,
        uuid: UUIDStr,
        result_data: GrlIqCheckerResults,
        category_result: GrlIqForensicCategoryResult,
        fingerprint: Optional[str] = None,
        fraud_score: Optional[int] = None,
        is_attempt_allowed: Optional[bool] = None,
    ) -> None:
        data = {"uuid": uuid}
        data["result_data"] = result_data.model_dump_json(exclude_none=True)
        data["category_result"] = category_result.model_dump_json()
        data["fingerprint"] = fingerprint
        data["fraud_score"] = fraud_score
        data["is_attempt_allowed"] = is_attempt_allowed

        query = sql.SQL(
            """
          UPDATE grliq_forensicdata
          SET result_data = %(result_data)s,
          category_result = %(category_result)s,
          fingerprint = %(fingerprint)s,
          fraud_score = %(fraud_score)s,
          is_attempt_allowed = %(is_attempt_allowed)s
          WHERE uuid = %(uuid)s
          """
        )
        with self.postgres_config.make_connection() as conn:
            with conn.cursor() as c:
                c.execute(query, data)
                if c.rowcount != 1:
                    raise ValueError(
                        f"Expected 1 row to be updated, but {c.rowcount} rows were affected."
                    )
                conn.commit()

        return None

    def update_fingerprint(self, iq_data: GrlIqData) -> None:
        # We should only run this if we modified the fingerprint algorithm
        if "fingerprint" in iq_data.__dict__:
            # make sure it's not cached
            del iq_data.__dict__["fingerprint"]
        data = {"uuid": iq_data.uuid, "fingerprint": iq_data.fingerprint}
        query = sql.SQL(
            """
         UPDATE grliq_forensicdata 
         SET fingerprint = %(fingerprint)s
         WHERE uuid = %(uuid)s
         """
        )
        with self.postgres_config.make_connection() as conn:
            with conn.cursor() as c:
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
        query = sql.SQL(
            """
         UPDATE grliq_forensicdata 
         SET data = %(data)s
         WHERE id = %(id)s
         """
        )
        with self.postgres_config.make_connection() as conn:
            with conn.cursor() as c:
                c.execute(query, data)
                if c.rowcount != 1:
                    raise ValueError(
                        f"Expected 1 row to be updated, but {c.rowcount} rows were affected."
                    )
                conn.commit()

    def get_data_if_exists(
        self, forensic_uuid: UUIDStr, load_events: bool = False
    ) -> Optional[GrlIqData]:
        try:
            return self.get_data(forensic_uuid=forensic_uuid, load_events=load_events)
        except AssertionError:
            return None

    def get_data(
        self,
        forensic_id: Optional[PositiveInt] = None,
        forensic_uuid: Optional[UUIDStr] = None,
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

            query = sql.SQL(
                """
                SELECT d.id, d.data, e.events, e.mouse_events, t.timing_data
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
            """
            )

        else:
            query = sql.SQL(
                """
                SELECT d.id, d.data
                FROM grliq_forensicdata d
            """
            )

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

        with self.postgres_config.make_connection() as conn:
            with conn.cursor() as c:
                c.execute(query=q1, params=(param_value,))
                x = c.fetchone()

        assert x is not None, f"GrlIqDataManager.get_data({forensic_uuid=}) not found"

        self.temporary_add_missing_fields(x["data"])
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
        created_between: Optional[Tuple[datetime, datetime]] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
    ) -> List[Dict]:
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
        with self.postgres_config.make_connection() as conn:
            with conn.cursor() as c:
                c.execute(query, params)
                res: List[Dict] = c.fetchall()
        for x in res:
            x["timing_data"] = TimingData.model_validate(x["timing_data"])

        return res

    def get_unique_user_count_by_fingerprint(
        self,
        product_id: str,
        fingerprint: str,
        product_user_id_not: str,
    ) -> NonNegativeInt:
        # This is used for filtering for other forensic posts with a certain
        #   fingerprint, in this product_id, but NOT for this user.
        query = sql.SQL(
            """
        SELECT COUNT(DISTINCT product_user_id) as user_count
        FROM grliq_forensicdata d
        WHERE product_id = %(product_id)s
            AND fingerprint = %(fingerprint)s
            AND product_user_id != %(product_user_id)s
            AND created_at > NOW() - INTERVAL '30 DAYS'
        """
        )
        params = {
            "product_id": product_id,
            "fingerprint": fingerprint,
            "product_user_id": product_user_id_not,
        }
        # print(query)
        with self.postgres_config.make_connection() as conn:
            with conn.cursor() as c:
                c.execute(query, params)
                user_count = c.fetchone()["user_count"]
        return int(user_count)

    def filter_data(
        self,
        session_uuid: Optional[str] = None,
        fingerprint: Optional[str] = None,
        fingerprints: Optional[Collection[str]] = None,
        product_id: Optional[str] = None,
        product_ids: Optional[Collection[str]] = None,
        uuids: Optional[Collection[str]] = None,
        created_after: Optional[datetime] = None,
        created_before: Optional[datetime] = None,
        created_between: Optional[Tuple[datetime, datetime]] = None,
        user: Optional[User] = None,
        users: Optional[Collection[User]] = None,
        phase: Optional[Phase] = None,
        order_by: str = "created_at DESC",
        limit: Optional[int] = None,
        offset: Optional[int] = None,
    ) -> List[GrlIqData]:

        res = self.filter(
            select_str="d.id, d.data",
            session_uuid=session_uuid,
            fingerprint=fingerprint,
            fingerprints=fingerprints,
            product_id=product_id,
            product_ids=product_ids,
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
        session_uuid: Optional[str] = None,
        uuid: Optional[str] = None,
        product_ids: Optional[Collection[str]] = None,
        product_id: Optional[str] = None,
        created_after: Optional[datetime] = None,
        created_before: Optional[datetime] = None,
        created_between: Optional[Tuple[datetime, datetime]] = None,
        user: Optional[User] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        order_by: str = "created_at DESC",
    ) -> List[GrlIqCheckerResults]:
        select_str = (
            "id, session_uuid, product_id, product_user_id, created_at, result_data"
        )
        res = self.filter(
            select_str=select_str,
            session_uuid=session_uuid,
            uuids=[uuid] if uuid else None,
            product_ids=product_ids,
            product_id=product_id,
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
        session_uuid: Optional[str] = None,
        uuid: Optional[str] = None,
        product_id: Optional[str] = None,
        product_ids: Optional[Collection[str]] = None,
        created_after: Optional[datetime] = None,
        created_before: Optional[datetime] = None,
        created_between: Optional[Tuple[datetime, datetime]] = None,
        user: Optional[User] = None,
        order_by: str = "created_at DESC",
        limit: Optional[int] = None,
        offset: Optional[int] = None,
    ) -> List[GrlIqForensicCategoryResult]:
        select_str = (
            "id, session_uuid, product_id, product_user_id, created_at, category_result"
        )
        res = self.filter(
            select_str=select_str,
            session_uuid=session_uuid,
            uuids=[uuid] if uuid else None,
            product_id=product_id,
            product_ids=product_ids,
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
        session_uuid: Optional[str] = None,
        fingerprint: Optional[str] = None,
        fingerprints: Optional[Collection[str]] = None,
        uuids: Optional[Collection[str]] = None,
        product_id: Optional[str] = None,
        product_ids: Optional[Collection[str]] = None,
        created_after: Optional[datetime] = None,
        created_before: Optional[datetime] = None,
        created_between: Optional[Tuple[datetime, datetime]] = None,
        user: Optional[User] = None,
        users: Optional[Collection[User]] = None,
        phase: Optional[Phase] = None,
    ) -> Tuple[str, Dict[str, Any]]:

        filters = []
        params: Dict[str, Any] = {}

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
            product_id = list(product_ids)[0]
            product_ids = None

        if product_ids:
            assert (
                users is None and user is None and product_id is None
            ), "user, users, product_id, and product_ids are mutually exclusive"
            params["product_ids"] = list(set(product_ids))
            filters.append("d.product_id = ANY(%(product_ids)s::UUID[])")

        if product_id:
            assert (
                users is None and user is None and product_ids is None
            ), "user, users, product_id, and product_ids are mutually exclusive"
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
            assert (
                created_after is None
            ), "Cannot pass both created_after and created_between"
            assert (
                created_before is None
            ), "Cannot pass both created_before and created_between"
            params["created_after"] = created_between[0]
            params["created_before"] = created_between[1]
            filters.append(
                "d.created_at BETWEEN %(created_after)s::timestamptz AND %(created_before)s::timestamptz"
            )

        if user:
            assert (
                product_ids is None and users is None
            ), "user, users, and product_ids are mutually exclusive"
            params["product_id"] = user.product_id
            params["product_user_id"] = user.product_user_id
            filters.append(
                "(d.product_id = %(product_id)s AND d.product_user_id = %(product_user_id)s)"
            )

        if users:
            assert (
                product_ids is None and user is None
            ), "user, users, and product_ids are mutually exclusive"
            user_args = ", ".join(
                [f"(%(bp_{i})s, %(bpuid_{i})s)" for i in range(len(users))]
            )
            filters.append(f"(d.product_id, d.product_user_id) IN ({user_args})")
            for i, user in enumerate(users):
                params[f"bp_{i}"] = user.product_id
                params[f"bpuid_{i}"] = user.product_user_id

        if phase:
            params["phase"] = phase.value
            filters.append("d.phase = %(phase)s")

        filter_str = " AND ".join(filters)
        filter_str = "WHERE " + filter_str if filter_str else ""
        return filter_str, params

    def filter_count(
        self,
        session_uuid: Optional[str] = None,
        fingerprint: Optional[str] = None,
        fingerprints: Optional[Collection[str]] = None,
        uuids: Optional[Collection[str]] = None,
        product_id: Optional[str] = None,
        product_ids: Optional[Collection[str]] = None,
        created_after: Optional[datetime] = None,
        created_before: Optional[datetime] = None,
        created_between: Optional[Tuple[datetime, datetime]] = None,
        user: Optional[User] = None,
        users: Optional[Collection[User]] = None,
        phase: Optional[Phase] = None,
    ) -> NonNegativeInt:
        filter_str, params = self.make_filter_str(
            session_uuid=session_uuid,
            fingerprint=fingerprint,
            fingerprints=fingerprints,
            uuids=uuids,
            product_id=product_id,
            product_ids=product_ids,
            created_after=created_after,
            created_before=created_before,
            created_between=created_between,
            user=user,
            users=users,
            phase=phase,
        )

        only_product_id = (
            product_id is not None
            and session_uuid is None
            and fingerprint is None
            and fingerprints is None
            and uuids is None
            and product_ids is None
            and created_after is None
            and created_before is None
            and created_between is None
            and user is None
            and users is None
            and phase is None
        )

        if only_product_id:
            try:
                with self.postgres_config.make_connection() as conn:
                    with conn.cursor() as c:
                        c.execute(
                            query="""
                                SELECT count AS c
                                FROM grliq_forensicdata_product_counts
                                WHERE product_id = %s
                                LIMIT 1
                            """,
                            params=(product_id,),
                        )
                        res = c.fetchone()
                        if res and res["c"] >= 0:
                            return int(res["c"])

            except (Exception,) as e:
                pass

        query = f"""
        SELECT COUNT(1) AS c
        FROM grliq_forensicdata d
        {filter_str}
        """
        with self.postgres_config.make_connection() as conn:
            with conn.cursor() as c:
                c.execute(query=query, params=params)
                res = c.fetchone()
        return int(res["c"])

    def filter(
        self,
        select_str: str,
        session_uuid: Optional[str] = None,
        fingerprint: Optional[str] = None,
        fingerprints: Optional[Collection[str]] = None,
        uuids: Optional[Collection[str]] = None,
        product_id: Optional[str] = None,
        product_ids: Optional[Collection[str]] = None,
        created_after: Optional[datetime] = None,
        created_before: Optional[datetime] = None,
        created_between: Optional[Tuple[datetime, datetime]] = None,
        user: Optional[User] = None,
        users: Optional[Collection[User]] = None,
        phase: Optional[Phase] = None,
        order_by: str = "created_at DESC",
        limit: Optional[int] = None,
        offset: Optional[int] = None,
    ) -> List[Dict]:
        """
        Accepts lots of optional filters.
        """
        if not limit:
            limit = 5000

        if not offset:
            offset = 0

        if product_ids:
            # It doesn't use the (product_id, created_at) index with multiple product_ids
            assert (
                offset == 0
            ), "Cannot paginate using product_ids, use product_id instead"

        filter_str, params = self.make_filter_str(
            session_uuid=session_uuid,
            fingerprint=fingerprint,
            fingerprints=fingerprints,
            uuids=uuids,
            product_id=product_id,
            product_ids=product_ids,
            created_after=created_after,
            created_before=created_before,
            created_between=created_between,
            user=user,
            users=users,
            phase=phase,
        )

        query = f"""
            SELECT {select_str}
            FROM grliq_forensicdata d
            {filter_str}
            ORDER BY {order_by} 
            LIMIT {limit} 
            OFFSET {offset}
        """
        # print(query)
        with self.postgres_config.make_connection() as conn:
            with conn.cursor() as c:
                c.execute(query=query, params=params)
                res: List = c.fetchall()

        for x in res:

            if "data" in x:
                self.temporary_add_missing_fields(x["data"])
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
    def temporary_add_missing_fields(d: Dict):
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

        # We made a mistake once and saved the grliq data object with the events fields set.
        #   Make sure they are not set here. We load them from the events table, not here!
        d.pop("events", None)
        d.pop("pointer_move_events", None)
        d.pop("mouse_events", None)
        d.pop("keyboard_events", None)
        d.pop("timing_data", None)
