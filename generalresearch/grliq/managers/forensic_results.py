from datetime import datetime
from typing import Any, Collection, Dict, List, Optional, Tuple

from generalresearch.grliq.models.forensic_result import (
    GrlIqForensicCategoryResult,
    Phase,
)
from generalresearch.grliq.models.useragents import GrlUserAgent
from generalresearch.models.thl.user import User
from generalresearch.pg_helper import PostgresConfig


class GrlIqCategoryResultsReader:
    def __init__(self, postgres_config: PostgresConfig):
        self.postgres_config = postgres_config

    def filter_category_results(
        self,
        session_uuid: Optional[str] = None,
        fingerprint: Optional[str] = None,
        phase: Optional[Phase] = None,
        uuids: Optional[Collection[str]] = None,
        product_ids: Optional[Collection[str]] = None,
        created_since: Optional[datetime] = None,
        created_between: Optional[Tuple[datetime, datetime]] = None,
        user: Optional[User] = None,
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        For retrieving GrlIqForensicCategoryResult objects from db.

        :return: List of Dict. Keys are below in the 'select_str'.
        """
        select_str = (
            "id, uuid, session_uuid, product_id, product_user_id, created_at,"
            " country_iso, client_ip, phase, data,"
            " data->>'user_agent_str' AS user_agent_str,"
            " category_result, is_attempt_allowed, fraud_score"
        )
        if not limit:
            limit = 5_000

        filters = []
        params = {}

        if session_uuid:
            params["session_uuid"] = session_uuid
            filters.append("d.session_uuid = %(session_uuid)s")
        if fingerprint:
            params["fingerprint"] = fingerprint
            filters.append("d.fingerprint = %(fingerprint)s")
        if phase:
            params["phase"] = phase.value
            filters.append("d.phase = %(phase)s")
        if product_ids:
            params["product_ids"] = product_ids
            filters.append("d.product_id = ANY(%(product_ids)s::UUID[])")
        if uuids:
            params["uuids"] = uuids
            filters.append("d.uuid = ANY(%(uuids)s)")
        if created_since:
            params["created_since"] = created_since
            filters.append(
                "d.created_at >= %(created_since)s::timestamp with time zone"
            )
        if created_between:
            assert (
                created_since is None
            ), "Cannot pass both created_until and created_between"
            params["created_since"] = created_between[0]
            params["created_until"] = created_between[1]
            filters.append(
                "d.created_at BETWEEN %(created_since)s::timestamptz AND %(created_until)s::timestamptz"
            )
        if user:
            assert product_ids is None, "Cannot pass both product_ids and user"
            params["product_id"] = user.product_id
            params["product_user_id"] = user.product_user_id
            filters.append(
                "(d.product_id = %(product_id)s AND d.product_user_id = %(product_user_id)s)"
            )

        filter_str = " AND ".join(filters)
        filter_str = "WHERE " + filter_str if filter_str else ""
        query = f"""
        SELECT {select_str}
        FROM grliq_forensicdata d
        {filter_str}
        ORDER BY created_at DESC LIMIT {limit}
        """

        with self.postgres_config.make_connection() as conn:
            with conn.cursor() as c:
                c.execute(query, params)
                res = c.fetchall()

        for x in res:
            x["client_ip"] = str(x["client_ip"])
            x["category_result"] = (
                GrlIqForensicCategoryResult.model_validate(x["category_result"])
                if x["category_result"]
                else None
            )
            if x.get("user_agent_str"):
                x["user_agent"] = GrlUserAgent.from_ua_str(x["user_agent_str"])
            x.pop("user_agent_str", None)

        return res
