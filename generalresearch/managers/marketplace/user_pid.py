from abc import ABC
from typing import Collection, Optional, List, Dict
from uuid import UUID

from generalresearch.managers.base import SqlManager
from generalresearch.models import Source
from generalresearch.sql_helper import SqlHelper


class UserPidManager(SqlManager, ABC):
    """
    For getting user pids across marketplaces
    """

    SOURCE: Source = None
    TABLE_NAME = None

    def filter(
        self,
        user_ids: Optional[Collection[int]] = None,
        pids: Optional[Collection[str]] = None,
    ) -> List[Dict[str, str]]:
        """
        Filter by user_id or user_pid
        """
        assert (user_ids or pids) and not (
            user_ids and pids
        ), "Must pass ONE of user_ids, pids"

        params = []
        if user_ids:
            assert len(user_ids) <= 500, "limit 500 user_ids"
            assert isinstance(
                user_ids, (list, set)
            ), "must pass a collection of user_ids"
            filter_str = "user_id IN %s"
            params.append(set(user_ids))
        else:
            assert len(pids) <= 500, "limit 500 pids"
            assert isinstance(pids, (list, set)), "must pass a collection of pids"
            pids = {UUID(x).hex for x in pids}
            filter_str = "pid IN %s"
            params.append(pids)
        query = f"""
            SELECT user_id, pid
            FROM {self.mysql_db_table}
            WHERE {filter_str}
            LIMIT 500;"""
        res = self.sql_helper.execute_sql_query(
            query=query,
            params=params,
        )
        for x in res:
            x["pid"] = UUID(x["pid"]).hex
        return sorted(res, key=lambda x: x["user_id"])

    @property
    def mysql_db_table(self):
        assert self.TABLE_NAME, "must subclass and set TABLE_NAME"
        return f"`{self.sql_helper.db}`.`{self.TABLE_NAME}`"


class UserPidMultiManager:
    """
    For looking up marketplace user_pids by user_id across multiple marketplaces
    """

    def __init__(self, sql_helper: SqlHelper, managers: List[UserPidManager]):
        self.sql_helper = sql_helper
        self.managers = managers

    def filter(self, user_ids: Optional[Collection[int]] = None):
        # You can only query across all marketplaces by user_id.
        #   If you are looking by user_pid, it is assumed
        #   you know which marketplace you are looking in.
        assert len(user_ids) <= 100, "limit 100 user_ids"
        assert isinstance(user_ids, (list, set)), "must pass a collection of user_ids"

        params = [set(user_ids)] * len(self.managers)
        queries = [
            f"""
        SELECT user_id, pid, '{m.SOURCE.value}' as source
        FROM {m.mysql_db_table}
        WHERE user_id IN %s
        """
            for m in self.managers
        ]
        query = "\nUNION ".join(queries)
        res = self.sql_helper.execute_sql_query(query=query, params=params)
        for x in res:
            x["pid"] = UUID(x["pid"]).hex
            x["source"] = Source(x["source"])

        # Note: the wxet user pid is just the thl_user.uuid. Whatever uses this
        #   should insert that in.
        return sorted(res, key=lambda x: (x["user_id"], x["source"].value))
