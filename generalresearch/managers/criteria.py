from abc import ABC
from datetime import datetime, timezone
from typing import Collection, Dict, Set

from more_itertools import chunked

from generalresearch.managers.base import SqlManager
from generalresearch.models.thl.survey import MarketplaceCondition


class CriteriaManager(SqlManager, ABC):
    """
    Using the terms "criteria" & "condition" interchangeably!
    """

    DB_FIELDS = [
        "hash",
        "question_id",
        "logical_operator",
        "values",
        "value_type",
        "negate",
    ]
    CONDITION_MODEL = None
    TABLE_NAME = ""

    def create(self, criterion: MarketplaceCondition) -> bool:
        """
        Create a single criterion
        """
        ...

    def filter(self, hashes: Collection[str]) -> Dict[str, MarketplaceCondition]:
        """
        Filter for criterion from the db
        """
        res = self.sql_helper.execute_sql_query(
            query=f"""
                SELECT {self.mysql_fields}
                FROM {self.mysql_db_table}
                WHERE `hash` IN %s;
            """,
            params=[hashes],
        )
        return {x["hash"]: self.CONDITION_MODEL.from_mysql(x) for x in res}

    def filter_exists(self, hashes: Set[str]) -> Set[str]:
        """Returns hashes that exist in the db"""
        res = self.sql_helper.execute_sql_query(
            query=f"""
                SELECT `hash`
                FROM {self.mysql_db_table}
                WHERE `hash` IN %s;
            """,
            params=[hashes],
        )
        return {x["hash"] for x in res}

    def update(self, conditions: Collection[MarketplaceCondition]) -> None:
        # Add any new hashes into the DB
        this_hashes = set([condition.criterion_hash for condition in conditions])
        known_hashes = self.filter_exists(this_hashes)
        new_hashes = this_hashes - known_hashes

        if new_hashes:
            now = datetime.now(tz=timezone.utc)
            values = [
                condition.to_mysql()
                for condition in conditions
                if condition.criterion_hash in new_hashes
            ]
            values = [
                v
                | {
                    "created": now,
                    "last_used": now,
                    "hash": v["criterion_hash"],
                }
                for v in values
            ]
            values_str = ",".join(
                [f"%({k})s" for k in self.DB_FIELDS + ["created", "last_used"]]
            )
            conn = self.sql_helper.make_connection()
            c = conn.cursor()
            for chunk in chunked(values, 100):
                c.executemany(
                    query=f"""
                        INSERT INTO {self.mysql_db_table}
                        ({self.mysql_fields}, `created`, `last_used`)
                        VALUES ({values_str});
                    """,
                    args=chunk,
                )
                conn.commit()

        return None

    @property
    def mysql_fields(self) -> str:
        return ", ".join([f"`{k}`" for k in self.DB_FIELDS])

    @property
    def mysql_db_table(self) -> str:
        return f"`{self.sql_helper.db}`.`{self.TABLE_NAME}`"
