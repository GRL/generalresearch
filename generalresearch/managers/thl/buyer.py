from datetime import datetime, timezone
from typing import Collection, Dict, Optional

from generalresearch.managers.base import PostgresManager, Permission
from generalresearch.models import Source
from generalresearch.models.thl.survey.buyer import Buyer
from generalresearch.pg_helper import PostgresConfig


class BuyerManager(PostgresManager):

    def __init__(
        self,
        pg_config: PostgresConfig,
        permissions: Collection[Permission] = None,
    ):
        super().__init__(pg_config=pg_config, permissions=permissions)
        # self.buyer_pk: Dict[Buyer, int] = dict()
        self.source_code_buyer: Dict[str, Buyer] = dict()
        self.source_code_pk: Dict[str, int] = dict()
        self.populate_caches()

    def populate_caches(self):
        query = """
        SELECT id, code, source, label, created
        FROM marketplace_buyer;"""
        res = self.pg_config.execute_sql_query(query)
        buyers = [Buyer.model_validate(d) for d in res]
        self.source_code_buyer = {b.source_code: b for b in buyers}
        self.source_code_pk = {b.source_code: b.id for b in buyers}

    def update_caches(self, buyers: Collection[Buyer]):
        self.source_code_buyer.update({b.source_code: b for b in buyers})
        self.source_code_pk.update({b.source_code: b.id for b in buyers})

    def get(self, source: Source, code: str) -> Buyer:
        return self.source_code_buyer[f"{source.value}:{code}"]

    def get_if_exists(self, source: Source, code: str) -> Optional[Buyer]:
        try:
            return self.get(source=source, code=code)
        except KeyError:
            return None

    def bulk_get_or_create(self, source: Source, codes: Collection[str]):
        now = datetime.now(tz=timezone.utc)
        buyers = []
        params_seq = []
        for code in codes:
            source_code = f"{source.value}:{code}"
            if source_code in self.source_code_buyer:
                buyers.append(self.source_code_buyer[source_code])
            else:
                params_seq.append({"source": source, "code": code, "created": now})

        # Insert those not in the cache. If the cache is stale, it doesn't
        #   really matter b/c we won't insert a dupe, and we'll fetch it
        #   back right after
        query = """
        INSERT INTO marketplace_buyer (
            source, code, created
        ) VALUES (
            %(source)s, %(code)s, %(created)s
        ) ON CONFLICT (source, code) DO NOTHING;"""
        with self.pg_config.make_connection() as conn:
            with conn.cursor() as c:
                c.executemany(query=query, params_seq=params_seq)
            conn.commit()

        lookup = [x["code"] for x in params_seq]
        query = """
        SELECT id, source, code, label, created
        FROM marketplace_buyer
        WHERE source = %(source)s AND
            code = ANY(%(lookup)s);
        """
        res = self.pg_config.execute_sql_query(
            query, params={"lookup": lookup, "source": source.value}
        )
        new_buyers = [Buyer.model_validate(d) for d in res]
        self.update_caches(new_buyers)
        buyers.extend(new_buyers)
        # Not required, just for ease of testing/deterministic
        buyers = sorted(buyers, key=lambda x: (x.source, x.code))
        assert len(buyers) == len(codes), "something went wrong"
        return buyers

    def update(self, buyer: Buyer):
        # label is the only thing that can be updated
        query = """
        UPDATE marketplace_buyer
        SET label = %(label)s
        WHERE source = %(source)s
            AND code = %(code)s
        RETURNING id;
        """
        params = {
            "source": buyer.source.value,
            "code": buyer.code,
            "label": buyer.label,
        }
        with self.pg_config.make_connection() as conn:
            with conn.cursor() as c:
                c.execute(query, params=params)
                assert c.rowcount == 1
                pk = c.fetchone()["id"]
            if buyer.id is not None:
                assert buyer.id == pk
            else:
                buyer.id = pk
            conn.commit()

        return None
