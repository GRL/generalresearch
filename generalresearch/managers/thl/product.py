import json
import logging
import operator
from datetime import timezone, datetime
from decimal import Decimal
from threading import Lock
from typing import Collection, Optional, List, TYPE_CHECKING, Union
from uuid import uuid4, UUID

from cachetools import TTLCache, cachedmethod, keys
from more_itertools import chunked
from psycopg import Cursor
from pydantic import ValidationError
from sentry_sdk import capture_exception

from generalresearch.decorators import LOG
from generalresearch.managers.base import (
    Permission,
    PostgresManager,
)
from generalresearch.models.custom_types import UUIDStr, is_valid_uuid
from generalresearch.pg_helper import PostgresConfig

logger = logging.getLogger()

if TYPE_CHECKING:
    from generalresearch.models.thl.product import Product
    from generalresearch.models.thl.product import (
        UserCreateConfig,
        PayoutConfig,
        SessionConfig,
        UserWalletConfig,
        SourcesConfig,
        UserHealthConfig,
        ProfilingConfig,
        SupplyConfigs,
    )


class ProductManager(PostgresManager):
    def __init__(
        self,
        pg_config: PostgresConfig,
        permissions: Collection[Permission] = None,
    ):
        super().__init__(pg_config=pg_config, permissions=permissions)
        self.uuid_cache = TTLCache(maxsize=1024, ttl=5 * 60)
        self.uuid_lock = Lock()

    def cache_clear(self, product_uuid: UUIDStr) -> None:
        # Calling get_by_uuid with or without kwargs hits different internal keys in the cache!
        with self.uuid_lock:
            self.uuid_cache.pop(keys.hashkey(product_uuid), None)
            self.uuid_cache.pop(keys.hashkey(product_uuid=product_uuid), None)

    @cachedmethod(
        operator.attrgetter("uuid_cache"), lock=operator.attrgetter("uuid_lock")
    )
    def get_by_uuid(
        self,
        product_uuid: UUIDStr,
    ) -> "Product":
        assert is_valid_uuid(product_uuid), "invalid uuid"
        res = self.fetch_uuids(
            product_uuids=[product_uuid],
        )
        # do this so we uniformly raise AssertionErrors
        assert len(res) == 1, "product not found"
        return res[0]

    def get_by_uuids(
        self,
        product_uuids: List[UUIDStr],
    ) -> List["Product"]:

        res = self.fetch_uuids(
            product_uuids=product_uuids,
        )
        assert len(product_uuids) == len(res), "incomplete product response"
        return res

    @cachedmethod(
        operator.attrgetter("uuid_cache"), lock=operator.attrgetter("uuid_lock")
    )
    def get_by_uuid_if_exists(
        self,
        product_uuid: UUIDStr,
    ) -> Optional["Product"]:
        # many=False, raise_on_error=False
        try:
            return self.fetch_uuids(
                product_uuids=[product_uuid],
            )[0]
        except (AssertionError,):
            return None
        except (IndexError,):
            return None

    def get_by_uuids_if_exists(
        self,
        product_uuids: List[UUIDStr],
    ) -> List["Product"]:
        # Same as .get_by_uuids but doesn't raise Exception if len(product_uuids) != len(res)
        return self.fetch_uuids(
            product_uuids=product_uuids,
        )

    def get_all(self, rand_limit: Optional[int]) -> List["Product"]:
        product_uuids = self.get_all_uuids(rand_limit=rand_limit)
        return self.fetch_uuids(product_uuids=product_uuids)

    def get_all_uuids(self, rand_limit: Optional[int]) -> List[UUIDStr]:

        if rand_limit:
            res = self.pg_config.execute_sql_query(
                query=f"""
                    SELECT p.id::uuid
                    FROM userprofile_brokerageproduct AS p
                    ORDER BY RANDOM()
                    LIMIT %s
                """,
                params=[rand_limit],
            )

        else:
            res = self.pg_config.execute_sql_query(
                query=f"""
                    SELECT p.id::uuid
                    FROM userprofile_brokerageproduct AS p
                """
            )
        return [i["id"] for i in res]

    def fetch_uuids(
        self,
        product_uuids: Optional[List[UUIDStr]] = None,
        business_uuids: Optional[List[UUIDStr]] = None,
        team_uuids: Optional[List[UUIDStr]] = None,
    ) -> List["Product"]:
        LOG.debug(f"PM.fetch_uuids({product_uuids=}, {business_uuids=}, {team_uuids=})")

        assert (
            sum(
                bool(x)  # This will also be False is the array is empty
                for x in [product_uuids, business_uuids, team_uuids]
            )
            == 1
        ), "Can only provide one set of identifiers"

        filter_column = None
        filter_uuids = None
        if bool(product_uuids):
            assert all(is_valid_uuid(v) for v in product_uuids), "invalid uuid passed"
            filter_column = "id"
            filter_uuids = product_uuids
        elif bool(business_uuids):
            assert all(is_valid_uuid(v) for v in business_uuids), "invalid uuid passed"
            filter_column = "business_id"
            filter_uuids = business_uuids
        elif bool(team_uuids):
            assert all(is_valid_uuid(v) for v in team_uuids), "invalid uuid passed"
            filter_column = "team_id"
            filter_uuids = team_uuids

        assert filter_column is not None

        if filter_uuids is None or len(filter_uuids) == 0:
            return []

        with self.pg_config.make_connection() as sql_connection:
            with sql_connection.cursor() as c:
                res = []
                for chunk in chunked(filter_uuids, 500):
                    res.extend(
                        self.fetch_uuids_(
                            c=c, filter_uuids=chunk, filter_column=filter_column
                        )
                    )
        return res

    def fetch_uuids_(
        self, c: Cursor, filter_uuids: List[UUIDStr], filter_column: str
    ) -> List["Product"]:
        from generalresearch.models.thl.product import Product

        assert len(filter_uuids) <= 500, "chunk me"
        assert filter_column in {"id", "business_id", "team_id"}

        # Step 1: Retrieve the basic columns from the "Product table"
        query = f"""
        SELECT  
            bp.id,
            bp.id_int,
            bp.name,
            bp.enabled,
            bp.created::timestamptz, 
            bp.team_id::uuid,
            bp.business_id::uuid,
            bp.commission AS commission_pct, 
            bp.grs_domain as harmonizer_domain,
            bp.redirect_url,
            bp.session_config::jsonb,
            bp.payout_config::jsonb,
            bp.user_create_config::jsonb, 
            bp.offerwall_config::jsonb,
            bp.profiling_config::jsonb,
            bp.user_health_config::jsonb,
            bp.yield_man_config::jsonb,
            t.tags
        FROM userprofile_brokerageproduct AS bp
        LEFT JOIN (
            SELECT product_id, STRING_AGG(tag, ',') as tags
            FROM userprofile_brokerageproducttag
            GROUP BY product_id
        ) t ON t.product_id = bp.id_int
        WHERE {filter_column} = ANY(%s)
        """

        c.execute(query, [list(filter_uuids)])

        res = c.fetchall()

        if len(res) == 0:
            return []
        for x in res:
            x["id"] = UUID(x["id"]).hex
            x["team_id"] = UUID(x["team_id"]).hex if x["team_id"] else None
            x["business_id"] = UUID(x["business_id"]).hex if x["business_id"] else None
            x["tags"] = set(x["tags"].split(",")) if x["tags"] else set()

        res1 = {i["id"]: i for i in res}

        # Step 2: Retrieve additional metadata from the "Product Config table"
        c.execute(
            query="""
            SELECT bpc.product_id::uuid as product_id, bpc.key, bpc.value::jsonb
            FROM userprofile_brokerageproductconfig AS bpc
            WHERE product_id = ANY(%s)
            AND key IN ('sources_config', 'user_wallet')
            """,
            # Pulling from keys b/c no reason to try to retrieve any config
            #   k,v rows for products that we know aren't in the other table.
            params=[list(res1.keys())],
        )
        kv_res = c.fetchall()
        for item in kv_res:
            item["value"] = item["value"][item["key"]]
            if item["key"] == "user_wallet":
                item["key"] = "user_wallet_config"

        # Step 2.1: go through them all, and add the key,vals to the correct
        #   Product in the dictionary
        for item in kv_res:
            k: str = item["key"]
            product_id: str = UUID(item["product_id"]).hex
            res1[product_id][k] = item["value"]
        r = []
        for k, v in res1.items():
            try:
                r.append(Product.model_validate(v))
            except ValidationError as e:
                logger.info(f"failed to parse product: {k}")
                raise e
        return r

    def create_dummy(
        self,
        product_id: Optional[UUIDStr] = None,
        team_id: Optional[UUIDStr] = None,
        business_id: Optional[UUIDStr] = None,
        name: Optional[str] = None,
        redirect_url: Optional[str] = None,
        harmonizer_domain: Optional[str] = None,
        commission_pct: Decimal = Decimal("0.05000"),
        sources_config: Optional[Union["SourcesConfig", "SupplyConfigs"]] = None,
        payout_config: Optional["PayoutConfig"] = None,
        session_config: Optional["SessionConfig"] = None,
        profiling_config: Optional["ProfilingConfig"] = None,
        user_wallet_config: Optional["UserWalletConfig"] = None,
        user_create_config: Optional["UserCreateConfig"] = None,
        user_health_config: Optional["UserHealthConfig"] = None,
    ) -> "Product":
        """To be used in tests, where we don't care about certain fields"""
        product_id = product_id if product_id else uuid4().hex
        team_id = team_id if team_id else uuid4().hex
        name = name if name else f"name-{product_id[:12]}"
        redirect_url = redirect_url if redirect_url else "https://www.example.com/"

        return self.create(
            product_id=product_id,
            team_id=team_id,
            business_id=business_id,
            name=name,
            redirect_url=redirect_url,
            harmonizer_domain=harmonizer_domain,
            commission_pct=commission_pct,
            sources_config=sources_config,
            payout_config=payout_config,
            session_config=session_config,
            profiling_config=profiling_config,
            user_wallet_config=user_wallet_config,
            user_create_config=user_create_config,
            user_health_config=user_health_config,
        )

    def create(
        self,
        product_id: UUIDStr,
        team_id: UUIDStr,
        name: str,
        redirect_url: str,
        business_id: Optional[UUIDStr] = None,
        harmonizer_domain: Optional[str] = None,
        commission_pct: Decimal = Decimal("0.05"),
        sources_config: Optional[Union["SourcesConfig", "SupplyConfigs"]] = None,
        payout_config: Optional["PayoutConfig"] = None,
        session_config: Optional["SessionConfig"] = None,
        profiling_config: Optional["ProfilingConfig"] = None,
        user_wallet_config: Optional["UserWalletConfig"] = None,
        user_create_config: Optional["UserCreateConfig"] = None,
        user_health_config: Optional["UserHealthConfig"] = None,
    ) -> "Product":
        """Create a Product with all the basic defaults and return the instance"""
        from generalresearch.models.thl.product import (
            UserCreateConfig,
            PayoutConfig,
            SessionConfig,
            UserWalletConfig,
            SourcesConfig,
            UserHealthConfig,
            ProfilingConfig,
            Product,
        )

        now = datetime.now(tz=timezone.utc)

        # TODO: Add product_id, and possibly name uniqueness validation to the
        #   pydantic model definition itself. The create manager doesn't need
        #   to do this IMO.. but it also means it'll need to be fast and simple
        #   in the model validation steps.

        product_data = {
            "id": product_id,
            "name": name,
            "created": now,
            "team_id": team_id,
            "business_id": business_id,
            "commission_pct": commission_pct,
            "redirect_url": redirect_url,
            "sources_config": sources_config or SourcesConfig(),
            "payout_config": payout_config or PayoutConfig(),
            "session_config": session_config or SessionConfig(),
            "profiling_config": profiling_config or ProfilingConfig(),
            "user_wallet_config": user_wallet_config or UserWalletConfig(),
            "user_create_config": user_create_config or UserCreateConfig(),
            "user_health_config": user_health_config or UserHealthConfig(),
        }
        # If not defined, we want the default to be used. So we can't pass
        #   it in or else the validators fail.
        if harmonizer_domain:
            product_data["harmonizer_domain"] = harmonizer_domain

        instance = Product.model_validate(product_data)

        # Notes: I intentionally removed the name update stuff in here. IMO
        #   we should have an update method on the manager to handle any of the
        #   possible update operations and be explicit about it.

        # Notes: I intentionally removed the ledger key lock now that we're
        #   not using it for any of the accounting work. It's not worth trying
        #   to carry forward in any form.

        # Goes in BPC: sources_config, user_wallet
        insert_data = instance.model_dump_mysql(
            include={
                "id",
                "name",
                "created",
                "enabled",
                "team_id",
                "business_id",
                "commission_pct",
                "harmonizer_domain",
                "redirect_url",
                # JSON configs
                "payout_config",
                "session_config",
                "user_create_config",
                # We haven't done anything with these, but for mysql
                # they need to be passed
                "offerwall_config",
                "profiling_config",
                "user_health_config",
                "yield_man_config",
            }
        )
        # These things don't have the same name in the db
        insert_data["commission"] = str(instance.commission_pct)
        insert_data["grs_domain"] = insert_data.pop("harmonizer_domain")
        insert_data["payments_enabled"] = instance.payments_enabled

        try:
            insert_data["id_int"] = list(
                self.pg_config.execute_sql_query(
                    f"""
            SELECT COALESCE(MAX(id_int), 0) + 1 as id_int
            FROM userprofile_brokerageproduct
            """
                )
            )[0]["id_int"]
            instance.id_int = insert_data["id_int"]

            query = """
            INSERT INTO userprofile_brokerageproduct (
            id, name, created, enabled, payments_enabled,
            team_id, business_id,
            commission, grs_domain, redirect_url,
            session_config, payout_config,
            user_create_config, offerwall_config,
            profiling_config, user_health_config, 
            yield_man_config, id_int
            )
            VALUES (
               %(id)s, %(name)s, %(created)s, %(enabled)s, %(payments_enabled)s,
               %(team_id)s, %(business_id)s,
               %(commission)s, %(grs_domain)s, %(redirect_url)s,
               %(session_config)s, %(payout_config)s,
               %(user_create_config)s, %(offerwall_config)s,
               %(profiling_config)s, %(user_health_config)s,
               %(yield_man_config)s, %(id_int)s
            );
            """
            with self.pg_config.make_connection() as conn:
                with conn.cursor() as c:
                    c.execute(query, params=insert_data)
                conn.commit()

        # I'm not going to be specific here because we will expand this soon
        # to store in a single table / new datastore
        #
        # from pymysql import IntegrityError
        # except IntegrityError as e:
        except (Exception,) as e:

            try:
                return self.get_by_uuid(product_uuid=instance.id)
            except (Exception,) as e2:
                pass
            finally:
                self.cache_clear(instance.id)

            # If we couldn't find the Product, then go ahead and raise.
            capture_exception(e)
            raise e

        bpconfig = instance.model_dump(
            include={"sources_config", "user_wallet"}, mode="json"
        )

        bpc = {k: json.dumps({k: v}) for k, v in bpconfig.items()}
        values = [[k, v, instance.id] for k, v in bpc.items()]

        query = """
            INSERT INTO userprofile_brokerageproductconfig
            (key,value,product_id) 
            VALUES (%s, %s, %s);
        """

        with self.pg_config.make_connection() as conn:
            with conn.cursor() as c:
                c.executemany(query, values)
            conn.commit()

        # We should clear the cache here, b/c we might have tried to get it before,
        #   using get_by_uuid_if_exists, which set the cache to None
        self.cache_clear(product_uuid=product_id)

        return instance

    def update(self, new_product: "Product") -> None:
        product_uuid = new_product.id
        old_product = self.get_by_uuid(product_uuid=product_uuid)
        old_dump = old_product.model_dump(mode="json")
        new_dump = new_product.model_dump(mode="json")
        assert set(old_dump.keys()) == set(new_dump.keys())

        keys_to_update = set()
        for k in set(old_dump.keys()):
            if old_dump[k] != new_dump[k]:
                keys_to_update.add(k)

        not_allowed = {"id", "created", "team_id", "business_id"}
        if keys_to_update & not_allowed:
            raise ValueError(f"Not allowed to change: {keys_to_update & not_allowed}")

        if not keys_to_update:
            return None

        in_bp_keys = {
            "name",
            "enabled",
            "team_id",
            "redirect_url",
            "session_config",
            "payout_config",
            "user_create_config",
            "offerwall_config",
            "profiling_config",
            "user_health_config",
            "yield_man_config",
            # naming ---- ...
            "commission",
            "harmonizer_domain",
            "grs_domain",
        }
        in_bpc_keys = {"sources_config", "user_wallet", "user_wallet_config"}
        if keys_to_update & in_bp_keys:
            data = new_product.model_dump_mysql()
            # These things don't have the same name in the db
            data["commission"] = str(new_product.commission_pct)
            data["grs_domain"] = data.pop("harmonizer_domain")
            data = {k: v for k, v in data.items() if k in in_bp_keys}
            data["id"] = product_uuid
            update_str = ", ".join(f"{k}=%({k})s" for k in data.keys())
            self.pg_config.execute_write(
                f"""
                UPDATE userprofile_brokerageproduct
                SET {update_str}
                WHERE id = %(id)s
            """,
                data,
            )

        if keys_to_update & in_bpc_keys:
            bpconfig = new_product.model_dump(
                include={"sources_config", "user_wallet"}, mode="json"
            )

            bpc = {k: json.dumps({k: v}) for k, v in bpconfig.items()}
            data = []
            if "sources_config" in keys_to_update:
                data.append(
                    {
                        "id": product_uuid,
                        "key": "sources_config",
                        "value": bpc["sources_config"],
                    },
                )
            if "user_wallet_config" in keys_to_update:
                data.append(
                    {
                        "id": product_uuid,
                        "key": "user_wallet",
                        "value": bpc["user_wallet"],
                    }
                )
            with self.pg_config.make_connection() as conn:
                with conn.cursor() as c:
                    for d in data:
                        c.execute(
                            """
                            UPDATE userprofile_brokerageproductconfig
                            SET value = %(value)s
                            WHERE product_id = %(id)s AND key = %(key)s
                            """,
                            d,
                        )
                conn.commit()

        self.cache_clear(product_uuid)
