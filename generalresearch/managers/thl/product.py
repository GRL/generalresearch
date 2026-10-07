from __future__ import annotations

import json
import logging
import operator
from collections.abc import Collection
from datetime import UTC, datetime
from decimal import Decimal
from threading import Lock
from typing import TYPE_CHECKING, Any

from cachetools import TTLCache, cachedmethod, keys
from psycopg import Connection
from pydantic import NonNegativeInt
from sentry_sdk import capture_exception

from generalresearch.managers.base import (
    PostgresManager,
)
from generalresearch.models.custom_types import UUIDStr, is_valid_uuid

if TYPE_CHECKING:
    from generalresearch.managers.base import (
        Permission,
    )
    from generalresearch.models.thl.product import (
        PayoutConfig,
        Product,
        ProfilingConfig,
        SessionConfig,
        SourcesConfig,
        SupplyConfig,
        UserCreateConfig,
        UserHealthConfig,
        UserWalletConfig,
    )
    from generalresearch.pg_helper import PostgresConfig

logger = logging.getLogger()


class ProductManager(PostgresManager):
    def __init__(
        self,
        pg_config: PostgresConfig,
        permissions: Collection[Permission] | None = None,
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
    ) -> Product:
        assert is_valid_uuid(product_uuid), "invalid uuid"
        res = self.filter_by(
            product_uuids=[product_uuid],
        )
        assert len(res) == 1, "product not found"
        return res[0]

    def get_by_uuid_if_exists(
        self,
        product_uuid: UUIDStr,
    ) -> Product | None:
        # Do not attach the cache decorator here, since we don't
        #   want to cache a None. The interior call is cached if
        #   the product exists.
        try:
            return self.get_by_uuid(product_uuid=product_uuid)
        except AssertionError as e:
            if "product not found" in str(e):
                return None
            raise

    def get_by_uuids(
        self,
        product_uuids: list[UUIDStr],
    ) -> list[Product]:
        res = self.filter_paginated(
            product_uuids=product_uuids,
        )
        assert len(product_uuids) == len(res), "incomplete product response"
        return res

    def get_by_uuids_if_exists(
        self,
        product_uuids: list[UUIDStr],
    ) -> list[Product]:
        # Same as .get_by_uuids but doesn't raise Exception if len(product_uuids) != len(res)
        return self.filter_paginated(
            product_uuids=product_uuids,
        )

    def get_all(self, rand_limit: int | None) -> list[Product]:
        product_uuids = self.get_all_uuids(rand_limit=rand_limit)
        return self.filter_paginated(product_uuids=product_uuids)

    def get_all_uuids(self, rand_limit: int | None) -> list[UUIDStr]:

        if rand_limit:
            res = self.pg_config.execute_sql_query(
                query="""
                    SELECT p.id::uuid
                    FROM userprofile_brokerageproduct AS p
                    ORDER BY RANDOM()
                    LIMIT %s
                """,
                params=[rand_limit],
            )

        else:
            res = self.pg_config.execute_sql_query(
                query="""
                    SELECT p.id::uuid
                    FROM userprofile_brokerageproduct AS p
                """
            )
        return [i["id"] for i in res]

    def filter_paginated(
        self,
        product_uuids: list[UUIDStr] | None = None,
        business_uuids: list[UUIDStr] | None = None,
        team_uuids: list[UUIDStr] | None = None,
        name_like: str | None = None,
        supplier_tags: list[str] | None = None,
        order_field: str = "created",
        descending: bool = False,
        conn: Connection | None = None,
    ):
        """
        Automatically paginate the results of a filter query.
        """
        res = []
        page = 1
        size = self.DEFAULT_PAGE_SIZE
        with self.connection(conn) as conn:
            while True:
                _res = self.filter_by(
                    product_uuids=product_uuids,
                    business_uuids=business_uuids,
                    team_uuids=team_uuids,
                    name_like=name_like,
                    supplier_tags=supplier_tags,
                    page=page,
                    size=size,
                    order_field=order_field,
                    descending=descending,
                    conn=conn,
                )
                res.extend(_res)
                if len(_res) < size:
                    break
                page += 1
        return res

    def filter_page(
        self,
        product_uuids: list[UUIDStr] | None = None,
        business_uuids: list[UUIDStr] | None = None,
        team_uuids: list[UUIDStr] | None = None,
        name_like: str | None = None,
        supplier_tags: list[str] | None = None,
        page: int | None = None,
        size: int | None = None,
        order_field: str = "created",
        descending: bool = False,
        conn: Connection | None = None,
    ) -> tuple[list[Product], int]:
        products = self.filter_by(
            product_uuids=product_uuids,
            business_uuids=business_uuids,
            team_uuids=team_uuids,
            name_like=name_like,
            supplier_tags=supplier_tags,
            page=page,
            size=size,
            order_field=order_field,
            descending=descending,
            conn=conn,
        )
        count = self.filter_count(
            product_uuids=product_uuids,
            business_uuids=business_uuids,
            team_uuids=team_uuids,
            name_like=name_like,
            supplier_tags=supplier_tags,
        )
        return products, count

    def filter_by(
        self,
        product_uuids: list[UUIDStr] | None = None,
        business_uuids: list[UUIDStr] | None = None,
        team_uuids: list[UUIDStr] | None = None,
        name_like: str | None = None,
        supplier_tags: list[str] | None = None,
        page: int | None = None,
        size: int | None = None,
        order_field: str = "created",
        descending: bool = False,
        conn: Connection | None = None,
    ):
        from generalresearch.models.thl.product import Product

        # Enforce explicit pagination if we've querying for more than 1 item
        identifiers = [product_uuids, business_uuids, team_uuids]
        if (
            (sum(len(x) for x in identifiers if x is not None) > 1)
            and page is None
            and size is None
        ):
            raise ValueError("must paginate")
        paginated_filter_str = ""
        if page is not None or size is not None:
            paginated_filter_str = self.validate_pagination(page=page, size=size)
        filter_str, params = self.make_filter_str(
            product_uuids=product_uuids,
            business_uuids=business_uuids,
            team_uuids=team_uuids,
            name_like=name_like,
            supplier_tags=supplier_tags,
        )
        order_fields = {
            "name": "bp.name",
            "created": "bp.created",
        }
        order_by_sql = (
            f"{order_fields[order_field]} {'DESC' if descending else 'ASC'}, bp.id"
        )
        query = f"""
        WITH selected_products AS MATERIALIZED (
            SELECT  
                bp.id,
                bp.id_int,
                bp.name,
                bp.team_id,
                bp.business_id,
                bp.created,
                bp.enabled,
                bp.payments_enabled,
                bp.commission,
                bp.redirect_url,
                bp.grs_domain,
                bp.profiling_config,
                bp.user_health_config,
                bp.yield_man_config,
                bp.offerwall_config,
                bp.session_config,
                bp.payout_config,
                bp.user_create_config,
                bp.cache_balance,
                bp.cache_user_wallet_balance,
                bp.cache_updated_at,
                bp.users_active_7d,
                bp.task_completes_7d,
                bp.balance_net_7d
                FROM userprofile_brokerageproduct bp
                {filter_str}
                ORDER BY {order_by_sql}
                {paginated_filter_str}
            )
        SELECT
            bp.*,
            bp.commission AS commission_pct, 
            bp.grs_domain AS harmonizer_domain,
            bp.cache_balance AS balance,
            bp.cache_balance AS private_balance,
            bp.cache_user_wallet_balance AS user_wallet_balance,
            COALESCE(t.tags, ARRAY[]::varchar[]) AS tags,
            sources.value -> 'sources_config' AS sources_config,
            wallet.value -> 'user_wallet' AS user_wallet_config
        FROM selected_products bp
        LEFT JOIN LATERAL (
            SELECT array_agg(pt.tag) AS tags
            FROM userprofile_brokerageproducttag pt
            WHERE pt.product_id = bp.id_int
        ) t ON true
        LEFT JOIN userprofile_brokerageproductconfig sources
            ON sources.product_id = bp.id
           AND sources.key = 'sources_config'
        LEFT JOIN userprofile_brokerageproductconfig wallet
            ON wallet.product_id = bp.id
           AND wallet.key = 'user_wallet'
        ORDER BY {order_by_sql}
        """

        with self.connection(conn) as conn, conn.cursor() as c:
            c.execute(query, params)
            res = c.fetchall()
        products = [Product.model_validate(x) for x in res]
        return products

    def filter_count(
        self,
        product_uuids: list[UUIDStr] | None = None,
        business_uuids: list[UUIDStr] | None = None,
        team_uuids: list[UUIDStr] | None = None,
        name_like: str | None = None,
        supplier_tags: list[str] | None = None,
        conn: Connection | None = None,
    ) -> NonNegativeInt:
        filter_str, params = self.make_filter_str(
            product_uuids=product_uuids,
            business_uuids=business_uuids,
            team_uuids=team_uuids,
            name_like=name_like,
            supplier_tags=supplier_tags,
        )
        query = f"""
        SELECT COUNT(1) AS cnt
        FROM userprofile_brokerageproduct AS bp
        {filter_str}
        """
        with self.connection(conn) as conn, conn.cursor() as c:
            c.execute(query, params)
            res = c.fetchone()
        return int(res["cnt"])

    @staticmethod
    def make_filter_str(
        product_uuids: list[UUIDStr] | None = None,
        business_uuids: list[UUIDStr] | None = None,
        team_uuids: list[UUIDStr] | None = None,
        name_like: str | None = None,
        supplier_tags: list[str] | None = None,
    ) -> tuple[str, dict[str, Any]]:
        params = {}
        filters = []
        identifiers = [product_uuids, business_uuids, team_uuids]
        assert sum([bool(x) for x in identifiers]) == 1, (
            "Must provide exactly one set of identifiers"
        )
        identifier = next(x for x in identifiers if x)
        assert all(is_valid_uuid(v) for v in identifier), "invalid uuid"

        if product_uuids is not None:
            filters.append("bp.id = ANY(%(product_uuids)s)")
            params["product_uuids"] = list(product_uuids)
        if business_uuids is not None:
            filters.append("bp.business_id = ANY(%(business_uuids)s)")
            params["business_uuids"] = list(business_uuids)
        if team_uuids is not None:
            filters.append("bp.team_id = ANY(%(team_uuids)s)")
            params["team_uuids"] = list(team_uuids)
        if name_like:
            filters.append("bp.name ILIKE '%%' || %(name_like)s || '%%'")
            params["name_like"] = name_like
        if supplier_tags:
            filters.append("""
                EXISTS (
                    SELECT 1
                    FROM userprofile_brokerageproducttag pt
                    WHERE pt.product_id = bp.id_int
                      AND pt.tag = ANY(%(supplier_tags)s::text[])
                )
            """)
            params["supplier_tags"] = list(supplier_tags)
        assert len(filters) > 0, "must pass at least 1 filter"
        return "WHERE " + " AND ".join(filters), params

    def create(
        self,
        product_id: UUIDStr,
        team_id: UUIDStr,
        name: str,
        redirect_url: str,
        business_id: UUIDStr | None = None,
        harmonizer_domain: str | None = None,
        commission_pct: Decimal = Decimal("0.05"),
        sources_config: SourcesConfig | SupplyConfig | None = None,
        payout_config: PayoutConfig | None = None,
        session_config: SessionConfig | None = None,
        profiling_config: ProfilingConfig | None = None,
        user_wallet_config: UserWalletConfig | None = None,
        user_create_config: UserCreateConfig | None = None,
        user_health_config: UserHealthConfig | None = None,
    ) -> Product:
        """Create a Product with all the basic defaults and return the instance"""
        from generalresearch.models.thl.product import (
            PayoutConfig,
            Product,
            ProfilingConfig,
            SessionConfig,
            SourcesConfig,
            UserCreateConfig,
            UserHealthConfig,
            UserWalletConfig,
        )

        now = datetime.now(tz=UTC)

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
            insert_data["id_int"] = next(
                iter(
                    self.pg_config.execute_sql_query(
                        query="""
            SELECT COALESCE(MAX(id_int), 0) + 1 as id_int
            FROM userprofile_brokerageproduct
            """
                    )
                )
            )["id_int"]
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
        except Exception as e:
            try:
                return self.get_by_uuid(product_uuid=instance.id)
            except AssertionError:
                pass
            finally:
                self.cache_clear(instance.id)

            # If we couldn't find the Product, then go ahead and raise.
            capture_exception(e)
            raise

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

    def update(self, new_product: Product) -> None:
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
            return

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
            update_str = ", ".join(f"{k}=%({k})s" for k in data)
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
