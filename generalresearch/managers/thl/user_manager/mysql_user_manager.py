from __future__ import annotations

import logging
import operator
from collections.abc import Collection
from datetime import UTC, datetime
from enum import StrEnum
from threading import Lock
from typing import TYPE_CHECKING, Any
from uuid import uuid4

import psycopg
from cachetools import LRUCache, cachedmethod
from psycopg import Connection, sql
from pydantic import NonNegativeInt

from generalresearch.managers.base import Permission, PostgresManager
from generalresearch.models.custom_types import UUIDStr, try_normalize_email
from generalresearch.models.thl.user import User

if TYPE_CHECKING:
    from generalresearch.pg_helper import PostgresConfig

logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.INFO)


class LastActivityColumn(StrEnum):
    LAST_SEEN = "last_seen"
    LAST_TASK_COMPLETED_AT = "last_task_completed_at"
    LAST_OFFERWALL_REQUESTED_AT = "last_offerwall_requested_at"
    LAST_PROFILING_SUBMISSION_AT = "last_profiling_submission_at"


class UserOrderField(StrEnum):
    CREATED = "created"
    LAST_SEEN = "last_seen"
    LAST_TASK_COMPLETED_AT = "last_task_completed_at"
    LAST_OFFERWALL_REQUESTED_AT = "last_offerwall_requested_at"
    LAST_PROFILING_SUBMISSION_AT = "last_profiling_submission_at"
    PRODUCT_USER_ID = "product_user_id"


class MysqlUserManager(PostgresManager):
    def __init__(
        self,
        pg_config: PostgresConfig,
        is_read_replica: bool,
        permissions: Collection[Permission] | None = None,
    ):
        self.is_read_replica = is_read_replica
        self.product_id_exists_cache = LRUCache(maxsize=5000)
        self.product_id_exists_cache_lock = Lock()
        super().__init__(pg_config=pg_config, permissions=permissions)

    def _set_last_activity(
        self,
        user: User,
        column: LastActivityColumn,
        conn: Connection | None = None,
    ) -> None:
        # Don't call this directly. Use UserManager.set_last_activity().
        assert not self.is_read_replica
        column = LastActivityColumn(column)

        with self.connection(conn) as conn, conn.cursor() as c:
            c.execute(
                sql.SQL("UPDATE thl_user SET {} = %s WHERE id = %s").format(
                    sql.Identifier(column)
                ),
                params=[getattr(user, column), user.user_id],
            )

    def _set_last_country_ip(
        self,
        user: User,
        last_ip: str,
        last_country_iso: str | None = None,
        conn: Connection | None = None,
    ) -> None:
        # Don't call this directly. Use UserManager.set_last_country_ip()
        assert not self.is_read_replica
        params = {
            "user_id": user.user_id,
            "last_ip": last_ip,
            "last_country_iso": last_country_iso,
        }
        with self.connection(conn) as conn, conn.cursor() as c:
            c.execute(
                """
            UPDATE thl_user
            SET last_ip = %(last_ip)s, last_country_iso = %(last_country_iso)s
            WHERE id = %(user_id)s
            AND (
                last_ip IS DISTINCT FROM %(last_ip)s OR
                last_country_iso IS DISTINCT FROM %(last_country_iso)s
            )
                """,
                params,
            )

    def _change_product_user_id(self, *, user: User, new_product_user_id: str) -> User:
        """Change a user's supplier-provided ID in the primary database."""
        assert not self.is_read_replica
        assert user.user_id is not None
        assert user.product_id is not None
        assert user.product_user_id is not None

        with self.pg_config.make_connection() as conn, conn.cursor() as c:
            c.execute(
                query="""
                    UPDATE thl_user
                    SET product_user_id = %(new_product_user_id)s
                    WHERE id = %(user_id)s
                        AND product_id = %(product_id)s
                        AND product_user_id = %(old_product_user_id)s
                    RETURNING id AS user_id, product_id, product_user_id,
                              uuid, blocked, created, last_seen
                    """,
                params={
                    "new_product_user_id": new_product_user_id,
                    "user_id": user.user_id,
                    "product_id": user.product_id,
                    "old_product_user_id": user.product_user_id,
                },
            )
            row = c.fetchone()

        if row is None:
            raise RuntimeError(
                "User was not updated; it may have been changed concurrently"
            )
        return User.from_db(row)

    def get_user_from_mysql(
        self,
        *,
        product_id: str | None = None,
        product_user_id: str | None = None,
        user_id: int | None = None,
        user_uuid: UUIDStr | None = None,
        can_use_read_replica: bool = True,
    ) -> User | None:

        logger.info(
            f"get_user_from_mysql: {product_id}, {product_user_id}, {user_id}, {user_uuid}"
        )
        assert (product_id and product_user_id) or user_id or user_uuid, (
            "Must pass either (product_id, product_user_id), or user_id, or uuid"
        )
        if product_id or product_user_id:
            assert product_id and product_user_id, (
                "Must pass both product_id and product_user_id"
            )
        assert sum(map(bool, [product_id or product_id, user_id, user_uuid])) == 1, (
            "Must pass only 1 of (product_id, product_user_id), or user_id, or uuid"
        )

        # Using RR: Assume we check redis first for newly created users
        if can_use_read_replica is False:
            assert self.is_read_replica is False

        if product_id:
            res = self.pg_config.execute_sql_query(
                query="""
                SELECT id AS user_id, product_id, product_user_id, 
                        uuid, blocked, created, last_seen
                FROM thl_user
                WHERE product_id = %s
                    AND product_user_id = %s
                LIMIT 1
            """,
                params=[product_id, product_user_id],
            )

        elif user_id:
            res = self.pg_config.execute_sql_query(
                query="""
                SELECT  id AS user_id, product_id, product_user_id, 
                        uuid, blocked, created, last_seen
                FROM thl_user
                WHERE id = %s
                LIMIT 1
            """,
                params=[user_id],
            )

        else:
            res = self.pg_config.execute_sql_query(
                query="""
                SELECT  id AS user_id, product_id, product_user_id, 
                        uuid, blocked, created, last_seen
                FROM thl_user
                WHERE uuid = %s
                LIMIT 1
            """,
                params=[user_uuid],
            )

        if res:
            res = res[0]
            # todo: add other cols into User (`last_ip`, `last_geoname_id`, `last_country_iso`)
            return User.from_db(res)

    def create_user(
        self,
        product_user_id: str,
        product_id: str,
        created: datetime | None = None,
    ) -> User:
        """Creates a thl_user record for a new user."""
        assert self.is_read_replica is False
        # assert that the product exists
        if not self.product_id_exists(product_id=product_id):
            raise ValueError(f"userprofile_brokerageproduct not found: {product_id}")

        now = created or datetime.now(tz=UTC)
        user_uuid = uuid4().hex
        params = {
            "user_uuid": user_uuid,
            "product_id": product_id,
            "product_user_id": product_user_id,
            "created": now,
            "last_seen": now,
        }

        query = sql.SQL("""
        INSERT INTO thl_user
        (uuid, product_id, product_user_id, created, 
         last_seen, blocked, last_country_iso, last_geoname_id, last_ip)
        VALUES (%(user_uuid)s, %(product_id)s, %(product_user_id)s, %(created)s,
        %(last_seen)s, FALSE, NULL, NULL, NULL)
        RETURNING id;
        """)

        try:
            with self.pg_config.make_connection() as conn, conn.cursor() as c:
                c.execute(query=query, params=params)
                user_id = c.fetchone()["id"]
                c.execute(
                    """
                    UPDATE userprofile_brokerageproduct
                    SET user_count = user_count + 1
                    WHERE id = %(product_id)s
                    """,
                    params=params,
                )
                assert c.rowcount == 1
        except psycopg.IntegrityError:
            # Two machines/processes are trying to create this same (product_id, product_user_id)
            #   at the same time. There's a unique index, so mysql will not let two be created.
            # The 2nd should get an IntegrityError, meaning this already exists, and we can just query it.
            logger.info(
                f"mysql_user_manager.create_user_new integrity error: {product_id} {product_user_id}"
            )
            user_mysql = self.get_user_from_mysql(
                product_id=product_id,
                product_user_id=product_user_id,
                can_use_read_replica=False,
            )
            if user_mysql:
                return user_mysql
            else:
                # We specifically queried the NON read-replica, and we got an IntegrityError, so
                #   something else must be wrong...
                raise
        else:
            user = User(
                user_id=user_id,
                product_id=product_id,
                product_user_id=product_user_id,
                uuid=user_uuid,
                last_seen=now,
                created=now,
            )

        return user

    @cachedmethod(
        operator.attrgetter("product_id_exists_cache"),
        lock=operator.attrgetter("product_id_exists_cache_lock"),
    )
    def product_id_exists(self, product_id: str) -> bool:
        # 'id' is the primary key, there can only be 0 or 1
        query = """
        SELECT id 
        FROM userprofile_brokerageproduct
        WHERE id = %s;
        """
        res = self.pg_config.execute_sql_query(query, [product_id])
        return len(res) > 0

    def _block_user(
        self,
        user: User,
    ) -> None:
        # Don't call this directly. Use UserManager.block_user()
        assert not self.is_read_replica
        # id is primary key, there can only be 1 row
        with self.pg_config.make_connection() as conn:
            with conn.cursor() as c:
                c.execute(
                    query="""
                    UPDATE thl_user SET blocked = %s
                    WHERE id = %s
                    """,
                    params=[True, user.user_id],
                )
                assert c.rowcount == 1, "User does not exist"
            conn.commit()

    def is_whitelisted(self, user: User):
        res = self.pg_config.execute_sql_query(
            """
            SELECT value
            FROM userprofile_userstat
            WHERE user_id = %s
            AND key = 'USER_HEALTH.access_control'""",
            [user.user_id],
        )
        if res and res[0]["value"] is not None:
            return bool(int(res[0]["value"]))
        return False

    def fetch_by_bpuids(
        self,
        *,
        product_id: str,
        product_user_ids: Collection[str],
    ) -> list[User]:
        assert product_id, "must pass product_id"
        assert len(product_user_ids) > 0, "must pass 1 or more product_user_ids"
        assert len(product_user_ids) <= 500, "limit 500 product_user_ids"
        assert isinstance(product_user_ids, (list, set)), (
            "must pass a collection of product_user_ids"
        )
        res = self.pg_config.execute_sql_query(
            query="""
            SELECT id AS user_id, product_id, product_user_id, 
                    uuid, blocked, created, last_seen
            FROM thl_user
            WHERE product_id = %(product_id)s
                AND product_user_id = ANY(%(product_user_ids)s)
            LIMIT 500
            """,
            params={
                "product_id": product_id,
                "product_user_ids": product_user_ids,
            },
        )
        return [User.from_db(x) for x in res]

    def fetch(
        self,
        *,
        user_ids: Collection[int] | None = None,
        user_uuids: Collection[str] | None = None,
    ) -> list[User]:
        assert (user_ids or user_uuids) and not (user_ids and user_uuids), (
            "Must pass ONE of user_ids, user_uuids"
        )
        if user_ids:
            assert isinstance(user_ids, (list, set)), (
                "must pass a collection of user_ids"
            )
            assert len(user_ids) <= 500, "limit 500 user_ids"

            res = self.pg_config.execute_sql_query(
                query="""
                SELECT id AS user_id, product_id, product_user_id, 
                        uuid, blocked, created, last_seen
                FROM thl_user
                WHERE id = ANY(%(user_ids)s)
                LIMIT 500
                """,
                params={"user_ids": user_ids},
            )
        else:
            assert isinstance(user_uuids, (list, set)), (
                "must pass a collection of user_uuids"
            )
            assert len(user_uuids) <= 500, "limit 500 user_uuids"
            res = self.pg_config.execute_sql_query(
                query="""
                SELECT id AS user_id, product_id, product_user_id, 
                        uuid, blocked, created, last_seen
                FROM thl_user
                WHERE uuid = ANY(%(user_uuids)s)
                LIMIT 500
                """,
                params={"user_uuids": user_uuids},
            )
        return [User.from_db(x) for x in res]

    @staticmethod
    def make_filter_str(
        product_id: str | None = None,
        business_id: str | None = None,
        team_id: str | None = None,
        product_user_id: str | None = None,
        email_startswith: str | None = None,
        display_name_like: str | None = None,
        allow_global_search: bool = False,
    ) -> tuple[str, dict[str, Any]]:
        filters = []
        params = {}

        if product_id is not None:
            params["product_id"] = product_id
            filters.append("product_id = %(product_id)s")
        if business_id:
            params["business_id"] = business_id
            filters.append("ubp.business_id = %(business_id)s")
        if team_id:
            params["team_id"] = team_id
            filters.append("ubp.team_id = %(team_id)s")
        if product_user_id:
            # This should only be allowed within the context of a product, team, or business,
            # unless we are admins
            if not allow_global_search:
                assert any([product_id, business_id, team_id]), (
                    "must scope to a product, team, or business"
                )
            params["product_user_id"] = product_user_id
            filters.append("u.product_user_id = %(product_user_id)s")
        if email_startswith is not None:
            if not allow_global_search:
                assert any([product_id, business_id, team_id]), (
                    "must scope to a product, team, or business"
                )
            params["email_startswith"] = f"{email_startswith}%"
            filters.append("email_address LIKE %(email_startswith)s")
            normalized_email = try_normalize_email(email_startswith)
            if normalized_email != email_startswith:
                params["normalized_email_startswith"] = f"{normalized_email}%"
                filters.append("canonical_email LIKE %(normalized_email_startswith)s")
        if display_name_like is not None:
            if not allow_global_search:
                assert any([product_id, business_id, team_id]), (
                    "must scope to a product, team, or business"
                )
            params["display_name_like"] = f"{display_name_like}%"
            filters.append("display_name ILIKE %(display_name_like)s")

        return "WHERE " + " AND ".join(filters) if filters else "", params

    def filter(
        self,
        product_id: str | None = None,
        business_id: str | None = None,
        team_id: str | None = None,
        product_user_id: str | None = None,
        email_startswith: str | None = None,
        display_name_like: str | None = None,
        page: int | None = None,
        size: int | None = None,
        order_field: UserOrderField = UserOrderField.CREATED,
        descending: bool = False,
        allow_global_search: bool = False,
    ) -> tuple[list[User], NonNegativeInt]:
        with self.pg_config.make_connection() as conn:
            items = self.filter_page(
                product_id=product_id,
                business_id=business_id,
                team_id=team_id,
                product_user_id=product_user_id,
                email_startswith=email_startswith,
                display_name_like=display_name_like,
                page=page,
                size=size,
                order_field=order_field,
                descending=descending,
                allow_global_search=allow_global_search,
                conn=conn,
            )
            total = self.filter_count(
                product_id=product_id,
                business_id=business_id,
                team_id=team_id,
                product_user_id=product_user_id,
                email_startswith=email_startswith,
                display_name_like=display_name_like,
                allow_global_search=allow_global_search,
                conn=conn,
            )
        return items, total

    def filter_page(
        self,
        product_id: str | None = None,
        business_id: str | None = None,
        team_id: str | None = None,
        product_user_id: str | None = None,
        email_startswith: str | None = None,
        display_name_like: str | None = None,
        page: int | None = None,
        size: int | None = None,
        order_field: UserOrderField = UserOrderField.CREATED,
        descending: bool = False,
        allow_global_search: bool = False,
        conn: Connection | None = None,
    ) -> list[User]:
        pagination_sql = self.validate_pagination(page=page, size=size)
        filter_str, params = self.make_filter_str(
            product_id=product_id,
            business_id=business_id,
            team_id=team_id,
            product_user_id=product_user_id,
            email_startswith=email_startswith,
            display_name_like=display_name_like,
            allow_global_search=allow_global_search,
        )
        order_field = UserOrderField(order_field)
        order_by_sql = (
            f"u.{order_field.value} {'DESC' if descending else 'ASC'}, u.uuid"
        )

        query = f"""
        SELECT u.id as user_id, u.uuid, u.product_id, u.product_user_id, u.blocked,
            u.created, u.last_seen, u.last_task_completed_at,
            u.last_offerwall_requested_at, u.last_profiling_submission_at,
            tu.email_address, tu.display_name, ubp.business_id, ubp.team_id
        FROM thl_user u
        LEFT JOIN thl_usermetadata tu on u.id = tu.user_id
        JOIN userprofile_brokerageproduct ubp ON ubp.id = u.product_id
        {filter_str}
        ORDER BY {order_by_sql}
        {pagination_sql}
        """
        with self.connection(conn) as conn, conn.cursor() as c:
            c.execute(query, params)
            res = c.fetchall()

        return [User.from_db(x) for x in res]

    def filter_count(
        self,
        product_id: str | None = None,
        business_id: str | None = None,
        team_id: str | None = None,
        product_user_id: str | None = None,
        email_startswith: str | None = None,
        display_name_like: str | None = None,
        allow_global_search: bool = False,
        conn: Connection | None = None,
    ) -> NonNegativeInt:

        filter_str, params = self.make_filter_str(
            product_id=product_id,
            business_id=business_id,
            team_id=team_id,
            product_user_id=product_user_id,
            email_startswith=email_startswith,
            display_name_like=display_name_like,
            allow_global_search=allow_global_search,
        )
        if not any([product_user_id, email_startswith, display_name_like]):
            # We're filtering on product, team, or business only. So use the
            # precomputed counts
            query = f"""
            SELECT SUM(user_count) AS cnt
            FROM userprofile_brokerageproduct ubp
                {filter_str}
            """
        else:
            query = f"""
            SELECT COUNT(1) AS cnt
            FROM thl_user u
            LEFT JOIN thl_usermetadata tu on u.id = tu.user_id
            JOIN userprofile_brokerageproduct ubp ON ubp.id = u.product_id
                {filter_str}
            """
        with self.connection(conn) as conn, conn.cursor() as c:
            c.execute(query, params)
            res = c.fetchall()
        return res[0]["cnt"]
