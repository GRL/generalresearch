import logging
from datetime import datetime, timezone
from functools import lru_cache
from typing import Collection, List, Optional
from uuid import uuid4

import psycopg
from psycopg import sql

from generalresearch.models.custom_types import UUIDStr
from generalresearch.models.thl.user import User
from generalresearch.pg_helper import PostgresConfig

logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.INFO)


class MysqlUserManager:
    def __init__(self, pg_config: PostgresConfig, is_read_replica: bool):
        self.pg_config = pg_config
        self.is_read_replica = is_read_replica

    def _set_last_seen(self, user: User) -> None:
        # Don't call this directly. Use UserManager.set_last_seen()
        assert not self.is_read_replica
        now = datetime.now(tz=timezone.utc)
        self.pg_config.execute_write(
            """
            UPDATE thl_user
            SET last_seen = %s
            WHERE id = %s
            """,
            params=[now, user.user_id],
        )

    def get_user_from_mysql(
        self,
        *,
        product_id: Optional[str] = None,
        product_user_id: Optional[str] = None,
        user_id: Optional[int] = None,
        user_uuid: Optional[UUIDStr] = None,
        can_use_read_replica: bool = True,
    ) -> Optional[User]:

        logger.info(
            f"get_user_from_mysql: {product_id}, {product_user_id}, {user_id}, {user_uuid}"
        )
        assert (
            (product_id and product_user_id) or user_id or user_uuid
        ), "Must pass either (product_id, product_user_id), or user_id, or uuid"
        if product_id or product_user_id:
            assert (
                product_id and product_user_id
            ), "Must pass both product_id and product_user_id"
        assert (
            sum(map(bool, [product_id or product_id, user_id, user_uuid])) == 1
        ), "Must pass only 1 of (product_id, product_user_id), or user_id, or uuid"

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
        created: Optional[datetime] = None,
    ) -> User:
        """Creates a thl_user record for a new user."""
        assert self.is_read_replica is False
        # assert that the product exists
        if not self.product_id_exists(product_id=product_id):
            raise ValueError(f"userprofile_brokerageproduct not found: {product_id}")

        now = created or datetime.now(tz=timezone.utc)
        user_uuid = uuid4().hex
        params = {
            "user_uuid": user_uuid,
            "product_id": product_id,
            "product_user_id": product_user_id,
            "created": now,
            "last_seen": now,
        }

        # in postgres, you do not include the auto-increment id column
        query = sql.SQL(
            """
        INSERT INTO thl_user
        (uuid, product_id, product_user_id, created, 
         last_seen, blocked, last_country_iso, last_geoname_id, last_ip)
        VALUES (%(user_uuid)s, %(product_id)s, %(product_user_id)s, %(created)s,
        %(last_seen)s, FALSE, NULL, NULL, NULL)
        RETURNING id;
        """
        )

        try:
            with self.pg_config.make_connection() as conn:
                with conn.cursor() as c:
                    c.execute(query=query, params=params)
                    user_id = c.fetchone()["id"]
        except psycopg.IntegrityError as e:
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
                raise e
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

    @lru_cache(maxsize=5000)
    def product_id_exists(self, product_id: str):
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
    ) -> List[User]:
        assert product_id, "must pass product_id"
        assert len(product_user_ids) > 0, "must pass 1 or more product_user_ids"
        assert len(product_user_ids) <= 500, "limit 500 product_user_ids"
        assert isinstance(
            product_user_ids, (list, set)
        ), "must pass a collection of product_user_ids"
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
        user_ids: Collection[int] = None,
        user_uuids: Collection[str] = None,
    ) -> List[User]:
        assert (user_ids or user_uuids) and not (
            user_ids and user_uuids
        ), "Must pass ONE of user_ids, user_uuids"
        if user_ids:
            assert len(user_ids) <= 500, "limit 500 user_ids"
            assert isinstance(
                user_ids, (list, set)
            ), "must pass a collection of user_ids"

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
            assert len(user_uuids) <= 500, "limit 500 user_uuids"
            assert isinstance(
                user_uuids, (list, set)
            ), "must pass a collection of user_uuids"
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
