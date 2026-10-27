from __future__ import annotations

import binascii
import logging
import os
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from psycopg import sql
from pydantic import PositiveInt

from generalresearch.managers.base import PostgresManager, PostgresManagerWithRedis
from generalresearch.models.custom_types import UUIDStr

if TYPE_CHECKING:
    from generalresearch.models.gr.authentication import GRToken, GRUser
    from generalresearch.pg_helper import PostgresConfig

LOG = logging.getLogger("gr")


class GRUserManager(PostgresManagerWithRedis):
    def create(
        self,
        email: str,
        name: str,
        avatar_url: str | None = None,
        is_superuser: bool = False,
    ) -> GRUser:
        from generalresearch.models.gr.authentication import GRUser

        now = datetime.now(tz=UTC)

        instance = GRUser.model_validate(
            {
                "email": email,
                "name": name,
                "avatar_url": avatar_url,
                "is_superuser": is_superuser,
                "date_joined": now,
            }
        )
        data = instance.model_dump(mode="json")

        with self.pg_config.make_connection() as conn:
            with conn.cursor() as c:
                query = sql.SQL("""
                    INSERT INTO gr_user (
                        email, name, is_superuser, date_joined, avatar_url
                    ) VALUES (
                        %(email)s, %(name)s, %(is_superuser)s, 
                        %(date_joined)s, %(avatar_url)s
                    ) RETURNING id;
                """)
                c.execute(query=query, params=data)
                gr_user_id: int = c.fetchone()["id"]
            conn.commit()

        instance.id = gr_user_id
        return instance

    def filter(
        self,
        *,
        gr_user_id: int | None = None,
        email: str | None = None,
    ) -> list[GRUser]:
        from generalresearch.models.gr.authentication import GRUser

        conditions = []
        params = {}

        if gr_user_id is not None:
            conditions.append("u.id = %(gr_user_id)s")
            params["gr_user_id"] = gr_user_id
        if email is not None:
            conditions.append("u.email = %(email)s")
            params["email"] = email

        filter_str = " AND ".join(conditions)
        filter_str = "WHERE " + filter_str if filter_str else ""

        query = f"""
        SELECT u.id, u.name, u.email, u.is_superuser, u.date_joined, u.avatar_url
        FROM gr_user AS u
        {filter_str}
        """
        with self.pg_config.make_connection() as conn, conn.cursor() as c:
            c.execute(query=query, params=params)
            res = c.fetchall()

        return [GRUser.from_postgresql(item) for item in res]

    def _get_one(self, *, raises: bool = True, **filters: int | str) -> GRUser | None:
        users = self.filter(**filters)

        if not users:
            if raises:
                raise ValueError("GRUser not found")
            return None

        assert len(users) == 1, "GRUserManager filter returned multiple users"
        return users[0]

    def get_by_id(self, gr_user_id: int) -> GRUser | None:
        return self._get_one(gr_user_id=gr_user_id)

    def get_by_email(self, email: str, raises=True) -> GRUser | None:
        return self._get_one(email=email, raises=raises)

    def get_all(self) -> list[GRUser]:
        return self.filter()

    def get_by_team(self, team_id: PositiveInt) -> list[GRUser]:
        from generalresearch.models.gr.authentication import GRUser

        with self.pg_config.make_connection() as conn, conn.cursor() as c:
            c.execute(
                query="""
                    SELECT u.id, u.name, u.email, u.avatar_url,
                           u.is_superuser, u.date_joined
                    FROM common_membership AS membership
                    INNER JOIN gr_user AS u 
                        ON u.id = membership.user_id
                    WHERE membership.team_id = %s
                """,
                params=(team_id,),
            )
            res = c.fetchall()

        return [GRUser.model_validate(item) for item in res]

    def list_product_uuids(
        self, user: GRUser, thl_pg_config: PostgresConfig
    ) -> list[UUIDStr] | None:
        if user.business_uuids is None:
            LOG.warning("prefetch not run")
            return None

        res = thl_pg_config.execute_sql_query(
            query="""
                SELECT bp.id::uuid as uuid
                FROM userprofile_brokerageproduct AS bp
                WHERE bp.business_id = ANY(%s)
            """,
            params=[user.business_uuids],
        )
        return [item["uuid"] for item in res]


class GRTokenManager(PostgresManager):
    def get_by_key(
        self,
        api_key: str,
    ) -> GRToken:
        """Return the GRToken for this API Token.

        :param api_key: an api value from http header
        :param jwks: a jwts dict from sso provider
        :param audience: an oidc client id
        :param issuer: a jwks_uri for sso provider
        :param gr_redis_config: redis

        :return GRToken instance (minified version, no relationships)
        :raises NotFoundException
        """
        from generalresearch.models.gr.authentication import GRToken

        # API Key
        with self.pg_config.make_connection() as conn, conn.cursor() as c:
            query = sql.SQL("""
            SELECT grk.* 
            FROM gr_token AS grk
            WHERE grk.key = %s
            LIMIT 2
            """)
            c.execute(query=query, params=(api_key,))
            res = c.fetchall()

        if len(res) == 0:
            raise ValueError(f"No GRUser with token of '{api_key}'")

        if len(res) > 1:
            raise ValueError(f"Too many GRUsers found with token of '{api_key}'")

        return GRToken.model_validate(res[0])

    def create(self, user_id: PositiveInt) -> None:
        # Taken directly from the DRF Token
        # https://github.com/encode/django-rest-framework/blob/0f39e0124d358b0098261f070175fa8e0359b739/rest_framework/authtoken/models.py#L35-L37
        from generalresearch.models.gr.authentication import GRToken

        token = GRToken.model_validate(
            {
                "key": binascii.hexlify(os.urandom(20)).decode(),
                "created": datetime.now(tz=UTC),
                "user_id": user_id,
            }
        )

        data = token.model_dump()
        data["user_id"] = token.user_id

        with self.pg_config.make_connection() as conn:
            with conn.cursor() as c:
                c.execute(
                    query=sql.SQL("""
                        INSERT INTO gr_token (key, user_id, created) 
                        VALUES (%(key)s, %(user_id)s, %(created)s) 
                    """),
                    params=data,
                )
            conn.commit()

    def get_by_user_id(self, user_id: PositiveInt) -> GRToken | None:
        # django authtoken_token table has (user_id) UNIQUE constraint
        # therefore, this will only return 0 or 1 GRTokens
        from generalresearch.models.gr.authentication import GRToken

        with self.pg_config.make_connection() as conn, conn.cursor() as c:
            query = sql.SQL("""
                    SELECT grt.*
                    FROM gr_token AS grt
                    LEFT JOIN gr_user AS u 
                        ON u.id = grt.user_id
                    WHERE u.id = %s
                    LIMIT 1;
                """)
            c.execute(query=query, params=(user_id,))
            result = c.fetchall()

        if not result:
            return None

        return GRToken.model_validate(result[0])
