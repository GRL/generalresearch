from __future__ import annotations

import binascii
import logging
import os
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from psycopg import sql
from pydantic import AnyHttpUrl, PositiveInt

from generalresearch.managers.base import PostgresManager, PostgresManagerWithRedis
from generalresearch.models.custom_types import UUIDStr

if TYPE_CHECKING:

    from generalresearch.models.gr.authentication import GRToken, GRUser
    from generalresearch.pg_helper import PostgresConfig
    from generalresearch.redis_helper import RedisConfig

LOG = logging.getLogger("gr")


class GRUserManager(PostgresManagerWithRedis):

    def create(
        self,
        sub: str,
        email: str | None = None,
        is_superuser: bool = False,
    ) -> GRUser:
        from generalresearch.models.gr.authentication import GRUser

        now = datetime.now(tz=UTC)

        instance = GRUser.model_validate(
            {
                "sub": sub,
                "email": email,
                "is_superuser": is_superuser,
                "date_joined": now,
            }
        )
        data = instance.model_dump(mode="json")

        with self.pg_config.make_connection() as conn:
            with conn.cursor() as c:
                query = sql.SQL("""
                    INSERT INTO gr_user 
                        (sub, email, is_superuser, date_joined)
                    VALUES (%(sub)s, %(email)s, %(is_superuser)s, %(date_joined)s)
                    RETURNING id
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
        sub: str | None = None,
        email: str | None = None,
    ) -> list[GRUser]:
        from generalresearch.models.gr.authentication import GRUser

        conditions = []
        params = {}

        if gr_user_id is not None:
            conditions.append("u.id = %(gr_user_id)s")
            params['gr_user_id'] = gr_user_id
        if sub is not None:
            conditions.append("u.sub = %(sub)s")
            params['sub'] = sub
        if email is not None:
            conditions.append("u.email = %(email)s")
            params['email'] = email

        filter_str = " AND ".join(conditions)
        filter_str = "WHERE " + filter_str if filter_str else ""

        query = f"""
        SELECT u.*
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

    def get_by_sub(self, sub: str, raises=True) -> GRUser | None:
        return self._get_one(sub=sub, raises=raises)

    def get_by_email(self, email: str, raises=True) -> GRUser | None:
        return self._get_one(email=email, raises=raises)

    def get_by_sub_or_create(self, sub: str) -> GRUser:
        return self.get_by_sub(sub=sub, raises=False) or self.create(sub=sub)

    def get_all(self) -> list[GRUser]:
        from generalresearch.models.gr.authentication import GRUser

        with self.pg_config.make_connection() as conn, conn.cursor() as c:
            c.execute(query="""
                        SELECT u.* 
                        FROM gr_user AS u
                    """)
            res = c.fetchall()

        return [GRUser.from_postgresql(i) for i in res]

    def get_by_team(self, team_id: PositiveInt) -> list[GRUser]:
        from generalresearch.models.gr.authentication import GRUser

        with self.pg_config.make_connection() as conn, conn.cursor() as c:
            c.execute(
                query="""
                    SELECT gru.*
                    FROM common_membership AS membership
                    INNER JOIN gr_user AS gru 
                        ON gru.id = membership.user_id
                    WHERE membership.team_id = %s
                """,
                params=(team_id,),
            )
            res = c.fetchall()

        for item in res:
            for k, v in item.items():
                if isinstance(v, datetime):
                    item[k] = item[k].replace(tzinfo=UTC)

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
        jwks: dict[str, Any] | None = None,
        audience: str | None = None,
        issuer: AnyHttpUrl | str | None = None,
        gr_redis_config: RedisConfig | None = None,
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
        from generalresearch.models.gr.authentication import Claims, GRToken

        # SSO Key
        if GRToken.is_sso(api_key):
            from jose import jwt

            payload = jwt.decode(
                token=api_key,
                key=jwks,
                algorithms=["RS256"],
                audience=audience,
                issuer=issuer,
            )
            claims = Claims.model_validate(payload)

            gr_um = GRUserManager(
                pg_config=self.pg_config, redis_config=gr_redis_config
            )
            gr_user = gr_um.get_by_sub_or_create(sub=claims.subject)
            gr_user.claims = claims

            gr_token = GRToken.model_validate(
                {
                    "key": api_key,
                    "user_id": gr_user.id,
                    "user": gr_user,
                    "created": datetime.now(tz=UTC),
                }
            )

            return gr_token

        # API Key
        with self.pg_config.make_connection() as conn, conn.cursor() as c:
            query = sql.SQL("""
                    SELECT grk.* 
                    FROM gr_token AS grk
                    WHERE grk.key = %s
                    LIMIT 1
                """)
            c.execute(query=query, params=(api_key,))
            res = c.fetchall()

        if len(res) == 0:
            raise ValueError(f"No GRUser with token of '{api_key}'")

        if len(res) > 1:
            raise ValueError(f"Too many GRUsers found with token of '{api_key}'")

        item = res[0]

        return GRToken.model_validate(item)

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

        res = result[0]

        for k in res:
            if isinstance(res[k], datetime):
                res[k] = res[k].replace(tzinfo=UTC)

        return GRToken.model_validate(res)
