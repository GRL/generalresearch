import binascii
import logging
import os
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union
from uuid import uuid4

from psycopg import sql
from pydantic import AnyHttpUrl, PositiveInt

from generalresearch.managers.base import PostgresManager, PostgresManagerWithRedis
from generalresearch.models.custom_types import UUIDStr
from generalresearch.pg_helper import PostgresConfig
from generalresearch.redis_helper import RedisConfig

LOG = logging.getLogger("gr")

if TYPE_CHECKING:
    from generalresearch.models.gr.authentication import GRToken, GRUser


class GRUserManager(PostgresManagerWithRedis):

    def create_dummy(
        self,
        sub: Optional[str] = None,
        is_superuser: bool = False,
    ) -> "GRUser":
        sub = sub or f"{uuid4().hex}-{uuid4().hex}"

        return self.create(
            sub=sub,
            is_superuser=is_superuser,
        )

    def create(
        self,
        sub: str,
        is_superuser: bool = False,
    ) -> "GRUser":
        from generalresearch.models.gr.authentication import GRUser

        now = datetime.now(tz=timezone.utc)

        instance = GRUser.model_validate(
            {
                "sub": sub,
                "is_superuser": is_superuser,
                "date_joined": now,
            }
        )
        data = instance.model_dump(mode="json")

        with self.pg_config.make_connection() as conn:
            with conn.cursor() as c:
                query = sql.SQL(
                    """
                    INSERT INTO gr_user 
                        (sub, is_superuser, date_joined)
                    VALUES (%(sub)s, %(is_superuser)s, %(date_joined)s)
                    RETURNING id
                """
                )
                c.execute(query=query, params=data)
                gr_user_id: int = c.fetchone()["id"]
            conn.commit()

        instance.id = gr_user_id
        return instance

    def get_by_id(self, gr_user_id: int) -> Optional["GRUser"]:
        from generalresearch.models.gr.authentication import GRUser

        with self.pg_config.make_connection() as conn:
            with conn.cursor() as c:
                c.execute(
                    query="""
                    SELECT u.* 
                    FROM gr_user AS u
                    WHERE u.id = %s
                    LIMIT 1;
                """,
                    params=(gr_user_id,),
                )
                res = c.fetchone()

        if res is None:
            raise ValueError("GRUser not found")
        assert isinstance(
            res, dict
        ), "GRUserManager.get_by_id query returned invalid results"

        # We can return None if no MySQL results were found... but raise an
        # error if returning failed for a different reason
        gr_user = GRUser.from_postgresql(res)
        assert isinstance(gr_user, GRUser), "GRUser not serialized correctly"
        return gr_user

    def get_by_sub(self, sub: str, raises=True) -> Optional["GRUser"]:
        from generalresearch.models.gr.authentication import GRUser

        with self.pg_config.make_connection() as conn:
            with conn.cursor() as c:
                c.execute(
                    query="""
                    SELECT u.* 
                    FROM gr_user AS u
                    WHERE u.sub = %s
                    LIMIT 1;
                """,
                    params=(sub,),
                )
                res = c.fetchone()

        if raises and res is None:
            raise ValueError("GRUser not found")

        if res is None:
            return None

        assert isinstance(
            res, dict
        ), "GRUserManager.get_by_id query returned invalid results"

        # We can return None if no MySQL results were found... but raise an
        # error if returning failed for a different reason
        gr_user = GRUser.from_postgresql(res)
        assert isinstance(gr_user, GRUser), "GRUser not serialized correctly"
        return gr_user

    def get_by_sub_or_create(self, sub: str) -> "GRUser":
        return self.get_by_sub(sub=sub, raises=False) or self.create(sub=sub)

    def get_all(self) -> List["GRUser"]:
        from generalresearch.models.gr.authentication import GRUser

        with self.pg_config.make_connection() as conn:
            with conn.cursor() as c:
                c.execute(
                    query="""
                        SELECT u.* 
                        FROM gr_user AS u
                    """
                )
                res = c.fetchall()

        return [GRUser.from_postgresql(i) for i in res]

    def get_by_team(self, team_id: PositiveInt) -> List["GRUser"]:
        from generalresearch.models.gr.authentication import GRUser

        with self.pg_config.make_connection() as conn:
            with conn.cursor() as c:
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
                if isinstance(item[k], datetime):
                    item[k] = item[k].replace(tzinfo=timezone.utc)

        return [GRUser.model_validate(item) for item in res]

    def list_product_uuids(
        self, user: "GRUser", thl_pg_config: PostgresConfig
    ) -> Optional[List[UUIDStr]]:
        if user.business_uuids is None:
            LOG.warning("prefetch not run")
            return None

        res = thl_pg_config.execute_sql_query(
            query=f"""
                SELECT bp.id
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
        jwks: Optional[Dict[str, Any]] = None,
        audience: Optional[str] = None,
        issuer: Optional[Union[AnyHttpUrl, str]] = None,
        gr_redis_config: Optional[RedisConfig] = None,
    ) -> "GRToken":
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
                    "created": datetime.now(tz=timezone.utc),
                }
            )

            return gr_token

        # API Key
        with self.pg_config.make_connection() as conn:
            with conn.cursor() as c:
                query = sql.SQL(
                    """
                    SELECT grk.* 
                    FROM gr_token AS grk
                    WHERE grk.key = %s
                    LIMIT 1
                """
                )
                c.execute(query=query, params=(api_key,))
                res = c.fetchall()

        if len(res) == 0:
            raise Exception(f"No GRUser with token of '{api_key}'")

        if len(res) > 1:
            raise Exception(f"Too many GRUsers found with token of '{api_key}'")

        item = res[0]

        return GRToken.model_validate(item)

    def create(self, user_id: PositiveInt) -> None:
        # Taken directly from the DRF Token
        # https://github.com/encode/django-rest-framework/blob/0f39e0124d358b0098261f070175fa8e0359b739/rest_framework/authtoken/models.py#L35-L37
        from generalresearch.models.gr.authentication import GRToken

        token = GRToken.model_validate(
            {
                "key": binascii.hexlify(os.urandom(20)).decode(),
                "created": datetime.now(tz=timezone.utc),
                "user_id": user_id,
            }
        )

        data = token.model_dump()
        data["user_id"] = token.user_id

        with self.pg_config.make_connection() as conn:
            with conn.cursor() as c:
                c.execute(
                    query=sql.SQL(
                        """
                        INSERT INTO gr_token (key, user_id, created) 
                        VALUES (%(key)s, %(user_id)s, %(created)s) 
                    """
                    ),
                    params=data,
                )
            conn.commit()

        return None

    def get_by_user_id(self, user_id: PositiveInt) -> Optional["GRToken"]:
        # django authtoken_token table has (user_id) UNIQUE constraint
        # therefore, this will only return 0 or 1 GRTokens
        from generalresearch.models.gr.authentication import GRToken

        with self.pg_config.make_connection() as conn:
            with conn.cursor() as c:
                query = sql.SQL(
                    """
                    SELECT grt.*
                    FROM gr_token AS grt
                    LEFT JOIN gr_user AS u 
                        ON u.id = grt.user_id
                    WHERE u.id = %s
                    LIMIT 1;
                """
                )

                c.execute(query=query, params=(user_id,))

                result = c.fetchall()

        if not result:
            return None

        res = result[0]

        for k, _ in res.items():
            if isinstance(res[k], datetime):
                res[k] = res[k].replace(tzinfo=timezone.utc)

        return GRToken.model_validate(res)
