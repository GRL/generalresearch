import binascii
import json
import os
from datetime import datetime, timezone
from random import randint
from uuid import uuid4

import pytest

SSO_ISSUER = ""


class TestGRUser:

    def test_init(self, gr_user):
        from generalresearch.models.gr.authentication import GRUser

        assert isinstance(gr_user, GRUser)
        assert not gr_user.is_superuser

        assert gr_user.teams is None
        assert gr_user.businesses is None
        assert gr_user.products is None

    @pytest.mark.skip(reason="TODO")
    def test_businesses(self):
        pass

    def test_teams(self, gr_user, membership, gr_db, gr_redis_config):
        from generalresearch.models.gr.team import Team

        assert gr_user.teams is None

        gr_user.prefetch_teams(pg_config=gr_db, redis_config=gr_redis_config)

        assert isinstance(gr_user.teams, list)
        assert len(gr_user.teams) == 1
        assert isinstance(gr_user.teams[0], Team)

    def test_prefetch_team_duplicates(
        self,
        gr_user_token,
        gr_user,
        membership,
        product_factory,
        membership_factory,
        team,
        thl_web_rr,
        gr_redis_config,
        gr_db,
    ):
        product_factory(team=team)
        membership_factory(team=team, gr_user=gr_user)

        gr_user.prefetch_teams(
            pg_config=gr_db,
            redis_config=gr_redis_config,
        )

        assert len(gr_user.teams) == 1

    def test_products(
        self,
        gr_user,
        product_factory,
        team,
        membership,
        gr_db,
        thl_web_rr,
        gr_redis_config,
    ):
        from generalresearch.models.thl.product import Product

        assert gr_user.products is None

        # Create a new Team membership, and then create a Product that
        #    is  part of that team
        membership.prefetch_team(pg_config=gr_db, redis_config=gr_redis_config)
        p: Product = product_factory(team=team)
        assert p.id_int
        assert team.uuid == membership.team.uuid
        assert p.team_id == team.uuid
        assert p.team_uuid == membership.team.uuid
        assert gr_user.id == membership.user_id

        gr_user.prefetch_products(
            pg_config=gr_db,
            thl_pg_config=thl_web_rr,
            redis_config=gr_redis_config,
        )
        assert isinstance(gr_user.products, list)
        assert len(gr_user.products) == 1
        assert isinstance(gr_user.products[0], Product)


class TestGRUserMethods:

    def test_cache_key(self, gr_user, gr_redis):
        assert isinstance(gr_user.cache_key, str)
        assert ":" in gr_user.cache_key
        assert str(gr_user.id) in gr_user.cache_key

    def test_to_redis(
        self,
        gr_user,
        gr_redis,
        team,
        business,
        product_factory,
        membership_factory,
    ):
        product_factory(team=team, business=business)
        membership_factory(team=team, gr_user=gr_user)

        res = gr_user.to_redis()
        assert isinstance(res, str)

        from generalresearch.models.gr.authentication import GRUser

        instance = GRUser.from_redis(res)
        assert isinstance(instance, GRUser)

    def test_set_cache(
        self,
        gr_user,
        gr_user_token,
        gr_redis,
        gr_db,
        thl_web_rr,
        gr_redis_config,
    ):
        assert gr_redis.get(name=gr_user.cache_key) is None
        assert gr_redis.get(name=f"{gr_user.cache_key}:team_uuids") is None
        assert gr_redis.get(name=f"{gr_user.cache_key}:business_uuids") is None
        assert gr_redis.get(name=f"{gr_user.cache_key}:product_uuids") is None

        gr_user.set_cache(
            pg_config=gr_db, thl_web_rr=thl_web_rr, redis_config=gr_redis_config
        )

        assert gr_redis.get(name=gr_user.cache_key) is not None
        assert gr_redis.get(name=f"{gr_user.cache_key}:team_uuids") is not None
        assert gr_redis.get(name=f"{gr_user.cache_key}:business_uuids") is not None
        assert gr_redis.get(name=f"{gr_user.cache_key}:product_uuids") is not None

    def test_set_cache_gr_user(
        self,
        gr_user,
        gr_user_token,
        gr_redis,
        gr_redis_config,
        gr_db,
        thl_web_rr,
        product_factory,
        team,
        membership_factory,
        thl_redis_config,
    ):
        from generalresearch.models.gr.authentication import GRUser

        p1 = product_factory(team=team)
        membership_factory(team=team, gr_user=gr_user)

        gr_user.set_cache(
            pg_config=gr_db, thl_web_rr=thl_web_rr, redis_config=gr_redis_config
        )

        res: str = gr_redis.get(name=gr_user.cache_key)
        gru2 = GRUser.from_redis(res)

        assert gr_user.model_dump_json(
            exclude={"businesses", "teams", "products"}
        ) == gru2.model_dump_json(exclude={"businesses", "teams", "products"})

        gru2.prefetch_products(
            pg_config=gr_db,
            thl_pg_config=thl_web_rr,
            redis_config=thl_redis_config,
        )
        assert gru2.product_uuids == [p1.uuid]

    def test_set_cache_team_uuids(
        self,
        gr_user,
        membership,
        gr_user_token,
        gr_redis,
        gr_db,
        thl_web_rr,
        product_factory,
        team,
        gr_redis_config,
    ):
        product_factory(team=team)

        gr_user.set_cache(
            pg_config=gr_db, thl_web_rr=thl_web_rr, redis_config=gr_redis_config
        )
        res = json.loads(gr_redis.get(name=f"{gr_user.cache_key}:team_uuids"))
        assert len(res) == 1
        assert gr_user.team_uuids == res

    @pytest.mark.skip
    def test_set_cache_business_uuids(
        self,
        gr_user,
        membership,
        gr_user_token,
        gr_redis,
        gr_db,
        thl_web_rr,
        product_factory,
        business,
        team,
        gr_redis_config,
    ):
        product_factory(team=team, business=business)

        gr_user.set_cache(
            pg_config=gr_db, thl_web_rr=thl_web_rr, redis_config=gr_redis_config
        )
        res = json.loads(gr_redis.get(name=f"{gr_user.cache_key}:business_uuids"))
        assert len(res) == 1
        assert gr_user.business_uuids == res

    def test_set_cache_product_uuids(
        self,
        gr_user,
        membership,
        gr_user_token,
        gr_redis,
        gr_db,
        thl_web_rr,
        product_factory,
        team,
        gr_redis_config,
    ):
        product_factory(team=team)

        gr_user.set_cache(
            pg_config=gr_db, thl_web_rr=thl_web_rr, redis_config=gr_redis_config
        )
        res = json.loads(gr_redis.get(name=f"{gr_user.cache_key}:product_uuids"))
        assert len(res) == 1
        assert gr_user.product_uuids == res


class TestGRToken:

    @pytest.fixture
    def gr_token(self, gr_user):
        from generalresearch.models.gr.authentication import GRToken

        now = datetime.now(tz=timezone.utc)
        token = binascii.hexlify(os.urandom(20)).decode()

        gr_token = GRToken(key=token, created=now, user_id=gr_user.id)

        return gr_token

    def test_init(self, gr_token):
        from generalresearch.models.gr.authentication import GRToken

        assert isinstance(gr_token, GRToken)
        assert gr_token.created

    def test_user(self, gr_token, gr_db, gr_redis_config):
        from generalresearch.models.gr.authentication import GRUser

        assert gr_token.user is None

        gr_token.prefetch_user(pg_config=gr_db, redis_config=gr_redis_config)

        assert isinstance(gr_token.user, GRUser)

    def test_auth_header(self, gr_token):
        assert isinstance(gr_token.auth_header, dict)


class TestClaims:

    def test_init(self):
        from generalresearch.models.gr.authentication import Claims

        d = {
            "iss": SSO_ISSUER,
            "sub": f"{uuid4().hex}{uuid4().hex}",
            "aud": uuid4().hex,
            "exp": randint(a=1_500_000_000, b=2_000_000_000),
            "iat": randint(a=1_500_000_000, b=2_000_000_000),
            "auth_time": randint(a=1_500_000_000, b=2_000_000_000),
            "acr": "goauthentik.io/providers/oauth2/default",
            "amr": ["pwd", "mfa"],
            "sid": f"{uuid4().hex}{uuid4().hex}",
            "email": "max@g-r-l.com",
            "email_verified": True,
            "name": "Max Nanis",
            "given_name": "Max Nanis",
            "preferred_username": "nanis",
            "nickname": "nanis",
            "groups": [
                "authentik Admins",
                "Developers",
                "Systems Admin",
                "Customer Support",
                "admin",
            ],
            "azp": uuid4().hex,
            "uid": uuid4().hex,
        }
        instance = Claims.model_validate(d)

        assert isinstance(instance, Claims)
