from __future__ import annotations

import binascii
import os
from collections.abc import Callable
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import pytest

from generalresearch.models.gr.authentication import GRToken, GRUser
from generalresearch.models.gr.team import Team

if TYPE_CHECKING:
    from generalresearch.models.gr.team import Membership
    from generalresearch.models.thl.product import Product
    from generalresearch.pg_helper import PostgresConfig
    from generalresearch.redis_helper import RedisConfig


class TestGRUser:
    def test_init(self, gr_user: GRUser):

        assert isinstance(gr_user, GRUser)
        assert not gr_user.is_superuser

        assert gr_user.teams is None
        assert gr_user.businesses is None
        assert gr_user.products is None

    @pytest.mark.skip(reason="TODO")
    def test_businesses(self):
        pass

    def test_teams(
        self,
        gr_user: GRUser,
        gr_membership: Membership,
        gr_db: PostgresConfig,
        gr_redis_config: RedisConfig,
    ):

        assert gr_user.teams is None

        gr_user.prefetch_teams(pg_config=gr_db, redis_config=gr_redis_config)

        assert isinstance(gr_user.teams, list)
        assert len(gr_user.teams) == 1
        assert isinstance(gr_user.teams[0], Team)

    def test_prefetch_team_duplicates(
        self,
        gr_user_token: GRToken,
        gr_user: GRUser,
        gr_membership: Membership,
        product_factory: Callable[..., Product],
        gr_membership_factory: Callable[..., Membership],
        gr_team: Team,
        thl_web_rr: PostgresConfig,
        gr_redis_config: RedisConfig,
        gr_db: PostgresConfig,
    ):
        product_factory(team=gr_team)
        gr_membership_factory(gr_team=gr_team, gr_user=gr_user)

        gr_user.prefetch_teams(
            pg_config=gr_db,
            redis_config=gr_redis_config,
        )

        assert len(gr_user.teams) == 1

    def test_products(
        self,
        gr_user: GRUser,
        product_factory: Callable[..., Product],
        gr_team: Team,
        gr_membership: Membership,
        gr_db: PostgresConfig,
        thl_web_rr: PostgresConfig,
        gr_redis_config: RedisConfig,
    ):
        from generalresearch.models.thl.product import Product

        assert gr_user.products is None

        # Create a new Team membership, and then create a Product that
        #    is  part of that team
        gr_membership.prefetch_team(pg_config=gr_db, redis_config=gr_redis_config)
        assert isinstance(gr_membership.team, Team)

        p: Product = product_factory(team=gr_team)
        assert p.id_int
        assert gr_team.uuid == gr_membership.team.uuid
        assert p.team_id == gr_team.uuid
        assert p.team_uuid == gr_membership.team.uuid
        assert gr_user.id == gr_membership.user_id

        gr_user.prefetch_products(
            pg_config=gr_db,
            thl_pg_config=thl_web_rr,
            redis_config=gr_redis_config,
        )
        assert isinstance(gr_user.products, list)
        assert len(gr_user.products) == 1
        assert isinstance(gr_user.products[0], Product)


class TestGRUserMethods:
    def test_cache_key(self, gr_user: GRUser):
        assert isinstance(gr_user.cache_key, str)
        assert ":" in gr_user.cache_key
        assert str(gr_user.id) in gr_user.cache_key


class TestGRToken:
    @pytest.fixture
    def gr_token(self, gr_user: GRUser):
        now = datetime.now(tz=UTC)
        token = binascii.hexlify(os.urandom(20)).decode()

        gr_token = GRToken(key=token, created=now, user_id=gr_user.id)

        return gr_token

    def test_init(self, gr_token: GRToken):
        assert isinstance(gr_token, GRToken)
        assert gr_token.created

    def test_user(
        self, gr_token: GRToken, gr_db: PostgresConfig, gr_redis_config: RedisConfig
    ):
        assert gr_token.user is None

        gr_token.prefetch_user(pg_config=gr_db, redis_config=gr_redis_config)

        assert isinstance(gr_token.user, GRUser)

    def test_auth_header(self, gr_token: GRToken):
        assert isinstance(gr_token.auth_header, dict)
