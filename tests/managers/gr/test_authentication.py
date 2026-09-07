import logging
from collections.abc import Callable
from uuid import uuid4

import pytest

from generalresearch.managers.gr.authentication import GRTokenManager, GRUserManager
from generalresearch.managers.gr.team import TeamManager
from generalresearch.models.gr.authentication import GRToken, GRUser
from generalresearch.pg_helper import PostgresConfig
from generalresearch.redis_helper import RedisConfig

SSO_ISSUER = ""


class TestGRUserManager:

    def test_create(self, gr_user: GRUser, gr_user_manager: GRUserManager):
        instance = gr_user_manager.get_by_id(gr_user.id)
        assert isinstance(instance, GRUser)
        assert gr_user.id == instance.id

        instance2 = gr_user_manager.get_by_id(gr_user.id)
        assert isinstance(instance2, GRUser)
        assert gr_user.model_dump_json() == instance2.model_dump_json()

    def test_get_by_id(self, gr_user: GRUser, gr_user_manager: GRUserManager):
        with pytest.raises(expected_exception=ValueError) as cm:
            gr_user_manager.get_by_id(gr_user_id=999_999_999)
        assert "GRUser not found" in str(cm.value)

        instance = gr_user_manager.get_by_id(gr_user_id=gr_user.id)
        assert isinstance(instance, GRUser)
        assert instance.sub == gr_user.sub

    def test_get_by_sub(self, gr_user: GRUser, gr_user_manager: GRUserManager):
        with pytest.raises(expected_exception=ValueError) as cm:
            gr_user_manager.get_by_sub(sub=uuid4().hex)
        assert "GRUser not found" in str(cm.value)

        instance = gr_user_manager.get_by_sub(sub=gr_user.sub)
        assert isinstance(instance, GRUser)
        assert instance.id == gr_user.id

    def test_get_by_sub_or_create(
        self, gr_user: GRUser, gr_user_manager: GRUserManager
    ):
        sub = f"{uuid4().hex}-{uuid4().hex}"

        with pytest.raises(expected_exception=ValueError) as cm:
            gr_user_manager.get_by_sub(sub=sub)
        assert "GRUser not found" in str(cm.value)

        instance = gr_user_manager.get_by_sub_or_create(sub=sub)
        assert isinstance(instance, GRUser)
        assert instance.sub == sub

    def test_get_all(
        self, gr_user_factory: Callable[..., GRUser], gr_user_manager: GRUserManager
    ):
        res1 = gr_user_manager.get_all()
        assert isinstance(res1, list)

        gr_user_factory(save=True)
        res2 = gr_user_manager.get_all()
        assert len(res1) == len(res2) - 1

    def test_get_by_team(self, gr_user_manager: GRUserManager):
        res = gr_user_manager.get_by_team(team_id=999_999_999)
        assert isinstance(res, list)
        assert res == []

    def test_list_product_uuids(
        self,
        caplog,
        gr_user: GRUser,
        gr_user_manager: GRUserManager,
        thl_web_rr: PostgresConfig,
    ):
        with caplog.at_level(logging.WARNING):
            gr_user_manager.list_product_uuids(user=gr_user, thl_pg_config=thl_web_rr)
        assert "prefetch not run" in caplog.text


class TestGRTokenManager:

    def test_create(self, gr_user: GRUser, gr_token_manager: GRTokenManager):
        assert gr_token_manager.create(user_id=gr_user.id) is None

        token = gr_token_manager.get_by_user_id(user_id=gr_user.id)
        assert isinstance(token, GRToken)
        assert gr_user.id == token.user_id

    def test_get_by_user_id(self, gr_user: GRUser, gr_token_manager: GRTokenManager):
        assert gr_token_manager.create(user_id=gr_user.id) is None

        token = gr_token_manager.get_by_user_id(user_id=gr_user.id)
        assert isinstance(token, GRToken)
        assert gr_user.id == token.user_id

    def test_prefetch_user(
        self,
        gr_user: GRUser,
        gr_token_manager: GRTokenManager,
        gr_db: PostgresConfig,
        gr_redis_config: RedisConfig,
    ):

        gr_token_manager.create(user_id=gr_user.id)

        token: GRToken | None = gr_token_manager.get_by_user_id(user_id=gr_user.id)
        assert isinstance(token, GRToken)
        assert token.user is None

        token.prefetch_user(pg_config=gr_db, redis_config=gr_redis_config)
        assert token.user.id == gr_user.id

    def test_get_by_key(
        self,
        gr_user: GRUser,
        gr_token_manager: GRTokenManager,
    ):
        gr_token_manager.create(user_id=gr_user.id)
        token = gr_token_manager.get_by_user_id(user_id=gr_user.id)
        assert isinstance(token, GRToken)

        instance = gr_token_manager.get_by_key(api_key=token.key)
        assert token.created == instance.created

        # Search for non-existent key
        with pytest.raises(expected_exception=Exception) as cm:
            gr_token_manager.get_by_key(api_key=uuid4().hex)
        assert "No GRUser with token of " in str(cm.value)

    @pytest.mark.skip(reason="no idea how to actually test this...")
    def test_get_by_sso_key(
        self,
        gr_team_manager: TeamManager,
        gr_redis_config: RedisConfig,
    ):

        api_key = "..."
        jwks = {
            # ...
        }

        instance = gr_team_manager.get_by_key(
            api_key=api_key,
            jwks=jwks,
            audience="...",
            issuer=SSO_ISSUER,
            gr_redis_config=gr_redis_config,
        )

        assert isinstance(instance, GRToken)
