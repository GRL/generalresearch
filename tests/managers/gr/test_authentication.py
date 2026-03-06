import logging
from random import randint
from uuid import uuid4

import pytest

from generalresearch.models.gr.authentication import GRUser
from test_utils.models.conftest import gr_user

SSO_ISSUER = ""


class TestGRUserManager:

    def test_create(self, gr_um):
        from generalresearch.models.gr.authentication import GRUser

        user: GRUser = gr_um.create_dummy()
        instance = gr_um.get_by_id(user.id)
        assert user.id == instance.id

        instance2 = gr_um.get_by_id(user.id)
        assert user.model_dump_json() == instance2.model_dump_json()

    def test_get_by_id(self, gr_user, gr_um):
        with pytest.raises(expected_exception=ValueError) as cm:
            gr_um.get_by_id(gr_user_id=999_999_999)
        assert "GRUser not found" in str(cm.value)

        instance = gr_um.get_by_id(gr_user_id=gr_user.id)
        assert instance.sub == gr_user.sub

    def test_get_by_sub(self, gr_user, gr_um):
        with pytest.raises(expected_exception=ValueError) as cm:
            gr_um.get_by_sub(sub=uuid4().hex)
        assert "GRUser not found" in str(cm.value)

        instance = gr_um.get_by_sub(sub=gr_user.sub)
        assert instance.id == gr_user.id

    def test_get_by_sub_or_create(self, gr_user, gr_um):
        sub = f"{uuid4().hex}-{uuid4().hex}"

        with pytest.raises(expected_exception=ValueError) as cm:
            gr_um.get_by_sub(sub=sub)
        assert "GRUser not found" in str(cm.value)

        instance = gr_um.get_by_sub_or_create(sub=sub)
        assert isinstance(instance, GRUser)
        assert instance.sub == sub

    def test_get_all(self, gr_um):
        res1 = gr_um.get_all()
        assert isinstance(res1, list)

        gr_um.create_dummy()
        res2 = gr_um.get_all()
        assert len(res1) == len(res2) - 1

    def test_get_by_team(self, gr_um):
        res = gr_um.get_by_team(team_id=999_999_999)
        assert isinstance(res, list)
        assert res == []

    def test_list_product_uuids(self, caplog, gr_user, gr_um, thl_web_rr):
        with caplog.at_level(logging.WARNING):
            gr_um.list_product_uuids(user=gr_user, thl_pg_config=thl_web_rr)
        assert "prefetch not run" in caplog.text


class TestGRTokenManager:

    def test_create(self, gr_user, gr_tm):
        assert gr_tm.create(user_id=gr_user.id) is None

        token = gr_tm.get_by_user_id(user_id=gr_user.id)
        assert gr_user.id == token.user_id

    def test_get_by_user_id(self, gr_user, gr_tm):
        assert gr_tm.create(user_id=gr_user.id) is None

        token = gr_tm.get_by_user_id(user_id=gr_user.id)
        assert gr_user.id == token.user_id

    def test_prefetch_user(self, gr_user, gr_tm, gr_db, gr_redis_config):
        from generalresearch.models.gr.authentication import GRToken

        gr_tm.create(user_id=gr_user.id)

        token: GRToken = gr_tm.get_by_user_id(user_id=gr_user.id)
        assert token.user is None

        token.prefetch_user(pg_config=gr_db, redis_config=gr_redis_config)
        assert token.user.id == gr_user.id

    def test_get_by_key(self, gr_user, gr_um, gr_tm):
        gr_tm.create(user_id=gr_user.id)
        token = gr_tm.get_by_user_id(user_id=gr_user.id)

        instance = gr_tm.get_by_key(api_key=token.key)
        assert token.created == instance.created

        # Search for non-existent key
        with pytest.raises(expected_exception=Exception) as cm:
            gr_tm.get_by_key(api_key=uuid4().hex)
        assert "No GRUser with token of " in str(cm.value)

    @pytest.mark.skip(reason="no idea how to actually test this...")
    def test_get_by_sso_key(self, gr_user, gr_um, gr_tm, gr_redis_config):
        from generalresearch.models.gr.authentication import GRToken

        api_key = "..."
        jwks = {
            # ...
        }

        instance = gr_tm.get_by_key(
            api_key=api_key,
            jwks=jwks,
            audience="...",
            issuer=SSO_ISSUER,
            gr_redis_config=gr_redis_config,
        )

        assert isinstance(instance, GRToken)
