from collections.abc import Callable
from uuid import uuid4

import pytest

from generalresearch.managers.gr.authentication import (
    GRUserManager,
)
from generalresearch.models.gr.authentication import GRUser


class TestGRUserManager:
    def test_create(self, gr_user: GRUser, gr_user_manager: GRUserManager):
        assert gr_user.id is not None
        instance = gr_user_manager.get_by_id(gr_user.id)
        assert isinstance(instance, GRUser)
        assert gr_user.id == instance.id

        assert gr_user.id is not None
        instance2 = gr_user_manager.get_by_id(gr_user.id)
        assert isinstance(instance2, GRUser)
        assert gr_user.model_dump_json() == instance2.model_dump_json()

    def test_get_by_id(self, gr_user: GRUser, gr_user_manager: GRUserManager):
        with pytest.raises(expected_exception=ValueError) as cm:
            gr_user_manager.get_by_id(gr_user_id=999_999_999)
        assert "GRUser not found" in str(cm.value)
        assert gr_user.id is not None
        instance = gr_user_manager.get_by_id(gr_user_id=gr_user.id)
        assert isinstance(instance, GRUser)
        assert instance.email == gr_user.email

    def test_get_by_email(
        self,
        gr_user_factory: Callable[..., GRUser],
        gr_user_manager: GRUserManager,
    ):
        email = f"{uuid4().hex}@example.com"
        gr_user = gr_user_factory(email=email)

        with pytest.raises(expected_exception=ValueError, match="GRUser not found"):
            gr_user_manager.get_by_email(email="missing@example.com")

        instance = gr_user_manager.get_by_email(email=email)
        assert isinstance(instance, GRUser)
        assert instance.id == gr_user.id

    def test_update(self, gr_user: GRUser, gr_user_manager: GRUserManager):
        gr_user.name = "Updated Name"
        gr_user.avatar_url = "https://example.com/avatar.png"

        assert gr_user_manager.update(gr_user) is None

        assert gr_user.id is not None
        instance = gr_user_manager.get_by_id(gr_user.id)
        assert isinstance(instance, GRUser)
        assert instance.name == "Updated Name"
        assert instance.avatar_url == "https://example.com/avatar.png"

        gr_user.name = None
        gr_user.avatar_url = None
        gr_user_manager.update(gr_user)

        instance = gr_user_manager.get_by_id(gr_user.id)
        assert isinstance(instance, GRUser)
        assert instance.name is None
        assert instance.avatar_url is None

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
