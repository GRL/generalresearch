from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from generalresearch.managers.base import Permission
from generalresearch.managers.thl.user_manager.redis_user_manager import (
    RedisUserManager,
)

if TYPE_CHECKING:
    from generalresearch.config import GRLBaseSettings
    from generalresearch.models.thl.user import User
    from generalresearch.pg_helper import PostgresConfig


class TestUserManagerRedis:

    def test_get_notset(self, redis_user_manager: RedisUserManager, user: User):
        redis_user_manager.clear_user_inmemory_cache(user=user)
        assert redis_user_manager.get_user(user_id=user.user_id) is None

    def test_get_user_id(self, redis_user_manager: RedisUserManager, user: User):
        redis_user_manager.set_user(user=user)

        assert redis_user_manager.get_user(user_id=user.user_id) == user

    def test_get_uuid(self, redis_user_manager: RedisUserManager, user: User):
        redis_user_manager.set_user(user=user)

        assert redis_user_manager.get_user(user_uuid=user.uuid) == user

    def test_get_ubp(self, redis_user_manager: RedisUserManager, user: User):
        redis_user_manager.set_user(user=user)

        assert (
            redis_user_manager.get_user(
                product_id=user.product_id, product_user_id=user.product_user_id
            )
            == user
        )

    @pytest.mark.skip(reason="TODO")
    def test_set(self):
        # I mean, the sets are implicitly tested by the get tests above. no point
        pass

    def test_get_with_cache_prefix(
        self,
        settings: GRLBaseSettings,
        user: User,
        thl_web_rw: PostgresConfig,
        thl_web_rr: PostgresConfig,
    ):
        """
        Confirm the prefix functionality is working; we do this so it
        is easier to migrate between any potentially breaking versions
        if we don't want any broken keys; not as important after
        pydantic usage...
        """
        from generalresearch.managers.thl.user_manager.user_manager import (
            UserManager,
        )

        um1 = UserManager(
            pg_config=thl_web_rw,
            pg_config_rr=thl_web_rr,
            sql_permissions=[Permission.UPDATE, Permission.CREATE],
            redis=settings.redis,
            redis_timeout=settings.redis_timeout,
        )

        um2 = UserManager(
            pg_config=thl_web_rw,
            pg_config_rr=thl_web_rr,
            sql_permissions=[Permission.UPDATE, Permission.CREATE],
            redis=settings.redis,
            redis_timeout=settings.redis_timeout,
            cache_prefix="user-lookup-v2",
        )

        um1.get_or_create_user(
            product_id=user.product_id, product_user_id=user.product_user_id
        )
        um2.get_or_create_user(
            product_id=user.product_id, product_user_id=user.product_user_id
        )

        assert isinstance(um1.redis_user_manager, RedisUserManager)
        res1 = um1.redis_user_manager.client.get(f"user-lookup:user_id:{user.user_id}")
        assert res1 is not None

        assert isinstance(um2.redis_user_manager, RedisUserManager)
        res2 = um2.redis_user_manager.client.get(
            f"user-lookup-v2:user_id:{user.user_id}"
        )
        assert res2 is not None

        assert res1 == res2
