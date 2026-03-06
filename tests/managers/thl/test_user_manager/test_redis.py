import pytest

from generalresearch.managers.base import Permission


class TestUserManagerRedis:

    def test_get_notset(self, user_manager, user):
        user_manager.clear_user_inmemory_cache(user=user)
        assert user_manager.redis_user_manager.get_user(user_id=user.user_id) is None

    def test_get_user_id(self, user_manager, user):
        user_manager.redis_user_manager.set_user(user=user)

        assert user_manager.redis_user_manager.get_user(user_id=user.user_id) == user

    def test_get_uuid(self, user_manager, user):
        user_manager.redis_user_manager.set_user(user=user)

        assert user_manager.redis_user_manager.get_user(user_uuid=user.uuid) == user

    def test_get_ubp(self, user_manager, user):
        user_manager.redis_user_manager.set_user(user=user)

        assert (
            user_manager.redis_user_manager.get_user(
                product_id=user.product_id, product_user_id=user.product_user_id
            )
            == user
        )

    @pytest.mark.skip(reason="TODO")
    def test_set(self):
        # I mean, the sets are implicitly tested by the get tests above. no point
        pass

    def test_get_with_cache_prefix(self, settings, user, thl_web_rw, thl_web_rr):
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

        res1 = um1.redis_user_manager.client.get(f"user-lookup:user_id:{user.user_id}")
        assert res1 is not None

        res2 = um2.redis_user_manager.client.get(
            f"user-lookup-v2:user_id:{user.user_id}"
        )
        assert res2 is not None

        assert res1 == res2
