import logging
from datetime import datetime, timezone
from random import randint
from uuid import uuid4

import pytest

from generalresearch.managers.thl.user_manager import (
    UserCreateNotAllowedError,
    get_bp_user_create_limit_hourly,
)
from generalresearch.managers.thl.user_manager.rate_limit import (
    RateLimitItemPerHourConstantKey,
)
from generalresearch.models.thl.product import UserCreateConfig, Product
from generalresearch.models.thl.user import User
from test_utils.models.conftest import (
    user,
    product,
    user_manager,
    product_manager,
)

logger = logging.getLogger()


class TestUserManager:

    def test_copying_lru_cache(self, user_manager, user):
        # Before adding the deepcopy_return decorator, this would fail b/c the returned user
        #   is mutable, and it would mutate in the cache
        # user_manager = self.get_user_manager()

        user_manager.clear_user_inmemory_cache(user)
        u = user_manager.get_user(user_id=user.user_id)
        assert not u.blocked

        u.blocked = True
        u = user_manager.get_user(user_id=user.user_id)
        assert not u.blocked

    def test_get_user_no_inmemory(self, user, user_manager):
        user_manager.clear_user_inmemory_cache(user)
        user_manager.get_user.__wrapped__.cache_clear()
        u = user_manager.get_user(user_id=user.user_id)
        # this should hit mysql
        assert u == user

        cache_info = user_manager.get_user.__wrapped__.cache_info()
        assert cache_info.hits == 0, cache_info
        assert cache_info.misses == 1, cache_info

        # this should hit the lru cache
        u = user_manager.get_user(user_id=user.user_id)
        assert u == user

        cache_info = user_manager.get_user.__wrapped__.cache_info()
        assert cache_info.hits == 1, cache_info
        assert cache_info.misses == 1, cache_info

    def test_get_user_with_inmemory(self, user_manager, user):
        # user_manager = self.get_user_manager()

        user_manager.set_user_inmemory_cache(user)
        user_manager.get_user.__wrapped__.cache_clear()
        u = user_manager.get_user(user_id=user.user_id)
        # this should hit inmemory cache
        assert u == user

        cache_info = user_manager.get_user.__wrapped__.cache_info()
        assert cache_info.hits == 0, cache_info
        assert cache_info.misses == 1, cache_info

        # this should hit the lru cache
        u = user_manager.get_user(user_id=user.user_id)
        assert u == user

        cache_info = user_manager.get_user.__wrapped__.cache_info()
        assert cache_info.hits == 1, cache_info
        assert cache_info.misses == 1, cache_info


class TestBlockUserManager:

    def test_block_user(self, product, user_manager):
        product_user_id = f"user-{uuid4().hex[:10]}"

        # mysql_user_manager to skip user creation limit check
        user: User = user_manager.mysql_user_manager.create_user(
            product_id=product.id, product_user_id=product_user_id
        )
        assert not user.blocked

        # get_user to make sure caches are populated
        user = user_manager.get_user(
            product_id=product.id, product_user_id=product_user_id
        )
        assert not user.blocked

        assert user_manager.block_user(user) is True
        assert user.blocked

        user = user_manager.get_user(
            product_id=product.id, product_user_id=product_user_id
        )
        assert user.blocked

        user = user_manager.get_user(user_id=user.user_id)
        assert user.blocked

    def test_block_user_whitelist(self, product, user_manager, thl_web_rw):
        product_user_id = f"user-{uuid4().hex[:10]}"

        # mysql_user_manager to skip user creation limit check
        user: User = user_manager.mysql_user_manager.create_user(
            product_id=product.id, product_user_id=product_user_id
        )
        assert not user.blocked

        now = datetime.now(tz=timezone.utc)
        # Adds user to whitelist
        thl_web_rw.execute_write(
            """
            INSERT INTO userprofile_userstat
            (user_id, key, value, date)
            VALUES (%(user_id)s, 'USER_HEALTH.access_control', 1, %(date)s)
            ON CONFLICT (user_id, key) DO UPDATE SET value=1""",
            params={"user_id": user.user_id, "date": now},
        )
        assert user_manager.is_whitelisted(user)
        assert user_manager.block_user(user) is False
        assert not user.blocked


class TestCreateUserManager:

    def test_create_user(self, product_manager, thl_web_rw, user_manager):
        product: Product = product_manager.create_dummy(
            user_create_config=UserCreateConfig(
                min_hourly_create_limit=10, max_hourly_create_limit=69
            ),
        )

        product_user_id = f"user-{uuid4().hex[:10]}"

        user: User = user_manager.mysql_user_manager.create_user(
            product_id=product.id, product_user_id=product_user_id
        )
        assert isinstance(user, User)
        assert user.product_id == product.id
        assert user.product_user_id == product_user_id

        assert user.user_id is not None
        assert user.uuid is not None

        # make sure thl_user row is created
        res_thl_user = thl_web_rw.execute_sql_query(
            query=f"""
                SELECT * 
                FROM thl_user AS u
                WHERE u.id = %s
            """,
            params=[user.user_id],
        )

        assert len(res_thl_user) == 1

        u2 = user_manager.get_user(
            product_id=product.id, product_user_id=product_user_id
        )
        assert u2.user_id == user.user_id
        assert u2.uuid == user.uuid

    def test_create_user_integrity_error(self, product_manager, user_manager, caplog):
        product: Product = product_manager.create_dummy(
            product_id=uuid4().hex,
            team_id=uuid4().hex,
            name=f"Test Product ID #{uuid4().hex[:6]}",
            user_create_config=UserCreateConfig(
                min_hourly_create_limit=10, max_hourly_create_limit=69
            ),
        )

        product_user_id = f"user-{uuid4().hex[:10]}"
        rand_msg = f"log-{uuid4().hex}"

        with caplog.at_level(logging.INFO):
            logger.info(rand_msg)
            user1 = user_manager.mysql_user_manager.create_user(
                product_id=product.id, product_user_id=product_user_id
            )

        assert len(caplog.records) == 1
        assert caplog.records[0].getMessage() == rand_msg

        # Should cause a constraint error, triggering a lookup instead
        with caplog.at_level(logging.INFO):
            user2 = user_manager.mysql_user_manager.create_user(
                product_id=product.id, product_user_id=product_user_id
            )

        assert len(caplog.records) == 3
        assert caplog.records[0].getMessage() == rand_msg
        assert (
            caplog.records[1].getMessage()
            == f"mysql_user_manager.create_user_new integrity error: {product.id} {product_user_id}"
        )
        assert (
            caplog.records[2].getMessage()
            == f"get_user_from_mysql: {product.id}, {product_user_id}, None, None"
        )

        assert user1 == user2

    def test_raise_allow_user_create(self, product_manager, user_manager):
        rand_num = randint(25, 200)
        product: Product = product_manager.create_dummy(
            product_id=uuid4().hex,
            team_id=uuid4().hex,
            name=f"Test Product ID #{uuid4().hex[:6]}",
            user_create_config=UserCreateConfig(
                min_hourly_create_limit=rand_num,
                max_hourly_create_limit=rand_num,
            ),
        )

        instance: Product = user_manager.product_manager.get_by_uuid(
            product_uuid=product.id
        )

        # get_bp_user_create_limit_hourly is dynamically generated, make sure
        #   we use this value in our tests and not the
        #   UserCreateConfig.max_hourly_create_limit value
        rl_value: int = get_bp_user_create_limit_hourly(product=instance)

        # This is a randomly generated product_id, which means we'll always
        #   use the global defaults
        assert rand_num == instance.user_create_config.min_hourly_create_limit
        assert rand_num == instance.user_create_config.max_hourly_create_limit
        assert rand_num == rl_value

        rl = RateLimitItemPerHourConstantKey(rl_value)
        assert str(rl) == f"{rand_num} per 1 hour"

        key = rl.key_for("thl-grpc", "allow_user_create", instance.id)
        assert key == f"LIMITER/thl-grpc/allow_user_create/{instance.id}"

        # make sure we clear the key or subsequent tests will fail
        user_manager.user_manager_limiter.storage.clear(key=key)

        n = 0
        with pytest.raises(expected_exception=UserCreateNotAllowedError) as cm:
            for n, _ in enumerate(range(rl_value + 5)):
                user_manager.user_manager_limiter.raise_allow_user_create(
                    product=product
                )
        assert rl_value == n


class TestUserManagerMethods:

    def test_audit_log(self, user_manager, user, audit_log_manager):
        from generalresearch.models.thl.userhealth import AuditLog

        res = audit_log_manager.filter_by_user_id(user_id=user.user_id)
        assert len(res) == 0

        msg = uuid4().hex
        user_manager.audit_log(user=user, level=30, event_type=msg)

        res = audit_log_manager.filter_by_user_id(user_id=user.user_id)
        assert len(res) == 1
        assert isinstance(res[0], AuditLog)
        assert res[0].event_type == msg
