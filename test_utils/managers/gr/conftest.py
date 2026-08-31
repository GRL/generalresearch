from __future__ import annotations

from collections.abc import Callable

import pytest
import redis.asyncio as redis_async
from pydantic import PostgresDsn
from redis import Redis

from generalresearch.config import GRLBaseSettings
from generalresearch.managers.gr.authentication import GRTokenManager, GRUserManager
from generalresearch.managers.gr.business import (
    BusinessAddressManager,
    BusinessBankAccountManager,
    BusinessManager,
)
from generalresearch.pg_helper import PostgresConfig
from generalresearch.redis_helper import RedisConfig


# === Msc ===
@pytest.fixture(scope="session")
def gr_redis(settings: GRLBaseSettings) -> Redis:
    assert "unittest" in str(settings.gr_redis) or "127.0.0.1" in str(settings.gr_redis)
    return Redis.from_url(
        url=str(settings.gr_redis),
        decode_responses=True,
        socket_timeout=settings.redis_timeout,
        socket_connect_timeout=settings.redis_timeout,
    )


@pytest.fixture
def gr_redis_async(settings: GRLBaseSettings) -> redis_async.Redis:
    assert "unittest" in str(settings.gr_redis) or "127.0.0.1" in str(settings.gr_redis)

    return redis_async.Redis.from_url(
        str(settings.gr_redis),
        decode_responses=True,
        socket_timeout=0.20,
        socket_connect_timeout=0.20,
    )


@pytest.fixture(scope="session")
def gr_redis_config(settings: GRLBaseSettings) -> RedisConfig:
    assert "unittest" in str(settings.gr_redis) or "127.0.0.1" in str(settings.gr_redis)

    return RedisConfig(
        dsn=settings.gr_redis,
        decode_responses=True,
        socket_timeout=settings.redis_timeout,
        socket_connect_timeout=settings.redis_timeout,
    )


@pytest.fixture(scope="session")
def gr_db(django_db_factory: Callable[..., PostgresDsn]) -> PostgresConfig:
    _dsn = django_db_factory("gr.common")
    print("DDDD:", _dsn)

    return PostgresConfig(
        dsn=_dsn,
        connect_timeout=1,
        statement_timeout=5,
    )


# === Managers ===


@pytest.fixture(scope="session")
def gr_user_manager(
    gr_db: PostgresConfig, gr_redis_config: RedisConfig
) -> GRUserManager:
    assert gr_db.dsn.path
    assert "/unittest-" in gr_db.dsn.path

    from generalresearch.managers.gr.authentication import GRUserManager

    return GRUserManager(pg_config=gr_db, redis_config=gr_redis_config)


@pytest.fixture(scope="session")
def gr_team_manager(gr_db: PostgresConfig) -> GRTokenManager:
    assert gr_db.dsn.path
    assert "/unittest-" in gr_db.dsn.path

    from generalresearch.managers.gr.authentication import GRTokenManager

    return GRTokenManager(pg_config=gr_db)


@pytest.fixture(scope="session")
def gr_business_manager(
    gr_db: PostgresConfig, gr_redis_config: RedisConfig
) -> BusinessManager:
    return BusinessManager(pg_config=gr_db, redis_config=gr_redis_config)


@pytest.fixture(scope="session")
def gr_business_bank_account_manager(
    gr_db: PostgresConfig,
) -> BusinessBankAccountManager:
    return BusinessBankAccountManager(pg_config=gr_db)


@pytest.fixture(scope="session")
def gr_business_address_manager(
    gr_db: PostgresConfig,
) -> BusinessAddressManager:
    return BusinessAddressManager(pg_config=gr_db)
