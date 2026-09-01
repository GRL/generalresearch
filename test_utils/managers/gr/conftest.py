from __future__ import annotations

import subprocess
from collections.abc import Callable, Generator
from random import randint
from typing import TYPE_CHECKING

import pytest
import redis
import redis.asyncio as redis_async
from pydantic import PostgresDsn

from generalresearch.managers.gr.business import (
    BusinessAddressManager,
    BusinessBankAccountManager,
    BusinessManager,
)
from generalresearch.pg_helper import PostgresConfig
from generalresearch.redis_helper import RedisConfig

if TYPE_CHECKING:
    from generalresearch.config import GRLBaseSettings
    from generalresearch.managers.gr.authentication import GRTokenManager, GRUserManager


# === Msc ===


@pytest.fixture(scope="session")
def gr_redis_config_db() -> str:
    return str(randint(99, 1_023))


@pytest.fixture(scope="session")
def gr_redis_config(
    settings: GRLBaseSettings, gr_redis_config_db: str
) -> Generator[RedisConfig]:
    assert "unittest" in str(settings.testing_redis) or "127.0.0.1" in str(
        settings.testing_redis
    )

    uri = f"redis://{settings.testing_redis}/{gr_redis_config_db}"

    res = subprocess.run(
        ["redis-cli", "-u", uri, "SET", "jenkins_lock", "1", "NX", "EX", "3600"],
        check=True,
        text=True,
        capture_output=True,
    )

    if res.stdout.strip() != "OK":
        raise ValueError("Redis already locked... aborting.")

    yield RedisConfig(
        dsn=uri,
        decode_responses=True,
        socket_timeout=settings.redis_timeout,
        socket_connect_timeout=settings.redis_timeout,
    )

    r = redis.from_url(uri)
    r.flushdb()


@pytest.fixture(scope="session")
def gr_db(django_db_factory: Callable[..., PostgresDsn]) -> PostgresConfig:
    _dsn = django_db_factory("gr.common")

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
