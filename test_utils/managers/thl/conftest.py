from __future__ import annotations

import subprocess
from collections.abc import Callable, Generator
from random import randint
from typing import TYPE_CHECKING

import pytest
import redis
from pydantic import PostgresDsn

from generalresearch.managers.base import Permission
from generalresearch.managers.thl.user_manager.mysql_user_manager import (
    MysqlUserManager,
)
from generalresearch.managers.thl.user_manager.redis_user_manager import (
    RedisUserManager,
)
from generalresearch.pg_helper import PostgresConfig
from generalresearch.redis_helper import RedisConfig

if TYPE_CHECKING:
    from generalresearch.config import GRLBaseSettings
    from generalresearch.managers.thl.buyer import BuyerManager
    from generalresearch.managers.thl.category import CategoryManager
    from generalresearch.managers.thl.ipinfo import (
        IPGeonameManager,
        IPInformationManager,
    )
    from generalresearch.managers.thl.payout import (
        BrokerageProductPayoutEventManager,
        BusinessPayoutEventManager,
        PayoutEventManager,
        UserPayoutEventManager,
    )
    from generalresearch.managers.thl.product import ProductManager
    from generalresearch.managers.thl.session import SessionManager
    from generalresearch.managers.thl.task_adjustment import (
        TaskAdjustmentManager,
    )
    from generalresearch.managers.thl.user_manager.user_manager import (
        UserManager,
    )
    from generalresearch.managers.thl.user_manager.user_metadata_manager import (
        UserMetadataManager,
    )
    from generalresearch.managers.thl.userhealth import (
        AuditLogManager,
        IPRecordManager,
    )
    from generalresearch.managers.thl.wall import (
        WallCacheManager,
        WallManager,
    )

# === Msc ===


@pytest.fixture(scope="session")
def thl_redis_config_db() -> str:
    return str(randint(99, 1_023))


@pytest.fixture(scope="session")
def thl_redis_config(
    settings: GRLBaseSettings, thl_redis_config_db: str
) -> Generator[RedisConfig]:
    assert "unittest" in str(settings.testing_redis) or "127.0.0.1" in str(
        settings.testing_redis
    )

    uri = f"redis://{settings.testing_redis}/{thl_redis_config_db}"

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
def thl_redis_client(thl_redis_config):
    return thl_redis_config.create_redis_client()


@pytest.fixture(scope="session")
def thl_web_rr(django_db_factory: Callable[..., PostgresDsn]) -> PostgresConfig:
    _dsn = django_db_factory("generalresearch.thl_django")

    return PostgresConfig(
        dsn=_dsn,
        connect_timeout=1,
        statement_timeout=5,
    )


@pytest.fixture(scope="session")
def thl_web_rw(thl_web_rr: PostgresConfig) -> PostgresConfig:
    return thl_web_rr


# === Managers ===


@pytest.fixture(scope="session")
def payout_event_manager(
    thl_web_rw: PostgresConfig, thl_redis_config: RedisConfig
) -> PayoutEventManager:
    assert thl_web_rw.dsn.path
    assert "/unittest-" in thl_web_rw.dsn.path

    from generalresearch.managers.thl.payout import PayoutEventManager

    return PayoutEventManager(
        pg_config=thl_web_rw,
        permissions=[Permission.CREATE, Permission.READ],
        redis_config=thl_redis_config,
    )


@pytest.fixture(scope="session")
def user_payout_event_manager(
    thl_web_rw: PostgresConfig, thl_redis_config: RedisConfig
) -> UserPayoutEventManager:
    assert thl_web_rw.dsn.path
    assert "/unittest-" in thl_web_rw.dsn.path

    from generalresearch.managers.thl.payout import UserPayoutEventManager

    return UserPayoutEventManager(
        pg_config=thl_web_rw,
        permissions=[Permission.CREATE, Permission.READ],
        redis_config=thl_redis_config,
    )


@pytest.fixture(scope="session")
def brokerage_product_payout_event_manager(
    thl_web_rw: PostgresConfig, thl_redis_config: RedisConfig
) -> BrokerageProductPayoutEventManager:
    assert thl_web_rw.dsn.path
    assert "/unittest-" in thl_web_rw.dsn.path

    from generalresearch.managers.thl.payout import (
        BrokerageProductPayoutEventManager,
    )

    return BrokerageProductPayoutEventManager(
        pg_config=thl_web_rw,
        permissions=[Permission.CREATE, Permission.READ],
        redis_config=thl_redis_config,
    )


@pytest.fixture()
def audit_log_manager(thl_web_rw: PostgresConfig) -> AuditLogManager:
    from generalresearch.managers.thl.userhealth import AuditLogManager

    return AuditLogManager(pg_config=thl_web_rw)


@pytest.fixture(scope="session")
def business_payout_event_manager(
    thl_web_rw: PostgresConfig, thl_redis_config: RedisConfig
) -> BusinessPayoutEventManager:
    assert thl_web_rw.dsn.path
    assert "/unittest-" in thl_web_rw.dsn.path

    from generalresearch.managers.thl.payout import (
        BusinessPayoutEventManager,
    )

    return BusinessPayoutEventManager(
        pg_config=thl_web_rw,
        permissions=[Permission.CREATE, Permission.READ],
        redis_config=thl_redis_config,
    )


@pytest.fixture(scope="session")
def product_manager(thl_web_rw: PostgresConfig) -> ProductManager:
    assert thl_web_rw.dsn
    assert thl_web_rw.dsn.path
    assert "/unittest-" in thl_web_rw.dsn.path

    from generalresearch.managers.thl.product import ProductManager

    return ProductManager(pg_config=thl_web_rw)


@pytest.fixture(scope="session")
def user_manager(
    settings: GRLBaseSettings,
    thl_web_rw: PostgresConfig,
    thl_web_rr: PostgresConfig,
    thl_redis_config: RedisConfig,
) -> UserManager:
    assert thl_web_rw.dsn
    assert thl_web_rw.dsn.path
    assert thl_web_rr.dsn
    assert thl_web_rr.dsn.path
    assert "/unittest-" in thl_web_rw.dsn.path
    assert "/unittest-" in thl_web_rr.dsn.path

    from generalresearch.managers.thl.user_manager.rate_limit import UserManagerLimiter
    from generalresearch.managers.thl.user_manager.user_manager import (
        UserManager,
    )

    um = UserManager(
        pg_config=thl_web_rw,
        pg_config_rr=thl_web_rr,
        redis=settings.redis,
    )

    # rc = thl_redis_config.create_redis_client()
    um.user_manager_limiter = UserManagerLimiter(redis=thl_redis_config.dsn)

    return um


@pytest.fixture(scope="session")
def mysql_user_manager(thl_web_rw: PostgresConfig) -> MysqlUserManager:
    return MysqlUserManager(pg_config=thl_web_rw, is_read_replica=False)


@pytest.fixture(scope="session")
def redis_user_manager(thl_redis_config: RedisConfig) -> RedisUserManager:
    return RedisUserManager(redis_dsn=thl_redis_config)


@pytest.fixture(scope="session")
def user_metadata_manager(thl_web_rw: PostgresConfig) -> UserMetadataManager:
    assert thl_web_rw.dsn
    assert thl_web_rw.dsn.path
    assert "/unittest-" in thl_web_rw.dsn.path

    from generalresearch.managers.thl.user_manager.user_metadata_manager import (
        UserMetadataManager,
    )

    return UserMetadataManager(pg_config=thl_web_rw)


@pytest.fixture(scope="session")
def session_manager(thl_web_rw: PostgresConfig) -> SessionManager:
    assert thl_web_rw.dsn
    assert thl_web_rw.dsn.path
    assert "/unittest-" in thl_web_rw.dsn.path

    from generalresearch.managers.thl.session import SessionManager

    return SessionManager(pg_config=thl_web_rw)


@pytest.fixture(scope="session")
def wall_manager(thl_web_rw: PostgresConfig) -> WallManager:
    assert thl_web_rw.dsn
    assert thl_web_rw.dsn.path
    assert "/unittest-" in thl_web_rw.dsn.path

    from generalresearch.managers.thl.wall import WallManager

    return WallManager(pg_config=thl_web_rw)


@pytest.fixture(scope="session")
def wall_cache_manager(
    thl_web_rw: PostgresConfig, thl_redis_config: RedisConfig
) -> WallCacheManager:
    # assert "/unittest-" in thl_web_rw.dsn.path

    from generalresearch.managers.thl.wall import WallCacheManager

    return WallCacheManager(pg_config=thl_web_rw, redis_config=thl_redis_config)


@pytest.fixture(scope="session")
def task_adjustment_manager(thl_web_rw: PostgresConfig) -> TaskAdjustmentManager:
    # assert "/unittest-" in thl_web_rw.dsn.path

    from generalresearch.managers.thl.task_adjustment import (
        TaskAdjustmentManager,
    )

    return TaskAdjustmentManager(pg_config=thl_web_rw)


@pytest.fixture(scope="session")
def category_manager(thl_web_rw: PostgresConfig) -> CategoryManager:
    assert thl_web_rw.dsn
    assert thl_web_rw.dsn.path
    assert "/unittest-" in thl_web_rw.dsn.path
    from generalresearch.managers.thl.category import CategoryManager

    return CategoryManager(pg_config=thl_web_rw)


@pytest.fixture(scope="session")
def buyer_manager(thl_web_rw: PostgresConfig) -> BuyerManager:
    # assert "/unittest-" in thl_web_rw.dsn.path
    from generalresearch.managers.thl.buyer import BuyerManager

    return BuyerManager(pg_config=thl_web_rw)


@pytest.fixture(scope="session")
def survey_manager(thl_web_rw: PostgresConfig):
    # assert "/unittest-" in thl_web_rw.dsn.path
    from generalresearch.managers.thl.survey import SurveyManager

    return SurveyManager(pg_config=thl_web_rw)


@pytest.fixture(scope="session")
def surveystat_manager(thl_web_rw: PostgresConfig):
    # assert "/unittest-" in thl_web_rw.dsn.path
    from generalresearch.managers.thl.survey import SurveyStatManager

    return SurveyStatManager(pg_config=thl_web_rw)


@pytest.fixture(scope="session")
def surveypenalty_manager(thl_redis_config: RedisConfig):
    from generalresearch.managers.thl.survey_penalty import SurveyPenaltyManager

    return SurveyPenaltyManager(redis_config=thl_redis_config)


# --- IP Geolocation ---


@pytest.fixture
def ip_geoname_manager(thl_web_rw: PostgresConfig) -> IPGeonameManager:
    from generalresearch.managers.thl.ipinfo import IPGeonameManager

    return IPGeonameManager(pg_config=thl_web_rw)


# --- IP Information ---


@pytest.fixture(scope="session")
def ip_information_manager(thl_web_rw: PostgresConfig) -> IPInformationManager:
    assert thl_web_rw.dsn.path
    assert "/unittest-" in thl_web_rw.dsn.path

    from generalresearch.managers.thl.ipinfo import IPInformationManager

    return IPInformationManager(pg_config=thl_web_rw)


# --- IP Record ---


@pytest.fixture(scope="session")
def ip_record_manager(
    thl_web_rw: PostgresConfig, thl_redis_config: RedisConfig
) -> IPRecordManager:
    assert thl_web_rw.dsn.path
    assert "/unittest-" in thl_web_rw.dsn.path

    from generalresearch.managers.thl.userhealth import IPRecordManager

    return IPRecordManager(pg_config=thl_web_rw, redis_config=thl_redis_config)
