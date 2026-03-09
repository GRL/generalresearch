import os
import shutil
from os.path import join as pjoin
from pathlib import Path
from typing import TYPE_CHECKING, Callable
from uuid import uuid4

import pytest
import redis
from _pytest.config import Config
from dotenv import load_dotenv
from pydantic import MariaDBDsn
from redis import Redis

from generalresearch.pg_helper import PostgresConfig
from generalresearch.redis_helper import RedisConfig
from generalresearch.sql_helper import SqlHelper

if TYPE_CHECKING:
    from datetime import datetime

    from generalresearch.config import GRLBaseSettings
    from generalresearch.currency import USDCent
    from generalresearch.models.thl.session import Status


@pytest.fixture(scope="session")
def env_file_path(pytestconfig: Config) -> str:
    root_path = pytestconfig.rootpath
    env_path = os.path.join(root_path, ".env.test")

    if os.path.exists(env_path):
        load_dotenv(dotenv_path=env_path, override=True)

    return env_path


@pytest.fixture(scope="session")
def settings(env_file_path: str) -> "GRLBaseSettings":
    from generalresearch.config import GRLBaseSettings

    s = GRLBaseSettings(_env_file=env_file_path)

    if s.thl_mkpl_rr_db is not None:
        if s.spectrum_rw_db is None:
            s.spectrum_rw_db = MariaDBDsn(f"{s.thl_mkpl_rw_db}unittest-thl-spectrum")
        if s.spectrum_rr_db is None:
            s.spectrum_rr_db = MariaDBDsn(f"{s.thl_mkpl_rr_db}unittest-thl-spectrum")

    s.mnt_gr_api_dir = pjoin("/tmp", f"test-{uuid4().hex[:12]}")

    return s


# === Database Connectors ===


@pytest.fixture(scope="session")
def thl_web_rr(settings: "GRLBaseSettings") -> PostgresConfig:
    assert settings.thl_web_rr_db is not None
    assert "/unittest-" in settings.thl_web_rr_db.path

    return PostgresConfig(
        dsn=settings.thl_web_rr_db,
        connect_timeout=1,
        statement_timeout=5,
    )


@pytest.fixture(scope="session")
def thl_web_rw(settings: "GRLBaseSettings") -> PostgresConfig:
    assert settings.thl_web_rw_db is not None
    assert "/unittest-" in settings.thl_web_rw_db.path

    return PostgresConfig(
        dsn=settings.thl_web_rw_db,
        connect_timeout=1,
        statement_timeout=5,
    )


@pytest.fixture(scope="session")
def gr_db(settings: "GRLBaseSettings") -> PostgresConfig:
    assert "/unittest-" in settings.gr_db.path
    return PostgresConfig(dsn=settings.gr_db, connect_timeout=5, statement_timeout=2)


@pytest.fixture(scope="session")
def spectrum_rw(settings: "GRLBaseSettings") -> SqlHelper:
    assert settings.spectrum_rw_db is not None
    assert "/unittest-" in settings.spectrum_rw_db.path

    return SqlHelper(
        dsn=settings.spectrum_rw_db,
        read_timeout=2,
        write_timeout=1,
        connect_timeout=2,
    )


@pytest.fixture(scope="session")
def grliq_db(settings: "GRLBaseSettings") -> PostgresConfig:
    assert settings.grliq_db is not None
    assert "/unittest-" in settings.grliq_db.path

    # test_words = {"localhost", "127.0.0.1", "unittest", "grliq-test"}
    # assert any(w in str(postgres_config.dsn) for w in test_words), "check grliq postgres_config"
    # assert "grliqdeceezpocymo" not in str(postgres_config.dsn), "check grliq postgres_config"

    return PostgresConfig(
        dsn=settings.grliq_db,
        connect_timeout=2,
        statement_timeout=2,
    )


@pytest.fixture(scope="session")
def thl_redis(settings: "GRLBaseSettings") -> "Redis":
    # todo: this should get replaced with redisconfig (in most places)
    # I'm not sure where this would be? in the domain name?
    assert "unittest" in str(settings.thl_redis) or "127.0.0.1" in str(
        settings.thl_redis
    )

    return redis.Redis.from_url(
        **{
            "url": str(settings.thl_redis),
            "decode_responses": True,
            "socket_timeout": settings.redis_timeout,
            "socket_connect_timeout": settings.redis_timeout,
        }
    )


@pytest.fixture(scope="session")
def thl_redis_config(settings: "GRLBaseSettings") -> RedisConfig:
    assert "unittest" in str(settings.thl_redis) or "127.0.0.1" in str(
        settings.thl_redis
    )
    return RedisConfig(
        dsn=settings.thl_redis,
        decode_responses=True,
        socket_timeout=settings.redis_timeout,
        socket_connect_timeout=settings.redis_timeout,
    )


@pytest.fixture(scope="session")
def gr_redis_config(settings: "GRLBaseSettings") -> "RedisConfig":
    assert "unittest" in str(settings.gr_redis) or "127.0.0.1" in str(settings.gr_redis)

    return RedisConfig(
        dsn=settings.gr_redis,
        decode_responses=True,
        socket_timeout=settings.redis_timeout,
        socket_connect_timeout=settings.redis_timeout,
    )


@pytest.fixture(scope="session")
def gr_redis(settings: "GRLBaseSettings") -> "Redis":
    assert "unittest" in str(settings.gr_redis) or "127.0.0.1" in str(settings.gr_redis)
    return redis.Redis.from_url(
        **{
            "url": str(settings.gr_redis),
            "decode_responses": True,
            "socket_timeout": settings.redis_timeout,
            "socket_connect_timeout": settings.redis_timeout,
        }
    )


@pytest.fixture
def gr_redis_async(settings: "GRLBaseSettings"):
    assert "unittest" in str(settings.gr_redis) or "127.0.0.1" in str(settings.gr_redis)

    import redis.asyncio as redis_async

    return redis_async.Redis.from_url(
        str(settings.gr_redis),
        decode_responses=True,
        socket_timeout=0.20,
        socket_connect_timeout=0.20,
    )


# === Random helpers ===


@pytest.fixture
def start() -> "datetime":
    from datetime import datetime, timezone

    return datetime(year=1900, month=1, day=1, tzinfo=timezone.utc)


@pytest.fixture
def wall_status(request) -> "Status":
    from generalresearch.models.thl.session import Status

    return request.param if hasattr(request, "wall_status") else Status.COMPLETE


@pytest.fixture
def utc_now() -> "datetime":
    from datetime import datetime, timezone

    return datetime.now(tz=timezone.utc)


@pytest.fixture
def utc_hour_ago() -> "datetime":
    from datetime import datetime, timedelta, timezone

    return datetime.now(tz=timezone.utc) - timedelta(hours=1)


@pytest.fixture
def utc_day_ago() -> "datetime":
    from datetime import datetime, timedelta, timezone

    return datetime.now(tz=timezone.utc) - timedelta(hours=24)


@pytest.fixture
def utc_90days_ago() -> "datetime":
    from datetime import datetime, timedelta, timezone

    return datetime.now(tz=timezone.utc) - timedelta(days=90)


@pytest.fixture
def utc_60days_ago() -> "datetime":
    from datetime import datetime, timedelta, timezone

    return datetime.now(tz=timezone.utc) - timedelta(days=60)


@pytest.fixture
def utc_30days_ago() -> "datetime":
    from datetime import datetime, timedelta, timezone

    return datetime.now(tz=timezone.utc) - timedelta(days=30)


# === Clean up ===


@pytest.fixture(scope="function")
def delete_df_collection(
    thl_web_rw: PostgresConfig, create_main_accounts: Callable[..., None]
) -> Callable[..., None]:

    from generalresearch.incite.collections import (
        DFCollection,
        DFCollectionType,
    )

    def _inner(coll: "DFCollection"):
        match coll.data_type:
            case DFCollectionType.LEDGER:
                for table in [
                    "ledger_transactionmetadata",
                    "ledger_entry",
                    "ledger_transaction",
                    "ledger_account",
                ]:
                    thl_web_rw.execute_write(
                        query=f"DELETE FROM {table};",
                    )
                create_main_accounts()

            case DFCollectionType.WALL | DFCollectionType.SESSION:
                with thl_web_rw.make_connection() as conn:
                    with conn.cursor() as c:
                        c.execute("SET CONSTRAINTS ALL DEFERRED")
                        for table in [
                            "thl_wall",
                            "thl_session",
                        ]:
                            c.execute(
                                query=f"DELETE FROM {table};",
                            )

            case DFCollectionType.USER:
                for table in ["thl_usermetadata", "thl_user"]:
                    thl_web_rw.execute_write(
                        query=f"DELETE FROM {table};",
                    )

            case _:
                thl_web_rw.execute_write(
                    query=f"DELETE FROM {coll.data_type.value};",
                )

    return _inner


# === GR Related ===


@pytest.fixture(scope="function")
def amount_1(request) -> "USDCent":
    from generalresearch.currency import USDCent

    return USDCent(1)


@pytest.fixture(scope="function")
def amount_100(request) -> "USDCent":
    from generalresearch.currency import USDCent

    return USDCent(100)


def clear_directory(path: Path):
    for entry in os.listdir(path):
        full_path = os.path.join(path, entry)
        if os.path.isfile(full_path) or os.path.islink(full_path):
            os.unlink(full_path)  # remove file or symlink
        elif os.path.isdir(full_path):
            shutil.rmtree(full_path)  # remove folder
