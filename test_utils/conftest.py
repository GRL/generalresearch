import os
import shutil
import sys
from datetime import datetime, timedelta, timezone
from os.path import join as pjoin
from pathlib import Path
from typing import TYPE_CHECKING, Callable, Generator
from uuid import uuid4

import django
import pytest
import redis
from _pytest.config import Config
from django.conf import settings as django_settings
from django.core.management import call_command
from dotenv import load_dotenv
from pydantic import MariaDBDsn, PostgresDsn
from redis import Redis

from generalresearch.pg_helper import PostgresConfig
from generalresearch.redis_helper import RedisConfig
from generalresearch.sql_helper import SqlHelper

if TYPE_CHECKING:
    from generalresearch.config import GRLBaseSettings
    from generalresearch.currency import USDCent
    from generalresearch.models.thl.session import Status


@pytest.fixture(scope="session")
def env_file_path(pytestconfig: Config) -> str:
    root_path = pytestconfig.rootpath
    env_file = ".env.test"

    candidates = [
        os.path.join(root_path, env_file),
        os.path.join(root_path, "..", env_file),
    ]

    for env_path in candidates:
        if os.path.exists(env_path):
            load_dotenv(dotenv_path=env_path, override=True)
            return os.path.normpath(env_path)

    raise AssertionError(f"No .env.test file found in: {', '.join(candidates)}")


@pytest.fixture(scope="session")
def settings(env_file_path: str) -> "GRLBaseSettings":
    from generalresearch.config import GRLBaseSettings

    print(f"{env_file_path=}")

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
def postgres_instance(settings: "GRLBaseSettings") -> Generator[PostgresDsn]:
    """Create a ephemeral postgresql instance for us to use during pytest.

    This is simplified, and only based off a single host. We don't want to
    create multiple migrated tmp databases for each rw/rr/ro connection
    """

    assert settings.thl_web_rw_db
    # assert settings.thl_web_rw_db.host

    dsn: PostgresDsn = settings.thl_web_rw_db

    # Connect to default DB to create the new one
    from psycopg import connect
    from psycopg.sql import SQL, Identifier

    now = datetime.now(timezone.utc)
    ts: str = now.strftime("%Y-%m-%d")

    db_name = f"unittest-{ts}-{uuid4().hex[:6]}"
    print("XXX", str(dsn))
    conn = connect(str(dsn))
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(SQL("CREATE DATABASE {}").format(Identifier(db_name)))
    cur.close()
    conn.close()

    host = dsn.hosts()[0]
    db_url = (
        f"postgres://{host['username']}:{host['password']}@{host['host']}/{db_name}"
    )

    yield PostgresDsn(db_url)

    # Teardown: drop the DB after the session
    conn = connect(str(dsn))
    conn.autocommit = True
    cur = conn.cursor()
    # cur.execute(SQL("DROP DATABASE {}").format(Identifier(db_name)))
    cur.close()
    conn.close()


@pytest.fixture(scope="session")
def django_db_setup(settings: "GRLBaseSettings") -> Callable[..., None]:

    def _inner():

        assert settings.thl_web_rw_db
        dsn: PostgresDsn = settings.thl_web_rw_db
        host = dsn.hosts()[0]

        # 1. Bootstrapping Django settings
        if not django_settings.configured:
            django_settings.configure(
                DATABASES={
                    "default": {
                        "ENGINE": "django.db.backends.postgresql",
                        # PostgresDsn stores path as "/dbname"
                        "NAME": str(dsn.path).lstrip("/"),
                        "USER": host["username"],
                        "PASSWORD": host["password"],
                        "HOST": host["host"],
                        "PORT": "5432",
                    }
                },
                INSTALLED_APPS=[
                    "django.contrib.postgres",
                    "django.contrib.contenttypes",
                    "generalresearch.thl_django",
                ],
            )
        django.setup()

        from django.apps import apps

        for model in apps.get_models():
            print(f"Discovered model: {model._meta.label}")

        # 2. Run migrations directly during fixture activation
        call_command("migrate")

    return _inner


@pytest.fixture(scope="session")
def thl_web_rr(
    settings: "GRLBaseSettings", postgres_instance: PostgresDsn, django_db_setup
) -> PostgresConfig:
    dsn = settings.thl_web_rr_db
    assert dsn
    assert dsn.path

    if dsn.path not in ["/", "/postgres"]:
        assert "/unittest-" in dsn.path

    db_path = postgres_instance.path
    host = dsn.hosts()[0]
    db_url = f"postgres://{host['username']}:{host['password']}@{host['host']}{db_path}"

    # Run Migrations now.
    django_db_setup()

    return PostgresConfig(
        dsn=PostgresDsn(db_url),
        connect_timeout=1,
        statement_timeout=5,
    )


@pytest.fixture(scope="session")
def thl_web_rw(
    settings: "GRLBaseSettings", postgres_instance: PostgresDsn, django_db_setup
) -> PostgresConfig:
    dsn = settings.thl_web_rw_db
    assert dsn
    assert dsn.path

    if dsn.path not in ["/", "/postgres"]:
        assert "/unittest-" in dsn.path

    db_path = postgres_instance.path
    host = dsn.hosts()[0]
    db_url = f"postgres://{host['username']}:{host['password']}@{host['host']}{db_path}"

    # Run Migrations now.
    django_db_setup()

    return PostgresConfig(
        dsn=PostgresDsn(db_url),
        connect_timeout=1,
        statement_timeout=5,
    )


@pytest.fixture(scope="session")
def gr_db(settings: "GRLBaseSettings") -> PostgresConfig:
    dsn = settings.gr_db
    assert dsn
    assert dsn.path

    if dsn.path not in ["/", "/postgres"]:
        assert "/unittest-" in dsn.path

    return PostgresConfig(dsn=settings.gr_db, connect_timeout=5, statement_timeout=2)


@pytest.fixture(scope="session")
def spectrum_rw(settings: "GRLBaseSettings") -> SqlHelper:
    dsn = settings.spectrum_rw_db
    assert dsn
    assert dsn.path

    if dsn.path not in ["/", "/postgres"]:
        assert "/unittest-" in dsn.path

    return SqlHelper(
        dsn=settings.spectrum_rw_db,
        read_timeout=2,
        write_timeout=1,
        connect_timeout=2,
    )


@pytest.fixture(scope="session")
def grliq_db(settings: "GRLBaseSettings") -> PostgresConfig:
    dsn = settings.grliq_db
    assert dsn
    assert dsn.path

    if dsn.path not in ["/", "/postgres"]:
        assert "/unittest-" in dsn.path

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
def utc_now() -> datetime:
    return datetime.now(tz=timezone.utc)


@pytest.fixture
def utc_hour_ago() -> datetime:
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
