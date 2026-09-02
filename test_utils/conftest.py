from __future__ import annotations

import os
import shutil
import stat
import subprocess
import sys
from collections.abc import Callable, Generator
from datetime import UTC, datetime, timedelta
from os.path import join as pjoin
from pathlib import Path
from typing import TYPE_CHECKING
from uuid import uuid4

import pytest
from _pytest.config import Config
from dotenv import load_dotenv
from pydantic import MariaDBDsn, PostgresDsn, TypeAdapter
from pytest import FixtureRequest, TempPathFactory

from generalresearch.currency import USDCent
from generalresearch.models.custom_types import InternalHostname, PostgresDict
from generalresearch.sql_helper import SqlHelper

if TYPE_CHECKING:
    from generalresearch.config import GRLBaseSettings
    from generalresearch.pg_helper import PostgresConfig


@pytest.fixture(scope="session")
def env_file_path(pytestconfig: Config) -> Path:
    root_path = pytestconfig.rootpath
    env_file = ".env.test"

    candidates = [
        os.path.join(root_path, env_file),
        os.path.join(root_path, "..", env_file),
    ]

    for env_path in candidates:
        if os.path.exists(env_path):
            load_dotenv(dotenv_path=env_path, override=True)
            return Path(os.path.normpath(env_path))

    raise AssertionError(f"No .env.test file found in: {', '.join(candidates)}")


@pytest.fixture(scope="session")
def settings(env_file_path: Path) -> GRLBaseSettings:
    from generalresearch.config import GRLBaseSettings

    s = GRLBaseSettings()

    if s.thl_mkpl_rr_db is not None:
        if s.spectrum_rw_db is None:
            s.spectrum_rw_db = MariaDBDsn(f"{s.thl_mkpl_rw_db}unittest-thl-spectrum")
        if s.spectrum_rr_db is None:
            s.spectrum_rr_db = MariaDBDsn(f"{s.thl_mkpl_rr_db}unittest-thl-spectrum")

    s.mnt_gr_api_dir = pjoin("/tmp", f"test-{uuid4().hex[:12]}")

    return s


# === Database Connectors ===


@pytest.fixture(scope="session")
def postgres_instance(settings: GRLBaseSettings) -> Generator[PostgresDsn]:
    """Create a ephemeral postgresql instance for us to use during pytest.

    This does not create any tables, or schema definitions within the instance.
    What this does is simply:

        1. Create a database on a known, consistent, staging or unittest
            defined Postgres server.

        2. Return the PostgresDsn of that table

        3. On shutdown, go ahead and delete that database after the
            tests have finished.
    """

    msg = "Must define Postgres test settings"
    assert settings.testing_postgres, msg
    assert settings.testing_postgres_user, msg
    assert settings.testing_postgres_pass, msg

    db_uri, db_user, db_pass = (
        settings.testing_postgres,
        settings.testing_postgres_user,
        settings.testing_postgres_pass,
    )

    # Connect to default DB to create the new one
    from psycopg import connect
    from psycopg.sql import SQL, Identifier

    now = datetime.now(UTC)
    ts: str = now.strftime("%Y-%m-%d")
    db_name = f"unittest-{ts}-{uuid4().hex[:6]}"

    db_path_connect = f"postgres://{db_user}:{db_pass}@{db_uri}"
    db_path = f"{db_path_connect}/{db_name}"

    # The DATABASE does NOT yet exist on the Postgres SERVER, thus
    # we first must connect only to the SERVER (eg: default postgres path used)
    conn = connect(f"{db_path_connect}/postgres")
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(SQL("CREATE DATABASE {}").format(Identifier(db_name)))
    cur.close()
    conn.close()

    yield PostgresDsn(db_path)

    # Teardown: drop the DB after the session
    conn = connect(f"{db_path_connect}/postgres")
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(SQL("DROP DATABASE {} WITH (FORCE)").format(Identifier(db_name)))
    cur.close()
    conn.close()


@pytest.fixture(scope="session")
def postgres_instance_dict(
    postgres_instance: PostgresDsn,
) -> Generator[PostgresDict]:
    host = postgres_instance.hosts()[0]
    assert host is not None

    msg = "Must have full Postgres details"
    assert host["host"], msg
    assert host["username"], msg
    assert host["password"], msg

    assert postgres_instance.path

    yield PostgresDict(
        username=host["username"],
        password=host["password"],
        host=host["host"],
        name=postgres_instance.path.lstrip("/"),
        port=5432,
    )


@pytest.fixture(scope="session")
def postgres_instance_host(
    postgres_instance_dict: PostgresDict,
) -> Generator[InternalHostname]:
    adapter = TypeAdapter(InternalHostname)
    value = adapter.validate_python(postgres_instance_dict["host"])
    yield value


@pytest.fixture(scope="session")
def git_key_path(
    tmp_path_factory: TempPathFactory,
    settings: GRLBaseSettings,
) -> Generator[Path]:
    # We are using the tmp_path_factory because unlike the tmp_path (which
    # is function scoped), this is session scoped.

    assert settings.git_creds, "Must define key to download alternative models"
    fn = tmp_path_factory.mktemp("keys") / "git_creds"
    key_content = settings.git_creds.replace("\\n", "\n")
    fn.write_text(key_content, encoding="utf-8")
    os.chmod(fn, stat.S_IRUSR | stat.S_IWUSR)

    yield Path(fn)

    os.unlink(fn)


@pytest.fixture(scope="session")
def gr_repo(
    git_key_path: Path,
    tmp_path_factory: TempPathFactory,
) -> Callable[..., Path | None]:
    repo_url = "ssh://code.g-r-l.com:6611/general-research/gr-carer.git"

    _ran = {}

    fn = tmp_path_factory.mktemp("repos")
    repo_path = fn / "gr-carer"

    def _inner() -> Path:

        if _ran.get(repo_url, False):
            print(f"Already ran django_db_factory.{repo_url}")
            return repo_path

        _ran[repo_url] = True

        ssh_cmd = (
            f'ssh -i "{git_key_path}" '
            "-o IdentitiesOnly=yes "
            "-o StrictHostKeyChecking=no "
        )
        env = {**os.environ, "GIT_SSH_COMMAND": ssh_cmd}

        if repo_path.exists():
            subprocess.run(["git", "-C", str(repo_path), "pull"], check=True, env=env)
        else:
            subprocess.run(
                ["git", "clone", "--depth", "1", repo_url, str(repo_path)],
                check=True,
                env=env,
            )

        return repo_path

    return _inner


@pytest.fixture(scope="session")
def django_settings_file(
    postgres_instance_dict: PostgresDict,
) -> Callable[..., Path]:

    def _inner(
        settings_dir: Path, extra_installed_apps: list[str] | None = None
    ) -> Path:
        installed_apps = [
            "django.contrib.postgres",
            "django.contrib.contenttypes",
        ] + (extra_installed_apps or [])
        """
            This returns the directory path of where the settings file is in,
            not the path of the settings file itself
        """

        settings_content = f"""DATABASES = {{
    "default": {{
        "ENGINE": "django.db.backends.postgresql",
        "NAME": {postgres_instance_dict["name"]!r},
        "USER": {postgres_instance_dict["username"]!r},
        "PASSWORD": {postgres_instance_dict["password"]!r},
        "HOST": {postgres_instance_dict["host"]!r},
        "PORT": {postgres_instance_dict["port"]!r},
    }}
}}
INSTALLED_APPS = {installed_apps!r}
DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"
LANGUAGE_CODE = "en-us"
TIME_ZONE = "UTC"
USE_I18N = True
USE_L10N = True
USE_TZ = True
"""
        settings_file_path = settings_dir / "test_settings.py"
        settings_file_path.unlink(missing_ok=True)
        settings_file_path.write_text(settings_content)

        return settings_dir

    return _inner


@pytest.fixture(scope="session")
def django_db_factory(
    request: FixtureRequest,
    postgres_instance: PostgresDsn,
    gr_repo: Callable[..., Path],
    django_settings_file: Callable[..., Path],
    postgres_instance_dict: PostgresDict,
    tmp_path_factory: TempPathFactory,
) -> Callable[..., PostgresDsn | None]:

    _ran = {}

    def _inner(
        django_project: str = "generalresearch.thl_django",
    ) -> PostgresDsn | None:

        if _ran.get(django_project, False):
            print(f"Already ran django_db_factory:{django_project}")
            return postgres_instance
        _ran[django_project] = True

        # This is the generalresearch project root path, it's
        #   1 directory up from test_utils/, or tests/
        base_dir = Path(request.config.rootpath).parent

        if django_project == "generalresearch.thl_django":
            _cwd = base_dir
            _manage_path = "generalresearch.thl_django.app.manage"
            _settings_dir = base_dir / "generalresearch/thl_django/app"
            _settings_module = "generalresearch.thl_django.app.test_settings"
            django_settings_file(
                settings_dir=_settings_dir,
                extra_installed_apps=[
                    "generalresearch.thl_django",
                ],
            )

        elif django_project == "gr.common":
            _cwd = gr_repo()
            _manage_path = "gr.app.manage"
            _settings_dir = gr_repo() / "gr/app"
            _settings_module = "gr.app.test_settings"
            django_settings_file(
                settings_dir=_settings_dir, extra_installed_apps=["gr.common"]
            )

        else:
            raise ValueError("Not implemented yet.")

        assert _settings_dir

        env = {"DJANGO_SETTINGS_MODULE": str(_settings_module)}
        res1 = subprocess.run(
            [
                sys.executable,
                "-m",
                _manage_path,
                "makemigrations",
                f"--settings={_settings_module}",
            ],
            cwd=str(_cwd),
            env=env,
            capture_output=True,
            text=True,
            check=True,
        )

        if res1.returncode != 0:
            print("STDOUT:", res1.stdout)
            print("STDERR:", res1.stderr)
        res1.check_returncode()

        res2 = subprocess.run(
            [
                sys.executable,
                "-m",
                _manage_path,
                "migrate",
                f"--settings={_settings_module}",
            ],
            env=env,
            cwd=str(_cwd),
            capture_output=True,
            text=True,
            check=True,
        )

        if res2.returncode != 0:
            print("STDOUT:", res2.stdout)
            print("STDERR:", res2.stderr)
        res2.check_returncode()

        # 3. Return the Dsn so the factory gives a way to connect
        return postgres_instance

    return _inner


@pytest.fixture(scope="session")
def spectrum_rw(settings: GRLBaseSettings) -> SqlHelper:
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


# === Random helpers ===


@pytest.fixture
def start() -> datetime:
    return datetime(year=1900, month=1, day=1, tzinfo=UTC)


@pytest.fixture
def utc_now() -> datetime:
    return datetime.now(tz=UTC)


@pytest.fixture
def utc_hour_ago() -> datetime:
    return datetime.now(tz=UTC) - timedelta(hours=1)


@pytest.fixture
def utc_day_ago() -> datetime:
    return datetime.now(tz=UTC) - timedelta(hours=24)


@pytest.fixture
def utc_90days_ago() -> datetime:
    return datetime.now(tz=UTC) - timedelta(days=90)


@pytest.fixture
def utc_60days_ago() -> datetime:
    return datetime.now(tz=UTC) - timedelta(days=60)


@pytest.fixture
def utc_30days_ago() -> datetime:
    return datetime.now(tz=UTC) - timedelta(days=30)


# === Clean up ===


@pytest.fixture(scope="function")
def delete_df_collection(
    thl_web_rw: PostgresConfig, create_main_accounts: Callable[..., None]
) -> Callable[..., None]:

    from generalresearch.incite.collections.base import (
        DFCollection,
        DFCollectionType,
    )

    def _inner(coll: DFCollection):
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
                with thl_web_rw.make_connection() as conn, conn.cursor() as c:
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
                assert coll.data_type

                thl_web_rw.execute_write(
                    query=f"DELETE FROM {coll.data_type.value};",
                )

    return _inner


# === GR Related ===


@pytest.fixture(scope="function")
def amount_1() -> USDCent:
    return USDCent(1)


@pytest.fixture(scope="function")
def amount_100() -> USDCent:
    return USDCent(100)


def clear_directory(path: Path | str):
    dir_path = Path(path)

    for entry in os.listdir(dir_path):

        full_path = os.path.join(path, entry)
        if os.path.isfile(full_path) or os.path.islink(full_path):
            os.unlink(full_path)  # remove file or symlink

        elif os.path.isdir(full_path):
            shutil.rmtree(full_path)  # remove folder
