import socket
import subprocess
from collections.abc import Callable
from typing import TYPE_CHECKING

from pydantic import PostgresDsn

from generalresearch.pg_helper import PostgresConfig

if TYPE_CHECKING:
    from generalresearch.models.custom_types import InternalHostname, PostgresDict


def is_port_open(host: InternalHostname, port: int = 5432, timeout: int = 3):
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except (TimeoutError, ConnectionRefusedError, OSError):
        return False


def can_ping(host: InternalHostname):
    return (
        subprocess.call(
            ["ping", "-c", "1", str(host)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        == 0
    )


class TestPostgresDSN:

    def test_ping(self, postgres_instance_host: InternalHostname):
        assert can_ping(host=postgres_instance_host)

    def test_port(self, postgres_instance_host: InternalHostname):
        assert is_port_open(host=postgres_instance_host)

    def test_conn(self, postgres_instance: PostgresDsn):
        config = PostgresConfig(
            dsn=postgres_instance,
            connect_timeout=1,
            statement_timeout=1,
        )
        res = config.execute_sql_query(query="SELECT 1;")
        assert len(res) == 1


class TestPostgresDjangoCreation:

    def test_ping(self, postgres_instance_dict: PostgresDict):
        assert can_ping(host=postgres_instance_dict["host"])

    def test_django_creation(
        self,
        django_db_factory: Callable[..., None],
    ):

        dsn = django_db_factory()
        assert isinstance(dsn, PostgresDsn)

    def test_django_tables(self, thl_web_rw: PostgresConfig):
        res = thl_web_rw.execute_sql_query(query="""
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema = 'public';
        """)
        assert len(res) == 1
        assert res[0]["count"] == 56
