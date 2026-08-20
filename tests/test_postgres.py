import socket
import subprocess

from pydantic import PostgresDsn

from generalresearch.models.custom_types import InternalHostname
from generalresearch.pg_helper import PostgresConfig


def is_port_open(host: InternalHostname, port: int = 5432, timeout: int = 3):
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except (socket.timeout, ConnectionRefusedError, OSError):
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
