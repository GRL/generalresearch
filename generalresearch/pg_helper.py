from datetime import timezone
from typing import Optional

import psycopg
from psycopg.adapt import Buffer
from psycopg.rows import RowFactory, dict_row
from psycopg.types.datetime import TimestampLoader
from psycopg.types.net import Address, InetLoader, Interface
from psycopg.types.string import TextLoader
from psycopg.types.uuid import UUIDLoader
from pydantic import PostgresDsn


class UUIDHexLoader(UUIDLoader):
    def load(self, data):
        value = super().load(data)
        return value.hex


class UTCTimestampLoader(TimestampLoader):
    def load(self, data):
        dt = super().load(data)
        if dt is None:
            return None
        assert dt.tzinfo is None, "expected naive dt"
        return dt.replace(tzinfo=timezone.utc)


class BPCharLoader(TextLoader):
    def load(self, data):
        data = super().load(data)
        if data is None:
            return None
        if type(data) is bytes:
            return data.decode("utf-8").rstrip(" ")
        else:
            return data.rstrip(" ")


class InetHostLoader(InetLoader):
    def load(self, data):
        data = super().load(data)
        if data is None:
            return None
        return str(data.exploded).split("/")[0]


class PostgresConfig:
    def __init__(
        self,
        dsn: PostgresDsn,
        connect_timeout: int,
        statement_timeout: float,
        schema: Optional[str] = None,
        row_factory: RowFactory = dict_row,
    ):
        """
        Hold configuration to enable postgres operations.

        :param dsn: See https://www.postgresql.org/docs/current/libpq-connect.html
        For timeouts and other options, see:
          https://www.postgresql.org/docs/current/runtime-config-client.html
        :param connect_timeout: (seconds) Maximum time to wait while connecting.
        :param statement_timeout: (seconds) Abort any statement that takes more than the specified amount of time.

        # Note, there is no read/write timeout. See idle_in_transaction_session_timeout, lock_timeout, etc.
        # There is also transaction_timeout also, but is only in the latest version? and I'm not sure the difference.
        """
        self.dsn = dsn
        self.connect_timeout = connect_timeout
        self.statement_timeout = statement_timeout
        self.schema = schema or dsn.path.lstrip("/")
        assert 0 < connect_timeout < 130, "connect_timeout should be in seconds"
        self.row_factory = row_factory

    @property
    def db(self):
        return self.dsn.path[1:]

    def make_connection(self) -> psycopg.Connection:
        options = [
            f"-c statement_timeout={round(self.statement_timeout*1000)}",
            "-c timezone=UTC",
            "-c client_encoding=UTF8",
        ]
        if self.schema:
            options.append(f"-c search_path={self.schema},public")
        options_str = " ".join(options)
        conn = psycopg.connect(
            str(self.dsn),
            connect_timeout=self.connect_timeout,
            options=options_str,
            row_factory=self.row_factory,
        )
        conn.adapters.register_loader("uuid", UUIDHexLoader)
        conn.adapters.register_loader("timestamp", UTCTimestampLoader)
        conn.adapters.register_loader("bpchar", BPCharLoader)
        conn.adapters.register_loader("inet", InetHostLoader)
        return conn

    def execute_sql_query(self, query, params=None):
        # This is only intended for SELECT queries
        assert "SELECT" in query.upper(), "Supports SELECTs only"

        with self.make_connection() as conn:
            with conn.cursor() as c:
                c.execute(query=query, params=params)
                return c.fetchall()

    def execute_write(self, query, params=None) -> int:
        cmd = query.lstrip().upper()
        assert (
            cmd.startswith("INSERT")
            or cmd.startswith("UPDATE")
            or cmd.startswith("DELETE")
        ), "Supports INSERT/UPDATE only"

        with self.make_connection() as conn:
            with conn.cursor() as c:
                c.execute(query=query, params=params)
                rowcount = c.rowcount
            conn.commit()
        return rowcount
