from typing import Collection

from psycopg import Cursor, sql

from generalresearch.managers.base import PostgresManager, Permission
from generalresearch.models.network.rdns import RDNSResult
from generalresearch.models.network.tool_run import ToolRun, PortScanRun, RDnsRun
from generalresearch.managers.network.nmap import NmapManager
from generalresearch.managers.network.rdns import RdnsManager
from generalresearch.pg_helper import PostgresConfig


class ToolRunManager(PostgresManager):
    def __init__(
        self,
        pg_config: PostgresConfig,
        permissions: Collection[Permission] = None,
    ):
        super().__init__(pg_config=pg_config, permissions=permissions)
        self.nmap_manager = NmapManager(self.pg_config)
        self.rdns_manager = RdnsManager(self.pg_config)

    def create_tool_run(self, run: PortScanRun | RDnsRun, c: Cursor):
        query = sql.SQL(
            """
        INSERT INTO network_toolrun (
            ip, scan_group_id, tool_class,
            tool_name, tool_version, started_at,
            finished_at, status, raw_command,
            config
        )
        VALUES (
            %(ip)s, %(scan_group_id)s, %(tool_class)s,
            %(tool_name)s, %(tool_version)s, %(started_at)s,
            %(finished_at)s, %(status)s, %(raw_command)s,
            %(config)s
        ) RETURNING id;
        """
        )
        params = run.model_dump_postgres()
        c.execute(query, params)
        run_id = c.fetchone()["id"]
        run.id = run_id
        return None

    def create_portscan_run(self, run: PortScanRun) -> PortScanRun:
        """
        Insert a PortScan + PortScanPorts from a Pydantic NmapRun.
        """
        with self.pg_config.make_connection() as conn:
            with conn.cursor() as c:
                self.create_tool_run(run, c)
                self.nmap_manager._create(run, c=c)
        return run

    def get_portscan_run(self, id: int) -> PortScanRun:
        query = """
        SELECT tr.*, np.parsed
        FROM network_toolrun tr
        JOIN network_portscan np ON tr.id = np.run_id
        WHERE id = %(id)s
        """
        params = {"id": id}
        res = self.pg_config.execute_sql_query(query, params)[0]
        return PortScanRun.model_validate(res)

    def create_rdns_run(self, run: RDnsRun) -> RDnsRun:
        """
        Insert a RDnsRun + RDNSResult
        """
        with self.pg_config.make_connection() as conn:
            with conn.cursor() as c:
                self.create_tool_run(run, c)
                self.rdns_manager._create(run, c=c)
        return run

    def get_rdns_run(self, id: int) -> RDnsRun:
        query = """
        SELECT tr.*, hostnames
        FROM network_toolrun tr
        JOIN network_rdnsresult np ON tr.id = np.run_id
        WHERE id = %(id)s
        """
        params = {"id": id}
        res = self.pg_config.execute_sql_query(query, params)[0]
        parsed = RDNSResult.model_validate(
            {"ip": res["ip"], "hostnames": res["hostnames"]}
        )
        res["parsed"] = parsed
        return RDnsRun.model_validate(res)
