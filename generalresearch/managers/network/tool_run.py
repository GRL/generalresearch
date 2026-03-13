from typing import Collection, List, Dict

from psycopg import Cursor, sql

from generalresearch.managers.base import PostgresManager, Permission

from generalresearch.managers.network.nmap import NmapRunManager
from generalresearch.managers.network.rdns import RDNSRunManager
from generalresearch.managers.network.mtr import MTRRunManager
from generalresearch.models.network.rdns.result import RDNSResult
from generalresearch.models.network.tool_run import (
    NmapRun,
    RDNSRun,
    MTRRun,
    ToolRun,
    ToolName,
)
from generalresearch.pg_helper import PostgresConfig


class ToolRunManager(PostgresManager):
    def __init__(
        self,
        pg_config: PostgresConfig,
        permissions: Collection[Permission] = None,
    ):
        super().__init__(pg_config=pg_config, permissions=permissions)
        self.nmap_manager = NmapRunManager(self.pg_config)
        self.rdns_manager = RDNSRunManager(self.pg_config)
        self.mtr_manager = MTRRunManager(self.pg_config)

    def _create_tool_run(self, run: NmapRun | RDNSRun | MTRRun, c: Cursor):
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

    def create_tool_run(self, run: NmapRun | RDNSRun | MTRRun):
        if type(run) is NmapRun:
            return self.create_nmap_run(run)
        elif type(run) is RDNSRun:
            return self.create_rdns_run(run)
        elif type(run) is MTRRun:
            return self.create_mtr_run(run)
        else:
            raise ValueError("unrecognized run type")

    def get_latest_runs_by_tool(self, ip: str) -> Dict[ToolName, ToolRun]:
        query = """
        SELECT DISTINCT ON (tool_name) *
        FROM network_toolrun
        WHERE ip = %(ip)s
        ORDER BY tool_name, started_at DESC;
        """
        params = {"ip": ip}
        res = self.pg_config.execute_sql_query(query, params=params)
        runs = [ToolRun.model_validate(x) for x in res]
        return {r.tool_name: r for r in runs}

    def create_nmap_run(self, run: NmapRun) -> NmapRun:
        """
        Insert a PortScan + PortScanPorts from a Pydantic NmapResult.
        """
        with self.pg_config.make_connection() as conn:
            with conn.cursor() as c:
                self._create_tool_run(run, c)
                self.nmap_manager._create(run, c=c)
        return run

    def get_nmap_run(self, id: int) -> NmapRun:
        query = """
        SELECT tr.*, np.parsed
        FROM network_toolrun tr
        JOIN network_portscan np ON tr.id = np.run_id
        WHERE id = %(id)s
        """
        params = {"id": id}
        res = self.pg_config.execute_sql_query(query, params)[0]
        return NmapRun.model_validate(res)

    def create_rdns_run(self, run: RDNSRun) -> RDNSRun:
        """
        Insert a RDnsRun + RDNSResult
        """
        with self.pg_config.make_connection() as conn:
            with conn.cursor() as c:
                self._create_tool_run(run, c)
                self.rdns_manager._create(run, c=c)
        return run

    def get_rdns_run(self, id: int) -> RDNSRun:
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
        return RDNSRun.model_validate(res)

    def create_mtr_run(self, run: MTRRun) -> MTRRun:
        with self.pg_config.make_connection() as conn:
            with conn.cursor() as c:
                self._create_tool_run(run, c)
                self.mtr_manager._create(run, c=c)
        return run

    def get_mtr_run(self, id: int) -> MTRRun:
        query = """
        SELECT tr.*, mtr.parsed, mtr.source_ip, mtr.facility_id
        FROM network_toolrun tr
        JOIN network_mtr mtr ON tr.id = mtr.run_id
        WHERE id = %(id)s
        """
        params = {"id": id}
        res = self.pg_config.execute_sql_query(query, params)[0]
        return MTRRun.model_validate(res)
