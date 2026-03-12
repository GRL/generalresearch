from typing import Optional

from psycopg import Cursor, sql

from generalresearch.managers.base import PostgresManager
from generalresearch.models.network.tool_run import NmapRun


class NmapRunManager(PostgresManager):

    def _create(self, run: NmapRun, c: Optional[Cursor] = None) -> None:
        """
        Insert a PortScan + PortScanPorts from a Pydantic NmapResult.
        Do not use this directly. Must only be used in the context of a toolrun
        """
        query = sql.SQL(
            """
        INSERT INTO network_portscan (
            run_id, xml_version, host_state,
            host_state_reason, latency_ms, distance,
            uptime_seconds, last_boot, raw_xml,
            parsed, scan_group_id, open_tcp_ports,
            started_at, ip
        )
        VALUES (
            %(run_id)s, %(xml_version)s, %(host_state)s,
            %(host_state_reason)s, %(latency_ms)s, %(distance)s,
            %(uptime_seconds)s, %(last_boot)s, %(raw_xml)s,
            %(parsed)s, %(scan_group_id)s, %(open_tcp_ports)s,
            %(started_at)s, %(ip)s
        );
        """
        )
        params = run.model_dump_postgres()

        query_ports = sql.SQL(
            """
        INSERT INTO network_portscanport (
            port_scan_id, protocol, port,
            state, reason, reason_ttl,
            service_name
        ) VALUES (
            %(port_scan_id)s, %(protocol)s, %(port)s,
            %(state)s, %(reason)s, %(reason_ttl)s,
            %(service_name)s
        )
        """
        )
        nmap_run = run.parsed
        params_ports = [p.model_dump_postgres(run_id=run.id) for p in nmap_run.ports]

        if c:
            c.execute(query, params)
            if nmap_run.ports:
                c.executemany(query_ports, params_ports)
        else:
            with self.pg_config.make_connection() as conn:
                with conn.cursor() as c:
                    c.execute(query, params)
                    if nmap_run.ports:
                        c.executemany(query_ports, params_ports)

        return None
