from typing import Optional

from psycopg import Cursor

from generalresearch.managers.base import PostgresManager
from generalresearch.models.network.tool_run import RDNSRun


class RDNSRunManager(PostgresManager):

    def _create(self, run: RDNSRun, c: Optional[Cursor] = None) -> None:
        """
        Do not use this directly. Must only be used in the context of a toolrun
        """
        query = """
        INSERT INTO network_rdnsresult (
            run_id, primary_hostname, primary_domain,
            hostname_count, hostnames,
            ip, started_at, scan_group_id
        )
        VALUES (
            %(run_id)s, %(primary_hostname)s, %(primary_domain)s,
            %(hostname_count)s, %(hostnames)s,
            %(ip)s, %(started_at)s, %(scan_group_id)s
        );
        """
        params = run.model_dump_postgres()
        if c:
            c.execute(query, params)
        else:
            with self.pg_config.make_connection() as conn:
                with conn.cursor() as c:
                    c.execute(query, params)
