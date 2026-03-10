from typing import Optional

from psycopg import Cursor

from generalresearch.managers.base import PostgresManager
from generalresearch.models.network.tool_run import RDnsRun


class RdnsManager(PostgresManager):

    def _create(self, run: RDnsRun, c: Optional[Cursor] = None) -> None:
        """
        Do not use this directly. Must only be used in the context of a toolrun
        """
        query = """
        INSERT INTO network_rdnsresult (
            run_id, primary_hostname, primary_domain,
            hostname_count, hostnames
        )
        VALUES (
            %(run_id)s, %(primary_hostname)s, %(primary_domain)s,
            %(hostname_count)s, %(hostnames)s
        );
        """
        params = run.model_dump_postgres()
        c.execute(query, params)