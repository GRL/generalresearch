from typing import Optional

from psycopg import Cursor

from generalresearch.managers.base import PostgresManager
from generalresearch.models.network.tool_run import MtrRun


class MtrManager(PostgresManager):

    def _create(self, run: MtrRun, c: Optional[Cursor] = None) -> None:
        """
        Do not use this directly. Must only be used in the context of a toolrun
        """
        query = """
        INSERT INTO network_mtr (
            run_id, source_ip, facility_id,
            protocol, port, parsed
        )
        VALUES (
            %(run_id)s, %(source_ip)s, %(facility_id)s,
            %(protocol)s, %(port)s, %(parsed)s
        );
        """
        params = run.model_dump_postgres()

        query_hops = """
        INSERT INTO network_mtrhop (
            hop, ip, domain, asn, mtr_run_id
        ) VALUES (
            %(hop)s, %(ip)s, %(domain)s,
            %(asn)s, %(mtr_run_id)s
        )
        """
        mtr_run = run.parsed
        params_hops = [h.model_dump_postgres(run_id=run.id) for h in mtr_run.hops]

        if c:
            c.execute(query, params)
            if params_hops:
                c.executemany(query_hops, params_hops)
        else:
            with self.pg_config.make_connection() as conn:
                with conn.cursor() as c:
                    c.execute(query, params)
                    if params_hops:
                        c.executemany(query_hops, params_hops)
