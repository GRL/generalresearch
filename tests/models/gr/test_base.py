from typing import Callable

from pydantic import PostgresDsn

from generalresearch.pg_helper import PostgresConfig


class TestGRPostgresDjangoCreation:

    def test_django_creation(
        self,
        django_db_factory: Callable[..., None],
    ):

        dsn = django_db_factory("gr_carer")
        assert isinstance(dsn, PostgresDsn)

    def test_django_tables(self, thl_web_rw: PostgresConfig):
        res = thl_web_rw.execute_sql_query(query="""
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema = 'public';
        """)
        assert len(res) == 1
        assert res[0]["count"] == 56
