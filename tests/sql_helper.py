from uuid import uuid4

import pytest
from pydantic import MySQLDsn, MariaDBDsn, ValidationError


class TestSqlHelper:
    def test_db_property(self):
        from generalresearch.sql_helper import SqlHelper

        db_name = uuid4().hex[:8]
        dsn = MySQLDsn(f"mysql://root@localhost/{db_name}")
        instance = SqlHelper(dsn=dsn)

        assert instance.db == db_name
        assert instance.db_name == db_name
        assert instance.dbname == db_name

    def test_scheme(self):
        from generalresearch.sql_helper import SqlHelper

        dsn = MySQLDsn(f"mysql://root@localhost/test")
        instance = SqlHelper(dsn=dsn)
        assert instance.is_mysql()

        # This needs psycopg2 installed, and don't need to make this a
        #   requirement of the package ... todo?
        # dsn = PostgresDsn(f"postgres://root@localhost/test")
        # instance = SqlHelper(dsn=dsn)
        # self.assertTrue(instance.is_postgresql())

        with pytest.raises(ValidationError):
            SqlHelper(dsn=MariaDBDsn(f"maria://root@localhost/test"))

    def test_row_decode(self):
        from generalresearch.sql_helper import decode_uuids

        valid_uuid4_1 = "bf432839fd0d4436ab1581af5eb98f26"
        valid_uuid4_2 = "e1d8683b9c014e9d80eb120c2fc95288"
        invalid_uuid4_2 = "2f3b9edf5a3da6198717b77604775ec1"

        row1 = {
            "b": valid_uuid4_1,
            "c": valid_uuid4_2,
        }

        row2 = {
            "a": valid_uuid4_1,
            "b": invalid_uuid4_2,
        }

        assert row1 == decode_uuids(row1)
        assert row1 != decode_uuids(row2)
