import logging
from typing import Any, Dict, List, Optional, Tuple, Union
from uuid import UUID

from pydantic import MariaDBDsn, MySQLDsn, PostgresDsn
from pymysql import Connection

ListOrTupleOfStrings = Union[List[str], Tuple[str, ...]]
ListOrTupleOfListOrTuple = Union[
    List[List], List[Tuple], Tuple[List, ...], Tuple[Tuple, ...]
]

DataBaseDsn = Union[MySQLDsn, MariaDBDsn, PostgresDsn]


class MultipleObjectsReturned(Exception):
    pass


class SqlConnector:
    """
    SqlConnector is GRL's simplified SQLAlchemy.. it's basic, it's raw
    it does whatever want. Maybe we overwrite this to just use SQLAlchemy
    on the backend... but it's just not worth it for now.
    """

    # For connection and cursor handling, and any difference between mysql
    # and postgresql
    def __init__(self, dsn: Optional[DataBaseDsn] = None, **kwargs):
        """
        Anything in kwargs gets passed into the engine_module's connect
        function. To be used for e.g.:
        s = SqlHelper('127.0.0.1', 'root', '', '300large', read_timeout=10)
        """

        self.dsn = dsn

        # I'm intentionally doing a match case here so that we'll make sure
        # we can NOT use this on old versions of python 😈
        if "mysql" in self.dsn.scheme:
            import pymysql as engine_module

            self.engine_module = engine_module
            self.cursor_class = engine_module.cursors.DictCursor
            self.quote_char = "`"

        elif "maria" in self.dsn.scheme:
            import pymysql as engine_module

            self.engine_module = engine_module
            self.cursor_class = engine_module.cursors.DictCursor
            self.quote_char = "`"

        if "autocommit" in kwargs:
            raise AssertionError("Be clear, be explicit.")

        self.kwargs = kwargs

    @property
    def dbname(self) -> str:
        return self.dsn.path[1:]

    @property
    def db_name(self) -> str:
        return self.dsn.path[1:]

    @property
    def db(self) -> str:
        return self.dsn.path[1:]

    def is_mysql(self) -> bool:
        return "mysql" in self.dsn.scheme

    def is_maria(self) -> bool:
        return "maria" in self.dsn.scheme

    def make_connection(self) -> Connection:
        # We are making a new connection for every cursor to make sure
        # multithreading/processing works correctly.
        if self.is_mysql():
            connection = self.engine_module.connect(
                host=self.dsn.host,
                user=self.dsn.username,
                password=self.dsn.password,
                db=self.dsn.path[1:],
                cursorclass=self.cursor_class,
                **self.kwargs,
            )
            return connection

        elif self.is_maria():
            # TODO: We want to to support this at some point, but will
            #   require alt handling of uuid hex values as MariaDB
            #   saves them as xxxx-xxxx-xxxx. We must:
            #   - Confirm evaluations and joins with with hex or non-hex versions
            #   - Decide if we return as hex version, or alter the UUIDStr custom_type
            #       as they'll now have more than 32 chars
            connection = self.engine_module.connect(
                host=self.dsn.host,
                user=self.dsn.username,
                password=self.dsn.password,
                database=self.dsn.path[1:],
                cursorclass=self.cursor_class,
            )
            return connection


def is_uuid4(s: Any) -> bool:
    if not isinstance(s, str):
        return False

    if len(s) not in (32, 36):
        return False

    try:
        u = UUID(s, version=4)
        return u.hex == s if len(s) == 32 else str(u) == s
    except (ValueError, AttributeError, TypeError):
        return False


def decode_uuids(row: Dict[str, Any]) -> Dict[str, Any]:
    return {
        key: (UUID(value, version=4).hex if is_uuid4(value) else value)
        for key, value in row.items()
    }


class SqlHelper(SqlConnector):

    def __init__(self, dsn: Optional[DataBaseDsn] = None, **kwargs):
        super(SqlHelper, self).__init__(dsn, **kwargs)

    def execute_sql_query(
        self, query: str, params: Optional[Dict[str, Any]] = None, commit: bool = False
    ) -> List[Dict[str, Any]]:
        for param in params if params else []:
            if isinstance(param, (tuple, list, set)) and len(param) == 0:
                logging.warning("param is empty. not executing query")
                return []
        connection = self.make_connection()
        c = connection.cursor()
        c.execute(query, params)
        if commit:
            connection.commit()

        if self.is_maria():
            return [decode_uuids(row) for row in c.fetchall()]

        else:
            return c.fetchall()

    def _quote(self, s) -> str:
        return f"{self.quote_char}{s}{self.quote_char}"

    def bulk_insert(
        self,
        table_name: str,
        field_names: ListOrTupleOfStrings,
        values_to_insert: ListOrTupleOfListOrTuple,
        cursor=None,
        ignore_existing: bool = False,
    ) -> None:
        """
        :param table_name: name of table
        :param field_names: list or tuple of field names, corresponding to their
            index/order in `values_to_insert`.
        :param values_to_insert: list of lists, where the inner list contains
            each row of values to be inserted. The order corresponds to the order
            of `field_names`.
        :param cursor: If cursor is passed, the insert is NOT committed!
        :param ignore_existing: adds 'ON CONFLICT DO NOTHING' to SQL statement.
        """
        assert len(set([len(x) for x in values_to_insert])) == 1
        if cursor is None:
            connection = self.make_connection()
            c = connection.cursor()
        else:
            c = cursor

        if self.is_mysql() or self.is_maria():
            values_to_insert = [
                [c.connection.escape_string(v) if isinstance(v, str) else v for v in vv]
                for vv in values_to_insert
            ]

        table_name_str = self._quote(table_name)
        field_name_str = ",".join(map(self._quote, field_names))
        values_str = ",".join(["%s"] * len(values_to_insert[0]))
        query = ""

        if self.is_mysql() or self.is_maria():
            ignore_str = "IGNORE" if ignore_existing else ""
            query = f"INSERT {ignore_str} INTO {table_name_str} ({field_name_str}) VALUES ({values_str});"

        c.executemany(query, values_to_insert)
        if cursor is None:
            c.connection.commit()

        return None

    def bulk_update(
        self,
        table_name: str,
        field_names: ListOrTupleOfStrings,
        values_to_insert: ListOrTupleOfListOrTuple,
        cursor=None,
    ) -> None:
        if len(values_to_insert) == 0:
            return None

        assert len(set([len(x) for x in values_to_insert])) == 1
        if cursor is None:
            connection = self.make_connection()
            c = connection.cursor()
        else:
            c = cursor

        values_to_insert = [
            [c.connection.literal(v) for v in vv] for vv in values_to_insert
        ]
        field_names = ["`" + x + "`" for x in field_names]
        field_name_str = ",".join(field_names)
        table_name_str = self._quote(table_name)

        values_str = ",\n".join(["(" + ",".join(x) + ")" for x in values_to_insert])
        update_col_str = ", ".join(f"{k}=VALUES({k})" for k in field_names)
        update_str = f"ON DUPLICATE KEY UPDATE {update_col_str}"
        query = f"INSERT INTO {table_name_str} ({field_name_str}) VALUES {values_str} {update_str};"
        c.execute(query)
        if cursor is None:
            c.connection.commit()

        return None

    def get_or_create(
        self,
        table_name: str,
        primary_key: str,
        lookup_dict: dict,
        update_dict: dict,
        cursor=None,
    ) -> Tuple[Union[str, int], bool]:
        """
        returns the value of the primary key ONLY, and bool (created)
        """
        # primary_key = "id"
        # lookup_dict = {"name": "Market Cube"}
        # table_name = "lucid_lucidaccount"
        # update_dict = {"name": "Market Cube"}
        lookup_fns = ",".join(
            ["`" + x + "`" for x in set(lookup_dict.keys()) | {primary_key}]
        )
        lookup_vals = " AND ".join([f"`{fn}`=%({fn})s" for fn in lookup_dict.keys()])
        table_name_str = self._quote(table_name)
        query = f"SELECT {lookup_fns} FROM {table_name_str} WHERE {lookup_vals} LIMIT 2"
        if cursor is None:
            connection = self.make_connection()
            c = connection.cursor()
        else:
            c = cursor

        c.execute(query, lookup_dict)
        res = c.fetchall()
        num = len(res)
        if num > 1:
            raise MultipleObjectsReturned(
                f"get() {table_name} returned more than one obj -- it returned {num}!"
            )
        if num == 1:
            return res[0][primary_key], False
        new_pk = self.create(table_name, update_dict, cursor=c)
        return new_pk, True

    def create(
        self,
        table_name: str,
        create_dict: dict,
        cursor=None,
        commit=True,
        primary_key=None,
    ) -> Optional[int]:
        """
        Create the item in table `table_name`.
        In postgresql, `primary_key` needs to be given in order to return the
        pk of the just created item
        """
        if cursor is None:
            connection = self.make_connection()
            c = connection.cursor()
        else:
            c = cursor
        field_names = ",".join(map(self._quote, create_dict))
        vals = ",".join([f"%({fn})s" for fn in create_dict.keys()])
        table_name_str = self._quote(table_name)
        query = f"INSERT INTO {table_name_str} ({field_names}) VALUES ({vals})"
        c.execute(query, create_dict)
        if self.is_mysql():
            new_pk = c.lastrowid
        else:
            new_pk = None
        if commit:
            c.connection.commit()
        return new_pk

    def filter(
        self,
        table_name: str,
        field_names: ListOrTupleOfStrings,
        filter_d=None,
        limit=None,
        cursor=None,
    ) -> List[Dict[Any, Any]]:

        if cursor is None:
            connection = self.make_connection()
            c = connection.cursor()
        else:
            c = cursor

        table_name_str = self._quote(table_name)
        field_names = ["`" + x + "`" for x in field_names]
        field_name_str = ",".join(field_names)
        if filter_d:
            lookup_vals = " AND ".join([f"`{fn}`=%({fn})s" for fn in filter_d.keys()])
            lookup_str = f" WHERE {lookup_vals}"
        else:
            lookup_str = ""
        limit_str = f"LIMIT {limit}" if limit else ""

        query = (
            f"SELECT {field_name_str} FROM {table_name_str} {lookup_str} {limit_str};"
        )
        c.execute(query, filter_d)

        return c.fetchall()

    def delete(self, table_name: str, field_name: str, values, cursor=None) -> None:
        if cursor is None:
            connection = self.make_connection()
            c = connection.cursor()
        else:
            c = cursor

        table_name_str = self._quote(table_name)
        field_name_str = self._quote(field_name)
        values_str = ",".join([c.connection.literal(v) for v in values])
        query = f"DELETE FROM {table_name_str} WHERE {field_name_str} IN ({values_str})"
        c.execute(query)
        if cursor is None:
            c.connection.commit()

        return None
