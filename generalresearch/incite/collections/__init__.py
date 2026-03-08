import logging
import os
import subprocess
import time
from datetime import datetime
from enum import Enum
from sys import platform
from typing import Any, Dict, List, Optional

import dask
import dask.dataframe as dd
import pandas as pd
import pyarrow.parquet as pq
from dask.distributed import Future
from distributed import Client, as_completed
from more_itertools import chunked
from pandera import DataFrameSchema
from psycopg import Cursor
from pydantic import Field, FilePath, ValidationInfo, field_validator
from sentry_sdk import capture_exception

from generalresearch.incite.base import CollectionBase, CollectionItemBase
from generalresearch.incite.schemas import (
    ARCHIVE_AFTER,
    ORDER_KEY,
    PARTITION_ON,
    empty_dataframe_from_schema,
)
from generalresearch.incite.schemas.thl_marketplaces import (
    InnovateSurveyHistorySchema,
    MorningSurveyTimeseriesSchema,
    SagoSurveyHistorySchema,
    SpectrumSurveyTimeseriesSchema,
)
from generalresearch.incite.schemas.thl_web import (
    LedgerSchema,
    THLIPInfoSchema,
    THLSessionSchema,
    THLTaskAdjustmentSchema,
    THLUserSchema,
    THLWallSchema,
    TransactionMetadataColumns,
    TxMetaSchema,
    TxSchema,
    UserHealthAuditLogSchema,
    UserHealthIPHistorySchema,
    UserHealthIPHistoryWSSchema,
)
from generalresearch.pg_helper import PostgresConfig
from generalresearch.sql_helper import SqlHelper

LOG = logging.getLogger("incite")

DT_STR = "%Y-%m-%d %H:%M:%S"


class DFCollectionType(str, Enum):
    TEST = "test"

    USER = "thl_user"
    SESSION = "thl_session"
    WALL = "thl_wall"
    TASK_ADJUSTMENT = "thl_taskadjustment"
    IP_INFO = "thl_ipinformation"

    AUDIT_LOG = "userhealth_auditlog"
    IP_HISTORY = "userhealth_iphistory"
    IP_HISTORY_WS = "userhealth_iphistory_ws"

    LEDGER = "ledger"

    INNOVATE_SURVEY_HISTORY = "innovate_surveyhistory"
    MORNING_SURVEY_TIMESERIES = "morning_surveytimeseries"
    SAGO_SURVEY_HISTORY = "sago_surveyhistory"
    SPECTRUM_SURVEY_TIMESERIES = "spectrum_surveytimeseries"


DFCollectionTypeSchemas = {
    DFCollectionType.USER: THLUserSchema,
    DFCollectionType.WALL: THLWallSchema,
    DFCollectionType.SESSION: THLSessionSchema,
    DFCollectionType.IP_INFO: THLIPInfoSchema,
    DFCollectionType.TASK_ADJUSTMENT: THLTaskAdjustmentSchema,
    DFCollectionType.IP_HISTORY: UserHealthIPHistorySchema,
    DFCollectionType.IP_HISTORY_WS: UserHealthIPHistoryWSSchema,
    DFCollectionType.AUDIT_LOG: UserHealthAuditLogSchema,
    DFCollectionType.LEDGER: LedgerSchema,
    DFCollectionType.INNOVATE_SURVEY_HISTORY: InnovateSurveyHistorySchema,
    DFCollectionType.MORNING_SURVEY_TIMESERIES: MorningSurveyTimeseriesSchema,
    DFCollectionType.SAGO_SURVEY_HISTORY: SagoSurveyHistorySchema,
    DFCollectionType.SPECTRUM_SURVEY_TIMESERIES: SpectrumSurveyTimeseriesSchema,
}


class DFCollectionItem(CollectionItemBase):

    # --- Properties ---
    @property
    def filename(self) -> str:
        return (
            f"{self._collection.data_type.name.lower()}-{self._collection.offset}"
            f"-{self.start.strftime('%Y-%m-%d-%H-%M-%S')}.parquet"
        )

    # --- Methods ---

    def has_mysql(self) -> bool:
        if self._collection.sql_helper is None:
            return False

        connected = True
        try:
            self._collection.sql_helper.execute_sql_query("""SELECT 1;""")
        except:
            connected = False

        return connected

    def has_postgres(self) -> bool:
        if self._collection.pg_config is None:
            return False

        connected = True
        try:
            self._collection.pg_config.execute_sql_query("""SELECT 1;""")
        except:
            connected = False

        return connected

    def has_db(self) -> bool:
        return self.has_mysql() or self.has_postgres()

    def update_partial_archive(self) -> bool:
        if not self.valid_archive(self.partial_path, sample=1000):
            LOG.error(f"invalid partial archive: {self.partial_path}")
            return self.create_partial_archive()
        df = pq.ParquetDataset(self.partial_path).read().to_pandas()

        order_key = self._collection._schema.metadata[ORDER_KEY]
        archive_after = self._collection._schema.metadata[ARCHIVE_AFTER]

        partial_max = df[order_key].max().to_pydatetime()

        since = partial_max - archive_after
        since = max([since, self.start])  # don't allow to query before the item's start
        df = df[df[order_key] < since].copy()

        _df = self.from_mysql(since=since)

        if _df is not None:
            df = pd.concat([df, _df])
            self.to_archive(ddf=dd.from_pandas(df, npartitions=1), is_partial=True)
        else:
            # The update to the partial returned no rows, but the partial
            # still exists, so we'll continue with whatever was calling this.
            # We don't need to re-write the partial or really do anything.
            pass
        return True

    def create_partial_archive(self) -> bool:
        _df = self.from_mysql()
        if _df is None:
            # Returned no rows, but the period is not closed, so we
            #   don't want to mark as empty. Do nothing.
            return False
        return self.to_archive(ddf=dd.from_pandas(_df, npartitions=1), is_partial=True)

    # --- ORM / Data handlers---
    def to_dict(self, *args, **kwargs) -> Dict[str, Any]:
        return self._to_dict()

    def from_mysql(self, since: Optional[datetime] = None) -> Optional[pd.DataFrame]:
        if self._collection.data_type == DFCollectionType.LEDGER:
            assert since is None, "Shouldn't pass since for Ledger item"
            assert self._collection.pg_config is not None
            return self.from_postgres_ledger()
        else:
            if self._collection.sql_helper:
                return self.from_mysql_standard(since=since)
            else:
                return self.from_postgres_standard(since=since)

    def from_mysql_standard(
        self, since: Optional[datetime] = None
    ) -> Optional[pd.DataFrame]:

        assert (
            self._collection.data_type != DFCollectionType.LEDGER
        ), "Can't call from_mysql_standard for Ledger DFCollectionItem"

        start, finish = self.start, self.finish
        LOG.debug(
            f"{self._collection.data_type.value}.from_mysql("
            f"start={start.strftime(DT_STR)}, "
            f"finish={finish.strftime(DT_STR)})"
        )
        coll = self._collection
        schema = coll._schema
        sql_helper = coll.sql_helper

        start = since or start
        order_key = schema.metadata[ORDER_KEY]
        cols = list(schema.columns.keys()) + [schema.index.name]
        cols_str = ",".join(map(sql_helper._quote, cols))
        db_name = sql_helper.db

        try:
            res = sql_helper.execute_sql_query(
                query=f"""
                    SELECT {cols_str}
                    FROM `{db_name}`.`{coll.data_type.value}`
                    WHERE `{order_key}` >= %s AND `{order_key}` < %s;
                """,
                params=[start, finish],
            )
        except (Exception,) as e:
            capture_exception(error=e)
            LOG.error(f"_from_mysql Exception: {e}")
            return None

        if not res:
            LOG.warning(f"_from_mysql query returned nothing")
            # Return an empty df.DataFrame with the correct columns
            return empty_dataframe_from_schema(coll._schema)

        df = pd.DataFrame.from_records(res).set_index(coll._schema.index.name)
        df = self.validate_df(df=df)

        if df is None:
            LOG.warning(f"_from_mysql query results failed validation")
            # Schema validation can fail...
            return None

        return df

    def from_postgres_standard(
        self, since: Optional[datetime] = None
    ) -> Optional[pd.DataFrame]:
        assert (
            self._collection.data_type != DFCollectionType.LEDGER
        ), "Can't call from_postgres_standard for Ledger DFCollectionItem"

        start, finish = self.start, self.finish
        LOG.debug(
            f"{self._collection.data_type.value}.from_postgres("
            f"start={start.strftime(DT_STR)}, "
            f"finish={finish.strftime(DT_STR)})"
        )
        coll = self._collection
        schema = coll._schema
        pg_config = coll.pg_config

        start = since or start
        order_key = schema.metadata[ORDER_KEY]
        cols = list(schema.columns.keys()) + [schema.index.name]
        cols_str = ", ".join(cols)

        try:
            res = pg_config.execute_sql_query(
                query=f"""
                    SELECT {cols_str}
                    FROM {coll.data_type.value}
                    WHERE {order_key} >= %s AND {order_key} < %s;
                """,
                params=[start, finish],
            )
        except (Exception,) as e:
            capture_exception(error=e)
            LOG.error(f"_from_postgres Exception: {e}")
            return None

        if not res:
            LOG.warning(f"_from_postgres query returned nothing")
            # Return an empty df.DataFrame with the correct columns
            return empty_dataframe_from_schema(coll._schema)

        df = pd.DataFrame.from_records(res).set_index(coll._schema.index.name)
        df = self.validate_df(df=df)

        if df is None:
            LOG.warning(f"_from_postgres query results failed validation")
            # Schema validation can fail...
            return None

        return df

    def from_postgres_ledger(self) -> Optional[pd.DataFrame]:
        assert (
            self._collection.data_type == DFCollectionType.LEDGER
        ), "Can only call from_postgres_ledger on Ledger DFCollectionItem"

        start, finish = self.start, self.finish
        LOG.info(
            f"{self._collection.data_type.value}.from_postgres_ledger("
            f"start={start.strftime(DT_STR)}, "
            f"finish={finish.strftime(DT_STR)})"
        )

        coll = self._collection
        pg_config: PostgresConfig = coll.pg_config

        limit = 20000
        offset = 0
        res = []
        while True:
            logging.info(
                f"{self._collection.data_type.value}.from_postgres_ledger({limit=}, {offset=})"
            )
            chunk = pg_config.execute_sql_query(
                query=f"""
                SELECT  lt.id AS tx_id, lt.created, lt.ext_description, lt.tag,
                        le.id AS entry_id, le.direction, le.amount, le.account_id,
                        la.display_name, la.qualified_name, la.account_type, 
                        la.normal_balance, la.reference_type, la.reference_uuid, 
                        la.currency
                FROM ledger_transaction AS lt
                LEFT JOIN ledger_entry AS le 
                    ON lt.id = le.transaction_id
                LEFT JOIN ledger_account AS la 
                    ON la.uuid = le.account_id
                WHERE lt.created >= %s AND lt.created < %s
                AND le.id IS NOT NULL
                ORDER BY lt.created
                LIMIT {limit} OFFSET {offset};
            """,
                params=[start, finish],
            )
            res.extend(chunk)
            if not chunk:
                break
            offset += limit

        if len(res) == 0:
            return None

        # Note (AND le.id IS NOT NULL): It is possible we have transactions with
        #   no ledger entries. This is because the transaction creation failed
        #   for some reason. The ledger is not unbalanced, it is just an orphan
        #   transaction. Just skip those here.

        tx_df = TxSchema.validate(
            check_obj=pd.DataFrame.from_records(res).set_index("entry_id"),
            lazy=True,
        )

        tx_ids = list(tx_df["tx_id"].unique())
        metadata_res = []
        # "MySQL server has gone away" if this is too big
        conn = pg_config.make_connection()
        c: Cursor = conn.cursor()
        for chunk in chunked(tx_ids, n=5_000):
            c.execute(
                query=f"""
                SELECT  ltm.transaction_id AS tx_id, 
                        ltm.id AS tx_metadata_id,
                        ltm.key, ltm.value
                FROM ledger_transactionmetadata AS ltm
                WHERE ltm.transaction_id = ANY(%s);
            """,
                params=[chunk],
            )
            metadata_res += c.fetchall()

        conn.close()

        tx_meta = (
            pd.DataFrame(
                TxMetaSchema.validate(
                    check_obj=pd.DataFrame.from_records(metadata_res).set_index(
                        ["tx_id", "tx_metadata_id"]
                    ),
                    lazy=True,
                ).pivot(columns="key", values="value"),
                # This makes sure we expand to have all the possible columns
                columns=[e.value for e in TransactionMetadataColumns],
            )
            .groupby("tx_id")
            .first()
        )

        df = tx_df.merge(tx_meta, how="left", left_on="tx_id", right_index=True)
        df = self.validate_df(df=df)

        if df is None:
            # Schema validation can fail...
            return None

        return df

    def to_archive(
        self,
        ddf: dd.DataFrame,
        is_partial: bool = False,
        overwrite: bool = False,
    ) -> bool:
        """
        :returns: bool (saved_successful)
        """
        assert isinstance(ddf, dd.DataFrame), "must pass dask df"

        client: Optional[Client] = self._collection._client
        # client = None

        if client:
            row_len = client.compute(collections=ddf.shape[0], sync=True)
        else:
            row_len = len(ddf.index)
        is_empty = row_len == 0

        if is_partial:
            return self.to_archive_numbered_partial(ddf=ddf)
        else:
            return self._to_archive(
                ddf=ddf,
                is_empty=is_empty,
                overwrite=overwrite,
            )

    def _to_archive(
        self,
        ddf: dd.DataFrame,
        is_empty: bool,
        overwrite: bool = False,
    ) -> bool:
        """
        For archiving an item. Will write an empty file if ddf is empty. This
        is NOT for writing partials.

        :returns: bool (saved_successful)
        """

        if ddf is None:
            return False

        should_archive = self.should_archive()
        if not should_archive:
            LOG.warning(f"Cannot create archive for such new data: {self.path}")
            return False

        if overwrite is False:
            has_archive = self.has_archive(include_empty=True)
            if has_archive:
                LOG.warning(f"archive already exists: {self.path}")
                return False

        if is_empty:
            # Create an .empty only if the Item is "archiveable" (which we checked above)
            self.set_empty()
            return True

        # Incase the file saving is interrupted, or otherwise fails
        # save it to a tmp file first, then rename once we can confirm
        # that it successfully loads
        tmp_path = self.tmp_path()
        try:
            schema = self._collection._schema
            partition = schema.metadata.get(PARTITION_ON, None)

            ddf.to_parquet(
                path=tmp_path,
                partition_on=partition,
                engine="pyarrow",
                overwrite=True,
                write_metadata_file=True,
                compression="brotli",
            )

        except (Exception,) as e:
            LOG.exception(e)
            self.delete_archive(tmp_path)
            return False

        # It was saved, but the file seems to be corrupt
        if not self.valid_archive(tmp_path):
            LOG.error(f"not valid archive: {tmp_path}")
            self.delete_archive(tmp_path)
            # File did not save correctly so return it as saved=False
            return False

        # To debug, just set this key to auto expire in 5 seconds
        # RC.set(name=f"_to_archive:{self.path.as_posix()}", value=1, ex=15)
        # with RC.lock(f"_to_archive:{self.path.as_posix()}:lock", timeout=15):

        if os.path.isfile(tmp_path):
            # If the file was saved okay, seems okay, rename it
            os.replace(tmp_path, self.path)
            os.remove(tmp_path)

        if os.path.isdir(tmp_path):
            if os.path.exists(self.path.as_posix()):
                if overwrite:
                    subprocess.call(["rm", "-r", self.path.as_posix()])
                    time.sleep(1)
                else:
                    LOG.error(f"already exists: {self.path.as_posix()}")
                    return False

            if platform == "darwin":
                subprocess.call(["mv", tmp_path.as_posix(), self.path.as_posix()])
            else:
                # -T will (should) cause the mv to fail if path wasn't successfully deleted
                subprocess.call(["mv", "-T", tmp_path.as_posix(), self.path.as_posix()])
        return True

    def to_archive_numbered_partial(self, ddf: Optional[dd.DataFrame] = None) -> bool:
        """
        For partial files/dirs only. Writes the .partial file with a number
        at the end (.partial.####) and then creates a symlink
        from .partial -> .partial.####

        :returns: bool (saved_successful)
        """
        if ddf is None:
            return False

        collection = self._collection
        schema = collection._schema
        client: Optional[Client] = collection._client

        next_numbered_path = self.next_numbered_path(self.partial_path)
        partial_path = self.partial_path
        # finish = self.finish

        # Make sure these are in the same dir. b/c the symlink has to be
        # relative, not an absolute path
        assert (
            partial_path.parent == next_numbered_path.parent
        ), "Can't have numbered_path in a different directory"
        target = (
            next_numbered_path.name
        )  # this is the symlink's target. it is a relative path (only the name)

        should_archive = self.should_archive()
        assert should_archive is False, "Don't write partial if the item is archiveable"

        if client:
            row_len = client.compute(collections=ddf.shape[0], sync=True)
        else:
            row_len = len(ddf.index)

        if row_len == 0:
            LOG.warning("Skipping, don't partial save an empty dd.DataFrame")
            return False

        try:
            partition = schema.metadata.get(PARTITION_ON, None)
            ddf.to_parquet(
                path=next_numbered_path,
                partition_on=partition,
                engine="pyarrow",
                overwrite=True,
                write_metadata_file=True,
                compression="brotli",
            )
        except (Exception,) as e:
            LOG.exception(e)
            self.delete_archive(next_numbered_path)
            return False

        if platform == "darwin":
            subprocess.call(["ln", "-sfn", target, partial_path])
        else:
            subprocess.call(["ln", "-sfnT", target, partial_path])

        return True

    def initial_load(self, overwrite: bool = False) -> bool:

        if overwrite is False:
            assert not self.has_archive(include_empty=True), "already archived"

        assert self.should_archive(), "not ready to archive!"

        df: Optional[pd.DataFrame] = self.from_mysql()

        if df is None:
            self.set_empty()
            return False

        ddf = dd.from_pandas(df, npartitions=1)
        return self.to_archive(ddf=ddf, is_partial=False, overwrite=overwrite)

    def clear_corrupt_archive(self):
        if self.has_archive(include_empty=False):
            if not self.valid_archive(self.path):
                LOG.warning(f"invalid archive, deleting: {self.path}")
                self.delete_archive(self.path)


class DFCollection(CollectionBase):
    data_type: Optional[DFCollectionType] = Field(default=None)

    # --- Private ---
    pg_config: Optional[PostgresConfig] = Field(default=None)
    sql_helper: Optional[SqlHelper] = Field(default=None)

    def __repr__(self):
        res = self.signature() + "\n"
        if len(self.items) > 6:
            items = self.items[:3] + ["..."] + self.items[-3:]
        else:
            items = self.items

        for i in items:
            res += f" – {repr(i) if isinstance(i, DFCollectionItem) else i}\n"

        return res

    def signature(self):
        arr = [
            1 if i.has_archive(include_empty=True) else 0
            for i in self.items
            if i.should_archive()
        ]
        repr_str = (
            f"items={len(self.items)}; start={self.start} @ {self.offset}; {int(sum(arr) / len(arr) * 100)}% "
            f"archived"
        )
        res = f"{self.__repr_name__()}({repr_str})"
        return res

    @field_validator("data_type")
    def check_data_type(cls, data_type, info: ValidationInfo):
        if data_type is None:
            raise ValueError("Must explicitly provide a data_type")

        if data_type not in DFCollectionTypeSchemas:
            raise ValueError("Must provide a supported data_type")

        return data_type

    # --- Properties ---
    @property
    def items(self) -> List[DFCollectionItem]:
        items = []
        for iv in self.interval_range:
            cm = DFCollectionItem(start=iv[0])
            cm._collection = self
            items.append(cm)
        return items

    @property
    def _schema(self) -> DataFrameSchema:
        return DFCollectionTypeSchemas[self.data_type]

    # --- Methods ---

    def initial_load(
        self,
        client: Optional[Client] = None,
        sync: bool = True,
        since: Optional[datetime] = None,
        client_resources: Optional[Dict[str, Any]] = None,
        timeout: Optional[float] = None,
    ) -> List[Future]:
        # This can be used to just build all local archive files
        # We typically want to go backwards first, so we can most quickly
        # populate the last 90 days for example

        client = client or self._client

        LOG.info(f"{self.data_type.value}.initial_load({since=}, {sync=})")

        items = self.items
        if since:
            items = self.get_items(since=since)

        if client is None:
            for item in reversed(items):
                if item.has_archive(include_empty=True):
                    continue
                if not item.should_archive():
                    continue
                item.initial_load()
            return []

        fs = []
        for item in items:
            if item.has_archive(include_empty=True):
                continue
            if not item.should_archive():
                continue
            f = dask.delayed(item.initial_load)()
            fs.append(f)

        if sync:
            fs = client.compute(fs, sync=False, priority=2, resources=client_resources)
            ac = as_completed(fs, timeout=timeout)
            return fs

        else:
            return client.compute(fs, sync=True, priority=2, resources=client_resources)

    def fetch_force_rr_latest(self, sources) -> List[FilePath]:
        LOG.info(
            f"{self.data_type.value}.fetch_force_rr_latest(sources={len(sources)})"
        )

        # We only want 'partial-able' items (those that can not yet be archived).
        rr_items = [
            i for i in self.items if not i.should_archive() and not i.is_empty()
        ]
        if rr_items:
            # If the ARCHIVE_AFTER time is > the collection offset (which it is always currently),
            #   then there typically wouldn't be more than 1 un-archivable item.
            _start = rr_items[0].start
            _end = rr_items[-1].finish
            rr_duration = (_end - _start).total_seconds()

            # TODO: Do we want to be smarter about any rr selects max durations?
            # allowing 2x the length of the offset. If we have more than this not archived,
            #   we want to run the archive first, not fetch from rr
            archive_after = self._schema.metadata[ARCHIVE_AFTER]
            allowed_rr_duration = (
                (pd.Timedelta(self.offset) * 2) + archive_after
            ).total_seconds()
            if rr_duration > allowed_rr_duration:
                raise ValueError(
                    f"rr select duration exceeds {pd.Timedelta(allowed_rr_duration)}"
                )

        for rr_item in rr_items:
            if (
                rr_item.has_partial_archive()
                and self.data_type != DFCollectionType.LEDGER
            ):
                saved = rr_item.update_partial_archive()
            else:
                saved = rr_item.create_partial_archive()
            if saved:
                sources.append(rr_item.partial_path)

        return sources

    def force_rr_latest(
        self,
        client: Client,
        client_resources: Optional[Dict[str, Any]] = None,
        sync: bool = True,
    ) -> List[Future]:

        # For forcing update of any partials asynchronously if desired
        LOG.info(f"{self.data_type.value}.force_rr_latest({client=})")

        rr_items = [
            i for i in self.items if not i.should_archive() and not i.is_empty()
        ]
        fs = []
        for rr_item in rr_items:
            if (
                rr_item.has_partial_archive()
                and self.data_type != DFCollectionType.LEDGER
            ):
                fs.append(dask.delayed(rr_item.update_partial_archive)())
            else:
                fs.append(dask.delayed(rr_item.create_partial_archive)())
        return client.compute(fs, sync=sync, priority=2, resources=client_resources)
