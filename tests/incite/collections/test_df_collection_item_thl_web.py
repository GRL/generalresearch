from __future__ import annotations

from collections.abc import Callable, Generator
from datetime import UTC, datetime, timedelta
from itertools import product as iter_product
from os.path import join as pjoin
from pathlib import Path, PurePath
from uuid import uuid4

import dask.dataframe as dd
import pandas as pd
import pytest
from dask.distributed import Client as DaskClient
from distributed import Client, Scheduler, Worker

# noinspection PyUnresolvedReferences
from distributed.utils_test import (
    gen_cluster,
)
from faker import Faker
from pandera.pandas import DataFrameSchema
from pydantic import FilePath

from generalresearch.incite.base import CollectionItemBase, GRLDatasets
from generalresearch.incite.collections import (
    DFCollection,
    DFCollectionItem,
    DFCollectionType,
)
from generalresearch.incite.schemas import ARCHIVE_AFTER
from generalresearch.managers.thl.ledger_manager.thl_ledger import ThlLedgerManager
from generalresearch.models.thl.product import Product
from generalresearch.models.thl.user import User
from generalresearch.pg_helper import PostgresConfig
from generalresearch.sql_helper import PostgresDsn

fake = Faker()

df_collections = [
    DFCollectionType.WALL,
    DFCollectionType.SESSION,
    DFCollectionType.LEDGER,
    DFCollectionType.TASK_ADJUSTMENT,
]

unsupported_mock_types = {
    DFCollectionType.IP_INFO,
    DFCollectionType.IP_HISTORY,
    DFCollectionType.IP_HISTORY_WS,
    DFCollectionType.TASK_ADJUSTMENT,
}


def combo_object() -> Generator[str]:
    for x in iter_product(
        df_collections,
        ["15min", "45min", "1H"],
    ):
        yield from x


class TestDFCollectionItemBase:
    def test_init(self):
        instance = CollectionItemBase()
        assert isinstance(instance, CollectionItemBase)
        assert isinstance(instance.start, datetime)


@pytest.mark.parametrize(
    argnames="df_collection_data_type, offset", argvalues=combo_object()
)
class TestDFCollectionItemProperties:

    def test_filename(
        self,
        df_collection_data_type: DFCollectionType,
        df_collection: DFCollection,
        offset: str,
    ):
        for i in df_collection.items:
            assert isinstance(i.filename, str)

            assert isinstance(i.path, PurePath)
            assert i.path.name == i.filename

            assert i._collection.data_type.name.lower() in i.filename
            assert i._collection.offset in i.filename
            assert i.start.strftime("%Y-%m-%d-%H-%M-%S") in i.filename


@pytest.mark.parametrize(
    argnames="df_collection_data_type, offset", argvalues=combo_object()
)
class TestDFCollectionItemPropertiesBase:

    def test_name(
        self,
        df_collection: DFCollection,
    ):
        for i in df_collection.items:
            assert isinstance(i.name, str)

    def test_finish(
        self,
        df_collection: DFCollection,
    ):
        for i in df_collection.items:
            assert isinstance(i.finish, datetime)

    def test_interval(
        self,
        df_collection: DFCollection,
    ):
        for i in df_collection.items:
            assert isinstance(i.interval, pd.Interval)

    def test_partial_filename(
        self,
        df_collection: DFCollection,
    ):
        for i in df_collection.items:
            assert isinstance(i.partial_filename, str)

    def test_empty_filename(
        self,
        df_collection: DFCollection,
    ):
        for i in df_collection.items:
            assert isinstance(i.empty_filename, str)

    def test_path(
        self,
        df_collection: DFCollection,
    ):
        for i in df_collection.items:
            assert isinstance(i.path, FilePath)

    def test_partial_path(
        self,
        df_collection: DFCollection,
    ):
        for i in df_collection.items:
            assert isinstance(i.partial_path, FilePath)

    def test_empty_path(
        self,
        df_collection: DFCollection,
    ):
        for i in df_collection.items:
            assert isinstance(i.empty_path, FilePath)


@pytest.mark.parametrize(
    argnames="df_collection_data_type, offset, duration",
    argvalues=list(
        iter_product(
            df_collections,
            ["12h", "10D"],
            [timedelta(days=10), timedelta(days=45)],
        )
    ),
)
class TestDFCollectionItemMethod:

    def test_has_mysql(
        self,
        df_collection: DFCollection,
        thl_web_rr: PostgresConfig,
        delete_df_collection: Callable[..., None],
    ):
        delete_df_collection(coll=df_collection)

        df_collection.pg_config = None
        for i in df_collection.items:
            assert not i.has_mysql()

        # Confirm that the regular connection should work as expected
        df_collection.pg_config = thl_web_rr
        for i in df_collection.items:
            assert i.has_mysql()

        # Make a fake connection and confirm it does NOT work
        df_collection.pg_config = PostgresConfig(
            dsn=PostgresDsn(f"postgres://root:@127.0.0.1/{uuid4().hex}"),
            connect_timeout=5,
            statement_timeout=1,
        )
        for i in df_collection.items:
            assert not i.has_mysql()

    @pytest.mark.skip
    def test_update_partial_archive(
        self,
    ):
        # for i in collection.items:
        #     assert i.update_partial_archive()
        # assert df.created.max() < _last_time_block[1]
        pass

    @pytest.mark.skip
    def test_create_partial_archive(
        self,
    ):
        assert 1 + 1 == 2

    def test_dict(
        self,
        df_collection: DFCollection,
        delete_df_collection: Callable[..., None],
    ):
        delete_df_collection(coll=df_collection)

        for item in df_collection.items:
            res = item.to_dict()
            assert isinstance(res, dict)
            assert len(res.keys()) == 6

            assert isinstance(res["should_archive"], bool)
            assert isinstance(res["has_archive"], bool)
            assert isinstance(res["path"], Path)
            assert isinstance(res["filename"], str)

            assert isinstance(res["start"], datetime)
            assert isinstance(res["finish"], datetime)
            assert res["start"] < res["finish"]

    def test_from_mysql(
        self,
        df_collection_data_type: DFCollectionType,
        df_collection: DFCollection,
        offset: str,
        duration: timedelta,
        create_main_accounts: Callable[..., None],
        thl_web_rw: PostgresConfig,
        user_factory: Callable[..., User],
        product: Product,
        incite_item_factory: Callable[..., None],
        delete_df_collection: Callable[..., None],
    ):

        if df_collection.data_type in unsupported_mock_types:
            return

        delete_df_collection(coll=df_collection)
        u1: User = user_factory(product=product)

        # No data has been loaded, but we can confirm the from_mysql returns
        #   back an empty data with the correct columns
        for item in df_collection.items:
            # Unlike .from_mysql_ledger(), .from_mysql_standard() will return
            #   back and empty df with the correct columns in place
            delete_df_collection(coll=df_collection)
            df = item.from_db()
            if df_collection.data_type == DFCollectionType.LEDGER:
                assert df is None
            else:
                assert isinstance(df, pd.DataFrame)
                assert df.empty
                assert set(df.columns) == set(df_collection._schema.columns.keys())

            incite_item_factory(user=u1, item=item)

            df = item.from_db()
            assert isinstance(df, pd.DataFrame)
            assert not df.empty
            assert set(df.columns) == set(df_collection._schema.columns.keys())
            if df_collection.data_type == DFCollectionType.LEDGER:
                # The number of rows in this dataframe will change depending
                #    on the mocking of data. It's because if the account has
                #   user wallet on, then there will be more transactions for
                #   example.
                assert df.shape[0] > 0

    def test_from_mysql_standard(
        self,
        df_collection_data_type: DFCollectionType,
        df_collection: DFCollection,
        offset: str,
        duration: timedelta,
        user_factory: Callable[..., User],
        product: Product,
        incite_item_factory: Callable[..., None],
        delete_df_collection: Callable[..., None],
    ):

        if df_collection.data_type in unsupported_mock_types:
            return
        u1: User = user_factory(product=product)

        delete_df_collection(coll=df_collection)

        for item in df_collection.items:
            item: DFCollectionItem

            if df_collection.data_type == DFCollectionType.LEDGER:
                # We're using parametrize, so this If statement is just to
                #   confirm other Item Types will always raise an assertion
                with pytest.raises(expected_exception=AssertionError) as cm:
                    _ = item.from_mysql_standard()
                assert (
                    "Can't call from_mysql_standard for Ledger DFCollectionItem"
                    in str(cm.value)
                )

                continue

            # Unlike .from_mysql_ledger(), .from_mysql_standard() will return
            #   back and empty df with the correct columns in place
            df = item.from_mysql_standard()
            assert isinstance(df, pd.DataFrame)
            assert df.empty
            assert set(df.columns) == set(df_collection._schema.columns.keys())

            incite_item_factory(user=u1, item=item)

            df = item.from_mysql_standard()
            assert isinstance(df, pd.DataFrame)
            assert not df.empty
            assert set(df.columns) == set(df_collection._schema.columns.keys())
            assert df.shape[0] > 0

    def test_from_mysql_ledger(
        self,
        df_collection: DFCollection,
        user: User,
        create_main_accounts: Callable[..., None],
        offset: str,
        duration: timedelta,
        thl_web_rw: PostgresConfig,
        thl_ledger_manager: ThlLedgerManager,
        df_collection_data_type: DFCollectionType,
        user_factory: Callable[..., User],
        product: Product,
        client_no_amm: DaskClient,
        incite_item_factory: Callable[..., None],
        delete_df_collection: Callable[..., None],
        mnt_filepath: GRLDatasets,
    ):

        if df_collection.data_type != DFCollectionType.LEDGER:
            return
        u1: User = user_factory(product=product)

        delete_df_collection(coll=df_collection)

        for item in df_collection.items:
            item: DFCollectionItem
            delete_df_collection(coll=df_collection)

            # Okay, now continue with the actual Ledger Item tests... we need
            #   to ensure that this item.start - item.finish range hasn't had
            #   any prior transactions created within that range.
            assert item.from_mysql_ledger() is None

            # Create main accounts doesn't matter because it doesn't
            # add any transactions to the db
            assert item.from_mysql_ledger() is None

            incite_item_factory(user=u1, item=item)
            df = item.from_mysql_ledger()
            assert isinstance(df, pd.DataFrame)

            # Not only is this a np.int64 to int comparison, but I also know it
            #   isn't actually measuring anything meaningful. However, it's useful
            #   as it tells us if the DF contains all the correct TX Entries. I
            #   figured it's better to count the amount rather than just the
            #   number of rows. DF == transactions * 2 because there are two
            #   entries per transactions
            # assert df.amount.sum() == total_amt
            # assert total_entries == df.shape[0]

            assert not df.tx_id.is_unique
            df["net"] = df.direction * df.amount
            assert df.groupby("tx_id").net.sum().sum() == 0

    def test_to_archive(
        self,
        df_collection: DFCollection,
        user: User,
        offset: str,
        duration: timedelta,
        df_collection_data_type: DFCollectionType,
        user_factory: Callable[..., User],
        product: Product,
        client_no_amm: DaskClient,
        incite_item_factory: Callable[..., None],
        delete_df_collection: Callable[..., None],
        mnt_filepath: GRLDatasets,
    ):

        if df_collection.data_type in unsupported_mock_types:
            return
        u1: User = user_factory(product=product)

        delete_df_collection(coll=df_collection)

        for item in df_collection.items:
            item: DFCollectionItem

            incite_item_factory(user=u1, item=item)

            # Load up the data that we'll be using for various to_archive
            #   methods.
            df = item.from_db()
            ddf = dd.from_pandas(df, npartitions=1)

            # (1) Write the basic archive, the issue is that because it's
            #   an empty pd.DataFrame, it never makes an actual parquet file
            assert item.to_archive(ddf=ddf, is_partial=False, overwrite=False)
            assert item.has_archive()
            assert item.has_archive(include_empty=False)

    def test__to_archive(
        self,
        df_collection_data_type: DFCollectionType,
        df_collection: DFCollection,
        user_factory: Callable[..., User],
        product: Product,
        offset: str,
        duration: timedelta,
        client_no_amm: DaskClient,
        user: User,
        incite_item_factory: Callable[..., None],
        delete_df_collection: Callable[..., None],
        mnt_filepath: GRLDatasets,
    ):
        """We already have a test for the "non-private" version of this,
        which primarily just uses the respective Client to determine if
        the ddf is_empty or not.

        Therefore, use the private test to check the manual behavior of
            passing in the is_empty or overwrite.
        """
        if df_collection.data_type in unsupported_mock_types:
            return

        delete_df_collection(coll=df_collection)
        u1: User = user_factory(product=product)

        for item in df_collection.items:
            item: DFCollectionItem

            incite_item_factory(user=u1, item=item)

            # Load up the data that we'll be using for various to_archive
            #   methods. Will always be empty pd.DataFrames for now...
            df = item.from_db()
            ddf = dd.from_pandas(df, npartitions=1)

            # (1) Confirm a missing ddf (shouldn't bc of type hint) should
            #   immediately return back False
            assert not item._to_archive(ddf=None, is_empty=True)
            assert not item._to_archive(ddf=None, is_empty=False)

            # (2) Setting empty overrides any possible state of the ddf
            for rand_val in [df, ddf, True, 1_000]:
                assert not item.empty_path.exists()
                item._to_archive(ddf=rand_val, is_empty=True)
                assert item.empty_path.exists()
                item.empty_path.unlink()

            # (3) Trigger a warning with overwrite. First write an empty,
            #   then write it again with override default to confirm it worked,
            #   then write it again with override=False to confirm it does
            #   not work.
            assert item._to_archive(ddf=ddf, is_empty=True)
            res1 = item.empty_path.stat()

            # Returns none because it knows the file (regular, empty, or
            #   partial) already exists
            assert not item._to_archive(ddf=ddf, is_empty=True, overwrite=False)

            # Currently override=True doesn't actually work on empty files
            #   because it's checked again in .set_empty() and isn't
            #   aware of the override flag that may be passed in to
            #   item._to_archive()
            with pytest.raises(expected_exception=AssertionError) as cm:
                item._to_archive(ddf=rand_val, is_empty=True, overwrite=True)
            assert "set_empty is already set; why are you doing this?" in str(cm.value)

            # We can assert the file stats are the same because we were never
            #   able to go ahead and rewrite or update it in anyway
            res2 = item.empty_path.stat()
            assert res1 == res2

    @pytest.mark.skip
    def test_to_archive_numbered_partial(
        self,
    ):
        pass

    @pytest.mark.skip
    def test_initial_load(
        self,
    ):
        pass

    @pytest.mark.skip
    def test_clear_corrupt_archive(
        self,
    ):
        pass


@pytest.mark.parametrize(
    argnames="df_collection_data_type, offset, duration",
    argvalues=list(iter_product(df_collections, ["12h", "10D"], [timedelta(days=15)])),
)
class TestDFCollectionItemMethodBase:

    @pytest.mark.skip
    def test_path_exists(
        self,
    ):
        pass

    @pytest.mark.skip
    def test_next_numbered_path(
        self,
    ):
        pass

    @pytest.mark.skip
    def test_search_highest_numbered_path(
        self,
        df_collection_data_type: DFCollectionType,
        offset: str,
        duration: timedelta,
    ):
        pass

    @pytest.mark.skip
    def test_tmp_filename(
        self,
    ):
        pass

    @pytest.mark.skip
    def test_tmp_path(
        self,
    ):
        pass

    def test_is_empty(
        self,
        df_collection: DFCollection,
    ):
        """
        test_has_empty was merged into this because item.has_empty is
            an alias for is_empty.. or vis-versa
        """

        for item in df_collection.items:
            assert not item.is_empty()
            assert not item.has_empty()

            item.empty_path.touch()

            assert item.is_empty()
            assert item.has_empty()

    def test_has_partial_archive(
        self,
        df_collection: DFCollection,
    ):
        for item in df_collection.items:
            assert not item.has_partial_archive()
            item.partial_path.touch()
            assert item.has_partial_archive()

    def test_has_archive(
        self,
        df_collection: DFCollection,
    ):
        for item in df_collection.items:
            # (1) Originally, nothing exists... so let's just make a file and
            #   confirm that it works if just touch that path (no validation
            #   occurs at all).
            assert not item.has_archive(include_empty=False)
            assert not item.has_archive(include_empty=True)
            item.path.touch()
            assert item.has_archive(include_empty=False)
            assert item.has_archive(include_empty=True)

            item.path.unlink()
            assert not item.has_archive(include_empty=False)
            assert not item.has_archive(include_empty=True)

            # (2) Same as the above, except make an empty directory
            #   instead of a file
            assert not item.has_archive(include_empty=False)
            assert not item.has_archive(include_empty=True)
            item.path.mkdir()
            assert item.has_archive(include_empty=False)
            assert item.has_archive(include_empty=True)

            item.path.rmdir()
            assert not item.has_archive(include_empty=False)
            assert not item.has_archive(include_empty=True)

            # (3) Rather than make a empty file or dir at the path, let's
            #   touch the empty_path and confirm the include_empty option
            #   works

            item.empty_path.touch()
            assert not item.has_archive(include_empty=False)
            assert item.has_archive(include_empty=True)

    def test_delete_archive(
        self,
        df_collection: DFCollection,
    ):
        for item in df_collection.items:
            item: DFCollectionItem
            # (1) Confirm that it doesn't raise an error or anything if we
            #   try to delete files or folders that do not exist
            CollectionItemBase.delete_archive(generic_path=item.path)
            CollectionItemBase.delete_archive(generic_path=item.empty_path)
            CollectionItemBase.delete_archive(generic_path=item.partial_path)

            item.path.touch()
            item.empty_path.touch()
            item.partial_path.touch()

            CollectionItemBase.delete_archive(generic_path=item.path)
            CollectionItemBase.delete_archive(generic_path=item.empty_path)
            CollectionItemBase.delete_archive(generic_path=item.partial_path)

            assert not item.path.exists()
            assert not item.empty_path.exists()
            assert not item.partial_path.exists()

    def test_should_archive(
        self,
        df_collection: DFCollection,
    ):
        schema: DataFrameSchema = df_collection._schema
        aa = schema.metadata[ARCHIVE_AFTER]

        # It shouldn't be None, it can be timedelta(seconds=0)
        assert isinstance(aa, timedelta)

        for item in df_collection.items:
            item: DFCollectionItem

            if datetime.now(tz=UTC) > item.finish + aa:
                assert item.should_archive()
            else:
                assert not item.should_archive()

    @pytest.mark.skip
    def test_set_empty(
        self,
    ):
        pass

    def test_valid_archive(
        self,
        df_collection: DFCollection,
    ):
        # Originally, nothing has been saved or anything.. so confirm it
        #   always comes back as None
        for item in df_collection.items:
            assert not item.valid_archive(generic_path=None, sample=None)

            _path = Path(pjoin(df_collection.archive_path, uuid4().hex))

            # (1) Fail if isfile, but doesn't exist and if we can't read
            #   it as valid ParquetFile
            assert not item.valid_archive(generic_path=_path, sample=None)
            _path.touch()
            assert not item.valid_archive(generic_path=_path, sample=None)
            _path.unlink()

            # (2) Fail if isdir and we can't read it as a valid ParquetFile
            _path.mkdir()
            assert _path.is_dir()
            assert not item.valid_archive(generic_path=_path, sample=None)
            _path.rmdir()

    @pytest.mark.skip
    def test_validate_df(
        self,
    ):
        pass

    @pytest.mark.skip
    def test_from_archive(
        self,
    ):
        pass

    def test__to_dict(
        self,
        df_collection: DFCollection,
    ):

        for item in df_collection.items:
            res = item._to_dict()
            assert isinstance(res, dict)
            assert len(res.keys()) == 6

            assert isinstance(res["should_archive"], bool)
            assert isinstance(res["has_archive"], bool)
            assert isinstance(res["path"], Path)
            assert isinstance(res["filename"], str)

            assert isinstance(res["start"], datetime)
            assert isinstance(res["finish"], datetime)
            assert res["start"] < res["finish"]

    @pytest.mark.skip
    def test_delete_partial(
        self,
    ):
        pass

    @pytest.mark.skip
    def test_cleanup_partials(
        self,
    ):
        pass

    @pytest.mark.skip
    def test_delete_dangling_partials(
        self,
    ):
        pass


@gen_cluster(client=True, nthreads=[("127.0.0.1", 1)])
async def test_client(client, s, worker):
    """c,s,a are all required - the secondary Worker (b) is not required"""

    assert isinstance(client, Client)
    assert isinstance(s, Scheduler)
    assert isinstance(worker, Worker)


@pytest.mark.parametrize(
    argnames="df_collection_data_type, offset",
    argvalues=combo_object(),
)
@gen_cluster(client=True, nthreads=[("127.0.0.1", 1)])
@pytest.mark.anyio
async def test_client_parametrize(
    c, s, w, df_collection_data_type: DFCollectionType, offset: str
):
    """c,s,a are all required - the secondary Worker (b) is not required"""

    assert isinstance(c, Client), f"c is not Client, it's {type(c)}"
    assert isinstance(s, Scheduler), f"s is not Scheduler, it's {type(s)}"
    assert isinstance(w, Worker), f"w is not Worker, it's {type(w)}"

    assert df_collection_data_type is not None
    assert isinstance(offset, str)


# I cannot figure out how to define the parametrize on the Test, but then have
#    sync or async methods within it, with some having or not having the
#    gen_cluster decorator set.


@pytest.mark.parametrize(
    argnames="df_collection_data_type, offset, duration",
    argvalues=list(iter_product(df_collections, ["12h", "10D"], [timedelta(days=15)])),
)
class TestDFCollectionItemFunctionalTest:

    def test_to_archive_and_ddf(
        self,
        client_no_amm: DaskClient,
        df_collection: DFCollection,
        user_factory: Callable[..., User],
        product: Product,
        incite_item_factory: Callable[..., None],
        delete_df_collection: Callable[..., None],
    ):

        if df_collection.data_type in unsupported_mock_types:
            return
        u1: User = user_factory(product=product)

        delete_df_collection(coll=df_collection)
        df_collection._client = client_no_amm

        # Assert that there are no pre-existing archives
        assert df_collection.progress.has_archive.eq(False).all()
        res = df_collection.ddf()
        assert res is None

        delete_df_collection(coll=df_collection)
        for item in df_collection.items:
            item: DFCollectionItem

            incite_item_factory(user=u1, item=item)
            item.initial_load()

            # I know it seems weird to delete items from the database before we
            #   proceed with the test. However, the content should have already
            #   been saved out into an parquet at this point, and I am too lazy
            #   to write a separate teardown for a collection (and not a
            #   single Item)

        # Now that we went ahead with the initial_load, Assert that all
        # items have archives files saved
        assert isinstance(df_collection.progress, pd.DataFrame)
        assert df_collection.progress.has_archive.eq(True).all()

        ddf = df_collection.ddf()
        shape = df_collection._client.compute(collections=ddf.shape, sync=True)
        assert shape[0] > 5

    def test_filesize_estimate(
        self,
        df_collection: DFCollection,
        user_factory: Callable[..., User],
        product: Product,
        incite_item_factory: Callable[..., None],
        delete_df_collection: Callable[..., None],
    ):
        """A functional test to write some Parquet files for the
        DFCollection and then confirm that the files get written
        correctly.

        Confirm the files are written correctly by:
            (1) Validating their passing the pandera schema
            (2) The file or dir has an expected size on disk
        """
        import os

        import pyarrow.parquet as pq

        if df_collection.data_type in unsupported_mock_types:
            return
        delete_df_collection(coll=df_collection)
        u1: User = user_factory(product=product)

        # Pick 3 random items to sample for correct filesize
        for item in df_collection.items:
            item: DFCollectionItem

            incite_item_factory(user=u1, item=item)
            item.initial_load(overwrite=True)

            total_bytes = 0
            for fp in pq.ParquetDataset(item.path).files:
                total_bytes += os.stat(fp).st_size

            total_mb = total_bytes / 1_048_576

            assert total_bytes > 1_000
            assert total_mb < 1

    def test_to_archive_client(
        self,
        client_no_amm: DaskClient,
        df_collection: DFCollection,
        user_factory: Callable[..., User],
        product: Product,
        incite_item_factory: Callable[..., None],
        delete_df_collection: Callable[..., None],
    ):

        delete_df_collection(coll=df_collection)
        df_collection._client = client_no_amm
        u1: User = user_factory(product=product)

        for item in df_collection.items:
            item: DFCollectionItem

            if df_collection.data_type in unsupported_mock_types:
                continue

            incite_item_factory(user=u1, item=item)

            # Load up the data that we'll be using for various to_archive
            #   methods. Will always be empty pd.DataFrames for now...
            df = item.from_db()
            ddf = dd.from_pandas(df, npartitions=1)
            assert isinstance(ddf, dd.DataFrame)

            # (1) Write the basic archive, the issue is that because it's
            #   an empty pd.DataFrame, it never makes an actual parquet file
            assert not item.has_archive()
            saved = item.to_archive(ddf=ddf, is_partial=False, overwrite=False)
            assert saved
            assert item.has_archive(include_empty=True)

    @pytest.mark.skip
    def test_get_items(
        self,
        df_collection: DFCollection,
    ):
        with pytest.warns(expected_warning=ResourceWarning) as cm:
            df_collection.get_items_last365()
        assert "DFCollectionItem has missing archives" in str(
            [w.message for w in cm.list]
        )

        res = df_collection.get_items_last365()
        assert len(res) == len(df_collection.items)

    def test_saving_protections(
        self,
        df_collection: DFCollection,
        incite_item_factory: Callable[..., None],
        delete_df_collection: Callable[..., None],
        user_factory: Callable[..., User],
        product: Product,
    ):
        """Don't allow creating an archive for data that will likely be
        overwritten or updated
        """

        if df_collection.data_type in unsupported_mock_types:
            return
        u1: User = user_factory(product=product)

        schema: DataFrameSchema = df_collection._schema
        aa = schema.metadata[ARCHIVE_AFTER]
        assert isinstance(aa, timedelta)

        delete_df_collection(df_collection)
        for item in df_collection.items:
            item: DFCollectionItem

            incite_item_factory(user=u1, item=item)

            should_archive = item.should_archive()
            res = item.initial_load()

            # self.assertIn("Cannot create archive for such new data", str(cm.records))

            # .to_archive() will return back True or False depending on if it
            #   was successful. We want to compare that result to the
            #   .should_archive() method result
            assert should_archive == res

    def test_empty_item(
        self,
        df_collection: DFCollection,
        delete_df_collection: Callable[..., None],
    ):
        delete_df_collection(coll=df_collection)

        for item in df_collection.items:
            assert not item.has_empty()
            df: pd.DataFrame = item.from_db()

            # We do this check b/c the Ledger returns back None and
            #   I don't want it to fail when we go to make a ddf
            if df is None:
                item.set_empty()
            else:
                ddf = dd.from_pandas(df, npartitions=1)
                item.to_archive(ddf=ddf)

            assert item.has_empty()

    def test_file_touching(
        self,
        client_no_amm: DaskClient,
        df_collection: DFCollection,
        incite_item_factory: Callable[..., None],
        delete_df_collection: Callable[..., None],
        user_factory: Callable[..., User],
        product: Product,
    ):

        delete_df_collection(coll=df_collection)
        df_collection._client = client_no_amm
        u1: User = user_factory(product=product)

        for item in df_collection.items:
            # Confirm none of the paths exist yet
            assert not item.has_archive()
            assert not item.path_exists(generic_path=item.path)
            assert not item.has_empty()
            assert not item.path_exists(generic_path=item.empty_path)

            if df_collection.data_type in unsupported_mock_types:
                assert not item.has_archive(include_empty=False)
                assert not item.has_empty()
                assert not item.path_exists(generic_path=item.empty_path)
            else:
                incite_item_factory(user=u1, item=item)
                item.initial_load()

                assert item.has_archive(include_empty=False)
                assert item.path_exists(generic_path=item.path)
                assert not item.has_empty()
