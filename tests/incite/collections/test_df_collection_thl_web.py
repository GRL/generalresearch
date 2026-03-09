from datetime import datetime
from itertools import product
from typing import TYPE_CHECKING

import dask.dataframe as dd
import pandas as pd
import pytest
from pandera import DataFrameSchema

from generalresearch.incite.collections import DFCollection, DFCollectionType

if TYPE_CHECKING:
    from generalresearch.incite.base import GRLDatasets


def combo_object():
    for x in product(
        [
            DFCollectionType.USER,
            DFCollectionType.WALL,
            DFCollectionType.SESSION,
            DFCollectionType.TASK_ADJUSTMENT,
            DFCollectionType.AUDIT_LOG,
            DFCollectionType.LEDGER,
        ],
        ["30min", "1H"],
    ):
        yield x


@pytest.mark.parametrize(
    argnames="df_collection_data_type, offset", argvalues=combo_object()
)
class TestDFCollection_thl_web:

    def test_init(self, df_collection_data_type, offset: str, df_collection):
        assert isinstance(df_collection_data_type, DFCollectionType)
        assert isinstance(df_collection, DFCollection)


@pytest.mark.parametrize(
    argnames="df_collection_data_type, offset", argvalues=combo_object()
)
class TestDFCollection_thl_web_Properties:

    def test_items(self, df_collection_data_type, offset: str, df_collection):
        assert isinstance(df_collection.items, list)
        for i in df_collection.items:
            assert i._collection == df_collection

    def test__schema(self, df_collection_data_type, offset: str, df_collection):
        assert isinstance(df_collection._schema, DataFrameSchema)


@pytest.mark.parametrize(
    argnames="df_collection_data_type, offset", argvalues=combo_object()
)
class TestDFCollection_thl_web_BaseProperties:

    @pytest.mark.skip
    def test__interval_range(self, df_collection_data_type, offset: str, df_collection):
        pass

    def test_interval_start(self, df_collection_data_type, offset: str, df_collection):
        assert isinstance(df_collection.interval_start, datetime)

    def test_interval_range(self, df_collection_data_type, offset: str, df_collection):
        assert isinstance(df_collection.interval_range, list)

    def test_progress(self, df_collection_data_type, offset: str, df_collection):
        assert isinstance(df_collection.progress, pd.DataFrame)


@pytest.mark.parametrize(
    argnames="df_collection_data_type, offset", argvalues=combo_object()
)
class TestDFCollection_thl_web_Methods:

    @pytest.mark.skip
    def test_initial_loads(self, df_collection_data_type, df_collection, offset):
        pass

    @pytest.mark.skip
    def test_fetch_force_rr_latest(
        self, df_collection_data_type, df_collection, offset: str
    ):
        pass

    @pytest.mark.skip
    def test_force_rr_latest(self, df_collection_data_type, df_collection, offset):
        pass


@pytest.mark.parametrize(
    argnames="df_collection_data_type, offset", argvalues=combo_object()
)
class TestDFCollection_thl_web_BaseMethods:

    def test_fetch_all_paths(self, df_collection_data_type, offset: str, df_collection):
        res = df_collection.fetch_all_paths(
            items=None, force_rr_latest=False, include_partial=False
        )
        assert isinstance(res, list)

    @pytest.mark.skip
    def test_ddf(self, df_collection_data_type, offset: str, df_collection):
        res = df_collection.ddf()
        assert isinstance(res, dd.DataFrame)

    # -- cleanup --
    @pytest.mark.skip
    def test_schedule_cleanup(
        self, df_collection_data_type, offset: str, df_collection
    ):
        pass

    @pytest.mark.skip
    def test_cleanup(self, df_collection_data_type, offset: str, df_collection):
        pass

    @pytest.mark.skip
    def test_cleanup_partials(
        self, df_collection_data_type, offset: str, df_collection
    ):
        pass

    @pytest.mark.skip
    def test_clear_tmp_archives(
        self, df_collection_data_type, offset: str, df_collection
    ):
        pass

    @pytest.mark.skip
    def test_clear_corrupt_archives(
        self, df_collection_data_type, offset: str, df_collection
    ):
        pass

    @pytest.mark.skip
    def test_rebuild_symlinks(
        self, df_collection_data_type, offset: str, df_collection
    ):
        pass

    # -- Source timing --
    @pytest.mark.skip
    def test_get_item(self, df_collection_data_type, offset: str, df_collection):
        pass

    @pytest.mark.skip
    def test_get_item_start(self, df_collection_data_type, offset: str, df_collection):
        pass

    @pytest.mark.skip
    def test_get_items(self, df_collection_data_type, offset: str, df_collection):
        # If we get all the items from the start of the collection, it
        #   should include all the items!
        res1 = df_collection.items
        res2 = df_collection.get_items(since=df_collection.start)
        assert len(res1) == len(res2)

    @pytest.mark.skip
    def test_get_items_from_year(
        self, df_collection_data_type, offset: str, df_collection
    ):
        pass

    @pytest.mark.skip
    def test_get_items_last90(
        self, df_collection_data_type, offset: str, df_collection
    ):
        pass

    @pytest.mark.skip
    def test_get_items_last365(
        self, df_collection_data_type, offset: str, df_collection
    ):
        pass
