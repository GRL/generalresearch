from datetime import datetime, timezone
from typing import TYPE_CHECKING

import pandas as pd
import pytest
from pandera import DataFrameSchema

from generalresearch.incite.collections import (
    DFCollection,
    DFCollectionType,
)
from test_utils.incite.conftest import mnt_filepath

if TYPE_CHECKING:
    from generalresearch.incite.base import GRLDatasets

df_collection_types = [e for e in DFCollectionType if e is not DFCollectionType.TEST]


@pytest.mark.parametrize("df_coll_type", df_collection_types)
class TestDFCollectionBase:
    """None of these tests are about the DFCollection with any specific
    data_type... that will be handled in other parameterized tests

    """

    def test_init(self, mnt_filepath: "GRLDatasets", df_coll_type: DFCollectionType):
        """Try to initialize the DFCollection with various invalid parameters"""
        with pytest.raises(expected_exception=ValueError) as cm:
            DFCollection(archive_path=mnt_filepath.data_src)
        assert "Must explicitly provide a data_type" in str(cm.value)

        # with pytest.raises(expected_exception=ValueError) as cm:
        #     DFCollection(
        #         data_type=DFCollectionType.TEST, archive_path=mnt_filepath.data_src
        #     )
        # assert "Must provide a supported data_type" in str(cm.value)

        instance = DFCollection(
            data_type=DFCollectionType.WALL, archive_path=mnt_filepath.data_src
        )
        assert instance.data_type == DFCollectionType.WALL


@pytest.mark.parametrize("df_coll_type", df_collection_types)
class TestDFCollectionBaseProperties:

    @pytest.mark.skip
    def test_df_collection_items(self, mnt_filepath: "GRLDatasets", df_coll_type):
        instance = DFCollection(
            data_type=df_coll_type,
            start=datetime(year=1800, month=1, day=1, tzinfo=timezone.utc),
            finished=datetime(year=1900, month=1, day=1, tzinfo=timezone.utc),
            offset="100d",
            archive_path=mnt_filepath.archive_path(enum_type=df_coll_type),
        )

        assert len(instance.interval_range) == len(instance.items)
        assert len(instance.items) == 366

    def test_df_collection_progress(self, mnt_filepath: "GRLDatasets", df_coll_type):
        instance = DFCollection(
            data_type=df_coll_type,
            start=datetime(year=1800, month=1, day=1, tzinfo=timezone.utc),
            finished=datetime(year=1900, month=1, day=1, tzinfo=timezone.utc),
            offset="100d",
            archive_path=mnt_filepath.archive_path(enum_type=df_coll_type),
        )

        # Progress returns a dataframe with a row each Item
        assert isinstance(instance.progress, pd.DataFrame)
        assert instance.progress.shape == (366, 6)

    def test_df_collection_schema(self, mnt_filepath: "GRLDatasets", df_coll_type):
        instance1 = DFCollection(
            data_type=DFCollectionType.WALL, archive_path=mnt_filepath.data_src
        )

        instance2 = DFCollection(
            data_type=DFCollectionType.SESSION, archive_path=mnt_filepath.data_src
        )

        assert instance1._schema != instance2._schema
        assert isinstance(instance1._schema, DataFrameSchema)
        assert isinstance(instance2._schema, DataFrameSchema)


class TestDFCollectionBaseMethods:

    @pytest.mark.skip
    def test_initial_load(self, mnt_filepath: "GRLDatasets", thl_web_rr):
        instance = DFCollection(
            pg_config=thl_web_rr,
            data_type=DFCollectionType.USER,
            start=datetime(year=2022, month=1, day=1, minute=0, tzinfo=timezone.utc),
            finished=datetime(year=2022, month=1, day=1, minute=5, tzinfo=timezone.utc),
            offset="2min",
            archive_path=mnt_filepath.data_src,
        )

        # Confirm that there are no archives available yet
        assert instance.progress.has_archive.eq(False).all()

        instance.initial_load()
        assert 47 == len(instance.ddf().index)
        assert instance.progress.should_archive.eq(True).all()

        # A few archives should have been made
        assert not instance.progress.has_archive.eq(False).all()

    @pytest.mark.skip
    def test_fetch_force_rr_latest(self):
        pass

    @pytest.mark.skip
    def test_force_rr_latest(self):
        pass
