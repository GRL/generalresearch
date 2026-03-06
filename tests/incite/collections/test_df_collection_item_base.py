from datetime import datetime, timezone

import pytest

from generalresearch.incite.collections import (
    DFCollectionType,
    DFCollectionItem,
    DFCollection,
)
from test_utils.incite.conftest import mnt_filepath

df_collection_types = [e for e in DFCollectionType if e is not DFCollectionType.TEST]


@pytest.mark.parametrize("df_coll_type", df_collection_types)
class TestDFCollectionItemBase:

    def test_init(self, mnt_filepath, df_coll_type):
        collection = DFCollection(
            data_type=df_coll_type,
            offset="100d",
            start=datetime(year=1800, month=1, day=1, tzinfo=timezone.utc),
            finished=datetime(year=1900, month=1, day=1, tzinfo=timezone.utc),
            archive_path=mnt_filepath.archive_path(enum_type=df_coll_type),
        )

        item = DFCollectionItem()
        item._collection = collection

        assert isinstance(item, DFCollectionItem)


@pytest.mark.parametrize("df_coll_type", df_collection_types)
class TestDFCollectionItemProperties:

    @pytest.mark.skip
    def test_filename(self, df_coll_type):
        pass


@pytest.mark.parametrize("df_coll_type", df_collection_types)
class TestDFCollectionItemMethods:

    def test_has_mysql_false(self, mnt_filepath, df_coll_type):
        collection = DFCollection(
            data_type=df_coll_type,
            offset="100d",
            start=datetime(year=1800, month=1, day=1, tzinfo=timezone.utc),
            finished=datetime(year=1900, month=1, day=1, tzinfo=timezone.utc),
            archive_path=mnt_filepath.archive_path(enum_type=df_coll_type),
        )

        instance1: DFCollectionItem = collection.items[0]
        assert not instance1.has_mysql()

    def test_has_mysql_true(self, thl_web_rr, mnt_filepath, df_coll_type):
        collection = DFCollection(
            data_type=df_coll_type,
            offset="100d",
            start=datetime(year=1800, month=1, day=1, tzinfo=timezone.utc),
            finished=datetime(year=1900, month=1, day=1, tzinfo=timezone.utc),
            archive_path=mnt_filepath.archive_path(enum_type=df_coll_type),
            pg_config=thl_web_rr,
        )

        # Has RR, assume unittest server is online
        instance2: DFCollectionItem = collection.items[0]
        assert instance2.has_mysql()

    @pytest.mark.skip
    def test_update_partial_archive(self, df_coll_type):
        pass
