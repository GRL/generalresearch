from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING

import pytest

from generalresearch.incite.collections.base import (
    MYSQL_ALLOWED_COLL_TYPES,
    DFCollection,
    DFCollectionItem,
    DFCollectionType,
)

if TYPE_CHECKING:
    from generalresearch.incite.base import GRLDatasets
    from generalresearch.pg_helper import PostgresConfig


@pytest.mark.parametrize("df_coll_type", MYSQL_ALLOWED_COLL_TYPES)
class TestDFCollectionItemBase:
    def test_init(self, mnt_filepath: GRLDatasets, df_coll_type: DFCollectionType):
        collection = DFCollection(
            data_type=df_coll_type,
            offset="100D",
            start=datetime(year=1800, month=1, day=1, tzinfo=UTC),
            finished=datetime(year=1900, month=1, day=1, tzinfo=UTC),
            archive_path=mnt_filepath.archive_path(enum_type=df_coll_type),
        )

        item = DFCollectionItem()
        item._collection = collection

        assert isinstance(item, DFCollectionItem)


@pytest.mark.parametrize("df_coll_type", MYSQL_ALLOWED_COLL_TYPES)
class TestDFCollectionItemProperties:
    @pytest.mark.skip
    def test_filename(self, df_coll_type: DFCollectionType):
        pass


@pytest.mark.parametrize("df_coll_type", MYSQL_ALLOWED_COLL_TYPES)
class TestDFCollectionItemMethods:
    def test_has_mysql_false(
        self, mnt_filepath: GRLDatasets, df_coll_type: DFCollectionType
    ):
        collection = DFCollection(
            data_type=df_coll_type,
            offset="100D",
            start=datetime(year=1800, month=1, day=1, tzinfo=UTC),
            finished=datetime(year=1900, month=1, day=1, tzinfo=UTC),
            archive_path=mnt_filepath.archive_path(enum_type=df_coll_type),
        )

        instance1: DFCollectionItem = collection.items[0]
        assert not instance1.has_mysql()

    def test_has_mysql_true(
        self,
        thl_web_rr: PostgresConfig,
        mnt_filepath: GRLDatasets,
        df_coll_type: DFCollectionType,
    ):
        collection = DFCollection(
            data_type=df_coll_type,
            offset="100D",
            start=datetime(year=1800, month=1, day=1, tzinfo=UTC),
            finished=datetime(year=1900, month=1, day=1, tzinfo=UTC),
            archive_path=mnt_filepath.archive_path(enum_type=df_coll_type),
            pg_config=thl_web_rr,
        )

        # Has RR, assume unittest server is online
        instance2: DFCollectionItem = collection.items[0]
        assert instance2.has_mysql()

    @pytest.mark.skip
    def test_update_partial_archive(self, df_coll_type: DFCollectionType):
        pass
