from datetime import datetime, timezone, timedelta
from itertools import product
from pathlib import PurePath

import pytest

from generalresearch.incite.mergers import MergeCollectionItem, MergeType
from generalresearch.incite.mergers.foundations.enriched_session import (
    EnrichedSessionMerge,
)
from generalresearch.incite.mergers.foundations.enriched_wall import (
    EnrichedWallMerge,
)
from test_utils.incite.mergers.conftest import merge_collection


@pytest.mark.parametrize(
    argnames="merge_type, offset, duration",
    argvalues=list(
        product(
            [MergeType.ENRICHED_SESSION, MergeType.ENRICHED_WALL],
            ["1h"],
            [timedelta(days=1)],
        )
    ),
)
class TestMergeCollectionItem:

    def test_file_naming(self, merge_collection, offset, duration, start):
        assert len(merge_collection.items) == 25

        items: list[MergeCollectionItem] = merge_collection.items

        for i in items:
            i: MergeCollectionItem

            assert isinstance(i.path, PurePath)
            assert i.path.name == i.filename

            assert i._collection.merge_type.name.lower() in i.filename
            assert i._collection.offset in i.filename
            assert i.start.strftime("%Y-%m-%d-%H-%M-%S") in i.filename

    def test_archives(self, merge_collection, offset, duration, start):
        assert len(merge_collection.items) == 25

        for i in merge_collection.items:
            assert not i.has_archive()
            assert not i.has_empty()
            assert not i.is_empty()
            assert not i.has_partial_archive()
            assert i.has_archive() == i.path_exists(generic_path=i.path)

        res = set([i.should_archive() for i in merge_collection.items])
        assert len(res) == 1

    def test_item_to_archive(self, merge_collection, offset, duration, start):
        for item in merge_collection.items:
            item: MergeCollectionItem
            assert not item.has_archive()

            # TODO: setup build methods
            # ddf = self.build
            # saved = instance.to_archive(ddf=ddf)
            # self.assertTrue(saved)
            # self.assertTrue(instance.has_archive())
