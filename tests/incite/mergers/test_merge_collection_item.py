from __future__ import annotations

from datetime import timedelta
from itertools import product
from pathlib import PurePath
from typing import TYPE_CHECKING

import pytest

from generalresearch.incite.mergers.base import MergeType

if TYPE_CHECKING:
    from generalresearch.incite.mergers.base import (
        MergeCollection,
        MergeCollectionItem,
    )


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

    def test_file_naming(
        self,
        merge_collection: MergeCollection,
    ):
        assert len(merge_collection.items) == 25

        items: list[MergeCollectionItem] = merge_collection.items

        for i in items:
            i: MergeCollectionItem

            assert isinstance(i.path, PurePath)
            assert i.path.name == i.filename

            assert i._collection.merge_type.name.lower() in i.filename
            assert i._collection.offset in i.filename
            assert i.start.strftime("%Y-%m-%d-%H-%M-%S") in i.filename

    def test_archives(
        self,
        merge_collection: MergeCollection,
    ):
        assert len(merge_collection.items) == 25

        for i in merge_collection.items:
            assert not i.has_archive()
            assert not i.has_empty()
            assert not i.is_empty()
            assert not i.has_partial_archive()
            assert i.has_archive() == i.path_exists(generic_path=i.path)

        res = {i.should_archive() for i in merge_collection.items}
        assert len(res) == 1

    def test_item_to_archive(
        self,
        merge_collection: MergeCollection,
    ):
        for item in merge_collection.items:
            item: MergeCollectionItem
            assert not item.has_archive()

            # TODO: setup build methods
            # ddf = self.build
            # saved = instance.to_archive(ddf=ddf)
            # self.assertTrue(saved)
            # self.assertTrue(instance.has_archive())
