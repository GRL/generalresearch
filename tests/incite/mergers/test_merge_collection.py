from datetime import datetime, timezone, timedelta
from itertools import product

import pandas as pd
import pytest
from pandera import DataFrameSchema

from generalresearch.incite.mergers import (
    MergeCollection,
    MergeType,
)
from test_utils.incite.conftest import mnt_filepath

merge_types = list(e for e in MergeType if e != MergeType.TEST)


@pytest.mark.parametrize(
    argnames="merge_type, offset, duration, start",
    argvalues=list(
        product(
            merge_types,
            ["5min", "6h", "14D"],
            [timedelta(days=30)],
            [
                (datetime.now(tz=timezone.utc) - timedelta(days=35)).replace(
                    microsecond=0
                )
            ],
        )
    ),
)
class TestMergeCollection:

    def test_init(self, mnt_filepath, merge_type, offset, duration, start):
        with pytest.raises(expected_exception=ValueError) as cm:
            MergeCollection(archive_path=mnt_filepath.data_src)
        assert "Must explicitly provide a merge_type" in str(cm.value)

        instance = MergeCollection(
            merge_type=merge_type,
            archive_path=mnt_filepath.archive_path(enum_type=merge_type),
        )
        assert instance.merge_type == merge_type

    def test_items(self, mnt_filepath, merge_type, offset, duration, start):
        instance = MergeCollection(
            merge_type=merge_type,
            offset=offset,
            start=start,
            finished=start + duration,
            archive_path=mnt_filepath.archive_path(enum_type=merge_type),
        )

        assert len(instance.interval_range) == len(instance.items)

    def test_progress(self, mnt_filepath, merge_type, offset, duration, start):
        instance = MergeCollection(
            merge_type=merge_type,
            offset=offset,
            start=start,
            finished=start + duration,
            archive_path=mnt_filepath.archive_path(enum_type=merge_type),
        )

        assert isinstance(instance.progress, pd.DataFrame)
        assert instance.progress.shape[0] > 0
        assert instance.progress.shape[1] == 7
        assert instance.progress["group_by"].isnull().all()

    def test_schema(self, mnt_filepath, merge_type, offset, duration, start):
        instance = MergeCollection(
            merge_type=merge_type,
            archive_path=mnt_filepath.archive_path(enum_type=merge_type),
        )

        assert isinstance(instance._schema, DataFrameSchema)

    def test_load(self, mnt_filepath, merge_type, offset, duration, start):
        instance = MergeCollection(
            merge_type=merge_type,
            start=start,
            finished=start + duration,
            offset=offset,
            archive_path=mnt_filepath.archive_path(enum_type=merge_type),
        )

        # Confirm that there are no archives available yet
        assert instance.progress.has_archive.eq(False).all()

    def test_get_items(self, mnt_filepath, merge_type, offset, duration, start):
        instance = MergeCollection(
            start=start,
            finished=start + duration,
            offset=offset,
            merge_type=merge_type,
            archive_path=mnt_filepath.archive_path(enum_type=merge_type),
        )

        # with pytest.raises(expected_exception=ResourceWarning) as cm:
        res = instance.get_items_last365()
        # assert "has missing archives", str(cm.value)
        assert len(res) == len(instance.items)
