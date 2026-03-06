from datetime import datetime, timezone, timedelta
from os.path import exists as pexists, join as pjoin
from pathlib import Path
from uuid import uuid4

import numpy as np
import pandas as pd
import pytest
from _pytest._code.code import ExceptionInfo

from generalresearch.incite.base import CollectionBase
from test_utils.incite.conftest import mnt_filepath

AGO_15min = (datetime.now(tz=timezone.utc) - timedelta(minutes=15)).replace(
    microsecond=0
)
AGO_1HR = (datetime.now(tz=timezone.utc) - timedelta(hours=1)).replace(microsecond=0)
AGO_2HR = (datetime.now(tz=timezone.utc) - timedelta(hours=2)).replace(microsecond=0)


class TestCollectionBase:
    def test_init(self, mnt_filepath):
        instance = CollectionBase(archive_path=mnt_filepath.data_src)
        assert instance.df.empty is True

    def test_init_df(self, mnt_filepath):
        # Only an empty pd.DataFrame can ever be provided
        instance = CollectionBase(
            df=pd.DataFrame({}), archive_path=mnt_filepath.data_src
        )
        assert isinstance(instance.df, pd.DataFrame)

        with pytest.raises(expected_exception=ValueError) as cm:
            cm: ExceptionInfo
            CollectionBase(
                df=pd.DataFrame(columns=[0, 1, 2]), archive_path=mnt_filepath.data_src
            )
        assert "Do not provide a pd.DataFrame" in str(cm.value)

        with pytest.raises(expected_exception=ValueError) as cm:
            cm: ExceptionInfo
            CollectionBase(
                df=pd.DataFrame(np.random.randint(100, size=(1000, 1)), columns=["A"]),
                archive_path=mnt_filepath.data_src,
            )
        assert "Do not provide a pd.DataFrame" in str(cm.value)

    def test_init_start(self, mnt_filepath):
        with pytest.raises(expected_exception=ValueError) as cm:
            cm: ExceptionInfo
            CollectionBase(
                start=datetime.now(tz=timezone.utc) - timedelta(days=10),
                archive_path=mnt_filepath.data_src,
            )
        assert "Collection.start must not have microseconds" in str(cm.value)

        with pytest.raises(expected_exception=ValueError) as cm:
            cm: ExceptionInfo
            tz = timezone(timedelta(hours=-5), "EST")

            CollectionBase(
                start=datetime(year=2000, month=1, day=1, tzinfo=tz),
                archive_path=mnt_filepath.data_src,
            )
        assert "Timezone is not UTC" in str(cm.value)

        instance = CollectionBase(archive_path=mnt_filepath.data_src)
        assert instance.start == datetime(
            year=2018, month=1, day=1, tzinfo=timezone.utc
        )

        with pytest.raises(expected_exception=ValueError) as cm:
            cm: ExceptionInfo
            CollectionBase(
                start=AGO_2HR, offset="3h", archive_path=mnt_filepath.data_src
            )
        assert "Offset must be equal to, or smaller the start timestamp" in str(
            cm.value
        )

    def test_init_archive_path(self, mnt_filepath):
        """DirectoryPath is apparently smart enough to confirm that the
        directory path exists.
        """

        # (1) Basic, confirm an existing path works
        instance = CollectionBase(archive_path=mnt_filepath.data_src)
        assert instance.archive_path == mnt_filepath.data_src

        # (2) It can't point to a file
        file_path = Path(pjoin(mnt_filepath.data_src, f"{uuid4().hex}.zip"))
        assert not pexists(file_path)
        with pytest.raises(expected_exception=ValueError) as cm:
            cm: ExceptionInfo
            CollectionBase(archive_path=file_path)
        assert "Path does not point to a directory" in str(cm.value)

        # (3) It doesn't create the directory if it doesn't exist
        new_path = Path(pjoin(mnt_filepath.data_src, f"{uuid4().hex}/"))
        assert not pexists(new_path)
        with pytest.raises(expected_exception=ValueError) as cm:
            cm: ExceptionInfo
            CollectionBase(archive_path=new_path)
        assert "Path does not point to a directory" in str(cm.value)

    def test_init_offset(self, mnt_filepath):
        with pytest.raises(expected_exception=ValueError) as cm:
            cm: ExceptionInfo
            CollectionBase(offset="1:X", archive_path=mnt_filepath.data_src)
        assert "Invalid offset alias provided. Please review:" in str(cm.value)

        with pytest.raises(expected_exception=ValueError) as cm:
            cm: ExceptionInfo
            CollectionBase(offset=f"59sec", archive_path=mnt_filepath.data_src)
        assert "Must be equal to, or longer than 1 min" in str(cm.value)

        with pytest.raises(expected_exception=ValueError) as cm:
            cm: ExceptionInfo
            CollectionBase(offset=f"{365 * 101}d", archive_path=mnt_filepath.data_src)
        assert "String should have at most 5 characters" in str(cm.value)


class TestCollectionBaseProperties:

    def test_items(self, mnt_filepath):
        with pytest.raises(expected_exception=NotImplementedError) as cm:
            cm: ExceptionInfo
            instance = CollectionBase(archive_path=mnt_filepath.data_src)
            x = instance.items
        assert "Must override" in str(cm.value)

    def test_interval_range(self, mnt_filepath):
        instance = CollectionBase(archive_path=mnt_filepath.data_src)
        # Private method requires the end parameter
        with pytest.raises(expected_exception=AssertionError) as cm:
            cm: ExceptionInfo
            instance._interval_range(end=None)
        assert "an end value must be provided" in str(cm.value)

        # End param must be same as started (which forces utc)
        tz = timezone(timedelta(hours=-5), "EST")
        with pytest.raises(expected_exception=AssertionError) as cm:
            cm: ExceptionInfo
            instance._interval_range(end=datetime.now(tz=tz))
        assert "Timezones must match" in str(cm.value)

        res = instance._interval_range(end=datetime.now(tz=timezone.utc))
        assert isinstance(res, pd.IntervalIndex)
        assert res.closed_left
        assert res.is_non_overlapping_monotonic
        assert res.is_monotonic_increasing
        assert res.is_unique

    def test_interval_range2(self, mnt_filepath):
        instance = CollectionBase(archive_path=mnt_filepath.data_src)
        assert isinstance(instance.interval_range, list)

        # 1 hrs ago has 2 x 30min + the future 30min
        OFFSET = "30min"
        instance = CollectionBase(
            start=AGO_1HR, offset=OFFSET, archive_path=mnt_filepath.data_src
        )
        assert len(instance.interval_range) == 3
        assert instance.interval_range[0][0] == AGO_1HR

        # 1 hrs ago has 1 x 60min + the future 60min
        OFFSET = "60min"
        instance = CollectionBase(
            start=AGO_1HR, offset=OFFSET, archive_path=mnt_filepath.data_src
        )
        assert len(instance.interval_range) == 2

    def test_progress(self, mnt_filepath):
        with pytest.raises(expected_exception=NotImplementedError) as cm:
            cm: ExceptionInfo
            instance = CollectionBase(
                start=AGO_15min, offset="3min", archive_path=mnt_filepath.data_src
            )
            x = instance.progress
        assert "Must override" in str(cm.value)

    def test_progress2(self, mnt_filepath):
        instance = CollectionBase(
            start=AGO_2HR,
            offset="15min",
            archive_path=mnt_filepath.data_src,
        )
        assert instance.df.empty

        with pytest.raises(expected_exception=NotImplementedError) as cm:
            df = instance.progress
        assert "Must override" in str(cm.value)

    def test_items2(self, mnt_filepath):
        """There can't be a test for this because the Items need a path whic
        isn't possible in the generic form
        """
        instance = CollectionBase(
            start=AGO_1HR, offset="5min", archive_path=mnt_filepath.data_src
        )

        with pytest.raises(expected_exception=NotImplementedError) as cm:
            cm: ExceptionInfo
            items = instance.items
        assert "Must override" in str(cm.value)

        # item = items[-3]
        # ddf = instance.ddf(items=[item], include_partial=True, force_rr_latest=False)
        # df = item.validate_ddf(ddf=ddf)
        # assert isinstance(df, pd.DataFrame)
        # assert len(df.columns) == 16
        # assert str(df.product_id.dtype) == "object"
        # assert str(ddf.product_id.dtype) == "string"

    def test_items3(self, mnt_filepath):
        instance = CollectionBase(
            start=AGO_2HR,
            offset="15min",
            archive_path=mnt_filepath.data_src,
        )
        with pytest.raises(expected_exception=NotImplementedError) as cm:
            item = instance.items[0]
        assert "Must override" in str(cm.value)


class TestCollectionBaseMethodsCleanup:
    def test_fetch_force_rr_latest(self, mnt_filepath):
        coll = CollectionBase(archive_path=mnt_filepath.data_src)

        with pytest.raises(expected_exception=Exception) as cm:
            cm: ExceptionInfo
            coll.fetch_force_rr_latest(sources=[])
        assert "Must override" in str(cm.value)

    def test_fetch_all_paths(self, mnt_filepath):
        coll = CollectionBase(archive_path=mnt_filepath.data_src)

        with pytest.raises(expected_exception=NotImplementedError) as cm:
            cm: ExceptionInfo
            coll.fetch_all_paths(
                items=None, force_rr_latest=False, include_partial=False
            )
        assert "Must override" in str(cm.value)


class TestCollectionBaseMethodsCleanup:
    @pytest.mark.skip
    def test_cleanup_partials(self, mnt_filepath):
        instance = CollectionBase(archive_path=mnt_filepath.data_src)
        assert instance.cleanup_partials() is None  # it doesn't return anything

    def test_clear_tmp_archives(self, mnt_filepath):
        instance = CollectionBase(archive_path=mnt_filepath.data_src)
        assert instance.clear_tmp_archives() is None  # it doesn't return anything

    @pytest.mark.skip
    def test_clear_corrupt_archives(self, mnt_filepath):
        """TODO: expand this so it actually has corrupt archives that we
        check to see if they're removed
        """
        instance = CollectionBase(archive_path=mnt_filepath.data_src)
        assert instance.clear_corrupt_archives() is None  # it doesn't return anything

    @pytest.mark.skip
    def test_rebuild_symlinks(self, mnt_filepath):
        instance = CollectionBase(archive_path=mnt_filepath.data_src)
        assert instance.rebuild_symlinks() is None


class TestCollectionBaseMethodsSourceTiming:

    def test_get_item(self, mnt_filepath):
        instance = CollectionBase(archive_path=mnt_filepath.data_src)
        i = pd.Interval(left=1, right=2, closed="left")

        with pytest.raises(expected_exception=NotImplementedError) as cm:
            instance.get_item(interval=i)
        assert "Must override" in str(cm.value)

    def test_get_item_start(self, mnt_filepath):
        instance = CollectionBase(archive_path=mnt_filepath.data_src)

        dt = datetime.now(tz=timezone.utc)
        start = pd.Timestamp(dt)

        with pytest.raises(expected_exception=NotImplementedError) as cm:
            instance.get_item_start(start=start)
        assert "Must override" in str(cm.value)

    def test_get_items(self, mnt_filepath):
        instance = CollectionBase(archive_path=mnt_filepath.data_src)

        dt = datetime.now(tz=timezone.utc)

        with pytest.raises(expected_exception=NotImplementedError) as cm:
            instance.get_items(since=dt)
        assert "Must override" in str(cm.value)

    def test_get_items_from_year(self, mnt_filepath):
        instance = CollectionBase(archive_path=mnt_filepath.data_src)

        with pytest.raises(expected_exception=NotImplementedError) as cm:
            instance.get_items_from_year(year=2020)
        assert "Must override" in str(cm.value)

    def test_get_items_last90(self, mnt_filepath):
        instance = CollectionBase(archive_path=mnt_filepath.data_src)

        with pytest.raises(expected_exception=NotImplementedError) as cm:
            instance.get_items_last90()
        assert "Must override" in str(cm.value)

    def test_get_items_last365(self, mnt_filepath):
        instance = CollectionBase(archive_path=mnt_filepath.data_src)

        with pytest.raises(expected_exception=NotImplementedError) as cm:
            instance.get_items_last365()
        assert "Must override" in str(cm.value)
