from datetime import datetime, timezone
from os.path import join as pjoin
from pathlib import Path
from uuid import uuid4

import dask.dataframe as dd
import pandas as pd
import pytest
from pydantic import ValidationError

from generalresearch.incite.base import CollectionItemBase


class TestCollectionItemBase:
    def test_init(self):
        dt = datetime.now(tz=timezone.utc).replace(microsecond=0)

        instance = CollectionItemBase()
        instance2 = CollectionItemBase(start=dt)

        assert isinstance(instance, CollectionItemBase)
        assert isinstance(instance2, CollectionItemBase)

        assert instance.start.second == instance2.start.second
        assert 0 == instance.start.microsecond == instance2.start.microsecond

    def test_init_start(self):
        dt = datetime.now(tz=timezone.utc)

        with pytest.raises(expected_exception=ValidationError) as cm:
            CollectionItemBase(start=dt)

        assert "CollectionItem.start must not have microsecond precision" in str(
            cm.value
        )


class TestCollectionItemBaseProperties:

    def test_finish(self):
        instance = CollectionItemBase()

        with pytest.raises(expected_exception=AttributeError) as cm:
            res = instance.finish

    def test_interval(self):
        instance = CollectionItemBase()

        with pytest.raises(expected_exception=AttributeError) as cm:
            res = instance.interval

    def test_filename(self):
        instance = CollectionItemBase()

        with pytest.raises(expected_exception=NotImplementedError) as cm:
            res = instance.filename

        assert "Do not use CollectionItemBase directly" in str(cm.value)

    def test_partial_filename(self):
        instance = CollectionItemBase()

        with pytest.raises(expected_exception=NotImplementedError) as cm:
            res = instance.filename

        assert "Do not use CollectionItemBase directly" in str(cm.value)

    def test_empty_filename(self):
        instance = CollectionItemBase()

        with pytest.raises(expected_exception=NotImplementedError) as cm:
            res = instance.filename

        assert "Do not use CollectionItemBase directly" in str(cm.value)

    def test_path(self):
        instance = CollectionItemBase()

        with pytest.raises(expected_exception=AttributeError) as cm:
            res = instance.path

    def test_partial_path(self):
        instance = CollectionItemBase()

        with pytest.raises(expected_exception=AttributeError) as cm:
            res = instance.partial_path

    def test_empty_path(self):
        instance = CollectionItemBase()

        with pytest.raises(expected_exception=AttributeError) as cm:
            res = instance.empty_path


class TestCollectionItemBaseMethods:

    @pytest.mark.skip
    def test_next_numbered_path(self):
        pass

    @pytest.mark.skip
    def test_search_highest_numbered_path(self):
        pass

    def test_tmp_filename(self):
        instance = CollectionItemBase()

        with pytest.raises(expected_exception=NotImplementedError) as cm:
            res = instance.tmp_filename()
        assert "Do not use CollectionItemBase directly" in str(cm.value)

    def test_tmp_path(self):
        instance = CollectionItemBase()

        with pytest.raises(expected_exception=AttributeError) as cm:
            res = instance.tmp_path()

    def test_is_empty(self):
        instance = CollectionItemBase()

        with pytest.raises(expected_exception=AttributeError) as cm:
            res = instance.is_empty()

    def test_has_empty(self):
        instance = CollectionItemBase()

        with pytest.raises(expected_exception=AttributeError) as cm:
            res = instance.has_empty()

    def test_has_partial_archive(self):
        instance = CollectionItemBase()

        with pytest.raises(expected_exception=AttributeError) as cm:
            res = instance.has_partial_archive()

    @pytest.mark.parametrize("include_empty", [True, False])
    def test_has_archive(self, include_empty):
        instance = CollectionItemBase()

        with pytest.raises(expected_exception=AttributeError) as cm:
            res = instance.has_archive(include_empty=include_empty)

    def test_delete_archive_file(self, mnt_filepath):
        path1 = Path(pjoin(mnt_filepath.data_src, f"{uuid4().hex}.zip"))

        # Confirm it doesn't exist, and that delete_archive() doesn't throw
        #   an error when trying to delete a non-existent file or folder
        assert not path1.exists()
        CollectionItemBase.delete_archive(generic_path=path1)
        # TODO: LOG.warning(f"tried removing non-existent file: {generic_path}")

        # Create it, confirm it exists, delete it, and confirm it doesn't exist
        path1.touch()
        assert path1.exists()
        CollectionItemBase.delete_archive(generic_path=path1)
        assert not path1.exists()

    def test_delete_archive_dir(self, mnt_filepath):
        path1 = Path(pjoin(mnt_filepath.data_src, f"{uuid4().hex}"))

        # Confirm it doesn't exist, and that delete_archive() doesn't throw
        #   an error when trying to delete a non-existent file or folder
        assert not path1.exists()
        CollectionItemBase.delete_archive(generic_path=path1)
        # TODO: LOG.warning(f"tried removing non-existent file: {generic_path}")

        # Create it, confirm it exists, delete it, and confirm it doesn't exist
        path1.mkdir()
        assert path1.exists()
        assert path1.is_dir()
        CollectionItemBase.delete_archive(generic_path=path1)
        assert not path1.exists()

    def test_should_archive(self):
        instance = CollectionItemBase()

        with pytest.raises(expected_exception=AttributeError) as cm:
            res = instance.should_archive()

    def test_set_empty(self):
        instance = CollectionItemBase()

        with pytest.raises(expected_exception=AttributeError) as cm:
            res = instance.set_empty()

    def test_valid_archive(self):
        instance = CollectionItemBase()

        with pytest.raises(expected_exception=AttributeError) as cm:
            res = instance.valid_archive(generic_path=None, sample=None)


class TestCollectionItemBaseMethodsORM:

    @pytest.mark.skip
    def test_from_archive(self):
        pass

    @pytest.mark.parametrize("is_partial", [True, False])
    def test_to_archive(self, is_partial):
        instance = CollectionItemBase()

        with pytest.raises(expected_exception=NotImplementedError) as cm:
            res = instance.to_archive(
                ddf=dd.from_pandas(data=pd.DataFrame()), is_partial=is_partial
            )
        assert "Must override" in str(cm.value)

    @pytest.mark.skip
    def test__to_dict(self):
        pass

    @pytest.mark.skip
    def test_delete_partial(self):
        pass

    @pytest.mark.skip
    def test_cleanup_partials(self):
        pass

    @pytest.mark.skip
    def test_delete_dangling_partials(self):
        pass
