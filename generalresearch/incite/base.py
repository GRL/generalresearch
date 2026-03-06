from __future__ import annotations

import glob
import logging
import os
import re
import shutil
import subprocess
import warnings
from concurrent.futures import Future
from datetime import datetime, timezone, timedelta
from os import access, R_OK, listdir
from os.path import join as pjoin, isdir
from pathlib import Path
from sys import platform
from typing import (
    Optional,
    Tuple,
    List,
    Sequence,
    Any,
    Union,
    Callable,
    TYPE_CHECKING,
)
from uuid import uuid4

import dask
import dask.dataframe as dd
import pandas as pd
import pyarrow.parquet as pq
from distributed import Client
from pandera import DataFrameSchema
from pydantic import (
    BaseModel,
    ConfigDict,
    DirectoryPath,
    PrivateAttr,
    Field,
    model_validator,
    FilePath,
    field_validator,
    ValidationInfo,
)
from pydantic.json_schema import SkipJsonSchema
from sentry_sdk import capture_exception
from typing_extensions import Self

from generalresearch.config import is_debug
from generalresearch.incite.schemas import (
    ARCHIVE_AFTER,
    empty_dataframe_from_schema,
)
from generalresearch.models.custom_types import AwareDatetimeISO

if TYPE_CHECKING:
    from generalresearch.incite.mergers import MergeType, MergeCollection
    from generalresearch.incite.collections.thl_marketplaces import (
        DFCollectionType,
    )
    from generalresearch.incite.collections import DFCollection

    Collection = Union[DFCollection, MergeCollection]

logging.basicConfig()
LOG = logging.getLogger()

# Item = Union["DFCollectionItem", "MergeCollectionItem"]
Item = Any
Items = Sequence[Item]
DT_STR = "%Y-%m-%d %H:%M:%S"


class NFSMount(BaseModel):
    address: str = Field(default="127.0.0.1")
    point: str = Field(default="grl-data-example")


class GRLDatasets(BaseModel):
    """
    The "idea" of this class is to manage the Mount point, or source of
    where Sambda or NFS data may be coming from.. I don't think it needs
    to manage individual folders directly, but it should be aware of if
    a drive is mounted, if it has correct permissions.

    Each field maps to a single network mount..
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    data_src: Optional[Path] = Field(default=None)
    incite: Optional[NFSMount] = Field(default=None)

    @model_validator(mode="after")
    def check_data_src_and_et_path(self) -> Self:
        from generalresearch.incite.mergers import MergeType
        from generalresearch.incite.collections.thl_marketplaces import (
            DFCollectionType,
        )

        # Create the base folders and confirm we have read access
        self.data_src.mkdir(parents=True, exist_ok=True)
        assert access(
            path=self.data_src, mode=R_OK
        ), f"can't access data_src: {self.data_src}"

        for enum_type in [MergeType, DFCollectionType]:
            for et in enum_type:
                et: MergeType | DFCollectionType

                if not is_debug() and "test" in et.value:
                    continue

                p = self.archive_path(enum_type=et)

                if is_debug():
                    # Try to make any of them
                    p.mkdir(parents=True, exist_ok=True)

                assert access(path=p, mode=R_OK), f"Cannot read {p}"
        return self

    def archive_path(self, enum_type: Union["MergeType", "DFCollectionType"]) -> Path:
        """
        TODO: Extend this so that it takes any type of Enum and that
            inputs in the correct parent dir for the respective Enum
            type..
        """

        from generalresearch.incite.mergers import MergeType

        folder = "mergers" if isinstance(enum_type, MergeType) else "raw/df-collections"
        return Path(
            pjoin(self.data_src, self.incite.point, folder, str(enum_type.value))
        )

    def has_data(self, enum_type: Union["MergeType", "DFCollectionType"]) -> bool:
        path_dir = self.archive_path(enum_type=enum_type)
        if isdir(path_dir):
            return bool(listdir(path_dir))
        else:
            return False


class CollectionBase(BaseModel):
    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        validate_assignment=True,
        # This is needed to auto assign a dask client
        validate_default=True,
        extra="forbid",
    )

    archive_path: DirectoryPath = Field(default="/tmp/")
    df: SkipJsonSchema[pd.DataFrame] = Field(
        default_factory=lambda: pd.DataFrame(), exclude=True
    )

    # I want to intentionally keep these as native python types, and not
    #   pandas specific types. This could also be called "duration" or
    #   "ItemSize". It is the length of time a CollectionItem stores.
    offset: str = Field(default="72h", max_length=5)

    start: AwareDatetimeISO = Field(
        default=datetime(year=2018, month=1, day=1, tzinfo=timezone.utc),
        description="This is the starting point in which data will be retrieved"
        "in chunks from.",
        frozen=True,
    )

    finished: Optional[AwareDatetimeISO] = Field(
        default=None,
        description="Finished is only set if we don't want a rolling window",
    )

    _client: Optional[Client] = PrivateAttr(default=None)

    # --- Validators ---
    @model_validator(mode="before")
    @classmethod
    def check_model_before(cls, data: Any) -> Any:

        assert isinstance(data, dict), "check_model_before.isinstance(data, dict)"

        # We must be able to read from the archive_path
        ap = data.get("archive_path", None)
        assert isinstance(ap, Path), "check_model_before.isinstance(ap, Path)"

        if not ap.is_dir():
            raise ValueError(f"Path does not point to a directory")

        if not access(path=ap, mode=R_OK):
            raise ValueError(f"Cannot read archive_path")

        df: Optional[pd.DataFrame] = data.get("df", None)
        if df is not None:
            if not df.empty or len(df.columns) != 0:
                raise ValueError("Do not provide a pd.DataFrame")

        return data

    @model_validator(mode="after")
    def check_model_after(self) -> Self:
        if self.offset is None or self.start is None:
            return self

        offset_total_sec = pd.Timedelta(self.offset).total_seconds()
        start_total_sec = (datetime.now(tz=timezone.utc) - self.start).total_seconds()

        if offset_total_sec > start_total_sec:
            raise ValueError("Offset must be equal to, or smaller the start timestamp")

        return self

    @field_validator("start")
    def check_start(
        cls, start: Optional[datetime], info: ValidationInfo
    ) -> Optional[datetime]:
        if start and start.microsecond != 0:
            raise ValueError("Collection.start must not have microseconds")
        return start

    @field_validator("offset")
    def check_offset(cls, v: Optional[str], info: ValidationInfo):
        # pd.offsets.__all__
        if v is None:
            # In MergeCollections, offset can be None
            return v
        try:
            pd.Timedelta(v)
        except (Exception,) as e:
            capture_exception(error=e)
            raise ValueError(
                "Invalid offset alias provided. Please review: "
                "https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#offset-aliases"
            )

        total_seconds: float = pd.Timedelta(v).total_seconds()

        if total_seconds < timedelta(minutes=1).total_seconds():
            raise ValueError("Must be equal to, or longer than 1 min")

        if total_seconds > timedelta(days=365).total_seconds() * 100:
            raise ValueError("Must be equal to, or less than 100 years")

        return v

    # --- Properties ---
    def _interval_range(self, end: datetime) -> pd.IntervalIndex:
        assert end, "an end value must be provided"

        _start = self.interval_start

        if end.tzinfo is None:
            # A Naive end was passed in. We probably did this on purpose.
            _start = _start.replace(tzinfo=None)

        assert _start.tzinfo == end.tzinfo, "Timezones must match"

        if self.offset:
            iv_r: pd.IntervalIndex = pd.interval_range(
                start=_start, end=end, freq=self.offset, closed="left"
            )
            res = iv_r.to_list()

            # If there is a defined start (there always should be),
            #   but the end isn't in the IntervalIndex range because
            #    the offset is longer than the end - start
            if self.start is not None and end not in iv_r[-1]:
                right = iv_r[-1].right + pd.Timedelta(self.offset)
                iv = pd.Interval(left=iv_r[-1].right, right=right)
                res.append(iv)

        else:
            iv_r: pd.IntervalIndex = pd.interval_range(
                start=_start, end=end, periods=1, closed="left"
            )
            res = iv_r.to_list()

        return pd.IntervalIndex.from_tuples(
            data=[(iv.left, iv.right) for iv in res], closed="left"
        )

    @property
    def interval_start(self) -> Optional[datetime]:
        # In DFCollections, start must be set, so the interval_start = start. In merged
        #   this may be overridden with different behavior.
        return self.start

    @property
    def interval_range(self) -> List[Tuple]:
        """closed='left', so 0 <= x < 5"""
        end = self.finished or datetime.now(tz=timezone.utc).replace(microsecond=0)
        iv_r = self._interval_range(end)
        return [(iv.left.to_pydatetime(), iv.right.to_pydatetime()) for iv in iv_r]

    @property
    def progress(self) -> pd.DataFrame:
        records = [i.to_dict() for i in self.items]
        end = self.finished if self.finished else datetime.now(tz=timezone.utc)
        return pd.DataFrame.from_records(records, index=self._interval_range(end))

    @property
    def items(self) -> pd.DataFrame:
        raise NotImplementedError("Must override")

    @property
    def _schema(self) -> DataFrameSchema:
        raise NotImplementedError("Must override")

    # --- Methods ---
    def fetch_force_rr_latest(self, sources) -> list:
        raise NotImplementedError("Must override")

    def fetch_all_paths(
        self,
        items: Optional[Items] = None,
        force_rr_latest=False,
        include_partial=False,
    ) -> List[FilePath]:
        LOG.info(
            f"CollectionBase.fetch_all(items={len(items or [])}, "
            f"{force_rr_latest=}, {include_partial=})"
        )

        items = items or self.items

        # (1) All the originally available archives
        sources: List[FilePath] = [
            i.path for i in items if i.has_archive(include_empty=False)
        ]

        # (2) All the "ephemerally" available rr fetched tmp archives
        # ---
        # Sometimes we may want to force in the latest CollectionItem if it's
        # important for some reason. However, many times we probably don't
        # need the absolutely most recent data... and it's not worth the slow
        # rr operation to do so
        if force_rr_latest:
            sources = self.fetch_force_rr_latest(sources)
            if include_partial is False:
                LOG.warning(
                    "If force_rr_latest, then by definition partial is included. "
                    "Set include_partial to True to remove this warning."
                )
                include_partial = True
        # (3)
        if include_partial:
            rr_items = [i for i in items if not i.has_archive()]
            for rr_item in rr_items:
                pp = rr_item.partial_path
                if rr_item.has_partial_archive() and pp not in sources:
                    sources.append(pp)

        return sources

    def ddf(
        self,
        items: Optional[Items] = None,
        force_rr_latest=False,
        columns=None,
        filters=None,
        categories=None,
        include_partial=False,
        graph: Optional[Callable] = None,
    ) -> Optional[dd.DataFrame]:
        """

        Args:
            items (list): These are any of the Collection Item that we want to
                pull rows from. If it is empty, it includes all of the .items
                in the DFCollection

            force_rr_latest (bool): Sometimes we may want to force in the latest
                CollectionItem if it's important for some reason. However, many
                times we probably don't need the absolutely most recent data
                and it's not worth the slow rr operation to do so

            columns: Often times it isn't required to return all of the columns
                in the DFCollection. This allows us to limit which are returned.

            filters: Apply these filters to an Item basis to limit the total
                rows that are returned. It uses the tuple syntax

            categories (list): Define any columns that may be categorical as it
                allows parquet to optimize how the data is read.

            include_partial (bool): when .fetch_all_paths() is called to get
                all the CollectionItems, this boolean is used to determine
                if the last items

        Returns:
            dd.DataFrame: this is a Dask Dataframe

        """

        if isinstance(items, list) and len(items):
            sources: List[FilePath] = [
                i.path for i in items if i.has_archive(include_empty=False)
            ]

            sources.extend(
                [
                    i.partial_path
                    for i in items
                    if i.has_partial_archive()
                    and not i.has_archive(include_empty=False)
                ]
            )

        else:
            sources: List[FilePath] = self.fetch_all_paths(
                items=None,
                force_rr_latest=force_rr_latest,
                include_partial=include_partial,
            )

        if len(sources) == 0:
            return None

        ddfs = []
        for s in sources:
            _ddf = dd.read_parquet(
                path=s,
                columns=columns,
                filters=filters,
                categories=categories,
                calculate_divisions=False,
                engine="pyarrow",
            )

            if graph:
                _ddf = graph(_ddf)

            ddfs.append(_ddf)

        if len(ddfs) == 0:
            raise AssertionError("Must provide parquet sources")

        # Look into interleave_partitions, default False
        ddf = dd.concat(ddfs)
        return ddf

    # --- Methods: Cleanup ---
    def schedule_cleanup(
        self, client=None, sync=True, client_resources=None
    ) -> Union[pd.DataFrame, Future]:
        LOG.info(f"cleanup(archive_path={self.archive_path})")

        fs = []
        for item in self.items:
            fs.append(dask.delayed(item.cleanup_partials)())
            fs.append(dask.delayed(item.clear_corrupt_archive)())
        fs.append(dask.delayed(self.clear_tmp_archives)())
        res = client.compute(
            collections=fs,
            sync=sync,
            priority=2,
            client_resources=client_resources,
        )
        return res

    def cleanup(self) -> None:
        # Same as schedule_cleanup but runs locally
        self.cleanup_partials()
        self.clear_tmp_archives()
        self.clear_corrupt_archives()
        # self.check_empty()  # what did this do??

        return None

    def cleanup_partials(self) -> None:
        """If an item is "closed", remove any partial files that may be around..."""
        for item in self.items:
            item.cleanup_partials()

        return None

    def clear_tmp_archives(self) -> None:
        regex = re.compile(r"\.parquet\.[0-9a-f]{32}", re.I)

        for fn in os.listdir(self.archive_path):
            if regex.search(fn):
                LOG.info(f"Removing {fn}")
                CollectionItemBase.delete_archive(
                    Path(os.path.join(self.archive_path, fn))
                )

        return None

    def clear_corrupt_archives(self) -> None:
        for item in self.items:
            item.clear_corrupt_archive()

        return None

    def rebuild_symlinks(self) -> None:
        """
        When copying "things" between filesystems, and using Sambda mmfsylinks,
        we can't ensure links are properly shared.
        """

        for item in reversed(self.items):
            item: CollectionItemBase
            reg_path = item.path.as_posix()
            partial_path = item.partial_path.as_posix()
            empty_path = item.empty_path.as_posix()

            # --- Partial Path ---
            if os.path.exists(partial_path) and os.path.isfile(partial_path):
                os.remove(partial_path)

                # Don't "continue" onto the next CollectionItem. Later on,
                # we may need to create a symlink for the most recent partial
                pass

            # --- Empty Path ---
            if os.path.exists(empty_path):
                # A symlink isn't used for empty path CollectionItems
                continue

            # --- Regular Path ---
            if os.path.exists(reg_path):

                if os.path.isfile(reg_path):
                    # These should never be a file, clean up
                    os.remove(reg_path)
                    continue

                if os.path.isdir(reg_path) and not os.path.islink(reg_path):
                    # All is good and how it should be!
                    continue

                if os.path.islink(reg_path):
                    # A symlink already exists for this CollectionItem. However,
                    # don't "continue" on because we will want to ensure
                    # it's at the most recent version.
                    pass

            highest_version: Path = item.search_highest_numbered_path()

            if highest_version is None:
                # No version of the file at all was found in the directory,
                # so don't try to make a symlink for it
                continue

            # Make sure these are in the same dir. b/c the symlink has to be
            # relative, not an absolute path
            assert (
                item.path.parent == highest_version.parent
            ), "Can't have numbered_path in a different directory"

            try:
                pq.ParquetDataset(highest_version).read().to_pandas()
            except (Exception,):
                # If the most recent version isn't valid, we don't want to
                # create a symlink to it.
                # TODO: We could try to be smart and iterate down the most recent
                #   available partials until we find one that isn't broken. However,
                #   this isn't a huge priority because it should fix itself upon
                #   the next sync cmd is run (every 1 min)
                continue

            # if os.path.exists(item.path.as_posix()) and not os.path.islink(path.as_posix()):
            #     This will fail when going from the old way to using symlinks, if self.path already exists
            # and is a directory.
            # raise ValueError(
            #     f"first time we run this, make sure the path doesn't exist: {path.as_posix()}")

            # After running for a while, it appears that symlinks have a
            # tendency to break for some reason. While it's unclear why, there
            # shouldn't be any harm in always removing the file before the
            # `ln` command is run.  -- Max 2024-07-26
            try:
                os.remove(item.path.as_posix())
            except FileNotFoundError as e:
                pass

            if platform == "darwin":
                subprocess.call(["ln", "-sfn", highest_version, item.path.as_posix()])
            else:
                subprocess.call(["ln", "-sfnT", highest_version, item.path.as_posix()])

        return None

    # -- Methods: Source timing
    def get_item(self, interval: pd.Interval) -> Item:
        return next(x for x in self.items if x.interval == interval)

    def get_item_start(self, start: pd.Timestamp) -> Items:
        return next(x for x in self.items if x.interval.left == start)

    def get_items(self, since: datetime) -> Items:
        res = []
        first_match = True

        for idx, item in enumerate(self.items):
            item: "DFCollectionItem"

            # TODO: This appears to be a bug. It should be using the
            #   IntervalRange overlaps approach  - Max 2024-06-07
            if item.start >= since:

                # We want to retrieve the item that falls before the
                # first item, so we aren't missing any partial time ranges
                if first_match and idx != 0:
                    res.append(self.items[idx - 1])

                res.append(item)
                first_match = False

        res: List[Item] = [i for i in res if not i.is_empty()]
        if len([1 for i in res if i.should_archive() and not i.has_archive()]):
            warnings.warn(
                message="DFCollectionItem has missing archives",
                category=ResourceWarning,
            )

        return res

    def get_items_from_year(self, year: int) -> Items:
        ts = datetime(year=year, month=1, day=1)
        return self.get_items(since=ts)

    def get_items_last90(self) -> Items:
        ts = datetime.now(tz=timezone.utc) - timedelta(days=90)
        return self.get_items(since=ts)

    def get_items_last365(self) -> Items:
        ts = datetime.now(tz=timezone.utc) - timedelta(days=365)
        return self.get_items(since=ts)


class CollectionItemBase(BaseModel):
    # I want to intentionally keep these as native python types, and not
    # pandas specific types.
    start: AwareDatetimeISO = Field(
        default_factory=lambda: datetime.now(tz=timezone.utc).replace(microsecond=0)
    )

    # --- Private attrs ---
    _collection: Collection = PrivateAttr()

    @property
    def name(self) -> str:
        coll = self._collection
        if hasattr(coll, "data_type"):
            name = coll.data_type.value
        else:
            name = coll.merge_type.value
        return name

    def __str__(self):
        coll = self._collection
        offset = coll.offset or "–"
        return f"{self.name}({self.interval.left.strftime('%x %X')} @ {offset})"

    # --- Validators ---
    @model_validator(mode="after")
    def check_start(self):
        """We don't want to support CollectionItems that start on a
        fractional second.
        """
        assert (
            self.start.microsecond == 0
        ), "CollectionItem.start must not have microsecond precision"
        return self

    # --- Properties ---
    @property
    def finish(self) -> datetime:
        return (
            pd.Timestamp(self.start) + pd.Timedelta(self._collection.offset)
        ).to_pydatetime()

    @property
    def interval(self) -> pd.Interval:
        return pd.Interval(
            left=pd.Timestamp(self.start),
            right=pd.Timestamp(self.finish),
            closed="left",
        )

    # --- Properties: paths + filenames ---
    @property
    def filename(self) -> str:
        raise NotImplementedError("Do not use CollectionItemBase directly.")

    @property
    def partial_filename(self) -> str:
        # This is an archive for a CollectionItem that is not yet closed. It is temporary and should
        #   never get backed up.
        return f"{self.filename}.partial"

    @property
    def empty_filename(self) -> str:
        # If this file or directory exists on disk, it means this CollectionItem
        # truly has no data. We want to use this to distinguish this from a
        # broken file or a failed query.
        return f"{self.filename}.empty"

    @property
    def path(self) -> FilePath:
        return FilePath(os.path.join(self._collection.archive_path, self.filename))

    @property
    def partial_path(self) -> FilePath:
        return FilePath(
            os.path.join(self._collection.archive_path, self.partial_filename)
        )

    @property
    def empty_path(self) -> FilePath:
        return FilePath(
            os.path.join(self._collection.archive_path, self.empty_filename)
        )

    # --- Methods ---

    @staticmethod
    def path_exists(generic_path: FilePath) -> bool:
        return os.path.exists(generic_path)

    @staticmethod
    def next_numbered_path(path: Path) -> Path:
        # We assume the item.path is pointing to the current version. To get the "next", we increment it.
        target = os.path.realpath(path)
        if path == target:
            # This is not yet a symlink, start with .00000
            return Path(f"{path}.{0:>05}")

        # We assume the target ends with ".####". If not, we'll append .00000
        try:
            left, right = target.rsplit(".", 1)
            right_int = int(right)
        except ValueError:
            return Path(f"{path}.{0:>05}")

        right_int += 1
        return Path(f"{path}.{right_int:>05}")

    def search_highest_numbered_path(self) -> Optional[Path]:
        """This is used for when things are broken, and we want to rebuild
        our symlinks. We can't trust or use any exist symlinks... so given
        a path or a partial path... find the highest available "versioned"
        build there is
        """
        coll: CollectionBase = self._collection

        # TODO: is_partial support???

        # regex = re.compile(r'\.parquet\.[0-9a-f]{32}', re.I)
        builds = []
        for fn in os.listdir(coll.archive_path):
            if fn.startswith(self.filename):

                # Don't include the "broken link" or mmfsymlink text file
                if fn != self.filename and fn != self.partial_filename:
                    builds.append(fn)

        if len(builds) == 0:
            return None

        # --- this doesn't work with if the incrementing file is a partial ---
        # nums = sorted([b.rsplit(".", 1)[1] for b in builds], reverse=True)
        # return Path(f"{self.path}.{nums[0]}")

        files: List[str] = sorted(
            builds, key=lambda b: b.rsplit(".", 1)[1], reverse=True
        )
        return Path(os.path.join(coll.archive_path, files[0]))

    def tmp_filename(self) -> str:
        # Not a @property b/c I don't want to accidentally have this get mixed
        # up as always returning the same tmp filename
        return f"{self.filename}.{uuid4().hex}"

    def tmp_path(self) -> FilePath:
        return FilePath(
            os.path.join(self._collection.archive_path, self.tmp_filename())
        )

    # --- --- --- ---
    # If it has a partial, it isn't always going to be a partial. However,
    #   if it has an empty, will we ever try to recheck?

    def is_empty(self) -> bool:
        return self.path_exists(self.empty_path)

    def has_empty(self) -> bool:
        return self.is_empty()

    def has_partial_archive(self) -> bool:
        return self.path_exists(self.partial_path)

    # --- --- --- ---

    def has_archive(self, include_empty=False) -> bool:
        if include_empty:
            return self.path_exists(generic_path=self.path) or self.path_exists(
                generic_path=self.empty_path
            )
        else:
            return self.path_exists(generic_path=self.path)

    @staticmethod
    def delete_archive(generic_path: Path) -> None:
        # If a partial directory or file exists, delete it.
        if os.path.exists(generic_path):

            if os.path.isfile(generic_path):
                os.remove(generic_path)

            if os.path.isdir(generic_path):
                # TODO: this is broken on Mac...
                # os.path.islink(path.as_posix()):
                shutil.rmtree(generic_path)
        else:
            LOG.warning(f"tried removing non-existent file: {generic_path}")
            pass

    def should_archive(self) -> bool:
        # Determine if enough time has passed to move out of a partial file into an
        # archive.
        archive_after: timedelta = self._collection._schema.metadata[ARCHIVE_AFTER]

        if archive_after is None:
            return False

        if datetime.now(tz=timezone.utc) > self.finish + archive_after:
            return True
        return False

    def set_empty(self):
        assert (
            self.should_archive()
        ), "Can not set_empty on an item that is not archive-able"
        assert not self.is_empty(), "set_empty is already set; why are you doing this?"
        self.empty_path.touch()
        assert self.is_empty(), "set_empty(): something is wrong"

    def valid_archive(
        self,
        generic_path: Optional[FilePath] = None,
        sample: Optional[int] = None,
    ) -> bool:
        """
        Attempts to confirm if the parquet file or directory that is
        written to Disk for a DFCollectionItem is not corrupted or otherwise
        in a state that would prevent its use.
        """
        path: str = generic_path.as_posix() if generic_path else self.path.as_posix()
        try:
            if os.path.isfile(path):
                parquet = pq.ParquetFile(path)
            elif os.path.isdir(path):
                # This will not fail on a empty directory. However, it will
                #   return the .read().to_pandas() as an empty pd.DataFrame
                #   without any rows or columns
                parquet = pq.ParquetDataset(path)
            else:
                # TODO: are there even other types; eg: are symlinks .isfile=True?
                raise ValueError("Unknown path type.")

            df = parquet.read().to_pandas()
        except (Exception,):
            LOG.warning(f"Invalid archive {path=}")
            df = None

        # Check if it's None or a totally empty pd.DataFrame before we waste
        #   any time on trying to hit pandera
        if df is None or sum(df.shape) == 0:
            return False

        return self.validate_df(df=df, sample=sample) is not None

    def validate_df(
        self, df: pd.DataFrame, sample: Optional[int] = None
    ) -> Optional[pd.DataFrame]:
        if sample is not None:
            sample = min(len(df), sample)
        try:
            schema: DataFrameSchema = self._collection._schema
            return schema.validate(check_obj=df, lazy=True, sample=sample)
        except Exception as e:
            LOG.exception(e)
            capture_exception(error=e)
            return None

    # def validate_ddf(self, ddf: dd.DataFrame) -> Optional[pd.DataFrame]:
    #     """ WARNING: this accepts a dd.DataFrame, but returns a pd.DataFrame
    #     """
    #     # TODO: This is absolutely a way to do this with pyArrow Schemas.. However,
    #     #   we'd first need to figure out how to go from Pandera to a pyArrow Schema.
    #     try:
    #         df = self._collection._client.compute(
    #             collections=ddf,
    #             sync=True,
    #             priority=1,
    #             resources=self._collection._client_resources
    #         )
    #     except (Exception,) as e:
    #         capture_exception(error=e)
    #         return None
    #
    #     return _validate_df(df=df, schema=self._collection._schema)

    # --- ORM / Data handlers---
    def from_archive(
        self,
        include_empty: bool = True,
        generic_path: Optional[FilePath] = None,
    ) -> Optional[dd.DataFrame]:

        if include_empty and self.path_exists(generic_path=self.empty_path):
            # Return an empty dd.DataFrame with the correct columns
            return dd.from_pandas(empty_dataframe_from_schema(self._collection._schema))

        if not self.path_exists(generic_path=generic_path or self.path):
            return None

        return dd.read_parquet(
            path=generic_path or self.path,
            calculate_divisions=False,
            engine="pyarrow",
        )

    def to_archive(self, ddf: dd.DataFrame, is_partial: bool = False) -> bool:
        raise NotImplementedError("Must override")

    # --- ORM / Data handlers---
    def _to_dict(self, *args, **kwargs) -> dict:
        return dict(
            should_archive=self.should_archive(),
            has_archive=self.has_archive(),
            filename=self.filename,
            path=self.path,
            start=self.start,
            finish=self.finish,
        )

    def delete_partial(self):
        # If a Collection Item is archived, we want to delete the partial file.
        assert self.should_archive(), "please wait until item is archived"
        if not self.path_exists(self.partial_path):
            LOG.info(f"no partial to delete: {self.partial_path}")
            return
        if not self.partial_path.is_symlink():
            LOG.warning(f"expected symlink: {self.partial_path}")
            return
        target = self.partial_path.parent / self.partial_path.readlink()
        os.remove(self.partial_path)
        shutil.rmtree(target)

    def cleanup_partials(self):
        if self.path_exists(self.partial_path):
            if self.should_archive() and self.has_archive(include_empty=True):
                self.delete_dangling_partials()
                self.delete_partial()
            else:
                self.delete_dangling_partials(keep_latest=2)

    def delete_dangling_partials(self, keep_latest=None, target_path=None) -> List[str]:
        # Specifically looking for numbered partials that are NOT associated
        # with a symlink. It does not matter if the item is archiveable or not.
        if target_path is None:
            target_path = self.partial_path
        fps = glob.glob(target_path.as_posix() + ".*")
        fps = {x for x in fps if x.split(".")[-1].isnumeric()}
        # Note: if the dir itself is sym-linked, this is going to be wrong.
        # Use the relative paths existing_link = os.path.realpath(target_path)
        if target_path.exists() and target_path.is_symlink():
            existing_link = target_path.parent / target_path.readlink()
            fps.discard(existing_link.as_posix())
        fps = sorted(fps)
        if keep_latest is not None:
            fps = fps[:-keep_latest]
        for fp in fps:
            self.delete_archive(fp)
        return fps
