import logging
import os.path
import subprocess
from datetime import datetime, timezone
from enum import Enum
from sys import platform
from typing import Optional, List, Type

import dask.dataframe as dd
import pandas as pd
from dask.distributed import Client
from pandera import DataFrameSchema
from pydantic import Field, field_validator, ValidationInfo, model_validator
from typing_extensions import Self

from generalresearch.incite.base import CollectionBase, CollectionItemBase
from generalresearch.incite.schemas import PARTITION_ON
from generalresearch.incite.schemas.mergers.foundations.enriched_session import (
    EnrichedSessionSchema,
)
from generalresearch.incite.schemas.mergers.foundations.enriched_task_adjust import (
    EnrichedTaskAdjustSchema,
)
from generalresearch.incite.schemas.mergers.foundations.enriched_wall import (
    EnrichedWallSchema,
)
from generalresearch.incite.schemas.mergers.foundations.user_id_product import (
    UserIdProductSchema,
)
from generalresearch.incite.schemas.mergers.nginx import (
    NGINXGRSSchema,
    NGINXCoreSchema,
    NGINXFSBSchema,
)
from generalresearch.incite.schemas.mergers.pop_ledger import (
    PopLedgerSchema,
)
from generalresearch.incite.schemas.mergers.ym_survey_wall import (
    YMSurveyWallSchema,
)
from generalresearch.incite.schemas.mergers.ym_wall_summary import (
    YMWallSummarySchema,
)
from generalresearch.models.custom_types import AwareDatetimeISO

LOG = logging.getLogger("incite")


class MergeType(str, Enum):
    TEST = "test"
    YM_SURVEY_WALL = "ym_survey_wall"
    YM_WALL_SUMMARY = "ym_wall_summary"

    NGINX_GRS = "nginx_grs"
    NGINX_FSB = "nginx_fsb"
    NGINX_CORE = "nginx_core"

    POP_LEDGER = "pop_ledger"

    # --- Foundations ---
    USER_ID_PRODUCT = "user_id_product"
    ENRICHED_WALL = "enriched_wall"
    ENRICHED_SESSION = "enriched_session"
    ENRICHED_TASK_ADJUST = "enriched_task_adjust"


MergeTypeSchemas = {
    MergeType.YM_SURVEY_WALL: YMSurveyWallSchema,
    MergeType.YM_WALL_SUMMARY: YMWallSummarySchema,
    MergeType.NGINX_GRS: NGINXGRSSchema,
    MergeType.NGINX_FSB: NGINXFSBSchema,
    MergeType.NGINX_CORE: NGINXCoreSchema,
    MergeType.POP_LEDGER: PopLedgerSchema,
    # --- Foundations ---
    MergeType.USER_ID_PRODUCT: UserIdProductSchema,
    MergeType.ENRICHED_WALL: EnrichedWallSchema,
    MergeType.ENRICHED_SESSION: EnrichedSessionSchema,
    MergeType.ENRICHED_TASK_ADJUST: EnrichedTaskAdjustSchema,
}


class MergeCollectionItem(CollectionItemBase):

    # --- Properties ---

    @property
    def finish(self) -> datetime:
        # A MergeCollection can have offset = None
        if self._collection.offset:
            return (
                pd.Timestamp(self.start) + pd.Timedelta(self._collection.offset)
            ).to_pydatetime()
        else:
            return datetime.now(tz=timezone.utc).replace(microsecond=0)

    @property
    def filename(self) -> str:
        grouped_key = self._collection.grouped_key
        offset = self._collection.offset
        start = self.start.strftime("%Y-%m-%d-%H-%M-%S")
        f = [self._collection.merge_type.name.lower()]
        if offset:
            f.append(offset)
        if grouped_key:
            f.append(grouped_key)
        if self._collection.start is not None:
            # This is a collection that is "looking back" 'offset' time (1 item).
            f.append(start)
        s = "-".join(f)
        s += ".parquet"
        return s

    # --- ORM / Data handlers---
    def to_dict(self, *args, **kwargs) -> dict:
        res = self._to_dict()
        res["group_by"] = self._collection.group_by
        return res

    def to_archive(
        self,
        client: Client,
        ddf: dd.DataFrame,
        is_partial: bool = False,
        client_resources=None,
    ) -> bool:
        assert is_partial is False, "use to_archive_symlink"
        return self._to_archive(client=client, ddf=ddf, client_resources=None)

    def _to_archive(
        self, client: Client, ddf: dd.DataFrame, client_resources=None
    ) -> bool:
        """
        For archiving an item. Will write an empty file if ddf is empty.
        This is NOT for writing partials.

        :returns: bool (saved_successful)
        """
        if ddf is None:
            return False

        row_len = client.compute(collections=ddf.shape[0], sync=True)
        assert row_len > 0, "empty ddf"

        tmp_path = self.tmp_path()
        schema = self._collection._schema
        partition = schema.metadata.get(PARTITION_ON, None)
        f = ddf.to_parquet(
            compute=False,
            path=tmp_path,
            partition_on=partition,
            engine="pyarrow",
            overwrite=True,
            write_metadata_file=True,
            compression="brotli",
        )
        client.compute(f, sync=True, priority=2, resources=client_resources)
        assert not os.path.exists(
            self.path.as_posix()
        ), f"already exits!: {self.path.as_posix()}"

        if platform == "darwin":
            subprocess.call(["mv", tmp_path.as_posix(), self.path.as_posix()])
        else:
            # -T will (should) cause the mv to fail if `path` wasn't successfully deleted
            subprocess.call(["mv", "-T", tmp_path.as_posix(), self.path.as_posix()])
        return True

    def to_archive_symlink(
        self,
        client: Client,
        ddf: dd.DataFrame,
        is_partial: bool = False,
        client_resources=None,
        validate_after=True,
    ) -> bool:
        """
        This differs from to_archive():
            1) to_parquet is run in this process. If the df is already
            computed, there is no point in sending it to another worker
            to write.

            2) symlink to next_numbered_path is created whether or not
            is_partial (to_archive only does this on partials)

            3) we do not validate the written file. seems not useful to do
            this, as the file will probably get overwritten on the next
            loop anyway
        """
        path = self.partial_path if is_partial else self.path
        next_numbered_path = self.next_numbered_path(path)
        collection = self._collection
        LOG.warning(f"{collection.merge_type.value}.to_archive_symlink()")

        if not isinstance(ddf, dd.DataFrame):
            raise ValueError("must pass a dask df")

        # We should validate before or after!!!
        # _validate_df(self.compute(ddf), coll._schema)
        target = (
            next_numbered_path.name
        )  # this is the symlink's target. it is a relative path (only the name)

        schema = self._collection._schema
        partition = schema.metadata.get(PARTITION_ON, None)
        f = ddf.to_parquet(
            compute=False,
            path=next_numbered_path.as_posix(),
            partition_on=partition,
            engine="pyarrow",
            overwrite=True,
            write_metadata_file=True,
            compression="brotli",
        )
        client.compute(f, sync=True, priority=2, resources=client_resources)

        if os.path.exists(path.as_posix()) and not os.path.islink(path.as_posix()):
            # This will fail when going from the old way to using symlinks,
            # if self.path already exists and is a directory.
            raise ValueError(
                f"first time we run this, make sure the path doesnt exist: {path.as_posix()}"
            )

        if platform == "darwin":
            subprocess.call(["ln", "-sfn", target, path.as_posix()])
        else:
            subprocess.call(["ln", "-sfnT", target, path.as_posix()])

        if validate_after:
            if not self.valid_archive(self.path):
                LOG.error(
                    f"{collection.merge_type.value} failed validation: {self.path}"
                )
                self.delete_archive(self.path)
                return False
        return True

    # todo: unclear what the common interface should be here ... ?
    def fetch(self, *args, **kwargs) -> pd.DataFrame | dd.DataFrame:
        raise NotImplementedError("implement in subclass")

    def build(self, *args, **kwargs) -> pd.DataFrame | dd.DataFrame:
        raise NotImplementedError("implement in subclass")


class MergeCollection(CollectionBase):
    """Mergers take instances of DFCollections, and/or other Mergers"""

    # In a merge, we can set offset = None which indicates that there is only 1
    #   period/item where the range is 'start' until now.
    offset: Optional[str] = Field(default="72h")
    # In a merge, we can set start = None which indicates that there is only 1
    #   period/item where the range is (now - offset) until now.
    start: Optional[AwareDatetimeISO] = Field(
        default=None,
        description="This is the starting point in which data will"
        " be retrieved in chunks from.",
        frozen=True,
    )

    merge_type: Optional[MergeType] = Field(default=None)
    group_by: Optional[str] = Field(default=None)
    grouped_key: Optional[str] = Field(default=None)
    collection_item_class: Type[MergeCollectionItem] = MergeCollectionItem

    @model_validator(mode="after")
    def check_start_and_offset_nullable(self) -> Self:
        if self.offset is None and self.start is None:
            raise AssertionError("cannot set both start and offset to None")
        return self

    @field_validator("merge_type")
    def check_merge_type(cls, merge_type, info: ValidationInfo):
        if merge_type is None:
            raise ValueError("Must explicitly provide a merge_type")

        if merge_type not in MergeTypeSchemas:
            raise ValueError("Must provide a supported merge_type")

        return merge_type

    # --- Properties ---
    @property
    def interval_start(self) -> Optional[datetime]:
        # if self.start is None and self.offset is set, the inferred start is (now - offset)
        if self.start is None:
            return datetime.now(tz=timezone.utc).replace(microsecond=0) - pd.Timedelta(
                self.offset
            )
        return self.start

    @property
    def items(self) -> List[MergeCollectionItem]:
        items = []
        for iv in self.interval_range:
            cm = self.collection_item_class(start=iv[0])
            cm._collection = self
            items.append(cm)
        return items

    @property
    def _schema(self) -> DataFrameSchema:
        return MergeTypeSchemas[self.merge_type]

    def signature(self) -> str:
        arr = [
            1 if i.has_archive(include_empty=True) else 0
            for i in self.items
            if i.should_archive()
        ]
        repr_str = (
            f"path={self.archive_path.as_posix()}; "
            f"items={len(self.items)}; start={self.start} @ {self.offset}; {int(sum(arr) / len(arr) * 100)}% "
            f"archived"
        )
        res = f"{self.__repr_name__()}({repr_str})"
        return res
