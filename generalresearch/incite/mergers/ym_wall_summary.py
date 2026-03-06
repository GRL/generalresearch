from datetime import timedelta, datetime, time
from typing import Literal, List, Optional, Type

import dask.dataframe as dd
import pandas as pd
from pydantic import Field, field_validator
from sentry_sdk import capture_exception

from generalresearch.incite.collections.thl_web import (
    SessionDFCollection,
    WallDFCollection,
)
from generalresearch.incite.mergers import (
    MergeCollection,
    MergeType,
    MergeCollectionItem,
)
from generalresearch.incite.mergers.foundations.user_id_product import (
    UserIdProductMerge,
)
from generalresearch.incite.schemas.mergers.ym_wall_summary import (
    YMWallSummarySchema,
)
from generalresearch.models.thl.definitions import Status, StatusCode1


class YMWallSummaryMergeItem(MergeCollectionItem):

    def fetch(
        self,
        wall_collection: WallDFCollection,
        session_collection: SessionDFCollection,
        user_id_product: UserIdProductMerge,
    ):
        ir = self.interval
        start, end = ir.left.to_pydatetime(), ir.right.to_pydatetime()

        wall_items = [w for w in wall_collection.items if w.interval.overlaps(ir)]
        ddf = wall_collection.ddf(
            items=wall_items, force_rr_latest=False, include_partial=True
        )
        ddf = ddf[ddf["started"].between(start, end)]

        # Then we need the sessions for these wall events. They'll have started
        #   up to 90 min before the wall event.
        session_ir = pd.Interval(
            ir.left - timedelta(minutes=90), ir.right, closed="left"
        )
        session_items = [
            s for s in session_collection.items if s.interval.overlaps(session_ir)
        ]
        sddf = session_collection.ddf(
            items=session_items,
            force_rr_latest=False,
            include_partial=True,
            columns=["user_id", "country_iso", "device_type"],
        )
        df: pd.DataFrame = self.compute(ddf.join(sddf, on="session_id", how="left"))
        user_ids = set(df.user_id.dropna().unique())

        udf = self.compute(
            user_id_product.ddf(filters=[("id", "in", user_ids)], include_partial=True)
        )

        x = udf.loc[udf.index.isin(user_ids)].copy()
        x["product_id"] = x["product_id"].astype(str)
        df = df.join(x, on="user_id", how="left")

        df["date"] = df["started"].dt.strftime("%Y-%m-%d")
        df = YMWallSummaryMerge.build_groupbys(df)

        self._collection._schema.validate(df)

        is_partial = not self.should_archive()
        ddf = dd.from_pandas(df, npartitions=4)
        self.to_archive(ddf, is_partial=is_partial)
        return df


class YMWallSummaryMerge(MergeCollection):
    merge_type: Literal[MergeType.YM_WALL_SUMMARY] = MergeType.YM_WALL_SUMMARY
    _schema = YMWallSummarySchema
    collection_item_class: Type[YMWallSummaryMergeItem] = YMWallSummaryMergeItem
    items: List[YMWallSummaryMergeItem] = Field(default_factory=list)

    @field_validator("offset")
    def check_offset_ym_wall_summary(cls, v: Optional[str]):
        # the offset MUST be on a whole day, no hourly
        assert v.endswith("D"), "offset must be in days"
        return v

    @field_validator("start")
    def check_start_ym_wall_summary(cls, v: Optional[datetime]):
        # the start MUST be start on midnight exactly
        assert v.time() == time(0, 0, 0, 0), "start must no have a time component"
        return v

    def build(
        self,
        wall_collection: WallDFCollection,
        session_collection: SessionDFCollection,
        user_id_product: UserIdProductMerge,
    ) -> None:

        for item in reversed(self.items):
            item: YMWallSummaryMergeItem

            # Skip which already exist
            if item.has_archive():
                continue

            try:
                # TODO: How should we do this generically?
                #   We're going to assume that we want to update the "latest"
                #   item every time build is run even if it isn't closed
                # if item.should_archive():
                item.fetch(wall_collection, session_collection, user_id_product)
            except (Exception,) as e:
                capture_exception(e)
                pass

    @staticmethod
    def build_groupbys(df: pd.DataFrame) -> pd.DataFrame:
        gb_cols = ["date", "product_id", "buyer_id", "country_iso", "source"]
        status_cols = [
            "Status.ABANDON",
            "Status.COMPLETE",
            "Status.FAIL",
            "Status.TIMEOUT",
            "StatusCode1.BUYER_FAIL",
        ]
        df.loc[df.status.isnull(), "status"] = Status.TIMEOUT.value
        gbs = [
            ["date", "source"],
            ["date", "source", "country_iso"],
            ["date", "source", "product_id"],
            ["date", "source", "buyer_id"],
            ["date", "source", "product_id", "country_iso"],
            ["date", "source", "buyer_id", "country_iso"],
        ]

        gdf = pd.DataFrame(columns=gb_cols + status_cols)
        gdf = gdf.astype({k: "string" for k in gb_cols} | {k: int for k in status_cols})
        for gb in gbs:
            s = df.groupby(gb)["status"].value_counts()
            s = s.reset_index().pivot_table(index=gb, columns="status", values="count")
            bf = (
                df[df["status_code_1"] == StatusCode1.BUYER_FAIL.value]
                .groupby(gb)
                .size()
                .rename("StatusCode1.BUYER_FAIL")
            )
            s = s.join(bf)
            s = (
                s.rename(
                    columns={
                        "a": "Status.ABANDON",
                        "c": "Status.COMPLETE",
                        "f": "Status.FAIL",
                        "t": "Status.TIMEOUT",
                    }
                )
                .reset_index()
                .rename_axis(None, axis=1)
            )
            s = s.reindex(columns=list(set(status_cols) | set(s.columns))).fillna(0)
            s = s.reindex(columns=list(set(gb_cols) | set(s.columns)))
            s = s.astype({k: "string" for k in gb_cols} | {k: int for k in status_cols})
            gdf = pd.concat([gdf, s])
        return gdf

    def save(self) -> None:
        # Once we build all the daily files, we can package them all up into 1 file
        # df = pq.ParquetDataset(self.archive_path).read().to_pandas()
        # df.to_parquet(str(self.archive_path) + ".all.parquet")
        pass

    def get_counts(self, product_id):
        # examples...
        product_id = ""
        df = dd.read_parquet(
            str(self.archive_path) + ".all.parquet",
            filters=[
                ("product_id", "=", product_id),
                ("country_iso", "is", None),
            ],
        ).compute()
        country_iso = "de"
        df = dd.read_parquet(
            str(self.archive_path) + ".all.parquet",
            filters=[
                ("product_id", "=", product_id),
                ("country_iso", "=", country_iso),
            ],
        ).compute()
