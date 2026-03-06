import logging
from datetime import timedelta
from typing import Optional, Literal

import dask.dataframe as dd
import pandas as pd
from distributed import Client
from sentry_sdk import capture_exception

from generalresearch.incite.collections.thl_web import WallDFCollection
from generalresearch.incite.mergers import (
    MergeCollection,
    MergeType,
    MergeCollectionItem,
)
from generalresearch.incite.mergers.foundations.enriched_session import (
    EnrichedSessionMerge,
)
from generalresearch.incite.schemas.mergers.ym_survey_wall import (
    YMSurveyWallSchema,
)
from generalresearch.models.custom_types import AwareDatetimeISO

LOG = logging.getLogger("incite")


class YMSurveyWallMergeCollectionItem(MergeCollectionItem):

    def build(
        self,
        wall_coll: WallDFCollection,
        enriched_session: EnrichedSessionMerge,
        client: Optional[Client] = None,
        client_resources=None,
    ) -> None:
        LOG.info(f"YMSurveyWallMerge.build({self.start=}, {self.finish=})")
        ir: pd.Interval = self.interval
        start, _ = self.start, self.finish
        ddf = wall_coll.ddf(
            items=wall_coll.get_items(start),
            force_rr_latest=False,
            include_partial=True,
            columns=[
                "source",
                "buyer_id",
                "survey_id",
                "session_id",
                "started",
                "finished",
                "status",
                "status_code_1",
                "status_code_2",
                "cpi",
                "report_value",
                "ext_status_code_1",
                "ext_status_code_2",
                "ext_status_code_3",
            ],
            filters=[("started", ">=", start)],
        )
        ddf = ddf[ddf["started"] > start]

        LOG.warning(f"YMSurveyWallMerge: merge session_collection")
        session_items = [
            s
            for s in enriched_session.items
            if s.interval.overlaps(
                pd.Interval(ir.left - timedelta(hours=2), ir.right, closed="both")
            )
        ]
        sdf = enriched_session.ddf(
            items=session_items,
            include_partial=True,
            force_rr_latest=False,
            columns=[
                "user_id",
                "country_iso",
                "device_type",
                "payout",
                "product_id",
                "team_id",
            ],
            filters=[
                ("started", ">=", start - timedelta(hours=2)),
            ],
        )
        ddf = dd.merge(
            ddf, sdf, left_on="session_id", right_index=True, how="left", npartitions=5
        )

        df = client.compute(ddf, resources=client_resources, sync=True)
        df["elapsed"] = (df["finished"] - df["started"]).dt.total_seconds()
        df["elapsed"] = df["elapsed"].round().astype("Int64")
        df = df.drop(columns={"finished", "payout"}, errors="ignore")
        df.dropna(subset="user_id", how="any", inplace=True)
        df.dropna(subset="product_id", how="any", inplace=True)
        df.sort_values(by="started", inplace=True)

        LOG.debug(f"YMSurveyWallMerge.build() validation")

        df = self.validate_df(df=df)
        if df is not None:
            ddf = dd.from_pandas(df, npartitions=4)
            LOG.info(f"YMSurveyWallMerge.build() saving")
            self.to_archive_symlink(client=client, ddf=ddf)
        else:
            LOG.warning("YMSurveyWallMerge failed validation")

        return None


class YMSurveyWallMerge(MergeCollection):
    merge_type: Literal[MergeType.YM_SURVEY_WALL] = MergeType.YM_SURVEY_WALL
    collection_item_class: Literal[YMSurveyWallMergeCollectionItem] = (
        YMSurveyWallMergeCollectionItem
    )
    start: Optional[AwareDatetimeISO] = None
    offset: str = "10D"
    _schema = YMSurveyWallSchema

    def build(
        self,
        client: Client,
        wall_coll: WallDFCollection,
        enriched_session: EnrichedSessionMerge,
        client_resources=None,
    ) -> None:

        LOG.info(
            f"YMSurveyWallMerge.build(wall_coll={wall_coll.signature()}, "
            f"enriched_session={enriched_session.signature()})"
        )
        assert (
            len(self.items) == 1
        ), "YMSurveyWallMerge can't have more than 1 CollectionItem."
        item: YMSurveyWallMergeCollectionItem = self.items[0]

        try:
            item.build(
                client=client,
                client_resources=client_resources,
                wall_coll=wall_coll,
                enriched_session=enriched_session,
            )
        except (Exception,) as e:
            capture_exception(error=e)
            pass

        item.delete_dangling_partials(keep_latest=2, target_path=item.path)
