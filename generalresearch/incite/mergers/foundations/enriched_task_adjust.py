import logging
from typing import Literal

import dask.dataframe as dd
import pandas as pd
from distributed import Client
from sentry_sdk import capture_exception

from generalresearch.incite.collections.thl_web import (
    TaskAdjustmentDFCollection,
)
from generalresearch.incite.mergers import (
    MergeCollection,
    MergeType,
    MergeCollectionItem,
)
from generalresearch.incite.mergers.foundations import (
    annotate_product_and_team_id,
)
from generalresearch.incite.mergers.foundations.enriched_wall import (
    EnrichedWallMerge,
)
from generalresearch.incite.schemas.mergers.foundations.enriched_task_adjust import (
    EnrichedTaskAdjustSchema,
)
from generalresearch.pg_helper import PostgresConfig

LOG = logging.getLogger("incite")


class EnrichedTaskAdjustMergeItem(MergeCollectionItem):
    """Because a single wall event can have multiple "alerted" times,
    we're basing the time event for the TaskAdjustDetailMergeCollection
    off the wall.started timestamp.
    """

    def build(
        self,
        task_adj_coll: TaskAdjustmentDFCollection,
        enriched_wall: EnrichedWallMerge,
        pg_config: PostgresConfig,
        client: Client,
        client_resources=None,
    ) -> None:
        """
        TaskAdjustments are always partial because they could be revoked
        at any moment
        """

        ir: pd.Interval = self.interval
        start, end = ir.left.to_pydatetime(), ir.right.to_pydatetime()

        LOG.warning(f"EnrichedReconMergeItem.build({ir})")

        # --- Task Adjustments ---
        LOG.warning(f"EnrichedReconMergeItem: get session_collection")
        task_adj_coll_items = [
            w for w in task_adj_coll.items if w.interval.overlaps(ir)
        ]

        if len(task_adj_coll_items) == 0:
            raise Exception("TaskAdjColl item collection failed")

        ddf: dd.DataFrame = task_adj_coll.ddf(
            items=task_adj_coll_items,
            include_partial=True,
            force_rr_latest=False,
            columns=[
                "adjusted_status",
                "amount",
                "user_id",
                "wall_uuid",
                "source",
                "survey_id",
                "alerted",
                "started",
            ],
            filters=[
                ("adjusted_status", "in", ("af", "ac")),
                ("started", ">=", start),
                ("started", "<", end),
            ],
        )
        # Naked compute... don't log
        # LOG.info(f"TaskAdjustmentDetailMergeCollectionItem.rows: {len(ddf.index)}")

        # --- Join on the wall table  --- #

        ew_items = [ew for ew in enriched_wall.items if ew.interval.overlaps(ir)]

        if len(ew_items) == 0:
            raise Exception(
                "EnrichedWall item collection failed for EnrichedTaskAdjColl"
            )

        wall_uuids = set(
            client.compute(collections=ddf.wall_uuid.dropna().values, sync=True)
        )
        wall_ddf = enriched_wall.ddf(
            # I try to take the adjustments within this IntervalRange and
            # figure out the respective range of when the surveys they're
            # for were started. This should help limit how many enriched
            # wall mergers needed to be loaded up
            items=ew_items,
            include_partial=True,
            force_rr_latest=False,
            columns=[
                "buyer_id",
                "country_iso",
                "device_type",
            ],
            filters=[("uuid", "in", wall_uuids)],
        )

        assert str(ddf.wall_uuid.dtype) == "string"
        assert str(wall_ddf.index.dtype) == "string"
        ddf = ddf.merge(
            wall_ddf,
            left_on="wall_uuid",
            right_on="uuid",
            how="left",
        )

        df = (
            ddf.sort_values("alerted")
            .compute()
            .groupby(["wall_uuid", "user_id", "source", "survey_id"])
            .agg(
                amount=("amount", "sum"),
                adjusted_status=("adjusted_status", "first"),
                adjusted_status_last=("adjusted_status", "last"),
                alerted=("alerted", "first"),
                alerted_last=("alerted", "last"),
                started=("started", "last"),
                buyer_id=("buyer_id", "last"),
                # Shouldn't matter (but some variation as of Sep 2024 -Max)
                country_iso=("country_iso", "last"),
                # Shouldn't matter (but some variation as of Sep 2024 -Max)
                device_type=("device_type", "last"),
                # Shouldn't matter (but some variation as of Sep 2024 -Max)
                adjustments=("amount", "count"),
            )
            .reset_index()
        )
        df.index.rename("uuid")

        # --- Add the product_id + product_user_id ---
        df = annotate_product_and_team_id(df=df, pg_config=pg_config)

        ddf = dd.from_pandas(df, npartitions=5)
        self.to_archive_symlink(
            client=client,
            ddf=ddf,
            is_partial=True,
            validate_after=False,
            client_resources=client_resources,
        )


class EnrichedTaskAdjustMerge(MergeCollection):
    merge_type: Literal[MergeType.ENRICHED_TASK_ADJUST] = MergeType.ENRICHED_TASK_ADJUST
    _schema = EnrichedTaskAdjustSchema
    collection_item_class: Literal[EnrichedTaskAdjustMergeItem] = (
        EnrichedTaskAdjustMergeItem
    )

    def build(
        self,
        client: Client,
        task_adjust_coll: TaskAdjustmentDFCollection,
        enriched_wall: EnrichedWallMerge,
        pg_config: PostgresConfig,
    ) -> None:
        """The Enriched TaskAdjustMerge is treated differently than most Merge
        Collections because it requires some special consideration:

            - Due to Duplicate Removal issues - where the same task is Adjusted
                multiple times, and due to the way Dask works.. we cannot break
                this out into Items. The Task that is Tasked multiple times may
                not be in the same Item so the aggregation would fail.

            - The thl_taskadjustment db table, and the task_adj DF Collection
                are updated sequentially based on the

        """

        LOG.info(
            f"EnrichedTaskAdjustMerge.build(task_adj_coll={task_adjust_coll.signature()}, "
            f"pg_config={pg_config})"
        )

        assert isinstance(task_adjust_coll, TaskAdjustmentDFCollection)
        assert isinstance(enriched_wall, EnrichedWallMerge)
        assert isinstance(pg_config, PostgresConfig)

        assert (
            len(self.items) == 1
        ), "EnrichedTaskAdjustMerge should only have 1 CollectionItem"
        item: EnrichedTaskAdjustMergeItem = self.items[0]

        # item.build(client, user_coll=user_coll, client_resources=client_resources)
        try:
            item.build(
                client=client,
                task_adj_coll=task_adjust_coll,
                enriched_wall=enriched_wall,
                pg_config=pg_config,
            )
        except (Exception,) as e:
            capture_exception(error=e)
            pass
