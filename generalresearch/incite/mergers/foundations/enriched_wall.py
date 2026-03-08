import logging
from datetime import timedelta
from typing import TYPE_CHECKING, Any, Dict, List, Literal, Optional

import dask.dataframe as dd
import pandas as pd
from distributed import Client

from generalresearch.incite.collections.thl_web import (
    SessionDFCollection,
    WallDFCollection,
)
from generalresearch.incite.mergers import (
    MergeCollection,
    MergeCollectionItem,
    MergeType,
)
from generalresearch.incite.mergers.foundations import annotate_product_id
from generalresearch.incite.schemas.admin_responses import (
    AdminPOPWallSchema,
)
from generalresearch.incite.schemas.mergers.foundations.enriched_wall import (
    EnrichedWallSchema,
)
from generalresearch.models.custom_types import UUIDStr
from generalresearch.models.thl.user import User
from generalresearch.pg_helper import PostgresConfig

if TYPE_CHECKING:
    from generalresearch.models.admin.request import ReportRequest

LOG = logging.getLogger("incite")


class EnrichedWallMergeItem(MergeCollectionItem):

    def build(
        self,
        wall_coll: WallDFCollection,
        session_coll: SessionDFCollection,
        pg_config: PostgresConfig,
        client: Optional[Client] = None,
        client_resources: Optional[Dict[str, Any]] = None,
    ) -> None:

        ir: pd.Interval = self.interval
        start, end = ir.left.to_pydatetime(), ir.right.to_pydatetime()

        LOG.warning(f"EnrichedWallMergeItem.build({ir})")

        # Skip which already exist
        if self.has_archive(include_empty=True):
            return None

        # --- Wall ---
        LOG.warning(f"EnrichedWallMergeItem: get wall_collection")
        wall_items = [w for w in wall_coll.items if w.interval.overlaps(ir)]
        if len(wall_items) == 0:
            LOG.warning(f"EnrichedWallMergeItem: no wall items. set_empty.")
            if self.should_archive():
                self.set_empty()
            return None

        wdf = wall_coll.ddf(
            items=wall_items,
            include_partial=True,
            force_rr_latest=False,
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
            filters=[("started", ">=", start), ("started", "<", end)],
        )

        if wdf is None:
            return None

        wdf = wdf.repartition(npartitions=1)
        wdf = wdf.reset_index(drop=False)

        # --- Sessions ---
        LOG.warning(f"EnrichedWallMergeItem: merge session_collection")
        session_items = [
            s
            for s in session_coll.items
            if s.interval.overlaps(
                pd.Interval(
                    ir.left - timedelta(hours=6),
                    ir.right + timedelta(hours=6),
                    closed="both",
                )
            )
        ]

        if len(session_items) == 0:
            LOG.error(f"EnrichedWallMergeItem: no session items. breaking early.")
            return None

        sdf = session_coll.ddf(
            items=session_items,
            include_partial=True,
            force_rr_latest=False,
            columns=["user_id", "country_iso", "device_type", "payout"],
            filters=[
                ("started", ">=", start - timedelta(hours=6)),
                ("started", "<", end + timedelta(hours=6)),
            ],
        )
        sdf = sdf.repartition(npartitions=1)

        mddf = dd.merge(
            wdf,
            sdf,
            left_on="session_id",
            right_index=True,
            how="left",
            npartitions=1,
        )
        client.persist(mddf)

        # --- Add product_id for the user --- #
        expected_df = mddf.copy()
        expected_df["product_id"] = pd.Series(dtype="str")
        res: dd.DataFrame = mddf.map_partitions(
            annotate_product_id, pg_config, meta=expected_df
        )

        # --- cleanup ---
        df: pd.DataFrame = client.compute(collections=res, sync=True)
        df = df[df["started"].between(start, end)]
        df = df.set_index("uuid")

        # is_missing = df[['product_id', 'session_id']].isna().sum().sum() > 0
        is_missing = False
        df = df.dropna(subset=["product_id", "session_id"], how="any")

        wall_is_partial = any([w.should_archive() is False for w in wall_items])
        is_partial = is_missing or wall_is_partial

        # Lots of downstream issues with this...
        df = df[df.product_id.notna()]
        df.product_id = df.product_id.astype(str)

        if is_partial:
            ddf = dd.from_pandas(df, npartitions=5)
            self.to_archive_symlink(
                client,
                ddf=ddf,
                is_partial=True,
                validate_after=False,
                client_resources=client_resources,
            )
        else:
            df = self.validate_df(df=df)
            ddf = dd.from_pandas(df, npartitions=5)
            self.to_archive(
                client,
                ddf=ddf,
                is_partial=False,
                client_resources=client_resources,
            )


class EnrichedWallMerge(MergeCollection):
    merge_type: Literal[MergeType.ENRICHED_WALL] = MergeType.ENRICHED_WALL
    _schema = EnrichedWallSchema
    collection_item_class: Literal[EnrichedWallMergeItem] = EnrichedWallMergeItem

    def build(
        self,
        client: Client,
        wall_coll: WallDFCollection,
        session_coll: SessionDFCollection,
        pg_config: PostgresConfig,
    ) -> None:

        LOG.info(
            f"EnrichedWallMerge.build(wall_coll={wall_coll.signature()}, "
            f"session_coll={session_coll.signature()}, "
            f"pg_config={pg_config})"
        )

        assert isinstance(wall_coll, WallDFCollection)
        assert isinstance(session_coll, SessionDFCollection)
        assert isinstance(pg_config, PostgresConfig)

        for item in reversed(self.items):
            if item.has_archive(include_empty=True):
                continue

            LOG.info(item)
            item.build(
                client=client,
                wall_coll=wall_coll,
                session_coll=session_coll,
                pg_config=pg_config,
            )

        # This does not work. deadlocks. I need to submit them gradually or something
        # fs = []
        # for item in self.items:
        #     if item.has_archive(include_empty=True):
        #         continue
        #     if not item.should_archive():
        #         continue
        #     f = dask.delayed(item.build)(wall_coll=wall_coll, session_coll=session_coll,
        #                                  user_id_product=user_id_product)
        #     fs.append(f)
        #
        # # self = enriched_wall
        # # item = self.items[0]
        # # fs = [dask.delayed(item.build)(wall_coll=wall_coll, session_coll=session_coll,
        # #                                  user_id_product=user_id_product)]
        # res = client.compute(collections=fs, sync=True, priority=1)
        # return res

    def to_admin_response(
        self,
        rr: "ReportRequest",
        client: Client,
        product_ids: Optional[List[UUIDStr]] = None,
        user: Optional[User] = None,
    ) -> pd.DataFrame:
        """We don't have the concept of a Team yet so product_ids will be a list"""

        filters = []

        if user:
            assert (
                len(product_ids) <= 1
            ), "Can't search more than 1 Product ID for a specific User"
            assert (
                user.product_id in product_ids
            ), "The provided user must be associated with the Product ID"
            filters.append(
                ("user_id", "==", user.user_id),
            )

        if product_ids:
            assert (
                len(product_ids) >= 1
            ), "Don't provide an empty list. Pass None if SELECT ALL is desired"
            filters.append(
                ("product_id", "in", product_ids),
            )

        ddf = self.ddf(
            force_rr_latest=False,
            include_partial=True,
            columns=[
                "product_id",
                "user_id",
                "source",
                "buyer_id",
                "survey_id",
                "session_id",
                "started",
                "finished",
                "status",
                "status_code_1",
                "status_code_2",
                "country_iso",
                "device_type",
                "payout",
            ],
            filters=filters,
        )

        if ddf is None:
            from generalresearch.incite.schemas import (
                empty_dataframe_from_schema,
            )

            return empty_dataframe_from_schema(schema=EnrichedWallSchema)

        ddf["elapsed"] = (ddf["finished"] - ddf["started"]).dt.total_seconds()

        ddf["status"] = ddf.status.fillna("e")
        ddf["status_code_1"] = ddf.status_code_1.fillna(0)
        ddf["status_code_2"] = ddf.status_code_2.fillna(0)
        ddf["complete"] = ddf.status.eq("c")

        dfa = client.compute(
            collections=ddf,
            sync=True,
            priority=1,
        )

        # --- Add wall index per session --
        assert rr.interval == "5min"
        group_arr = [pd.Grouper(key="started", freq=rr.interval), rr.index1]

        df = dfa.groupby(group_arr).aggregate(
            elapsed_avg=("elapsed", "mean"),
            elapsed_total=("elapsed", "sum"),
            payout_total=("payout", "sum"),
            entrances=("complete", "size"),
            completes=("complete", "sum"),
            users=("user_id", "nunique"),
            buyers=("buyer_id", "nunique"),
            surveys=("survey_id", "nunique"),
            sessions=("session_id", "nunique"),
        )

        # Completes only
        df_completes = (
            dfa[(dfa.status == "c") & (dfa.payout > 0)]
            .groupby(group_arr)
            .aggregate(
                payout_avg=("payout", "mean"),
                elapsed_avg=("elapsed", "mean"),
                elapsed_total=("elapsed", "sum"),
            )
        )

        df["payout_avg"] = df_completes.payout_avg
        df["conversion"] = df.completes / df.entrances  # system conversion
        df["epc"] = df.payout_total / df.entrances  # earnings per click
        df["eph"] = df.payout_total / (df.elapsed_total / 3_600)  # earnings per hour
        # df["eph"] = df.payout_total / (df_completes.elapsed_total / 3_600)  # earnings per hour
        df["cpc"] = df_completes.payout_avg * df.conversion

        df.index = df.index.set_names(names=["index0", "index1"])
        return AdminPOPWallSchema.validate(df).fillna(0)
