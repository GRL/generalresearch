import logging
from datetime import timedelta
from typing import TYPE_CHECKING, Any, Dict, List, Literal, Optional

import dask.dataframe as dd
import pandas as pd
from dask.distributed import as_completed
from distributed import Client
from more_itertools import chunked, flatten

from generalresearch.incite.collections.thl_web import (
    SessionDFCollection,
    WallDFCollection,
)
from generalresearch.incite.mergers import (
    MergeCollection,
    MergeCollectionItem,
    MergeType,
)
from generalresearch.incite.mergers.foundations import (
    lookup_product_and_team_id,
)
from generalresearch.incite.schemas import empty_dataframe_from_schema
from generalresearch.incite.schemas.admin_responses import (
    AdminPOPSessionSchema,
)
from generalresearch.incite.schemas.mergers.foundations.enriched_session import (
    EnrichedSessionSchema,
)
from generalresearch.models.custom_types import UUIDStr
from generalresearch.models.thl.user import User
from generalresearch.pg_helper import PostgresConfig

if TYPE_CHECKING:
    from generalresearch.models.admin.request import ReportRequest

LOG = logging.getLogger("incite")


class EnrichedSessionMergeItem(MergeCollectionItem):

    def build(
        self,
        session_coll: SessionDFCollection,
        wall_coll: WallDFCollection,
        pg_config: PostgresConfig,
        client: Optional[Client] = None,
        client_resources: Optional[Dict[str, Any]] = None,
    ) -> None:

        ir: pd.Interval = self.interval
        start, end = ir.left.to_pydatetime(), ir.right.to_pydatetime()

        LOG.warning(f"EnrichedSessionMergeItem.build({ir})")

        # Skip which already exist
        if self.has_archive(include_empty=True):
            return None

        # --- Session ---
        LOG.warning(f"EnrichedSessionMergeItem: get session_collection")
        session_items = [w for w in session_coll.items if w.interval.overlaps(ir)]
        if len(session_items) == 0:
            LOG.warning(f"EnrichedSessionMergeItem: no session items. set_empty.")
            if self.should_archive():
                self.set_empty()
            return None
        if not (
            session_items[-1].has_partial_archive() or session_items[-1].has_archive()
        ):
            LOG.warning(f"EnrichedSessionMergeItem: session isn't updated!")
            return None

        sddf = session_coll.ddf(
            items=session_items,
            include_partial=True,
            force_rr_latest=False,
            filters=[("started", ">=", start), ("started", "<", end)],
        )

        # --- Walls ---
        LOG.warning(f"EnrichedSessionMergeItem: merge wall_collection")
        wall_items = [
            w
            for w in wall_coll.items
            if w.interval.overlaps(
                pd.Interval(
                    ir.left - timedelta(hours=2),
                    ir.right + timedelta(hours=2),
                    closed="both",
                )
            )
        ]

        if len(wall_items) == 0:
            LOG.error(f"EnrichedSessionMergeItem: no wall items")
            return None

        wddf = wall_coll.ddf(
            items=wall_items,
            include_partial=True,
            force_rr_latest=False,
            columns=["session_id"],
            filters=[
                ("started", ">=", start - timedelta(hours=2)),
                ("started", "<", end + timedelta(hours=2)),
            ],
        )

        if wddf is None:
            return None

        attempt_cnt_ddf = (
            wddf.groupby("session_id").size().rename("attempt_count").to_frame()
        )
        ddf = sddf.join(attempt_cnt_ddf, how="left", npartitions=12)
        ddf["attempt_count"] = ddf["attempt_count"].fillna(0)
        # ddf = ddf.repartition(npartitions=4)
        ddf = ddf.reset_index()

        # Unclear if this is needed. We are client.computing ddf literally
        # in the next line, so I think it is not.
        # client.persist(ddf)

        df: pd.DataFrame = client.compute(ddf, sync=True)

        user_ids = set(
            map(int, df["user_id"].unique())
        )  # must int, otherwise it's a np.int sigh

        # Submit at most N tasks at a time. Will be useful when we have 32 workers again.
        futures = set()
        for chunk in chunked(user_ids, 500):
            ac = as_completed(futures)
            while ac.count() >= 4:
                next(ac)  # Wait for tasks to finish before submitting a new one
            futures.add(client.submit(lookup_product_and_team_id, chunk, pg_config))

        try:
            results = client.gather(list(futures))
        except Exception as e:
            client.cancel(futures, asynchronous=False, force=True)
            raise e

        dfp = pd.DataFrame(
            list(flatten(results)), columns=["user_id", "product_id", "team_id"]
        ).astype({"user_id": int, "product_id": str, "team_id": str})
        df = df.merge(dfp, on="user_id", how="left")

        df = df.set_index("id")

        df = df[df["started"].between(start, end)]

        is_missing = df[["product_id"]].isna().sum().sum() > 0
        session_is_partial = any([w.should_archive() is False for w in session_items])
        session_is_missing = any(
            [
                w.should_archive() is True and w.has_archive() is False
                for w in session_items
            ]
        )
        wall_is_missing = any(
            [
                w.should_archive() is True and w.has_archive() is False
                for w in wall_items
            ]
        )
        is_partial = (
            is_missing or session_is_partial or session_is_missing or wall_is_missing
        )

        LOG.warning(f"missing user_ids: {df[df.product_id.isnull()].user_id.unique()}")
        # Lots of downstream issues with this...
        df = df[df.product_id.notna()]
        df.product_id = df.product_id.astype(str)

        if is_partial:
            ddf = dd.from_pandas(df, npartitions=6)
            self.to_archive_symlink(
                client,
                ddf=ddf,
                is_partial=True,
                validate_after=False,
                client_resources=client_resources,
            )
        else:
            df = self.validate_df(df=df)
            ddf = dd.from_pandas(df, npartitions=6)
            self.to_archive(
                client,
                ddf=ddf,
                is_partial=False,
                client_resources=client_resources,
            )


class EnrichedSessionMerge(MergeCollection):
    merge_type: Literal[MergeType.ENRICHED_SESSION] = MergeType.ENRICHED_SESSION
    _schema = EnrichedSessionSchema
    collection_item_class: Literal[EnrichedSessionMergeItem] = EnrichedSessionMergeItem

    def build(
        self,
        client: Client,
        session_coll: SessionDFCollection,
        wall_coll: WallDFCollection,
        pg_config: PostgresConfig,
    ) -> None:
        LOG.info(
            f"EnrichedSessionMerge.build(session_coll={session_coll.signature()}, "
            f"wall_coll={wall_coll.signature()}, "
            f"pg_config={pg_config})"
        )

        assert isinstance(session_coll, SessionDFCollection)
        assert isinstance(wall_coll, WallDFCollection)
        assert isinstance(pg_config, PostgresConfig)

        for item in reversed(self.items):
            if item.has_archive(include_empty=True):
                continue
            LOG.info(item)
            item.build(
                client=client,
                session_coll=session_coll,
                wall_coll=wall_coll,
                pg_config=pg_config,
            )

    def to_admin_response(
        self,
        rr: "ReportRequest",
        client: Client,
        product_ids: Optional[List[UUIDStr]] = None,
        user: Optional[User] = None,
    ) -> pd.DataFrame:
        """
        We don't have the concept of a Team yet so product_ids will be a list
        """

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

        # es_items = [w for w in self.items if w.interval.overlaps(rr.pd_interval)]
        ddf = self.ddf(
            # items=es_items,
            force_rr_latest=False,
            include_partial=True,
            columns=[
                "product_id",
                "user_id",
                "started",
                "finished",
                "attempt_count",
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
            return empty_dataframe_from_schema(schema=EnrichedSessionSchema)

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
        # dfa = dfa[dfa["started"].between(rr.start, rr.finish)]

        assert rr.interval == "5min"
        group_arr = [pd.Grouper(key="started", freq=rr.interval), rr.index1]
        df = dfa.groupby(group_arr).aggregate(
            elapsed_avg=("elapsed", "mean"),
            elapsed_total=("elapsed", "sum"),
            payout_total=("payout", "sum"),
            attempts_avg=("attempt_count", "mean"),
            attempts_total=("attempt_count", "sum"),
            entrances=("complete", "size"),
            completes=("complete", "sum"),
            users=("user_id", "nunique"),
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
        return AdminPOPSessionSchema.validate(df).fillna(0)
