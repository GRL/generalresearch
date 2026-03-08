import logging
from typing import Any, Dict, Literal, Optional

import dask.dataframe as dd
import pandas as pd
from distributed import Client
from more_itertools import flatten

from generalresearch.incite.collections.thl_web import LedgerDFCollection
from generalresearch.incite.mergers import (
    MergeCollection,
    MergeCollectionItem,
    MergeType,
)
from generalresearch.incite.schemas.mergers.pop_ledger import PopLedgerSchema
from generalresearch.models.thl.ledger import Direction, TransactionType

LOG = logging.getLogger("incite")


class PopLedgerMergeItem(MergeCollectionItem):

    def build(
        self,
        ledger_coll: LedgerDFCollection,
        client: Optional[Client] = None,
        client_resources: Optional[Dict[str, Any]] = None,
    ) -> None:
        ir: pd.Interval = self.interval

        is_partial = not self.should_archive()
        start, end = ir.left.to_pydatetime(), ir.right.to_pydatetime()

        ledger_items = [
            s
            for s in ledger_coll.items
            if s.interval.overlaps(pd.Interval(ir.left, ir.right, closed="left"))
        ]

        ddf = ledger_coll.ddf(
            items=ledger_items,
            include_partial=True,
            force_rr_latest=False,
        )

        if ddf is None:
            return None

        ddf = ddf[ddf["created"].between(start, end)]
        df: pd.DataFrame = client.compute(ddf, resources=client_resources, sync=True)

        if df.empty:
            # self.set_empty()
            return None

        df["direction_name"] = df["direction"].apply(lambda x: Direction(x).name)

        # The smallest "unit time interval" supported by this merge. It can be
        # resampled to anything larger in the future, but not smaller. We use
        # the dt.floor so the intervals do not overlap
        df["time_idx"] = df["created"].dt.floor("1min")

        # For each time interval and Ledger Account (this is different from a
        # product_id), we want the raw amounts and their respective direction
        # for every type of transaction that is possible
        x = (
            df.groupby(by=["time_idx", "account_id", "tx_type", "direction_name"])
            .amount.sum()
            .reset_index()
        )

        # We want to keep the positive and negatives for each type. For example,
        #   for bp_adjustment, we want to know the amount increased and the
        #   amount decreased, not just the net.
        x["tx_type.direction"] = x["tx_type"] + "." + x["direction_name"]
        s = (
            x.pivot_table(
                index=["time_idx", "account_id"],
                columns="tx_type.direction",
                values="amount",
                aggfunc="sum",
            )
            .fillna(0)
            .reset_index()
        )

        columns = set(
            flatten(
                [[e.value + ".CREDIT", e.value + ".DEBIT"] for e in TransactionType]
            )
        )
        s = s.reindex(columns=columns | set(s.columns)).fillna(0)
        s = s.reset_index(drop=True)
        s.index.name = "id"
        # The "columns were named" tx_type.direction from the above pivot. This
        #   made it confusing when viewing in a console, so we rename it here,
        #   it doesn't provide any functional change
        s.columns.name = None

        s = PopLedgerSchema.validate(s)
        ddf = dd.from_pandas(s, npartitions=1)

        if is_partial:
            self.to_archive_symlink(
                client=client,
                ddf=ddf,
                is_partial=True,
                validate_after=False,
                client_resources=client_resources,
            )
        else:
            self.to_archive(client=client, ddf=ddf)


class PopLedgerMerge(MergeCollection):
    merge_type: Literal[MergeType.POP_LEDGER] = MergeType.POP_LEDGER
    _schema = PopLedgerSchema
    collection_item_class: Literal[PopLedgerMergeItem] = PopLedgerMergeItem

    def build(self, client: Client, ledger_coll: LedgerDFCollection) -> None:

        LOG.info(f"PopLedgerMerge.build(wall_coll={ledger_coll.signature()}")

        assert isinstance(ledger_coll, LedgerDFCollection)

        for item in reversed(self.items):
            if item.has_archive(include_empty=True):
                continue

            LOG.debug(msg=item)
            item.build(client=client, ledger_coll=ledger_coll)
