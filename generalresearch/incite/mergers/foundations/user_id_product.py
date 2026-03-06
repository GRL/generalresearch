import logging
from typing import Literal

from distributed import Client

from generalresearch.incite.collections.thl_web import UserDFCollection
from generalresearch.incite.mergers import (
    MergeCollectionItem,
    MergeCollection,
    MergeType,
)

LOG = logging.getLogger("incite")


class UserIdProductMergeItem(MergeCollectionItem):

    def build(
        self, client: Client, user_coll: UserDFCollection, client_resources=None
    ) -> None:
        LOG.warning(f"UserIdProductMergeItem.build({self.interval})")

        udf = user_coll.ddf(
            include_partial=True, force_rr_latest=False, columns=["product_id"]
        )
        udf = udf.repartition(npartitions=40)
        udf = udf.categorize(columns=["product_id"])
        # This is the best way I think. Each worker can read, categorize,
        # and write its own chunk, and data doesn't have to be sent back
        # and forth. We can validate the df afterward!
        self.to_archive_symlink(client, client_resources=client_resources, ddf=udf)


class UserIdProductMerge(MergeCollection):
    merge_type: Literal[MergeType.USER_ID_PRODUCT] = MergeType.USER_ID_PRODUCT
    collection_item_class: Literal[UserIdProductMergeItem] = UserIdProductMergeItem
    offset: None = None

    def build(
        self, client: Client, user_coll: UserDFCollection, client_resources=None
    ) -> None:
        LOG.info(f"UserIdProductMerge.build(user_coll={user_coll.signature()})")

        assert (
            len(self.items) == 1
        ), "UserIdProductMerge should only have 1 CollectionItem"
        item: UserIdProductMergeItem = self.items[0]

        item.build(client, user_coll=user_coll, client_resources=client_resources)
