from datetime import timedelta, datetime, timezone
from itertools import product

import pandas as pd
import pytest

# noinspection PyUnresolvedReferences
from distributed.utils_test import (
    gen_cluster,
    client_no_amm,
    loop,
    loop_in_thread,
    cleanup,
    cluster_fixture,
    client,
)

from generalresearch.incite.mergers.foundations.user_id_product import (
    UserIdProductMergeItem,
)
from test_utils.incite.mergers.conftest import user_id_product_merge


@pytest.mark.parametrize(
    argnames="offset, duration, start",
    argvalues=list(
        product(
            ["12h", "3D"],
            [timedelta(days=5)],
            [
                (datetime.now(tz=timezone.utc) - timedelta(days=35)).replace(
                    microsecond=0
                )
            ],
        )
    ),
)
class TestUserIDProduct:

    @pytest.mark.skip
    def test_base(self, client_no_amm, user_id_product_merge):
        ddf = user_id_product_merge.ddf()
        df = client_no_amm.compute(collections=ddf, sync=True)
        assert isinstance(df, pd.DataFrame)
        assert not df.empty

    @pytest.mark.skip
    def test_base_item(self, client_no_amm, user_id_product_merge, user_collection):
        assert len(user_id_product_merge.items) == 1

        for item in user_id_product_merge.items:
            assert isinstance(item, UserIdProductMergeItem)

            path = item.path

            try:
                modified_time1 = path.stat().st_mtime
            except (Exception,):
                modified_time1 = 0

            user_id_product_merge.build(client=client_no_amm, user_coll=user_collection)
            modified_time2 = path.stat().st_mtime

            assert modified_time2 > modified_time1

    @pytest.mark.skip
    def test_read(self, client_no_amm, user_id_product_merge):
        users_ddf = user_id_product_merge.ddf()
        df = client_no_amm.compute(collections=users_ddf, sync=True)

        assert isinstance(df, pd.DataFrame)
        assert len(df.columns) == 1
        assert str(df.product_id.dtype) == "category"
