from datetime import timedelta
from itertools import product as iter_product

import dask.dataframe as dd
import pandas as pd
import pytest

from test_utils.incite.collections.conftest import (
    wall_collection,
    task_adj_collection,
    session_collection,
)
from test_utils.incite.mergers.conftest import enriched_wall_merge


@pytest.mark.parametrize(
    argnames="offset, duration,",
    argvalues=list(
        iter_product(
            ["12h", "3D"],
            [timedelta(days=5)],
        )
    ),
)
class TestEnrichedTaskAdjust:

    @pytest.mark.skip
    def test_base(
        self,
        client_no_amm,
        user_factory,
        product,
        task_adj_collection,
        wall_collection,
        session_collection,
        enriched_wall_merge,
        enriched_task_adjust_merge,
        incite_item_factory,
        delete_df_collection,
        thl_web_rr,
    ):
        from generalresearch.models.thl.user import User

        # -- Build & Setup
        delete_df_collection(coll=session_collection)
        u1: User = user_factory(product=product)

        for item in session_collection.items:
            incite_item_factory(user=u1, item=item)
            item.initial_load()
        for item in wall_collection.items:
            item.initial_load()

        enriched_wall_merge.build(
            client=client_no_amm,
            session_coll=session_collection,
            wall_coll=wall_collection,
            pg_config=thl_web_rr,
        )

        enriched_task_adjust_merge.build(
            client=client_no_amm,
            task_adjust_coll=task_adj_collection,
            enriched_wall=enriched_wall_merge,
            pg_config=thl_web_rr,
        )

        # --

        ddf = enriched_task_adjust_merge.ddf()
        assert isinstance(ddf, dd.DataFrame)

        df = client_no_amm.compute(collections=ddf, sync=True)
        assert isinstance(df, pd.DataFrame)

        assert not df.empty
