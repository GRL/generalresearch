from datetime import timedelta, timezone, datetime
from decimal import Decimal
from itertools import product as iter_product
from typing import Optional

import dask.dataframe as dd
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

from generalresearch.incite.mergers.foundations.enriched_wall import (
    EnrichedWallMergeItem,
)
from test_utils.incite.collections.conftest import (
    session_collection,
    wall_collection,
)
from test_utils.incite.conftest import incite_item_factory
from test_utils.incite.mergers.conftest import (
    enriched_wall_merge,
)


@pytest.mark.parametrize(
    argnames="offset, duration",
    argvalues=list(iter_product(["48h", "3D"], [timedelta(days=5)])),
)
class TestEnrichedWall:

    def test_base(
        self,
        client_no_amm,
        product,
        user_factory,
        wall_collection,
        thl_web_rr,
        session_collection,
        enriched_wall_merge,
        delete_df_collection,
        incite_item_factory,
    ):
        from generalresearch.models.thl.user import User

        # -- Build & Setup
        delete_df_collection(coll=session_collection)
        delete_df_collection(coll=wall_collection)
        u1: User = user_factory(product=product, created=session_collection.start)

        for item in session_collection.items:
            incite_item_factory(item=item, user=u1)
            item.initial_load()

        for item in wall_collection.items:
            item.initial_load()

        enriched_wall_merge.build(
            client=client_no_amm,
            wall_coll=wall_collection,
            session_coll=session_collection,
            pg_config=thl_web_rr,
        )

        # --

        ddf = enriched_wall_merge.ddf()
        assert isinstance(ddf, dd.DataFrame)

        df = client_no_amm.compute(collections=ddf, sync=True)
        assert isinstance(df, pd.DataFrame)

        assert not df.empty

    def test_base_item(
        self,
        client_no_amm,
        product,
        user_factory,
        wall_collection,
        session_collection,
        enriched_wall_merge,
        delete_df_collection,
        thl_web_rr,
        incite_item_factory,
    ):
        # -- Build & Setup
        delete_df_collection(coll=session_collection)
        u = user_factory(product=product, created=session_collection.start)

        for item in session_collection.items:
            incite_item_factory(item=item, user=u)
            item.initial_load()
        for item in wall_collection.items:
            item.initial_load()

        enriched_wall_merge.build(
            client=client_no_amm,
            wall_coll=wall_collection,
            session_coll=session_collection,
            pg_config=thl_web_rr,
        )

        # --

        for item in enriched_wall_merge.items:
            assert isinstance(item, EnrichedWallMergeItem)

            path = item.path

            try:
                modified_time1 = path.stat().st_mtime
            except (Exception,):
                modified_time1 = 0

            item.build(
                client=client_no_amm,
                wall_coll=wall_collection,
                session_coll=session_collection,
                pg_config=thl_web_rr,
            )
            modified_time2 = path.stat().st_mtime

            # Merger Items can't be updated unless it's a partial, confirm
            #   that even after attempting to rebuild, it doesn't re-touch
            #   the file
            assert modified_time2 == modified_time1

    # def test_admin_pop_session_device_type(ew_merge_setup):
    #     self.build()
    #
    #     rr = ReportRequest(
    #         report_type=ReportType.POP_EVENT,
    #         index0="started",
    #         index1="device_type",
    #         freq="min",
    #         start=start,
    #     )
    #
    #     df, categories, updated = self.instance.to_admin_response(
    #         rr=rr, product_ids=[self.product.id], client=client
    #     )
    #
    #     assert isinstance(df, pd.DataFrame)
    #     device_types_str = [str(e.value) for e in DeviceType]
    #     device_types = df.index.get_level_values(1).values
    #     assert all([dt in device_types_str for dt in device_types])


class TestEnrichedWallToAdmin:

    @pytest.fixture
    def start(self) -> "datetime":
        return datetime(year=2020, month=3, day=14, tzinfo=timezone.utc)

    @pytest.fixture
    def offset(self) -> str:
        return "1d"

    @pytest.fixture
    def duration(self) -> Optional["timedelta"]:
        return timedelta(days=5)

    def test_empty(self, enriched_wall_merge, client_no_amm, start):
        from generalresearch.models.admin.request import ReportRequest

        rr = ReportRequest.model_validate({"interval": "5min", "start": start})

        res = enriched_wall_merge.to_admin_response(
            rr=rr,
            client=client_no_amm,
        )

        assert isinstance(res, pd.DataFrame)

        assert res.empty
        assert len(res.columns) > 5

    def test_to_admin_response(
        self,
        event_report_request,
        enriched_wall_merge,
        client_no_amm,
        wall_collection,
        session_collection,
        thl_web_rr,
        user,
        session_factory,
        delete_df_collection,
        product_factory,
        user_factory,
        start,
    ):
        delete_df_collection(coll=wall_collection)
        delete_df_collection(coll=session_collection)

        p1 = product_factory()
        p2 = product_factory()

        for p in [p1, p2]:
            u = user_factory(product=p)
            for i in range(50):
                s = session_factory(
                    user=u,
                    wall_count=2,
                    wall_req_cpi=Decimal("1.00"),
                    started=start + timedelta(minutes=i, seconds=1),
                )

        wall_collection.initial_load(client=None, sync=True)
        session_collection.initial_load(client=None, sync=True)

        enriched_wall_merge.build(
            client=client_no_amm,
            wall_coll=wall_collection,
            session_coll=session_collection,
            pg_config=thl_web_rr,
        )

        df = enriched_wall_merge.to_admin_response(
            rr=event_report_request, client=client_no_amm
        )

        assert isinstance(df, pd.DataFrame)
        assert not df.empty
        # assert len(df) == 1
        # assert user.product_id == df.reset_index().loc[0, "index1"]
        assert df.index.get_level_values(1).nunique() == 2
