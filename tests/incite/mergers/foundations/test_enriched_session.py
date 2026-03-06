from datetime import timedelta, timezone, datetime
from decimal import Decimal
from itertools import product
from typing import Optional

from generalresearch.incite.schemas.admin_responses import (
    AdminPOPSessionSchema,
)

import dask.dataframe as dd
import pandas as pd
import pytest

from test_utils.incite.collections.conftest import (
    session_collection,
    wall_collection,
)


@pytest.mark.parametrize(
    argnames="offset, duration",
    argvalues=list(
        product(
            ["12h", "3D"],
            [timedelta(days=5)],
        )
    ),
)
class TestEnrichedSession:

    def test_base(
        self,
        client_no_amm,
        product,
        user_factory,
        wall_collection,
        session_collection,
        enriched_session_merge,
        thl_web_rr,
        delete_df_collection,
        incite_item_factory,
    ):
        from generalresearch.models.thl.user import User

        delete_df_collection(coll=session_collection)

        u1: User = user_factory(product=product, created=session_collection.start)

        for item in session_collection.items:
            incite_item_factory(item=item, user=u1)
            item.initial_load()

        for item in wall_collection.items:
            item.initial_load()

        enriched_session_merge.build(
            client=client_no_amm,
            wall_coll=wall_collection,
            session_coll=session_collection,
            pg_config=thl_web_rr,
        )

        # --

        ddf = enriched_session_merge.ddf()
        assert isinstance(ddf, dd.DataFrame)

        df = client_no_amm.compute(collections=ddf, sync=True)
        assert isinstance(df, pd.DataFrame)

        assert not df.empty

        # -- Teardown
        delete_df_collection(session_collection)


class TestEnrichedSessionAdmin:

    @pytest.fixture
    def start(self) -> "datetime":
        return datetime(year=2020, month=3, day=14, tzinfo=timezone.utc)

    @pytest.fixture
    def offset(self) -> str:
        return "1d"

    @pytest.fixture
    def duration(self) -> Optional["timedelta"]:
        return timedelta(days=5)

    def test_to_admin_response(
        self,
        event_report_request,
        enriched_session_merge,
        client_no_amm,
        wall_collection,
        session_collection,
        thl_web_rr,
        session_report_request,
        user_factory,
        start,
        session_factory,
        product_factory,
        delete_df_collection,
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
                    wall_count=1,
                    wall_req_cpi=Decimal("1.00"),
                    started=start + timedelta(minutes=i, seconds=1),
                )
        wall_collection.initial_load(client=None, sync=True)
        session_collection.initial_load(client=None, sync=True)

        enriched_session_merge.build(
            client=client_no_amm,
            session_coll=session_collection,
            wall_coll=wall_collection,
            pg_config=thl_web_rr,
        )

        df = enriched_session_merge.to_admin_response(
            rr=session_report_request, client=client_no_amm
        )

        assert isinstance(df, pd.DataFrame)
        assert not df.empty
        assert isinstance(AdminPOPSessionSchema.validate(df), pd.DataFrame)
        assert df.index.get_level_values(1).nunique() == 2
