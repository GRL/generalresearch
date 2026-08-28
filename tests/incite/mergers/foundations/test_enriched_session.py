from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from itertools import product

import dask.dataframe as dd
import pandas as pd
import pytest
from dask.distributed import Client as DaskClient

from generalresearch.incite.collections.thl_web import (
    SessionDFCollection,
    WallDFCollection,
)
from generalresearch.incite.mergers.foundations.enriched_session import (
    EnrichedSessionMerge,
)
from generalresearch.incite.schemas.admin_responses import (
    AdminPOPSessionSchema,
)
from generalresearch.models.admin.request import (
    ReportRequest,
)
from generalresearch.models.thl.product import Product
from generalresearch.models.thl.session import Session
from generalresearch.models.thl.user import User
from generalresearch.pg_helper import PostgresConfig


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
        client_no_amm: DaskClient,
        product: Product,
        user_factory: Callable[..., User],
        wall_collection: WallDFCollection,
        session_collection: SessionDFCollection,
        enriched_session_merge: EnrichedSessionMerge,
        thl_web_rr: PostgresConfig,
        delete_df_collection: Callable[..., None],
        incite_item_factory: Callable[..., None],
    ):

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
    def start(self) -> datetime:
        return datetime(year=2020, month=3, day=14, tzinfo=UTC)

    @pytest.fixture
    def offset(self) -> str:
        return "1d"

    @pytest.fixture
    def duration(self) -> timedelta | None:
        return timedelta(days=5)

    def test_to_admin_response(
        self,
        event_report_request: ReportRequest,
        enriched_session_merge: EnrichedSessionMerge,
        client_no_amm: DaskClient,
        wall_collection: WallDFCollection,
        session_collection: SessionDFCollection,
        thl_web_rr: PostgresConfig,
        session_report_request: ReportRequest,
        user_factory: Callable[..., User],
        start: datetime,
        session_factory: Callable[..., Session],
        product_factory: Callable[..., Product],
        delete_df_collection: Callable[..., None],
    ):
        delete_df_collection(coll=wall_collection)
        delete_df_collection(coll=session_collection)

        p1 = product_factory()
        p2 = product_factory()

        for p in [p1, p2]:
            u = user_factory(product=p)
            for i in range(50):
                _ = session_factory(
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
