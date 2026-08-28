from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from itertools import product

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
from generalresearch.incite.mergers.ym_survey_wall import YMSurveyWallMerge
from generalresearch.models.thl.product import Product
from generalresearch.models.thl.user import User
from generalresearch.pg_helper import PostgresConfig

# noinspection PyUnresolvedReferences


@pytest.mark.parametrize(
    argnames="offset, duration, start",
    argvalues=list(
        product(
            ["12h", "3D"],
            [timedelta(days=30)],
            [(datetime.now(tz=UTC) - timedelta(days=35)).replace(microsecond=0)],
        )
    ),
)
class TestYMSurveyMerge:
    """We override start, not because it's needed on the YMSurveyWall merge,
    which operates on a rolling 10-day window, but because we don't want
    to mock data in the wall collection and enriched_session_merge from
    the 1800s and then wonder why there is no data available in the past
    10 days in the database.
    """

    def test_base(
        self,
        client_no_amm: DaskClient,
        user_factory: Callable[..., User],
        product: Product,
        ym_survey_wall_merge: YMSurveyWallMerge,
        wall_collection: WallDFCollection,
        session_collection: SessionDFCollection,
        enriched_session_merge: EnrichedSessionMerge,
        delete_df_collection: Callable[..., None],
        incite_item_factory: Callable[..., None],
        thl_web_rr: PostgresConfig,
    ):

        delete_df_collection(coll=session_collection)
        user: User = user_factory(product=product, created=session_collection.start)

        # -- Build & Setup
        assert ym_survey_wall_merge.start is None
        assert ym_survey_wall_merge.offset == "10D"

        for item in session_collection.items:
            incite_item_factory(item=item, user=user)
            item.initial_load()
        for item in wall_collection.items:
            item.initial_load()

        # Confirm any of the items are archived
        assert session_collection.progress.has_archive.eq(True).all()
        assert wall_collection.progress.has_archive.eq(True).all()

        enriched_session_merge.build(
            client=client_no_amm,
            session_coll=session_collection,
            wall_coll=wall_collection,
            pg_config=thl_web_rr,
        )
        assert enriched_session_merge.progress.has_archive.eq(True).all()

        ddf = enriched_session_merge.ddf()
        df1: pd.DataFrame | None = client_no_amm.compute(collections=ddf, sync=True)

        assert isinstance(df1, pd.DataFrame)
        assert not df1.empty

        # --

        ym_survey_wall_merge.build(
            client=client_no_amm,
            wall_coll=wall_collection,
            enriched_session=enriched_session_merge,
        )
        assert ym_survey_wall_merge.progress.has_archive.eq(True).all()

        # --

        ddf = ym_survey_wall_merge.ddf()
        df2: pd.DataFrame | None = client_no_amm.compute(collections=ddf, sync=True)

        assert isinstance(df2, pd.DataFrame)
        assert not df2.empty

        # --
        assert df2.product_id.nunique() == 1
        assert df2.team_id.nunique() == 1
        assert df2.source.nunique() > 1

        started_min_ts = df2.started.min()
        started_max_ts = df2.started.max()

        assert type(started_min_ts) is pd.Timestamp
        assert type(started_max_ts) is pd.Timestamp

        started_min: datetime = datetime.fromisoformat(str(started_min_ts))
        started_max: datetime = datetime.fromisoformat(str(started_max_ts))

        started_delta = started_max - started_min
        assert started_delta >= timedelta(days=3)
