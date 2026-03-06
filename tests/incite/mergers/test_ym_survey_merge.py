from datetime import timedelta, timezone, datetime
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

from test_utils.incite.collections.conftest import wall_collection, session_collection
from test_utils.incite.mergers.conftest import (
    enriched_session_merge,
    ym_survey_wall_merge,
)


@pytest.mark.parametrize(
    argnames="offset, duration, start",
    argvalues=list(
        product(
            ["12h", "3D"],
            [timedelta(days=30)],
            [
                (datetime.now(tz=timezone.utc) - timedelta(days=35)).replace(
                    microsecond=0
                )
            ],
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
        client_no_amm,
        user_factory,
        product,
        ym_survey_wall_merge,
        wall_collection,
        session_collection,
        enriched_session_merge,
        delete_df_collection,
        incite_item_factory,
        thl_web_rr,
    ):
        from generalresearch.models.thl.user import User

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
        df: pd.DataFrame = client_no_amm.compute(collections=ddf, sync=True)

        assert isinstance(df, pd.DataFrame)
        assert not df.empty

        # --

        ym_survey_wall_merge.build(
            client=client_no_amm,
            wall_coll=wall_collection,
            enriched_session=enriched_session_merge,
        )
        assert ym_survey_wall_merge.progress.has_archive.eq(True).all()

        # --

        ddf = ym_survey_wall_merge.ddf()
        df: pd.DataFrame = client_no_amm.compute(collections=ddf, sync=True)

        assert isinstance(df, pd.DataFrame)
        assert not df.empty

        # --
        assert df.product_id.nunique() == 1
        assert df.team_id.nunique() == 1
        assert df.source.nunique() > 1

        started_min_ts = df.started.min()
        started_max_ts = df.started.max()

        assert type(started_min_ts) is pd.Timestamp
        assert type(started_max_ts) is pd.Timestamp

        started_min: datetime = datetime.fromisoformat(str(started_min_ts))
        started_max: datetime = datetime.fromisoformat(str(started_max_ts))

        started_delta = started_max - started_min
        assert started_delta >= timedelta(days=3)
