from __future__ import annotations

import os
from collections.abc import Callable
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd
from dask.distributed import Client as DaskClient
from distributed.utils_test import (
    client_no_amm,
)

from generalresearch.models.gr.business import Business
from generalresearch.models.gr.team import Team
from generalresearch.models.thl.product import Product

if TYPE_CHECKING:
    from generalresearch.incite.base import GRLDatasets
    from generalresearch.incite.collections.thl_web import (
        SessionDFCollection,
        WallDFCollection,
    )
    from generalresearch.incite.mergers.foundations.enriched_session import (
        EnrichedSessionMerge,
    )
    from generalresearch.incite.mergers.foundations.enriched_wall import (
        EnrichedWallMerge,
    )
    from generalresearch.managers.gr.team import MembershipManager, TeamManager
    from generalresearch.models.gr.authentication import GRUser
    from generalresearch.models.gr.team import Membership
    from generalresearch.models.thl.session import Session
    from generalresearch.models.thl.user import User
    from generalresearch.pg_helper import PostgresConfig
    from generalresearch.redis_helper import RedisConfig


class TestTeam:

    def test_init(self, team: Team):

        assert isinstance(team, Team)
        assert isinstance(team.id, int)
        assert isinstance(team.uuid, str)

    def test_memberships_none(self, team: Team, gr_db: PostgresConfig):
        assert team.memberships is None

        team.prefetch_memberships(pg_config=gr_db)
        assert isinstance(team.memberships, list)
        assert len(team.memberships) == 0

    def test_memberships(
        self,
        team: Team,
        gr_user: GRUser,
        gr_user_factory: Callable[..., GRUser],
        membership_manager: MembershipManager,
        gr_db: PostgresConfig,
    ):
        assert team.memberships is None

        team.prefetch_memberships(pg_config=gr_db)
        assert isinstance(team.memberships, list)
        assert len(team.memberships) == 1
        assert team.memberships[0].user_id == gr_user.id

        # Create another new Membership
        membership_manager.create(team=team, gr_user=gr_user_factory())
        assert len(team.memberships) == 1
        team.prefetch_memberships(pg_config=gr_db)
        assert len(team.memberships) == 2

    def test_gr_users(
        self,
        team: Team,
        gr_user_factory: Callable[..., GRUser],
        membership_manager: MembershipManager,
        gr_db: PostgresConfig,
        gr_redis_config: RedisConfig,
    ):
        assert team.gr_users is None

        team.prefetch_gr_users(pg_config=gr_db, redis_config=gr_redis_config)
        assert isinstance(team.gr_users, list)
        assert len(team.gr_users) == 0

        # Create a new Membership
        membership_manager.create(team=team, gr_user=gr_user_factory())
        assert len(team.gr_users) == 0
        team.prefetch_gr_users(pg_config=gr_db, redis_config=gr_redis_config)
        assert len(team.gr_users) == 1

        # Create another Membership
        membership_manager.create(team=team, gr_user=gr_user_factory())
        assert len(team.gr_users) == 1
        team.prefetch_gr_users(pg_config=gr_db, redis_config=gr_redis_config)
        assert len(team.gr_users) == 2

    def test_businesses(
        self,
        team: Team,
        business: Business,
        team_manager: TeamManager,
        gr_db: PostgresConfig,
        gr_redis_config: RedisConfig,
    ):

        assert team.businesses is None

        team.prefetch_businesses(pg_config=gr_db, redis_config=gr_redis_config)
        assert isinstance(team.businesses, list)
        assert len(team.businesses) == 0

        team_manager.add_business(team=team, business=business)
        assert len(team.businesses) == 0
        team.prefetch_businesses(pg_config=gr_db, redis_config=gr_redis_config)
        assert len(team.businesses) == 1
        assert isinstance(team.businesses[0], Business)
        assert team.businesses[0].uuid == business.uuid

    def test_products(
        self,
        team: Team,
        product_factory: Callable[..., Product],
        thl_web_rr: PostgresConfig,
    ):

        assert team.products is None

        team.prefetch_products(thl_pg_config=thl_web_rr)
        assert isinstance(team.products, list)
        assert len(team.products) == 0

        product_factory(team=team)
        assert len(team.products) == 0
        team.prefetch_products(thl_pg_config=thl_web_rr)
        assert len(team.products) == 1
        assert isinstance(team.products[0], Product)


class TestTeamMethods:

    def test_cache_key(self, team: Team):
        assert isinstance(team.cache_key, str)
        assert ":" in team.cache_key
        assert str(team.uuid) in team.cache_key

    def test_set_cache(
        self,
        team: Team,
        gr_db: PostgresConfig,
        thl_web_rr: PostgresConfig,
        gr_redis_config: RedisConfig,
        client_no_amm: DaskClient,
        mnt_filepath: GRLDatasets,
        mnt_gr_api_dir: Path,
        enriched_wall_merge: EnrichedWallMerge,
        enriched_session_merge: EnrichedSessionMerge,
    ):
        client = gr_redis_config.create_redis_client()
        assert client.get(name=team.cache_key) is None

        team.set_cache(
            pg_config=gr_db,
            thl_web_rr=thl_web_rr,
            redis_config=gr_redis_config,
            client=client_no_amm,
            ds=mnt_filepath,
            mnt_gr_api=mnt_gr_api_dir,
            enriched_wall=enriched_wall_merge,
            enriched_session=enriched_session_merge,
        )

        assert client.hgetall(name=team.cache_key) is not None

    def test_set_cache_team(
        self,
        gr_user: GRUser,
        gr_db: PostgresConfig,
        thl_web_rr: PostgresConfig,
        product_factory: Callable[..., Product],
        team: Team,
        membership_factory: Callable[..., Membership],
        gr_redis_config: RedisConfig,
        mnt_filepath: GRLDatasets,
        mnt_gr_api_dir: Path,
        enriched_wall_merge: EnrichedWallMerge,
        enriched_session_merge: EnrichedSessionMerge,
    ):
        from generalresearch.models.gr.team import Team

        p1 = product_factory(team=team)
        membership_factory(team=team, gr_user=gr_user)

        team.set_cache(
            pg_config=gr_db,
            thl_web_rr=thl_web_rr,
            redis_config=gr_redis_config,
            client=client_no_amm,
            ds=mnt_filepath,
            mnt_gr_api=mnt_gr_api_dir,
            enriched_wall=enriched_wall_merge,
            enriched_session=enriched_session_merge,
        )

        team2 = Team.from_redis(
            uuid=team.uuid,
            fields=["id", "memberships", "gr_users", "businesses", "products"],
            gr_redis_config=gr_redis_config,
        )

        assert isinstance(team2, Team)
        assert isinstance(team2.products, list)
        assert isinstance(team2.gr_users, list)
        assert team.model_dump_json() == team2.model_dump_json()
        assert p1.uuid in [p.uuid for p in team2.products]
        assert len(team2.gr_users) == 1
        assert gr_user.id in [gru.id for gru in team2.gr_users]

    def test_prebuild_enriched_session_parquet(
        self,
        enriched_session_merge: EnrichedSessionMerge,
        client_no_amm: DaskClient,
        wall_collection: WallDFCollection,
        session_collection: SessionDFCollection,
        thl_web_rr: PostgresConfig,
        user_factory: Callable[..., User],
        start: datetime,
        session_factory: Callable[..., Session],
        product_factory: Callable[..., Product],
        delete_df_collection: Callable[..., None],
        mnt_filepath: GRLDatasets,
        mnt_gr_api_dir: Path,
        team: Team,
    ):

        delete_df_collection(coll=wall_collection)
        delete_df_collection(coll=session_collection)

        p1 = product_factory(team=team)
        p2 = product_factory(team=team)

        for p in [p1, p2]:
            u = user_factory(product=p)
            for i in range(50):
                session_factory(
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

        team.prebuild_enriched_session_parquet(
            thl_pg_config=thl_web_rr,
            ds=mnt_filepath,
            client=client_no_amm,
            mnt_gr_api=mnt_gr_api_dir,
            enriched_session=enriched_session_merge,
        )

        # Now try to read from path
        df = pd.read_parquet(
            os.path.join(mnt_gr_api_dir, "pop_session", f"{team.file_key}.parquet")
        )
        assert isinstance(df, pd.DataFrame)

    def test_prebuild_enriched_wall_parquet(
        self,
        enriched_wall_merge: EnrichedWallMerge,
        client_no_amm: DaskClient,
        wall_collection: WallDFCollection,
        session_collection: EnrichedSessionMerge,
        thl_web_rr: PostgresConfig,
        user_factory: Callable[..., User],
        start: datetime,
        session_factory: Callable[..., Session],
        product_factory: Callable[..., Product],
        delete_df_collection: Callable[..., None],
        mnt_filepath: GRLDatasets,
        mnt_gr_api_dir: Path,
        team: Team,
    ):

        delete_df_collection(coll=wall_collection)
        delete_df_collection(coll=session_collection)

        p1 = product_factory(team=team)
        p2 = product_factory(team=team)

        for p in [p1, p2]:
            u = user_factory(product=p)
            for i in range(50):
                session_factory(
                    user=u,
                    wall_count=1,
                    wall_req_cpi=Decimal("1.00"),
                    started=start + timedelta(minutes=i, seconds=1),
                )
        wall_collection.initial_load(client=None, sync=True)
        session_collection.initial_load(client=None, sync=True)

        enriched_wall_merge.build(
            client=client_no_amm,
            session_coll=session_collection,
            wall_coll=wall_collection,
            pg_config=thl_web_rr,
        )

        team.prebuild_enriched_wall_parquet(
            thl_pg_config=thl_web_rr,
            ds=mnt_filepath,
            client=client_no_amm,
            mnt_gr_api=mnt_gr_api_dir,
            enriched_wall=enriched_wall_merge,
        )

        # Now try to read from path
        df = pd.read_parquet(
            os.path.join(mnt_gr_api_dir, "pop_event", f"{team.file_key}.parquet")
        )
        assert isinstance(df, pd.DataFrame)
