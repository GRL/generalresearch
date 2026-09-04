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
    from generalresearch.managers.gr.authentication import GRUserManager
    from generalresearch.managers.gr.business import BusinessManager
    from generalresearch.managers.gr.team import MembershipManager, TeamManager
    from generalresearch.managers.thl.product import ProductManager
    from generalresearch.models.gr.authentication import GRUser
    from generalresearch.models.gr.team import Membership
    from generalresearch.models.thl.session import Session
    from generalresearch.models.thl.user import User
    from generalresearch.pg_helper import PostgresConfig
    from generalresearch.redis_helper import RedisConfig


class TestTeam:
    def test_init(self, gr_team: Team):

        assert isinstance(gr_team, Team)
        assert isinstance(gr_team.id, int)
        assert isinstance(gr_team.uuid, str)

    def test_memberships_none(
        self, gr_team: Team, gr_membership_manager: MembershipManager
    ):
        assert gr_team.memberships is None

        gr_team.prefetch_memberships(gr_membership_manager=gr_membership_manager)
        assert isinstance(gr_team.memberships, list)
        assert len(gr_team.memberships) == 0

    def test_memberships(
        self,
        gr_team: Team,
        gr_user: GRUser,
        gr_user_factory: Callable[..., GRUser],
        gr_membership_manager: MembershipManager,
    ):
        assert gr_team.memberships is None

        gr_team.prefetch_memberships(gr_membership_manager=gr_membership_manager)
        assert isinstance(gr_team.memberships, list)
        assert len(gr_team.memberships) == 1
        assert gr_team.memberships[0].user_id == gr_user.id

        # Create another new Membership
        gr_membership_manager.create(team=gr_team, gr_user=gr_user_factory())
        assert len(gr_team.memberships) == 1
        gr_team.prefetch_memberships(gr_membership_manager=gr_membership_manager)
        assert len(gr_team.memberships) == 2

    def test_gr_users(
        self,
        gr_team: Team,
        gr_user_factory: Callable[..., GRUser],
        gr_membership_manager: MembershipManager,
        gr_user_manager: GRUserManager,
    ):
        assert gr_team.gr_users is None

        gr_team.prefetch_gr_users(gr_user_manager=gr_user_manager)
        assert isinstance(gr_team.gr_users, list)
        assert len(gr_team.gr_users) == 0

        # Create a new Membership
        gr_membership_manager.create(team=gr_team, gr_user=gr_user_factory())
        assert len(gr_team.gr_users) == 0
        gr_team.prefetch_gr_users(gr_user_manager=gr_user_manager)
        assert len(gr_team.gr_users) == 1

        # Create another Membership
        gr_membership_manager.create(team=gr_team, gr_user=gr_user_factory())
        assert len(gr_team.gr_users) == 1
        gr_team.prefetch_gr_users(gr_user_manager=gr_user_manager)
        assert len(gr_team.gr_users) == 2

    def test_businesses(
        self,
        gr_team: Team,
        business: Business,
        team_manager: TeamManager,
        gr_business_manager: BusinessManager,
    ):

        assert gr_team.businesses is None

        gr_team.prefetch_businesses(gr_business_manager=gr_business_manager)
        assert isinstance(gr_team.businesses, list)
        assert len(gr_team.businesses) == 0

        team_manager.add_business(team=gr_team, business=business)
        assert len(gr_team.businesses) == 0
        gr_team.prefetch_businesses(gr_business_manager=gr_business_manager)
        assert len(gr_team.businesses) == 1
        assert isinstance(gr_team.businesses[0], Business)
        assert gr_team.businesses[0].uuid == business.uuid

    def test_products(
        self,
        gr_team: Team,
        product_factory: Callable[..., Product],
        thl_web_rr: PostgresConfig,
        product_manager: ProductManager,
    ):

        assert gr_team.products is None

        gr_team.prefetch_products(product_manager=product_manager)
        assert isinstance(gr_team.products, list)
        assert len(gr_team.products) == 0

        product_factory(team=gr_team)
        assert len(gr_team.products) == 0
        gr_team.prefetch_products(product_manager=product_manager)
        assert len(gr_team.products) == 1
        assert isinstance(gr_team.products[0], Product)


class TestTeamMethods:
    def test_cache_key(self, gr_team: Team):
        assert isinstance(gr_team.cache_key, str)
        assert ":" in gr_team.cache_key
        assert str(gr_team.uuid) in gr_team.cache_key

    def test_set_cache(
        self,
        gr_team: Team,
        gr_db: PostgresConfig,
        thl_web_rr: PostgresConfig,
        gr_redis_config: RedisConfig,
        client_no_amm: DaskClient,
        mnt_filepath: GRLDatasets,
        mnt_gr_api_dir: Path,
        enriched_wall_merge: EnrichedWallMerge,
        enriched_session_merge: EnrichedSessionMerge,
        product_manager: ProductManager,
        gr_user_manager: GRUserManager,
        gr_business_manager: BusinessManager,
        gr_membership_manager: MembershipManager,
    ):
        client = gr_redis_config.create_redis_client()
        assert client.get(name=gr_team.cache_key) is None

        gr_team.set_cache(
            product_manager=product_manager,
            gr_user_manager=gr_user_manager,
            gr_business_manager=gr_business_manager,
            gr_membership_manager=gr_membership_manager,
            thl_web_rr=thl_web_rr,
            redis_config=gr_redis_config,
            client=client_no_amm,
            ds=mnt_filepath,
            mnt_gr_api=mnt_gr_api_dir,
            enriched_wall=enriched_wall_merge,
            enriched_session=enriched_session_merge,
        )

        assert client.hgetall(name=gr_team.cache_key) is not None

    def test_set_cache_team(
        self,
        gr_user: GRUser,
        gr_db: PostgresConfig,
        thl_web_rr: PostgresConfig,
        product_factory: Callable[..., Product],
        gr_team: Team,
        membership_factory: Callable[..., Membership],
        gr_redis_config: RedisConfig,
        mnt_filepath: GRLDatasets,
        mnt_gr_api_dir: Path,
        enriched_wall_merge: EnrichedWallMerge,
        enriched_session_merge: EnrichedSessionMerge,
        product_manager: ProductManager,
        gr_user_manager: GRUserManager,
        gr_business_manager: BusinessManager,
        gr_membership_manager: MembershipManager,
    ):
        from generalresearch.models.gr.team import Team

        p1 = product_factory(team=gr_team)
        membership_factory(team=gr_team, gr_user=gr_user)

        gr_team.set_cache(
            product_manager=product_manager,
            gr_user_manager=gr_user_manager,
            gr_business_manager=gr_business_manager,
            gr_membership_manager=gr_membership_manager,
            thl_web_rr=thl_web_rr,
            redis_config=gr_redis_config,
            client=client_no_amm,
            ds=mnt_filepath,
            mnt_gr_api=mnt_gr_api_dir,
            enriched_wall=enriched_wall_merge,
            enriched_session=enriched_session_merge,
        )

        team2 = Team.from_redis(
            uuid=gr_team.uuid,
            fields=["id", "memberships", "gr_users", "businesses", "products"],
            gr_redis_config=gr_redis_config,
        )

        assert isinstance(team2, Team)
        assert isinstance(team2.products, list)
        assert isinstance(team2.gr_users, list)
        assert gr_team.model_dump_json() == team2.model_dump_json()
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
        gr_team: Team,
        product_manager: ProductManager,
    ):

        delete_df_collection(coll=wall_collection)
        delete_df_collection(coll=session_collection)

        p1 = product_factory(team=gr_team)
        p2 = product_factory(team=gr_team)

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

        gr_team.prebuild_enriched_session_parquet(
            product_manager=product_manager,
            ds=mnt_filepath,
            client=client_no_amm,
            mnt_gr_api=mnt_gr_api_dir,
            enriched_session=enriched_session_merge,
        )

        # Now try to read from path
        df = pd.read_parquet(
            os.path.join(mnt_gr_api_dir, "pop_session", f"{gr_team.file_key}.parquet")
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
        gr_team: Team,
        product_manager: ProductManager,
    ):

        delete_df_collection(coll=wall_collection)
        delete_df_collection(coll=session_collection)

        p1 = product_factory(team=gr_team)
        p2 = product_factory(team=gr_team)

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

        gr_team.prebuild_enriched_wall_parquet(
            product_manager=product_manager,
            ds=mnt_filepath,
            client=client_no_amm,
            mnt_gr_api=mnt_gr_api_dir,
            enriched_wall=enriched_wall_merge,
        )

        # Now try to read from path
        df = pd.read_parquet(
            os.path.join(mnt_gr_api_dir, "pop_event", f"{gr_team.file_key}.parquet")
        )
        assert isinstance(df, pd.DataFrame)
