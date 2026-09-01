from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING
from uuid import uuid4

from generalresearch.models.gr.team import Membership, Team

if TYPE_CHECKING:
    from generalresearch.managers.gr.authentication import GRUserManager
    from generalresearch.managers.gr.team import MembershipManager, TeamManager
    from generalresearch.models.gr.authentication import GRUser
    from generalresearch.models.thl.product import Product
    from generalresearch.pg_helper import PostgresConfig
    from generalresearch.redis_helper import RedisConfig


class TestMembershipManager:

    def test_init(self, membership_manager: MembershipManager, gr_db: PostgresConfig):
        assert membership_manager.pg_config == gr_db


class TestTeamManager:

    def test_init(self, team_manager: TeamManager, gr_db: PostgresConfig):
        assert team_manager.pg_config == gr_db

    def test_get_or_create(self, team_manager: TeamManager):
        from generalresearch.models.gr.team import Team

        new_uuid = uuid4().hex

        team: Team = team_manager.get_or_create(uuid=new_uuid)

        assert isinstance(team, Team)
        assert isinstance(team.id, int)
        assert team.uuid == new_uuid
        assert team.name == "< Unknown >"

    def test_get_all(self, team_manager: TeamManager):
        res1 = team_manager.get_all()
        assert isinstance(res1, list)

        team_manager.create_dummy()
        res2 = team_manager.get_all()
        assert len(res1) == len(res2) - 1

    def test_create(self, team_manager: TeamManager):

        team: Team = team_manager.create_dummy()
        assert isinstance(team, Team)
        assert isinstance(team.id, int)

    def test_add_user(
        self,
        team: Team,
        team_manager: TeamManager,
        gr_um: GRUserManager,
        gr_db: PostgresConfig,
        gr_redis_config: RedisConfig,
    ):

        user: GRUser = gr_um.create_dummy()

        instance = team_manager.add_user(team=team, gr_user=user)
        assert isinstance(instance, Membership)

        # assert team.gr_users is None
        team.prefetch_gr_users(pg_config=gr_db, redis_config=gr_redis_config)
        assert isinstance(team.gr_users, list)
        assert len(team.gr_users)
        assert team.gr_users == [user]

    def test_get_by_uuid(self, team_manager: TeamManager):

        team: Team = team_manager.create_dummy()

        instance = team_manager.get_by_uuid(team_uuid=team.uuid)
        assert team.id == instance.id

    def test_get_by_id(self, team_manager: TeamManager):

        team: Team = team_manager.create_dummy()

        instance = team_manager.get_by_id(team_id=team.id)
        assert team.uuid == instance.uuid

    def test_get_by_user(
        self, team: Team, team_manager: TeamManager, gr_um: GRUserManager
    ):

        user: GRUser = gr_um.create_dummy()
        team_manager.add_user(team=team, gr_user=user)

        res = team_manager.get_by_user(gr_user=user)
        assert isinstance(res, list)
        assert len(res) == 1
        instance = res[0]
        assert isinstance(instance, Team)
        assert instance.uuid == team.uuid

    def test_get_by_user_duplicates(
        self,
        gr_user: GRUser,
        product_factory: Callable[..., Product],
        membership_factory: Callable[..., Membership],
        team: Team,
        gr_redis_config: RedisConfig,
        gr_db: PostgresConfig,
    ):
        product_factory(team=team)
        membership_factory(team=team, gr_user=gr_user)

        gr_user.prefetch_teams(
            pg_config=gr_db,
            redis_config=gr_redis_config,
        )

        assert len(gr_user.teams) == 1

    # def test_create_raise_on_duplicate(self):
    #     t_uuid = uuid4().hex
    #
    #     # Make the first one
    #     team = TeamManager.create(
    #         uuid=t_uuid,
    #         name=f"test-{t_uuid[:6]}")
    #     assert isinstance(team, Team)
    #
    #     # Try to make it again
    #     with pytest.raises(expected_exception=psycopg.errors.UniqueViolation):
    #         TeamManager.create(
    #             uuid=t_uuid,
    #             name=f"test-{t_uuid[:6]}")
    #
