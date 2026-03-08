from datetime import datetime, timezone
from typing import TYPE_CHECKING, List, Optional
from uuid import uuid4

from psycopg import sql
from pydantic import PositiveInt

from generalresearch.managers.base import (
    PostgresManager,
    PostgresManagerWithRedis,
)
from generalresearch.models.custom_types import UUIDStr
from generalresearch.models.gr.team import Membership, MembershipPrivilege

if TYPE_CHECKING:
    from generalresearch.models.gr.authentication import GRUser
    from generalresearch.models.gr.business import Business
    from generalresearch.models.gr.team import (
        Membership,
        MembershipPrivilege,
        Team,
    )


class MembershipManager(PostgresManager):
    """The Membership Manager controls the relationships between a
    GR User and a Team.

    GRUsers do not have direct connections to Businesses or Products,
    they're all connected through a Team and a GRUser's relationship to
    a Team can have various levels of permissions and rights.
    """

    def create(
        self,
        team: "Team",
        gr_user: "GRUser",
        privilege: MembershipPrivilege = MembershipPrivilege.READ,
    ) -> Membership:
        membership = Membership(
            uuid=uuid4().hex,
            privilege=MembershipPrivilege.READ,
            owner=False,
            team_id=team.id,
            user_id=gr_user.id,
            created=datetime.now(tz=timezone.utc),
        )

        data = membership.model_dump(by_alias=True)
        data["team_id"] = team.id
        data["user_id"] = gr_user.id

        # 'user_id' = {int} 5774
        # 'team_id' = {int} 20736

        assert gr_user.id, "GR User must be saved"
        assert team.id, "Team must be saved"
        existing = self.exists(gr_user_id=gr_user.id, team_id=team.id)
        if existing:
            return existing

        gr_user_memberships = self.get_by_gr_user_id(gr_user_id=gr_user.id)
        if len(gr_user_memberships) > 5:
            raise ValueError("Should this GR User really be in more than 5 Teams?")

        with self.pg_config.make_connection() as conn:
            with conn.cursor() as c:
                c.execute(
                    query=sql.SQL(
                        """
                    INSERT INTO common_membership
                        (uuid, privilege, owner, team_id, user_id, created)
                    VALUES (%(uuid)s, %(privilege)s, %(owner)s, %(team_id)s, 
                            %(user_id)s, %(created)s)
                    RETURNING id
                """
                    ),
                    params=data,
                )
                membership_id: int = c.fetchone()["id"]  # type: ignore
            conn.commit()

        membership.id = membership_id
        return membership

    def exists(
        self, gr_user_id: PositiveInt, team_id: PositiveInt
    ) -> Optional[Membership]:
        with self.pg_config.make_connection() as conn:
            with conn.cursor() as c:
                c.execute(
                    query=sql.SQL(
                        """
                            SELECT  id, uuid, privilege, owner, created, 
                                    user_id, team_id
                            FROM common_membership
                            WHERE team_id = %s AND user_id = %s
                            LIMIT 1
                        """
                    ),
                    params=(team_id, gr_user_id),
                )
                res = c.fetchone()

        if not res:
            return None

        return Membership.model_validate(res)

    def get_by_team_id(self, team_id: PositiveInt) -> List[Membership]:
        with self.pg_config.make_connection() as conn:
            with conn.cursor() as c:
                c.execute(
                    query=sql.SQL(
                        """
                            SELECT  id, uuid, privilege, owner, created, 
                                    user_id, team_id
                            FROM common_membership
                            WHERE team_id = %s
                            LIMIT 250
                        """
                    ),
                    params=(team_id,),
                )
                res = c.fetchall()

        return [Membership.model_validate(i) for i in res]

    def get_by_gr_user_id(self, gr_user_id: PositiveInt) -> List[Membership]:
        with self.pg_config.make_connection() as conn:
            with conn.cursor() as c:
                c.execute(
                    query=sql.SQL(
                        """
                            SELECT  id, uuid, privilege, owner, created, 
                                    user_id, team_id
                            FROM common_membership
                            WHERE user_id = %s
                            LIMIT 250
                        """
                    ),
                    params=(gr_user_id,),
                )
                res = c.fetchall()

        return [Membership.model_validate(i) for i in res]


class TeamManager(PostgresManagerWithRedis):

    def get_or_create(
        self, uuid: Optional[UUIDStr] = None, name: Optional[str] = None
    ) -> "Team":

        team = self.get_by_uuid(team_uuid=uuid)

        if team:
            return team

        return self.create(uuid=uuid, name=name or "< Unknown >")

    def get_all(self) -> List["Team"]:
        from generalresearch.models.gr.team import Team

        with self.pg_config.make_connection() as conn:
            with conn.cursor() as c:
                c.execute(
                    query=sql.SQL(
                        """
                SELECT t.id, t.uuid, t.name
                FROM common_team AS t
            """
                    )
                )
                res = c.fetchall()

        return [Team.model_validate(i) for i in res]

    def create_dummy(
        self, uuid: Optional[UUIDStr] = None, name: Optional[str] = None
    ) -> "Team":
        uuid = uuid or uuid4().hex
        name = name or f"name-{uuid4().hex[:12]}"

        return self.create(uuid=uuid, name=name)

    def create(
        self,
        name: str,
        uuid: Optional[UUIDStr] = None,
    ) -> "Team":
        from generalresearch.models.gr.team import Team

        team = Team.model_validate({"uuid": uuid or uuid4().hex, "name": name})

        with self.pg_config.make_connection() as conn:
            with conn.cursor() as c:
                c.execute(
                    query=sql.SQL(
                        """
                    INSERT INTO common_team (uuid, name) 
                    VALUES (%s, %s)
                    RETURNING id
                """
                    ),
                    params=[team.uuid, team.name],
                )
                team_id = c.fetchone()["id"]  # type: ignore
            conn.commit()
            team.id = team_id

        return team

    def add_user(self, team: "Team", gr_user: "GRUser") -> "Membership":
        """Create a Membership between a GRUser and a Team"""

        team.prefetch_gr_users(pg_config=self.pg_config, redis_config=self.redis_config)

        assert gr_user not in team.gr_users, (
            "Can't create multiple Memberships for " "the same User to the same Team"
        )
        mm = MembershipManager(pg_config=self.pg_config)

        return mm.create(team=team, gr_user=gr_user)

    def add_business(self, team: "Team", business: "Business") -> None:
        with self.pg_config.make_connection() as conn:
            with conn.cursor() as c:
                c.execute(
                    query=sql.SQL(
                        """
                            INSERT INTO common_team_businesses 
                                (team_id, business_id) 
                            VALUES (%s, %s)
                        """
                    ),
                    params=(
                        team.id,
                        business.id,
                    ),
                )
            conn.commit()

        return None

    def get_by_uuid(self, team_uuid: UUIDStr) -> Optional["Team"]:
        from generalresearch.models.gr.team import Team

        with self.pg_config.make_connection() as conn:
            with conn.cursor() as c:
                c.execute(
                    query="""
                    SELECT t.* 
                    FROM common_team AS t
                    WHERE t.uuid = %s
                    LIMIT 1;
                """,
                    params=(team_uuid,),
                )

                res = c.fetchone()

        if not isinstance(res, dict):
            return None

        return Team.model_validate(res)

    def get_by_id(self, team_id: PositiveInt) -> Optional["Team"]:
        from generalresearch.models.gr.team import Team

        with self.pg_config.make_connection() as conn:
            with conn.cursor() as c:
                c.execute(
                    query=sql.SQL(
                        """
                    SELECT t.id, t.uuid, t.name
                    FROM common_team AS t
                    WHERE t.id = %s
                    LIMIT 1;
                """
                    ),
                    params=(team_id,),
                )

                res = c.fetchone()

        if not isinstance(res, dict):
            return None

        return Team.model_validate(res)

    def get_by_user(self, gr_user: "GRUser") -> List["Team"]:
        from generalresearch.models.gr.team import Team

        with self.pg_config.make_connection() as conn:
            with conn.cursor() as c:
                c.execute(
                    query=sql.SQL(
                        """
                    SELECT team.*
                    FROM common_team AS team
                    INNER JOIN common_membership AS mem 
                        ON mem.team_id = team.id
                    WHERE mem.user_id = %s
                """
                    ),
                    params=(gr_user.id,),
                )

                res = c.fetchall()

        return [Team.model_validate(item) for item in res]
