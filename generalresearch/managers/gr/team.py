from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING
from uuid import uuid4

from psycopg import Connection, sql
from pydantic import PositiveInt

from generalresearch.managers.base import (
    PostgresManager,
    PostgresManagerWithRedis,
)
from generalresearch.models.custom_types import UUIDStr
from generalresearch.models.gr.team import (
    Membership,
    MembershipPrivilege,
    TeamUpdate,
)

if TYPE_CHECKING:
    from generalresearch.models.gr.authentication import GRUser
    from generalresearch.models.gr.team import Team


class MembershipManager(PostgresManager):
    """The Membership Manager controls the relationships between a
    GR User and a Team.

    GRUsers do not have direct connections to Businesses or Products,
    they're all connected through a Team and a GRUser's relationship to
    a Team can have various levels of permissions and rights.
    """

    def create(
        self,
        team: Team,
        gr_user: GRUser,
        privilege: MembershipPrivilege = MembershipPrivilege.MEMBER,
    ) -> Membership:
        membership = Membership(
            uuid=uuid4().hex,
            privilege=privilege,
            team_id=team.id,
            user_id=gr_user.id,
            created=datetime.now(tz=UTC),
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
                    query=sql.SQL("""
                    INSERT INTO common_membership
                        (uuid, privilege, team_id, user_id, created)
                    VALUES (%(uuid)s, %(privilege)s, %(team_id)s, 
                            %(user_id)s, %(created)s)
                    RETURNING id
                """),
                    params=data,
                )
                membership_id: int = c.fetchone()["id"]  # type: ignore
            conn.commit()

        membership.id = membership_id
        return membership

    def exists(
        self, gr_user_id: PositiveInt, team_id: PositiveInt
    ) -> Membership | None:
        memberships = self.filter(gr_user_id=gr_user_id, team_id=team_id)
        if len(memberships) > 1:
            raise ValueError("User has multiple memberships in the same Team")
        return memberships[0] if memberships else None

    def get_by_team_id(self, team_id: PositiveInt) -> list[Membership]:
        return self.filter(team_id=team_id)

    def get_by_gr_user_id(self, gr_user_id: PositiveInt) -> list[Membership]:
        return self.filter(gr_user_id=gr_user_id)

    def filter(
        self,
        *,
        gr_user_id: PositiveInt | None = None,
        team_id: PositiveInt | None = None,
    ) -> list[Membership]:
        """Return memberships matching all the supplied identifiers."""
        conditions = []
        params = {}

        if gr_user_id is not None:
            conditions.append("m.user_id = %(user_id)s")
            params["user_id"] = gr_user_id
        if team_id is not None:
            conditions.append("m.team_id = %(team_id)s")
            params["team_id"] = team_id

        filter_str = " AND ".join(conditions)
        filter_str = "WHERE " + filter_str if filter_str else ""

        query = f"""
            SELECT
                m.id,
                m.uuid,
                m.privilege,
                m.created,
                m.user_id,
                m.team_id
            FROM common_membership AS m
            {filter_str}
            LIMIT 250
        """

        with self.pg_config.make_connection() as conn, conn.cursor() as c:
            c.execute(query=query, params=params)
            res = c.fetchall()

        return [Membership.model_validate(item) for item in res]


class TeamManager(PostgresManagerWithRedis):
    def get_or_create(
        self, uuid: UUIDStr | None = None, name: str | None = None
    ) -> Team:

        team = self.get_by_uuid(team_uuid=uuid)

        if team:
            return team

        return self.create(uuid=uuid, name=name or "< Unknown >")

    def get_all(self) -> list[Team]:
        from generalresearch.models.gr.team import Team

        with self.pg_config.make_connection() as conn, conn.cursor() as c:
            c.execute(
                query=sql.SQL("""
                SELECT t.id, t.uuid, t.name
                FROM common_team AS t
            """)
            )
            res = c.fetchall()

        return [Team.model_validate(i) for i in res]

    def create(
        self,
        name: str,
        uuid: UUIDStr | None = None,
    ) -> Team:
        from generalresearch.models.gr.team import TeamCreate

        team = TeamCreate.model_validate({"uuid": uuid or uuid4().hex, "name": name})

        with self.pg_config.make_connection() as conn:
            with conn.cursor() as c:
                c.execute(
                    query=sql.SQL("""
                    INSERT INTO common_team (uuid, name) 
                    VALUES (%s, %s)
                    RETURNING id
                """),
                    params=[team.uuid, team.name],
                )
                team_id = c.fetchone()["id"]  # type: ignore
            conn.commit()
        persisted_team = self.get_by_id(team_id=team_id)
        assert persisted_team is not None
        return persisted_team

    def update(
        self,
        team_id: PositiveInt,
        update: TeamUpdate,
    ) -> Team:
        data = update.model_dump(exclude_unset=True)

        assignments = sql.SQL(", ").join(
            sql.SQL("{} = %s").format(sql.Identifier(field)) for field in data
        )

        query = sql.SQL("""
        UPDATE common_team
        SET {assignments}
        WHERE id = %s
        """).format(assignments=assignments)

        with self.pg_config.make_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, [*data.values(), team_id])
                if cursor.rowcount != 1:
                    raise LookupError(f"Team {team_id} does not exist")
            conn.commit()

        team = self.get_by_id(team_id)
        assert team is not None
        self.redis_client.delete(team.cache_key)
        return team

    def add_business(
        self,
        team_id: PositiveInt,
        business_id: PositiveInt,
        conn: Connection | None = None,
    ) -> None:
        with self.connection(conn) as conn, conn.cursor() as c:
            c.execute(
                query=sql.SQL("""
                INSERT INTO common_team_businesses
                    (team_id, business_id)
                VALUES (%s, %s)
                """),
                params=(
                    team_id,
                    business_id,
                ),
            )
            conn.commit()

    def remove_business(
        self,
        team_id: PositiveInt,
        business_id: PositiveInt,
        conn: Connection | None = None,
    ) -> None:
        with self.connection(conn) as conn, conn.cursor() as c:
            c.execute(
                query=sql.SQL("""
                DELETE FROM common_team_businesses
                WHERE team_id = %s and business_id = %s
                """),
                params=(
                    team_id,
                    business_id,
                ),
            )
            assert c.rowcount == 1, "Team Business Association not found"
            conn.commit()

    def get_by_uuid(self, team_uuid: UUIDStr) -> Team | None:
        from generalresearch.models.gr.team import Team

        with self.pg_config.make_connection() as conn, conn.cursor() as c:
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

    def get_by_id(self, team_id: PositiveInt) -> Team | None:
        from generalresearch.models.gr.team import Team

        with self.pg_config.make_connection() as conn, conn.cursor() as c:
            c.execute(
                query=sql.SQL("""
                    SELECT t.id, t.uuid, t.name
                    FROM common_team AS t
                    WHERE t.id = %s
                    LIMIT 1;
                """),
                params=(team_id,),
            )

            res = c.fetchone()

        if not isinstance(res, dict):
            return None

        return Team.model_validate(res)

    def get_by_user(self, gr_user: GRUser) -> list[Team]:
        from generalresearch.models.gr.team import Team

        with self.pg_config.make_connection() as conn, conn.cursor() as c:
            c.execute(
                query=sql.SQL("""
                    SELECT team.*
                    FROM common_team AS team
                    INNER JOIN common_membership AS mem 
                        ON mem.team_id = team.id
                    WHERE mem.user_id = %s
                """),
                params=(gr_user.id,),
            )

            res = c.fetchall()

        return [Team.model_validate(item) for item in res]

    def get_uuids(self, user_id: PositiveInt | None) -> list[UUIDStr]:
        """
        :param user_id: If given, restrict to Teams this GRUser has permission to view.
            If None, return every Team.
        :return: Team UUIDStrs
        """

        if user_id is None:
            query = sql.SQL("""
                SELECT team.uuid
                FROM common_team AS team
            """)
            params: tuple[int, ...] = ()
        else:
            query = sql.SQL("""
                SELECT team.uuid
                FROM common_team AS team
                INNER JOIN common_membership AS mem
                    ON mem.team_id = team.id
                WHERE mem.user_id = %s
            """)
            params = (user_id,)

        with self.pg_config.make_connection() as conn, conn.cursor() as c:
            c.execute(query=query, params=params)
            res = c.fetchall()

        return [i["uuid"] for i in res]
