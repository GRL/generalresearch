from __future__ import annotations

from typing import TYPE_CHECKING
from uuid import UUID, uuid4

from psycopg import sql
from pydantic import PositiveInt
from pydantic_extra_types.phone_numbers import PhoneNumber

from generalresearch.managers.base import (
    PostgresManager,
    PostgresManagerWithRedis,
)
from generalresearch.models.custom_types import UUIDStr
from generalresearch.models.gr.business import (
    Business,
    BusinessBankAccount,
)
from generalresearch.models.gr.definitions import BusinessType, TransferMethod

if TYPE_CHECKING:

    from generalresearch.models.gr.business import (
        BusinessAddress,
    )
    from generalresearch.models.gr.team import Team


class BusinessBankAccountManager(PostgresManager):

    def create(
        self,
        business_id: PositiveInt,
        uuid: UUIDStr,
        transfer_method: TransferMethod,
        account_number: str | None = None,
        routing_number: str | None = None,
        iban: str | None = None,
        swift: str | None = None,
    ) -> BusinessBankAccount:
        ba = BusinessBankAccount.model_validate(
            {
                "business_id": business_id,
                "uuid": uuid,
                "transfer_method": transfer_method,
                "account_number": account_number,
                "routing_number": routing_number,
                "iban": iban,
                "swift": swift,
            }
        )

        data = ba.model_dump(mode="json")

        with self.pg_config.make_connection() as conn:
            with conn.cursor() as c:
                c.execute(
                    query=sql.SQL("""
                        INSERT INTO common_bankaccount 
                            (uuid, transfer_method, account_number, 
                             routing_number, iban, swift, business_id) 
                        VALUES 
                            (%(uuid)s, %(transfer_method)s, %(account_number)s, 
                             %(routing_number)s,  %(iban)s, %(swift)s, %(business_id)s)
                        RETURNING id
                    """),
                    params=data,
                )
                ba_id = c.fetchone()["id"]  # type: ignore
            conn.commit()

        ba.id = ba_id
        return ba

    def get_by_business_id(self, business_id: UUIDStr) -> list[BusinessBankAccount]:

        with self.pg_config.make_connection() as conn, conn.cursor() as c:
            c.execute(
                query=sql.SQL("""
                        SELECT ba.* 
                        FROM common_bankaccount AS ba
                        WHERE ba.business_id = %s
                    """),
                params=(business_id,),
            )
            res = c.fetchall()

        return [BusinessBankAccount.model_validate(item) for item in res]


class BusinessAddressManager(PostgresManager):

    def create(
        self,
        business_id: PositiveInt,
        uuid: UUIDStr,
        line_1: str | None = None,
        line_2: str | None = None,
        city: str | None = None,
        state: str | None = None,
        postal_code: str | None = None,
        phone_number: PhoneNumber | None = None,
        country: str | None = None,
    ) -> BusinessAddress:
        from generalresearch.models.gr.business import BusinessAddress

        ba = BusinessAddress.model_validate(
            {
                "business_id": business_id,
                "uuid": uuid,
                "line_1": line_1,
                "line_2": line_2,
                "city": city,
                "state": state,
                "postal_code": postal_code,
                "phone_number": phone_number,
                "country": country,
            }
        )
        data = ba.model_dump()

        with self.pg_config.make_connection() as conn:
            with conn.cursor() as c:
                c.execute(
                    query=sql.SQL("""
                    INSERT INTO common_businessaddress 
                        (uuid, line_1, line_2, city, country, state, 
                         postal_code, phone_number, business_id) 
                    VALUES 
                        (%(uuid)s, %(line_1)s, %(line_2)s, %(city)s, 
                         %(country)s, %(state)s, %(postal_code)s, 
                         %(phone_number)s, %(business_id)s)
                    RETURNING id
                    """),
                    params=data,
                )
                ba_id = c.fetchone()["id"]  # type: ignore
            conn.commit()

        ba.id = ba_id
        return ba


class BusinessManager(PostgresManagerWithRedis):
    """This can and often references many data sources so it's important
    to stay organized.

    - The GR-* project maintains its own PostgresSQL
        database with Business metadata, contact information, relationship
        to Teams and authentication details
    - The thl-web brokerage table is ultimately our sense of truth
        for which businesses exist and live Products under that
        business
    - The gr-redis instance stores cached values that may be commonly
        referenced by the gr-api services

    """

    def get_or_create(
        self,
        uuid: UUIDStr,
        name: str | None = None,
        team: Team | None = None,
        kind: BusinessType | None = None,
        tax_number: str | None = None,
    ) -> Business:
        """
        Warning: this ** does not ** update the name, team, kind, tax_number
            values if they differ from what was passed in for the
            respective uuid
        """

        business = self.get_by_uuid(business_uuid=uuid)

        if business:
            return business

        assert name, "Must provide Business name if creating"
        return self.create(
            uuid=uuid, name=name, team=team, kind=kind, tax_number=tax_number
        )

    def create(
        self,
        name: str,
        kind: BusinessType | None = None,
        uuid: UUIDStr | None = None,
        team: Team | None = None,
        tax_number: str | None = None,
    ) -> Business:
        """
        Behavior: does this raise on duplicate?
        """
        # Business.model_rebuild()
        business = Business.model_validate(
            {
                "uuid": uuid or uuid4().hex,
                "name": name,
                "kind": kind or BusinessType.COMPANY,
                "tax_number": tax_number,
            }
        )
        data = business.model_dump()
        data["tax_number"] = business.tax_number

        with self.pg_config.make_connection() as conn:
            with conn.cursor() as c:
                c.execute(
                    query=sql.SQL("""
                INSERT INTO common_business (uuid, kind, name, tax_number) 
                VALUES (%(uuid)s, %(kind)s, %(name)s, %(tax_number)s)
                RETURNING id
                """),
                    params=data,
                )
                business_id = c.fetchone()["id"]  # type: ignore
            conn.commit()
        business.id = business_id

        if team:
            from generalresearch.managers.gr.team import TeamManager

            tm = TeamManager(pg_config=self.pg_config, redis_config=self.redis_config)
            tm.add_business(team=team, business=business)

        return business

    def get_all(self) -> list[Business]:
        """WARNING: This should be access by the /god/ page only, and only
            used by GRUser.is_staff as it doesn't provide any authentication
            on it's own. This is used because the .get_by_team_id() and
            .get_by_user_id() use the table relationships, and it's often too
            tedious to ensure every GRL admin is manually added to each and
            every Team in order to manage or view details about it.

        :return:
        """
        from generalresearch.models.gr.business import Business

        with self.pg_config.make_connection() as conn, conn.cursor() as c:
            c.execute(query=sql.SQL("""
                SELECT b.id, b.uuid, b.kind, b.name, b.tax_number
                FROM common_business AS b
            """))
            res = c.fetchall()

        response = []
        for i in res:
            # i["contact"] = BusinessContact.model_validate(i)
            # i["address"] = BusinessAddress.model_validate(i)
            i["contact"] = None
            i["address"] = None

            response.append(Business.model_validate(i))

        return response

    def get_by_team(
        self,
        team_id: PositiveInt,
    ) -> list[Business]:

        # conn: psycopg.Connection = GR_POSTGRES_C.make_connection()
        with self.pg_config.make_connection() as conn, conn.cursor() as c:
            c.execute(
                query=sql.SQL("""
                SELECT  b.id, b.uuid, b.kind, b.name, b.tax_number
                FROM common_business AS b 
                INNER JOIN common_team_businesses as tb
                    ON tb.business_id = b.id
                WHERE tb.team_id = %s
            """),
                params=(team_id,),
            )

            res = c.fetchall()

        response = []

        for i in res:
            # i["contact"] = BusinessContact.model_validate(i)
            # i["address"] = BusinessAddress.model_validate(i)
            response.append(Business.model_validate(i))

        return response

    def get_by_user_id(
        self,
        user_id: PositiveInt,
    ) -> list[Business]:
        from generalresearch.models.gr.business import Business

        with self.pg_config.make_connection() as conn, conn.cursor() as c:
            c.execute(
                query=sql.SQL("""
                        SELECT  b.id, b.uuid, b.kind, b.name, b.tax_number 
                        FROM common_business AS b
                        INNER JOIN common_team_businesses AS tb 
                            ON tb.business_id = b.id
                        INNER JOIN common_membership AS m 
                            ON m.team_id = tb.team_id
                        WHERE m.user_id = %s
                    """),
                params=(user_id,),
            )

            res = c.fetchall()

        response = []
        for i in res:
            # i["contact"] = BusinessContact.model_validate(i)
            # i["address"] = BusinessAddress.model_validate(i)
            response.append(Business.model_validate(i))

        return response

    def get_ids_by_user_id(self, user_id: PositiveInt) -> list[PositiveInt]:
        """
        :return: Every Business UUIDStr that this GRUser has permission to view
        """

        with self.pg_config.make_connection() as conn, conn.cursor() as c:
            c.execute(
                query=sql.SQL("""
                SELECT b.id
                FROM common_business AS b
                INNER JOIN common_team_businesses AS tb 
                    ON tb.business_id = b.id
                INNER JOIN common_membership AS cm 
                    ON tb.team_id = cm.team_id
                WHERE cm.user_id = %s
            """),
                params=(user_id,),
            )

            res = c.fetchall()

        return [i["id"] for i in res]

    def get_uuids_by_user_id(self, user_id: PositiveInt) -> list[UUIDStr]:
        """
        :return: Every Business UUIDStr that this GRUser has permission to view
        """

        with self.pg_config.make_connection() as conn, conn.cursor() as c:
            c.execute(
                query=sql.SQL("""
                SELECT b.uuid
                FROM common_business AS b
                INNER JOIN common_team_businesses AS tb 
                    ON tb.business_id = b.id
                INNER JOIN common_membership AS cm 
                    ON tb.team_id = cm.team_id
                WHERE cm.user_id = %s
            """),
                params=(user_id,),
            )

            res = c.fetchall()

        return [i["uuid"] for i in res]

    def get_by_uuid(
        self,
        business_uuid: UUIDStr,
    ) -> Business | None:
        assert UUID(hex=business_uuid).hex == business_uuid

        with self.pg_config.make_connection() as conn, conn.cursor() as c:
            c.execute(
                query=sql.SQL("""
                        SELECT id, uuid, kind, name, tax_number
                        FROM common_business
                        WHERE uuid = %s
                        LIMIT 1;
                    """),
                params=(business_uuid,),
            )

            res = c.fetchall()

        if len(res) == 0:
            return None

        assert len(res) == 1, "BusinessManager.get_by_uuid returned invalid results"
        data = res[0]
        # data["address"] = BusinessAddress.model_validate(data)
        # data["contact"] = BusinessContact.model_validate(data)
        return Business.model_validate(data)

    def get_by_id(self, business_id: PositiveInt) -> Business | None:
        assert isinstance(business_id, int)

        with self.pg_config.make_connection() as conn, conn.cursor() as c:
            c.execute(
                query=sql.SQL("""
                        SELECT id, uuid, kind, name, tax_number
                        FROM common_business
                        WHERE id = %s
                        LIMIT 1;
                    """),
                params=(business_id,),
            )

            res = c.fetchall()

        if len(res) == 0:
            return None

        return Business.model_validate(res[0])
