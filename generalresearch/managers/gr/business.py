from typing import Optional, List, TYPE_CHECKING
from uuid import UUID, uuid4

from psycopg import sql
from pydantic import PositiveInt
from pydantic_extra_types.phone_numbers import PhoneNumber

from generalresearch.managers.base import (
    PostgresManagerWithRedis,
    PostgresManager,
)
from generalresearch.models.custom_types import UUIDStr

if TYPE_CHECKING:
    from generalresearch.models.gr.team import Team
    from generalresearch.models.gr.business import (
        Business,
        BusinessType,
        BusinessAddress,
        BusinessBankAccount,
        TransferMethod,
    )


class BusinessBankAccountManager(PostgresManager):

    def create_dummy(
        self,
        business_id: PositiveInt,
        uuid: Optional[UUID] = None,
        transfer_method: Optional["TransferMethod"] = None,
        account_number: Optional[str] = None,
        routing_number: Optional[str] = None,
        iban: Optional[str] = None,
        swift: Optional[str] = None,
    ):
        from generalresearch.models.gr.business import TransferMethod

        uuid = uuid or uuid4().hex
        transfer_method = transfer_method or TransferMethod.ACH
        account_number = account_number or uuid4().hex[:6]
        routing_number = routing_number or uuid4().hex[:6]
        iban = iban or uuid4().hex[:6]
        swift = swift or uuid4().hex[:6]

        return self.create(
            business_id=business_id,
            uuid=uuid,
            transfer_method=transfer_method,
            account_number=account_number,
            routing_number=routing_number,
            iban=iban,
            swift=swift,
        )

    def create(
        self,
        business_id: PositiveInt,
        uuid: UUIDStr,
        transfer_method: "TransferMethod",
        account_number: Optional[str] = None,
        routing_number: Optional[str] = None,
        iban: Optional[str] = None,
        swift: Optional[str] = None,
    ) -> "BusinessBankAccount":
        from generalresearch.models.gr.business import BusinessBankAccount

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
                    query=sql.SQL(
                        """
                        INSERT INTO common_bankaccount 
                            (uuid, transfer_method, account_number, 
                             routing_number, iban, swift, business_id) 
                        VALUES 
                            (%(uuid)s, %(transfer_method)s, %(account_number)s, 
                             %(routing_number)s,  %(iban)s, %(swift)s, %(business_id)s)
                        RETURNING id
                    """
                    ),
                    params=data,
                )
                ba_id = c.fetchone()["id"]
            conn.commit()

        ba.id = ba_id
        return ba

    def get_by_business_id(self, business_id: UUIDStr) -> List["BusinessBankAccount"]:
        from generalresearch.models.gr.business import BusinessBankAccount

        with self.pg_config.make_connection() as conn:
            with conn.cursor() as c:
                c.execute(
                    query=sql.SQL(
                        """
                        SELECT ba.* 
                        FROM common_bankaccount AS ba
                        WHERE ba.business_id = %s
                    """
                    ),
                    params=(business_id,),
                )
                res = c.fetchall()

        return [BusinessBankAccount.model_validate(item) for item in res]


class BusinessAddressManager(PostgresManager):

    def create_dummy(
        self,
        business_id: PositiveInt,
        uuid: Optional[UUIDStr] = None,
        line_1: Optional[str] = None,
        line_2: Optional[str] = None,
        city: Optional[str] = None,
        state: Optional[str] = None,
        postal_code: Optional[str] = None,
        phone_number: Optional[PhoneNumber] = None,
        country: Optional[str] = None,
    ):
        uuid = uuid or uuid4().hex
        line_1 = line_1 or "abc"
        line_2 = line_2 or "bczx"
        city = city or "Downingtown"
        state = state or "CA"
        postal_code = postal_code or "94041"
        phone_number = None
        country = country or "US"

        return self.create(
            business_id=business_id,
            uuid=uuid,
            line_1=line_1,
            line_2=line_2,
            city=city,
            state=state,
            postal_code=postal_code,
            phone_number=phone_number,
            country=country,
        )

    def create(
        self,
        business_id: PositiveInt,
        uuid: UUIDStr,
        line_1: Optional[str] = None,
        line_2: Optional[str] = None,
        city: Optional[str] = None,
        state: Optional[str] = None,
        postal_code: Optional[str] = None,
        phone_number: Optional[PhoneNumber] = None,
        country: Optional[str] = None,
    ) -> "BusinessAddress":
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
                    query=sql.SQL(
                        """
                    INSERT INTO common_businessaddress 
                        (uuid, line_1, line_2, city, country, state, 
                         postal_code, phone_number, business_id) 
                    VALUES 
                        (%(uuid)s, %(line_1)s, %(line_2)s, %(city)s, %(country)s, %(state)s, 
                         %(postal_code)s, %(phone_number)s, %(business_id)s)
                    RETURNING id
                    """
                    ),
                    params=data,
                )
                ba_id = c.fetchone()["id"]
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
        name: Optional[str] = None,
        team: Optional["Team"] = None,
        kind: Optional["BusinessType"] = None,
        tax_number: Optional[str] = None,
    ) -> "Business":
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

    def create_dummy(
        self,
        uuid: Optional[UUIDStr] = None,
        name: Optional[str] = None,
        team: Optional["Team"] = None,
        kind: Optional["BusinessType"] = None,
        tax_number: Optional[str] = None,
    ) -> "Business":
        from random import randint

        uuid = uuid or uuid4().hex
        name = name or "< Unknown >"
        tax_number = tax_number or str(randint(1, 999_999_999))

        return self.create(
            uuid=uuid, name=name, team=team, kind=kind, tax_number=tax_number
        )

    def create(
        self,
        name: str,
        kind: Optional["BusinessType"] = None,
        uuid: Optional[UUIDStr] = None,
        team: Optional["Team"] = None,
        tax_number: Optional[str] = None,
    ) -> "Business":
        """
        Behavior: does this raise on duplicate?
        """
        from generalresearch.models.gr.business import (
            Business,
            BusinessType,
        )

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
                    query=sql.SQL(
                        """
                INSERT INTO common_business (uuid, kind, name, tax_number) 
                VALUES (%(uuid)s, %(kind)s, %(name)s, %(tax_number)s)
                RETURNING id
                """
                    ),
                    params=data,
                )
                business_id = c.fetchone()["id"]
            conn.commit()
        business.id = business_id

        if team:
            from generalresearch.managers.gr.team import TeamManager

            tm = TeamManager(pg_config=self.pg_config, redis_config=self.redis_config)
            tm.add_business(team=team, business=business)

        return business

    def get_all(self) -> List["Business"]:
        """WARNING: This should be access by the /god/ page only, and only
            used by GRUser.is_staff as it doesn't provide any authentication
            on it's own. This is used because the .get_by_team_id() and
            .get_by_user_id() use the table relationships, and it's often too
            tedious to ensure every GRL admin is manually added to each and
            every Team in order to manage or view details about it.

        :return:
        """
        from generalresearch.models.gr.business import Business

        with self.pg_config.make_connection() as conn:
            with conn.cursor() as c:
                c.execute(
                    query=sql.SQL(
                        """
                SELECT b.id, b.uuid, b.kind, b.name, b.tax_number
                FROM common_business AS b
            """
                    )
                )
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
    ) -> List["Business"]:

        # conn: psycopg.Connection = GR_POSTGRES_C.make_connection()
        with self.pg_config.make_connection() as conn:
            with conn.cursor() as c:
                c.execute(
                    query=sql.SQL(
                        """
                SELECT  b.id, b.uuid, b.kind, b.name, b.tax_number
                FROM common_business AS b 
                INNER JOIN common_team_businesses as tb
                    ON tb.business_id = b.id
                WHERE tb.team_id = %s
            """
                    ),
                    params=(team_id,),
                )

                res = c.fetchall()

        response = []
        from generalresearch.models.gr.business import Business

        for i in res:
            # i["contact"] = BusinessContact.model_validate(i)
            # i["address"] = BusinessAddress.model_validate(i)
            response.append(Business.model_validate(i))

        return response

    def get_by_user_id(
        self,
        user_id: PositiveInt,
    ) -> List["Business"]:
        from generalresearch.models.gr.business import Business

        with self.pg_config.make_connection() as conn:
            with conn.cursor() as c:
                c.execute(
                    query=sql.SQL(
                        """
                        SELECT  b.id, b.uuid, b.kind, b.name, b.tax_number 
                        FROM common_business AS b
                        INNER JOIN common_team_businesses AS tb 
                            ON tb.business_id = b.id
                        INNER JOIN common_membership AS m 
                            ON m.team_id = tb.team_id
                        WHERE m.user_id = %s
                    """
                    ),
                    params=(user_id,),
                )

                res = c.fetchall()

        response = []
        for i in res:
            # i["contact"] = BusinessContact.model_validate(i)
            # i["address"] = BusinessAddress.model_validate(i)
            response.append(Business.model_validate(i))

        return response

    def get_ids_by_user_id(self, user_id: PositiveInt) -> List[PositiveInt]:
        """
        :return: Every Business UUIDStr that this GRUser has permission to view
        """

        with self.pg_config.make_connection() as conn:
            with conn.cursor() as c:
                c.execute(
                    query=sql.SQL(
                        """
                SELECT b.id
                FROM common_business AS b
                INNER JOIN common_team_businesses AS tb 
                    ON tb.business_id = b.id
                INNER JOIN common_membership AS cm 
                    ON tb.team_id = cm.team_id
                WHERE cm.user_id = %s
            """
                    ),
                    params=(user_id,),
                )

                res = c.fetchall()

        return [i["id"] for i in res]

    def get_uuids_by_user_id(self, user_id: PositiveInt) -> List[UUIDStr]:
        """
        :return: Every Business UUIDStr that this GRUser has permission to view
        """

        with self.pg_config.make_connection() as conn:
            with conn.cursor() as c:
                c.execute(
                    query=sql.SQL(
                        """
                SELECT b.uuid
                FROM common_business AS b
                INNER JOIN common_team_businesses AS tb 
                    ON tb.business_id = b.id
                INNER JOIN common_membership AS cm 
                    ON tb.team_id = cm.team_id
                WHERE cm.user_id = %s
            """
                    ),
                    params=(user_id,),
                )

                res = c.fetchall()

        return [i["uuid"] for i in res]

    def get_by_uuid(
        self,
        business_uuid: UUIDStr,
    ) -> Optional["Business"]:
        from generalresearch.models.gr.business import Business

        assert UUID(hex=business_uuid).hex == business_uuid

        with self.pg_config.make_connection() as conn:
            with conn.cursor() as c:
                c.execute(
                    query=sql.SQL(
                        """
                        SELECT id, uuid, kind, name, tax_number
                        FROM common_business
                        WHERE uuid = %s
                        LIMIT 1;
                    """
                    ),
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

    def get_by_id(self, business_id: PositiveInt) -> Optional["Business"]:
        from generalresearch.models.gr.business import Business

        assert isinstance(business_id, int)

        with self.pg_config.make_connection() as conn:
            with conn.cursor() as c:
                c.execute(
                    query=sql.SQL(
                        """
                        SELECT id, uuid, kind, name, tax_number
                        FROM common_business
                        WHERE id = %s
                        LIMIT 1;
                    """
                    ),
                    params=(business_id,),
                )

                res = c.fetchall()

        if len(res) == 0:
            return None

        return Business.model_validate(res[0])
