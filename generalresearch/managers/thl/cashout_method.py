from copy import copy
from datetime import datetime, timezone
from typing import Any, Collection, Dict, List, Optional
from uuid import UUID, uuid4

from pydantic import NonNegativeInt

from generalresearch.managers.base import PostgresManager
from generalresearch.models.thl.user import User
from generalresearch.models.thl.wallet import PayoutType
from generalresearch.models.thl.wallet.cashout_method import (
    CashMailCashoutMethodData,
    CashoutMethod,
    PaypalCashoutMethodData,
)


class CashoutMethodManager(PostgresManager):

    def create(self, cm: CashoutMethod) -> None:
        now = datetime.now(tz=timezone.utc)
        query = """
        INSERT INTO accounting_cashoutmethod (
            id, last_updated, is_live, provider, 
            ext_id, name, data, user_id
        ) VALUES (
           %(id)s, %(last_updated)s, %(is_live)s, %(provider)s,
           %(ext_id)s, %(name)s, %(data)s, %(user_id)s
        );
        """
        values = {
            "id": cm.id,
            "last_updated": now,
            "is_live": True,
            "provider": cm.type.value,
            "ext_id": cm.ext_id,
            "name": cm.name,
            "data": cm.model_dump_json(exclude={"user"}),
            "user_id": cm.user.user_id if cm.user else None,
        }

        self.pg_config.execute_write(query, values)

        return None

    def delete_cashout_method(self, cm_id: str):
        db_res = self.pg_config.execute_sql_query(
            query="""
        SELECT id::uuid, user_id
        FROM accounting_cashoutmethod
        WHERE id = %s AND is_live
        LIMIT 1;""",
            params=[cm_id],
        )
        res = next(iter(db_res), None)
        assert res, f"cashout method id {cm_id} not found"
        # Don't let anyone delete a non-user-scoped cashout method
        assert (
            res["user_id"] is not None
        ), "error trying to delete non user-scoped cashout method"

        self.pg_config.execute_write(
            query="""
                UPDATE accounting_cashoutmethod SET is_live = FALSE
                WHERE id = %s;""",
            params=[cm_id],
        )

    def create_cash_in_mail_cashout_method(
        self, data: CashMailCashoutMethodData, user: User
    ) -> str:
        """
        Each user can create 1 or more "cash in mail" cashout method. This
            stores their address and possible shipping requests ? Each address
            must be unique.

        :return: the uuid of the created cashout method
        """
        # todo: validate shipping address?

        cm = CashoutMethod(
            name="Cash in Mail",
            description="USPS delivery of cash",
            id=uuid4().hex,
            currency="USD",
            image_url="https://www.shutterstock.com/shutterstock/photos/2175413929/display_1500/stock-vector-opened"
            "-envelope-with-money-dollar-bills-salary-earning-and-savings-concept-d-web-vector-2175413929.jpg",
            min_value=500,  # $5.00
            max_value=25000,  # $250.00
            data=data,
            type=PayoutType.CASH_IN_MAIL,
            user=user,
            ext_id=data.delivery_address.md5sum(),
        )

        # Make sure this user doesn't already have an identical cashout
        #   method (same address)
        res = self.filter(
            user=user,
            is_live=True,
            payout_types=[PayoutType.CASH_IN_MAIL],
            ext_id=data.delivery_address.md5sum(),
        )
        if res:
            # Already exists with the same address
            assert len(res) == 1
            return res[0].id

        self.create(cm)

        return cm.id

    def create_paypal_cashout_method(
        self, data: PaypalCashoutMethodData, user: User
    ) -> str:
        """
        If it already exists, and the emails are the same, do nothing. If the
        email is different, raises an error

        :param data:
        :param user:
        :return: the uuid of the created cashout method
        """
        cm = CashoutMethod(
            name="PayPal",
            description="Cashout via PayPal",
            id=uuid4().hex,
            currency="USD",
            image_url="https://cdn.mmfwcl.com/images/brands/p439786-1200w-326ppi.png",
            min_value=100,  # $1.00
            max_value=25_000,  # $250.00
            data=data,
            type=PayoutType.PAYPAL,
            user=user,
            ext_id=data.email,
        )
        # Make sure this user doesn't already have one
        res = self.filter(user=user, payout_types=[PayoutType.PAYPAL], is_live=True)
        if res:
            assert len(res) == 1
            if res[0].data.email == data.email:
                # Already exists with the same email, just return it
                return res[0].id
            else:
                raise ValueError(
                    "User already has a cashout method of this type. "
                    "Delete the existing one and try again."
                )
        else:
            self.create(cm)
            return cm.id

    @staticmethod
    def make_filter_str(
        uuid: Optional[str] = None,
        user: Optional[User] = None,
        ext_id: Optional[str] = None,
        payout_types: Optional[Collection[PayoutType]] = None,
        is_live: Optional[bool] = True,
    ):
        filters = []
        params = dict()
        if uuid is not None:
            params["uuid"] = uuid
            filters.append("id = %(uuid)s")
        if user is not None:
            params["user_id"] = user.user_id
            filters.append("user_id = %(user_id)s")
        if ext_id is not None:
            params["ext_id"] = ext_id
            filters.append("ext_id = %(ext_id)s")
        if payout_types is not None:
            assert isinstance(payout_types, (list, set, tuple))
            params["payout_types"] = [x.value for x in payout_types]
            filters.append("provider = ANY(%(payout_types)s)")
        if is_live is not None:
            params["is_live"] = is_live
            filters.append("is_live = %(is_live)s")
        assert filters, "must pass at least one filter"

        filter_str = "WHERE " + " AND ".join(filters) if filters else ""
        return filter_str, params

    def filter_count(
        self,
        uuid: Optional[str] = None,
        user: Optional[User] = None,
        ext_id: Optional[str] = None,
        payout_types: Optional[Collection[PayoutType]] = None,
        is_live: Optional[bool] = True,
    ) -> NonNegativeInt:
        filter_str, params = self.make_filter_str(
            uuid=uuid,
            user=user,
            ext_id=ext_id,
            payout_types=payout_types,
            is_live=is_live,
        )
        res = self.pg_config.execute_sql_query(
            query=f"""
                SELECT COUNT(1) as cnt
                FROM accounting_cashoutmethod
                {filter_str}
            """,
            params=params,
        )
        return int(res[0]["cnt"])  # type: ignore

    def filter(
        self,
        uuid: Optional[str] = None,
        user: Optional[User] = None,
        ext_id: Optional[str] = None,
        payout_types: Optional[Collection[PayoutType]] = None,
        is_live: Optional[bool] = True,
    ) -> List[CashoutMethod]:
        filter_str, params = self.make_filter_str(
            uuid=uuid,
            user=user,
            ext_id=ext_id,
            payout_types=payout_types,
            is_live=is_live,
        )
        res = self.pg_config.execute_sql_query(
            query=f"""
                SELECT id::uuid, provider, ext_id, data::jsonb as _data_, user_id
                FROM accounting_cashoutmethod
                {filter_str}
            """,
            params=params,
        )
        return [self.format_from_db(x, user=user) for x in res]

    def get_cashout_methods(self, user: User) -> List[CashoutMethod]:
        """
        The provider column is PayoutType. Some are only user-scoped,
        and some are global.

        :param user: The user whose cashout methods we are requesting.
        """
        user.prefetch_product(pg_config=self.pg_config)
        product = user.product

        supported_payout_types = copy(product.user_wallet_config.supported_payout_types)
        if product.user_wallet_config.amt:
            supported_payout_types.add(PayoutType.AMT)

        user_scoped_payout_types = [PayoutType.PAYPAL, PayoutType.CASH_IN_MAIL]
        params = {
            "user_scoped_payout_types": [x.value for x in user_scoped_payout_types],
            "supported_payout_types": [x.value for x in supported_payout_types],
            "user_id": user.user_id,
        }
        query = """
        SELECT id::uuid, provider, ext_id, data::jsonb as _data_, user_id
        FROM accounting_cashoutmethod
        WHERE is_live 
        AND (
          (provider = ANY(%(user_scoped_payout_types)s) AND user_id = %(user_id)s)
          OR (provider != ANY(%(user_scoped_payout_types)s) AND user_id IS NULL)
        )
        AND provider = ANY(%(supported_payout_types)s)
        LIMIT 1000;"""

        res = self.pg_config.execute_sql_query(query, params=params)
        if len(res) >= 1000:
            raise ValueError(f"Unexpectedly large number of cashout_methods: {user=}")

        cms = [self.format_from_db(x, user=user) for x in res]

        # Only allow AMT if the BP is marked as AMT (already should have been
        #   filtered in query)
        cms = [
            x
            for x in cms
            if (x.type == PayoutType.AMT and product.user_wallet_config.amt)
            or (x.type != PayoutType.AMT)
        ]
        return cms

    @staticmethod
    def format_from_db(x: Dict[str, Any], user: Optional[User] = None) -> CashoutMethod:
        x["id"] = UUID(x["id"]).hex

        # The data column here is inconsistent. Pulling keys from the mysql 'data' col
        #   and putting them into the base level. Renamed so that we don't overwrite
        #   a col called "data" within the "_data_" field.
        for k in list(x["_data_"].keys()):
            if k in CashoutMethod.model_fields:
                x[k] = x["_data_"].pop(k)

        x["type"] = PayoutType(x["provider"].upper())
        if "data" not in x:
            x["data"] = dict()
        x["data"].update(x.pop("_data_"))
        x["data"]["type"] = x["type"]
        if user and x["type"] in {PayoutType.PAYPAL, PayoutType.CASH_IN_MAIL}:
            x["user"] = user

        return CashoutMethod.model_validate(x)
