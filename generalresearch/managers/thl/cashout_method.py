from __future__ import annotations

from collections.abc import Collection
from copy import copy
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any
from uuid import UUID, uuid4

from pydantic import NonNegativeInt

from generalresearch.currency import USDCent
from generalresearch.managers.base import PostgresManager
from generalresearch.models.thl.user_ref import UserRef
from generalresearch.models.thl.wallet.definitions import (
    SUPPORTED_CURRENCIES,
    Currency,
    PayoutType,
)

if TYPE_CHECKING:
    from generalresearch.models.thl.user import User
    from generalresearch.models.thl.wallet.cashout_method import (
        CashMailCashoutMethodData,
        CashoutMethod,
        PaypalCashoutMethodData,
    )


class CashoutMethodManager(PostgresManager):
    def create(self, cm: CashoutMethod) -> None:
        now = datetime.now(tz=UTC)
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

    def delete_cashout_method(self, cm_id: str) -> None:
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
        assert res["user_id"] is not None, (
            "error trying to delete non user-scoped cashout method"
        )

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
        from generalresearch.models.thl.wallet.cashout_method import CashoutMethod

        cm = CashoutMethod(
            name="Cash in Mail",
            description="USPS delivery of cash",
            id=uuid4().hex,
            currency=Currency.USD,
            image_url="https://www.shutterstock.com/shutterstock/photos/2175413929/display_1500/stock-vector-opened"
            "-envelope-with-money-dollar-bills-salary-earning-and-savings-concept-d-web-vector-2175413929.jpg",
            min_value=500,  # $5.00
            max_value=25000,  # $250.00
            data=data,
            type=PayoutType.CASH_IN_MAIL,
            user=user.to_user_ref(),
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
        from generalresearch.models.thl.wallet.cashout_method import CashoutMethod

        cm = CashoutMethod(
            name="PayPal",
            description="Cashout via PayPal",
            id=uuid4().hex,
            currency=Currency.USD,
            image_url="https://cdn.mmfwcl.com/images/brands/p439786-1200w-326ppi.png",
            min_value=100,  # $1.00
            max_value=25_000,  # $250.00
            data=data,
            type=PayoutType.PAYPAL,
            user=user.to_user_ref(),
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
        uuid: str | None = None,
        user: UserRef | None = None,
        ext_id: str | None = None,
        payout_types: Collection[PayoutType] | None = None,
        is_live: bool | None = True,
    ):
        filters = []
        params = {}
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
        uuid: str | None = None,
        user: UserRef | None = None,
        ext_id: str | None = None,
        payout_types: Collection[PayoutType] | None = None,
        is_live: bool | None = True,
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
        uuid: str | None = None,
        user: UserRef | None = None,
        ext_id: str | None = None,
        payout_types: Collection[PayoutType] | None = None,
        is_live: bool | None = True,
    ) -> list[CashoutMethod]:
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

    def get_cashout_methods(self, user: User) -> list[CashoutMethod]:
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
        # This is b/c there might be some Tango cards in here for currency we don't support
        cms = [
            x
            for x in cms
            if x.original_currency is None
            or x.original_currency in SUPPORTED_CURRENCIES
        ]
        return cms

    def get_user_cashout_methods(
        self,
        user: User,
        country_iso: str,
        usd_exchange_rate: dict[Currency, float],
    ):
        """
        Get the cashout methods allowed for this user. Does not check financial stuff at all.
        This checks the user's country for filtering purposes.
        Gets cashoutmethods from accounting_cashoutmethod, along with a consistent UUID.
        If BP has a min_cashout, modifies the cashout method's min_value based on the BP's min cashout.
        :return: Dict with keys: the UIID, values: Dict with keys: 'id', 'provider', 'ext_id', 'data',
            where 'data' is provider-specific JSON data containing details about the cashout method.

        The available cashout methods is based off the user's latest IP address -> country, and
          also (in the future) a risk assessment (maybe riskier user aren't allowed certain
          methods). Also, a user may have different minimums based off "stuff".
        # country_iso = get_user_latest_country(user_iph_manager, user) or "us"
        If a user requests their cashout methods before they ever enter a survey, we won't have
          saved their IP address, and this will fail. I think this call should just default to US.
        assert country_iso, "unknown country from IP address"
        """

        # BP can set their own min in USD or equivalent
        user.prefetch_product(pg_config=self.pg_config)
        product = user.product
        min_value_usd = product.user_wallet_config.min_cashout or 0
        min_value = USDCent(round(min_value_usd * 100))

        cms = self.get_cashout_methods(user=user)

        for x in cms:
            # assets in non-USD need to be converted to USD here
            if x.original_currency is not None:
                if x.original_currency == Currency.USD:
                    x.usd_exchange_rate = 1.0
                else:
                    x.usd_exchange_rate = usd_exchange_rate[x.original_currency]
                # If the user has foreign cards available, we need to show their min_value in USD
                x.min_value_usd = USDCent(round(x.min_value * x.usd_exchange_rate))
                x.max_value_usd = USDCent(round(x.max_value * x.usd_exchange_rate))
                # Adjust min_value for BP
                x.min_value = max(x.min_value_usd, min_value)

        cms = [
            cm
            for cm in cms
            if (
                cm.type == PayoutType.TANGO and country_iso.lower() in cm.data.countries
            )
            or cm.type != PayoutType.TANGO
        ]

        return {x.id: x for x in cms}

    @staticmethod
    def format_from_db(x: dict[str, Any], user: User | None = None) -> CashoutMethod:
        x["id"] = UUID(x["id"]).hex

        # The data column here is inconsistent. Pulling keys from the mysql 'data' col
        #   and putting them into the base level. Renamed so that we don't overwrite
        #   a col called "data" within the "_data_" field.
        from generalresearch.models.thl.wallet.cashout_method import CashoutMethod

        for k in list(x["_data_"].keys()):
            if k in CashoutMethod.model_fields:
                x[k] = x["_data_"].pop(k)

        x["type"] = PayoutType(x["provider"].upper())
        if "data" not in x:
            x["data"] = {}
        x["data"].update(x.pop("_data_"))
        x["data"]["type"] = x["type"]
        if user and x["type"] in {PayoutType.PAYPAL, PayoutType.CASH_IN_MAIL}:
            x["user"] = user.to_user_ref()
        x["original_currency"] = x.get("currency") or Currency.USD
        x["currency"] = Currency.USD
        return CashoutMethod.model_validate(x)
