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
    CURRENCY_FORMATTER,
    SUPPORTED_CURRENCIES,
    Currency,
    PayoutType,
)

if TYPE_CHECKING:
    from generalresearch.managers.thl.wallet.tango import TangoManager
    from generalresearch.models.thl.product import Product
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
        self, data: CashMailCashoutMethodData, user: UserRef | User
    ) -> CashoutMethod:
        """
        Each user can create 1 or more "cash in mail" cashout method. This
            stores their address and possible shipping requests ? Each address
            must be unique.

        :return: the uuid of the created cashout method
        """
        # todo: validate shipping address?
        from generalresearch.models.thl.wallet.cashout_method import CashoutMethod

        user = user if isinstance(user, UserRef) else user.to_user_ref()

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
            return res[0]

        self.create(cm)

        return cm

    def create_paypal_cashout_method(
        self, data: PaypalCashoutMethodData, user: UserRef | User
    ) -> CashoutMethod:
        """
        If it already exists, and the emails are the same, do nothing. If the
        email is different, it raises an error

        :return: the uuid of the created cashout method
        """
        from generalresearch.models.thl.wallet.cashout_method import CashoutMethod

        user = user if isinstance(user, UserRef) else user.to_user_ref()

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
            user=user,
            ext_id=data.email,
        )
        # Make sure this user doesn't already have one
        res = self.filter(user=user, payout_types=[PayoutType.PAYPAL], is_live=True)
        if res:
            assert len(res) == 1
            if res[0].data.email == data.email:
                # Already exists with the same email, just return it
                return res[0]
            else:
                raise ValueError(
                    "User already has a cashout method of this type. "
                    "Delete the existing one and try again."
                )
        else:
            self.create(cm)
            return cm

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

    def get_cashout_method(
        self,
        cashout_method_id: str,
        product_id: str,
        usd_exchange_rate: dict[Currency, float],
    ) -> CashoutMethod:
        from generalresearch.managers.thl.product import ProductManager

        product = ProductManager(pg_config=self.pg_config).get_by_uuid(
            product_uuid=product_id
        )
        supported_payout_types = copy(product.user_wallet_config.supported_payout_types)
        if product.user_wallet_config.amt:
            supported_payout_types.add(PayoutType.AMT)
        res = self.pg_config.execute_sql_query(
            query="""
                SELECT ac.id::uuid, ac.provider, ac.ext_id,
                    ac.data::jsonb as _data_, ac.user_id, u.product_user_id
                FROM accounting_cashoutmethod AS ac
                LEFT JOIN thl_user AS u
                    ON ac.user_id = u.id AND u.product_id = %(product_id)s
                WHERE ac.id = %(cashout_method_id)s
                AND ac.is_live
                AND (ac.user_id IS NULL OR u.id IS NOT NULL)
                AND ac.provider = ANY(%(supported_payout_types)s)
                LIMIT 1
            """,
            params={
                "cashout_method_id": cashout_method_id,
                "product_id": product_id,
                "supported_payout_types": [
                    payout_type.value for payout_type in supported_payout_types
                ],
            },
        )
        assert res, (
            f"No cashout method found with id {cashout_method_id} "
            f"for product {product_id}"
        )
        row = res[0]
        user = None
        if row["user_id"] is not None:
            user = UserRef(
                user_id=row["user_id"],
                product_id=product_id,
                product_user_id=row.pop("product_user_id"),
            )
        cashout_method = self.format_from_db(row, user=user)
        cashout_method = self.apply_currency_adjustments(
            cashout_method=cashout_method,
            product=product,
            usd_exchange_rate=usd_exchange_rate,
        )
        assert cashout_method is not None, (
            f"Cashout method {cashout_method_id} is unavailable for product {product_id}"
        )
        return cashout_method

    @staticmethod
    def apply_currency_adjustments(
        cashout_method: CashoutMethod,
        product: Product,
        usd_exchange_rate: dict[Currency, float],
    ) -> CashoutMethod | None:
        min_value_usd = product.user_wallet_config.min_cashout or 0
        product_min_value = USDCent(round(min_value_usd * 100))

        if cashout_method.original_currency in {None, Currency.USD}:
            exchange_rate = 1.0
        else:
            exchange_rate = usd_exchange_rate[cashout_method.original_currency]

        method_min_value_usd = USDCent(round(cashout_method.min_value * exchange_rate))
        min_value = max(method_min_value_usd, product_min_value)
        max_value = USDCent(round(cashout_method.max_value * exchange_rate))
        if min_value > max_value:
            return None

        cashout_method.usd_exchange_rate = exchange_rate
        cashout_method.min_value_usd = min_value
        cashout_method.max_value_usd = max_value
        cashout_method.min_value = max(
            cashout_method.min_value,
            round(int(product_min_value) / exchange_rate),
        )
        return cashout_method

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

        cms = self.get_cashout_methods(user=user)

        # Filter out by country before bothering w exchange rates
        cms = [
            cm
            for cm in cms
            if (
                cm.type == PayoutType.TANGO and country_iso.lower() in cm.data.countries
            )
            or cm.type != PayoutType.TANGO
        ]

        cms = [
            self.apply_currency_adjustments(x, product, usd_exchange_rate) for x in cms
        ]
        cms = [x for x in cms if x is not None]

        return {x.id: x for x in cms}

    @staticmethod
    def format_from_db(
        x: dict[str, Any], user: User | UserRef | None = None
    ) -> CashoutMethod:
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
            user = user if isinstance(user, UserRef) else user.to_user_ref()
            x["user"] = user
        x["original_currency"] = x.get("original_currency") or Currency.USD
        x["currency"] = Currency.USD
        return CashoutMethod.model_validate(x)

    def get_expected_foreign_redemption_value(
        self, cashout_method_id: str, amount: USDCent, tango_manager: TangoManager
    ) -> tuple[int, Currency]:
        """
        Convert USD cents to a Tango card's smallest currency unit.
        for e.g.: A user wants a variable value CAD visa card. He redeems $10 USD (amount=1000),
            this function returns the amount that will be redeemed in CAD.
        :param cashout_method_id: ID of tango card. expected to be non USD. If USD, just returns the amount.
        :param amount: amount the user is redeeming from their wallet in USD integer cents.
        :return: amount that will be redeemed through tango on their foreign card, in the card's
            (specified by the UTID) foreign currency (in integer units of the lowest denomination)
        # example CAD visa: U121653
        """
        assert type(amount) is USDCent
        # We don't **need** the method to be live? They can see the rate but won't
        #  be able to request it.
        res = self.filter(uuid=cashout_method_id, is_live=None)
        assert len(res) == 1, f"No cashout method found with id {cashout_method_id}"
        cm = res[0]
        if cm.type == PayoutType.TANGO:
            assert cm.original_currency is not None
            if cm.original_currency == Currency.USD:
                return int(amount), Currency.USD
            foreign_amount = round(
                int(amount) / tango_manager.get_exchange_rates()[cm.original_currency]
            )
            return foreign_amount, cm.original_currency
        else:
            # todo: bitcoin... etc
            return int(amount), Currency.USD

    def format_currency(self, amount: int, currency: Currency):
        return CURRENCY_FORMATTER[currency](amount)
