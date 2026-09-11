from __future__ import annotations

from datetime import timedelta
from decimal import Decimal
from threading import Lock
from typing import Any

from cachetools import TTLCache, cachedmethod

from generalresearch.currency import USDCent
from generalresearch.managers.thl.cashout_method import CashoutMethodManager
from generalresearch.managers.thl.tango_api import TangoClient
from generalresearch.models.thl.wallet.definitions import (
    CURRENCY_FORMATTER,
    Currency,
    PayoutType,
)


class TangoManager:
    def __init__(
        self,
        tango_client: TangoClient,
        tango_account_id: str,
        tango_customer_id: str,
        cashout_method_manager: CashoutMethodManager,
    ) -> None:
        self.tango_client = tango_client
        self.tango_account_id = tango_account_id
        self.tango_customer_id = tango_customer_id
        self.cashout_method_manager = cashout_method_manager
        self.supported_currencies = frozenset({currency.value for currency in Currency})
        self._exchange_rate_cache = TTLCache(
            maxsize=1, ttl=timedelta(minutes=30).total_seconds()
        )
        self._name_cache = TTLCache(
            maxsize=1000, ttl=timedelta(minutes=30).total_seconds()
        )
        self._exchange_rate_lock = Lock()
        self._name_lock = Lock()

    @cachedmethod(
        cache=lambda self: self._exchange_rate_cache,
        lock=lambda self: self._exchange_rate_lock,
    )
    def get_exchange_rates(self) -> dict[Currency, float]:
        """Return supported foreign-currency-to-USD Tango exchange rates."""
        rates = self.tango_client.get_exchange_rates()["exchangeRates"]
        return {
            Currency(rate["baseCurrency"]): rate["baseFx"]
            for rate in rates
            if rate["rewardCurrency"] == "USD"
            and rate["baseCurrency"] in self.supported_currencies
        }

    def get_order_detail(self, tango_order_id: str) -> dict[str, Any]:
        return self.tango_client.get_order(tango_order_id)

    def make_request(
        self, amount_usd: Decimal, cashout_method: Any, external_ref_id: str
    ) -> dict[str, Any]:
        """Build the data needed to place a Tango order."""
        assert type(amount_usd) is Decimal
        utid = cashout_method.data.utid
        amount: Decimal | float = amount_usd
        currency = cashout_method.original_currency
        currency_code = getattr(currency, "value", currency)
        if currency_code and currency_code != "USD":
            amount = round(float(amount) / self.get_exchange_rates()[currency_code], 2)
        return {
            "accountIdentifier": self.tango_account_id,
            "customerIdentifier": self.tango_customer_id,
            "utid": utid,
            "amount": str(amount),
            "amount_usd": str(amount_usd),
            "campaign": "300large",
            "sendEmail": False,
            "externalRefID": external_ref_id,
            "description": self.get_name(utid),
        }

    @cachedmethod(
        cache=lambda self: self._name_cache,
        lock=lambda self: self._name_lock,
    )
    def get_name(self, utid: str) -> str:
        methods = self.cashout_method_manager.filter(
            ext_id=utid, payout_types=[PayoutType.TANGO], is_live=None
        )
        cashout_method = next(iter(methods), None)
        return cashout_method.name if cashout_method else "Tango Gift Card"

    def get_expected_redemption_value(
        self, cashout_method_id: str, amount: USDCent
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
        res = self.cashout_method_manager.filter(uuid=cashout_method_id, is_live=True)
        if not res:
            raise ValueError(f"no cashout method found for {cashout_method_id!r}")
        cashout_method = res[0]
        assert cashout_method.type == PayoutType.TANGO
        assert cashout_method.original_currency is not None

        currency = cashout_method.original_currency
        if currency == Currency.USD:
            return int(amount), currency

        foreign_amount = round(int(amount) / self.get_exchange_rates()[currency])
        return foreign_amount, currency

    def format_currency(self, amount: int, currency: Currency):
        return CURRENCY_FORMATTER[currency](amount)

    def clear_caches(self) -> None:
        with self._exchange_rate_lock:
            self._exchange_rate_cache.clear()
        with self._name_lock:
            self._name_cache.clear()
