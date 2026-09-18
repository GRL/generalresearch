from __future__ import annotations

from datetime import timedelta
from decimal import Decimal
from threading import Lock
from typing import TYPE_CHECKING, Any

from cachetools import TTLCache, cachedmethod

from generalresearch.currency import USDCent
from generalresearch.models.thl.wallet.cashout_method import (
    CashoutMethod,
    TangoCashoutMethodRequestData,
)
from generalresearch.models.thl.wallet.definitions import (
    Currency,
    PayoutType,
)

if TYPE_CHECKING:
    from generalresearch.managers.thl.cashout_method import CashoutMethodManager
    from generalresearch.managers.thl.tango_api import TangoClient



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
        self, amount: USDCent, cashout_method: CashoutMethod, payout_event_id: str
    ) -> TangoCashoutMethodRequestData:
        """Build the data needed to place a Tango order."""
        assert type(amount) is USDCent
        utid = cashout_method.data.utid
        currency = cashout_method.original_currency
        if currency and currency != Currency.USD:
            amount = round(float(amount) / self.get_exchange_rates()[currency])
        amount_in_currency = Decimal(float(amount) / 100).quantize(Decimal('0.01'))
        return TangoCashoutMethodRequestData.model_validate(
            {
                "accountIdentifier": self.tango_account_id,
                "customerIdentifier": self.tango_customer_id,
                "utid": utid,
                "amount": amount_in_currency,
                "campaign": "300large",
                "sendEmail": False,
                "externalRefID": payout_event_id,
                "description": self.get_name(utid),
            }
        )

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

    def clear_caches(self) -> None:
        with self._exchange_rate_lock:
            self._exchange_rate_cache.clear()
        with self._name_lock:
            self._name_cache.clear()
