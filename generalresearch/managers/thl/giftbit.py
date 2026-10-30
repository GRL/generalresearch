from __future__ import annotations

import logging
from collections.abc import Collection
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING, Any
from uuid import uuid4

import requests

from generalresearch.currency import USDCent
from generalresearch.locales import Localelator
from generalresearch.managers.base import Permission, PostgresManagerWithRedis
from generalresearch.managers.thl.cashout_method import CashoutMethodManager
from generalresearch.managers.thl.ledger_manager.thl_ledger import ThlLedgerManager
from generalresearch.models.thl.definitions import PayoutStatus
from generalresearch.models.thl.payout import UserPayoutEvent
from generalresearch.models.thl.user import User
from generalresearch.models.thl.wallet.cashout_method import (
    CashoutMethod,
    GiftbitCashoutMethodData,
    GiftbitCashoutMethodRequestData,
)
from generalresearch.models.thl.wallet.definitions import Currency, PayoutType
from generalresearch.pg_helper import PostgresConfig
from generalresearch.redis_helper import RedisConfig

if TYPE_CHECKING:
    from generalresearch.managers.thl.wallet.user_payout import UserPayoutEventManager

BASE_URL = "https://api-testbed.giftbit.com/papi/v1"
locales = Localelator()

logging.basicConfig()
logger = logging.getLogger()
logger.setLevel("INFO")


class GiftbitError(RuntimeError):
    pass


# 3 is "global"
region_to_country_iso = {
    1: "ca",
    2: "us",
    4: "au",
    5: "at",
    6: "be",
    7: "cz",
    8: "fi",
    9: "gr",
    10: "lu",
    11: "nl",
    12: "in",
    13: "jp",
    14: "nz",
    15: "fr",
    16: "de",
    17: "ie",
    18: "it",
    19: "es",
    20: "gb",
    21: "mx",
    22: "ar",
    23: "br",
    24: "cl",
    25: "co",
    26: "pe",
    27: "uy",
    28: "ve",
    29: "cn",
    30: "ch",
    31: "dk",
    32: "hu",
    33: "no",
    34: "pl",
    35: "ro",
    36: "se",
    37: "tr",
    38: "hk",
    39: "sg",
    40: "ph",
    41: "my",
    42: "id",
    43: "th",
    44: "kr",
    45: "tw",
    46: "vn",
    47: "bh",
    48: "eg",
    49: "jo",
    50: "om",
    51: "qa",
    52: "ae",
    53: "ke",
    54: "pt",
    55: "bg",
    56: "hr",
    57: "ee",
    58: "lv",
    59: "lt",
    60: "sa",
    61: "sk",
    62: "si",
    63: "za",
}


class GiftbitClient:
    def __init__(
        self,
        *,
        token: str,
    ) -> None:
        self.base_url = BASE_URL.rstrip("/")
        self.timeout = 5

        self.session = requests.Session()
        self.session.headers.update(
            {
                "Accept": "application/json",
                "Content-Type": "application/json",
                "Authorization": f"Bearer {token}",
            }
        )

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json: dict[str, Any] | None = None,
    ) -> Any:
        response = self.session.request(
            method=method,
            url=f"{self.base_url}{path}",
            params=params,
            json=json,
            timeout=self.timeout,
        )

        if response.ok:
            if not response.content:
                return None
            return response.json()

        try:
            body = response.json()
        except ValueError:
            body = response.text

        raise GiftbitError(f"Giftbit API error {response.status_code}" + f": {body}")

    def ping(self):
        return self._request("GET", "/ping")

    def get_regions(self):
        # retarded endpoint... e.g. {'id': 5, 'name': 'Austria'},
        # Claude converted it to ISO for me
        res = self._request("GET", "/regions")["regions"]

    def list_brands_page(self, limit: int = 100, offset: int = 0) -> dict[str, Any]:
        params = {"limit": limit, "offset": offset}
        _res = self._request("GET", "/brands", params=params)
        return _res

    def list_brands(self) -> list[dict[str, Any]]:
        offset = 0
        limit = 100
        res = []
        while True:
            _res = self.list_brands_page(limit=limit, offset=offset)
            logger.info(f"list_brands {limit=} {offset=} total={_res['total_count']}")
            res.extend(_res["brands"])
            if _res["total_count"] == len(res):
                break
            offset += len(_res["brands"])
        return res

    def get_brand(self, brand_code: str) -> dict[str, Any]:
        return self._request("GET", f"/brands/{brand_code}")["brand"]

    def get_supported_brands(self):
        brands = self.list_brands()
        cms = []
        for b in brands:
            b = self.get_brand(b["brand_code"])
            cm = self.format_brand_to_cashout_method(b)
            if cm is not None:
                cms.append(cm)
        return cms

    def format_brand_to_cashout_method(self, c: dict):
        if c["fund_currencyisocode"] not in Currency:
            logger.info(f"Unsupported currency: {c['fund_currencyisocode']}")
            return None
        currency = Currency(c["fund_currencyisocode"])
        countries = None
        if c["regions"] == [3]:
            # Global
            pass
        else:
            countries = [region_to_country_iso.get(x) for x in c["regions"]]
            if not all(countries):
                logger.info(f"Unsupported countries: {c['regions']})")
                return None
        if c["variable_price"]:
            min_value = c["min_price_in_cents"]
            max_value = c["max_price_in_cents"]
        else:
            # todo: support multiple fixed prices
            price = sorted(c["allowed_prices_in_cents"])[0]
            min_value, max_value = price, price

        return CashoutMethod(
            name=c["name"],
            description=c["name"],
            id=uuid4().hex,
            currency=Currency.USD,
            original_currency=currency,
            image_url=c["image_url"],
            min_value=min_value,
            max_value=max_value,
            data=GiftbitCashoutMethodData(
                brand_code=c["brand_code"],
                variable_price=c["variable_price"],
                min_price_in_cents=c.get("min_price_in_cents"),
                max_price_in_cents=c.get("max_price_in_cents"),
                allowed_prices_in_cents=c.get("allowed_prices_in_cents"),
                countries=countries,
                disclaimer=c["disclaimer"],
                description=c["description"],
            ),
            type=PayoutType.GIFTBIT,
            user=None,
            ext_id=c["brand_code"],
        )

    def get_order(self, order_id: str) -> dict[str, Any]:
        res = self._request("GET", f"/campaign/{order_id}")
        d = res["campaign"]

        try:
            res = self._request("GET", f"/links/{order_id}")
            d["direct_link"] = res["directlinks"][0]["link"]
        except GiftbitError as e:
            if "Links no longer available" in e.args[0]:
                # Can't retrieve the link once it has been claimed
                d["direct_link"] = None
            else:
                raise
        return d

    def get_order_if_exists(self, order_id: str) -> dict[str, Any] | None:
        try:
            return self.get_order(order_id)
        except GiftbitError as e:
            if "Campaign not found" not in e.args[0]:
                raise
            return None

    def create_order(self, cashout_request_id: str, brand_code: str, amount: Decimal):
        """

        :param cashout_request_id: unique identifier of the cashout request
        :param brand_code: brand_code of the brand (from Giftbit)
        :param amount: amount of the cashout (in the noted currency)
        :return:
        """
        assert type(amount) is Decimal
        data = {
            "id": cashout_request_id,
            "price_in_cents": round(amount * 100),
            "brand_codes": [brand_code],
            "link_count": 1,
            "expiry": (datetime.now(tz=UTC) + timedelta(days=364)).strftime("%Y-%m-%d"),
        }

        res = self._request("POST", "/direct_links", json=data)
        assert len(res["direct_links"]) == 1
        campaign = res["campaign"]
        campaign["direct_link"] = res["direct_links"][0]
        return campaign


class GiftbitManager(PostgresManagerWithRedis):
    def __init__(
        self,
        *,
        giftbit_token: str,
        pg_config: PostgresConfig,
        redis_config: RedisConfig,
        permissions: Collection[Permission] | None = None,
        cache_prefix: str | None = None,
    ):
        self.client = GiftbitClient(token=giftbit_token)
        self.cashout_method_manager = CashoutMethodManager(pg_config=pg_config)
        super().__init__(
            pg_config=pg_config,
            permissions=permissions,
            redis_config=redis_config,
            cache_prefix=cache_prefix,
        )

    def make_request(
        self, amount: USDCent, cashout_method: CashoutMethod, payout_event_id: str
    ) -> GiftbitCashoutMethodRequestData:
        """Build the data needed to place an order."""
        assert type(amount) is USDCent
        brand_code = cashout_method.data.brand_code
        currency = cashout_method.original_currency
        assert currency == Currency.USD
        amount_in_currency = Decimal(float(amount) / 100).quantize(Decimal("0.01"))
        return GiftbitCashoutMethodRequestData.model_validate(
            {
                "brand_code": brand_code,
                "amount": amount_in_currency,
                "currency": currency,
                "cashout_request_id": payout_event_id,
            }
        )

    def complete_order(
        self,
        user: User,
        payout_event: UserPayoutEvent,
        user_payout_event_manager: UserPayoutEventManager,
        ledger_manager: ThlLedgerManager,
    ):
        """
        We approved the card redemption. Actually request the card.
        """
        assert payout_event.status in {
            PayoutStatus.PENDING,
            PayoutStatus.FAILED,
        }, (
            "attempting to manage payout that is not pending (you can also retry a failed order)"
        )
        request = GiftbitCashoutMethodRequestData.model_validate(
            payout_event.request_data
        )

        # Note: we'll use the cashout_request_id to uniquify orders
        assert request.currency == Currency.USD
        try:
            order = self.client.create_order(
                cashout_request_id=request.cashout_request_id,
                brand_code=request.brand_code,
                amount=request.amount,
            )

        except AssertionError:
            # todo: its possible the order went through, but something else was wrong
            # we should try to retrieve the order by its ref_id and confirm it really
            # failed...
            user_payout_event_manager.update(payout_event, status=PayoutStatus.FAILED)
            return payout_event

        # update Payout Event with the order data
        user_payout_event_manager.update(
            payout_event,
            status=PayoutStatus.COMPLETE,
            ext_ref_id=order["uuid"],
            order_data=order,
        )

        ledger_manager.create_tx_user_payout_complete(
            user,
            payout_event=payout_event,
        )

        return payout_event

    def update_cashout_method_db(self):
        # This only should be run once, or when brands/cards offered change.
        cms = self.client.get_supported_brands()

        existing = self.cashout_method_manager.filter(payout_types=[PayoutType.GIFTBIT])
        existing_id = {cm.ext_id: cm.id for cm in existing}
        for cm in cms:
            if cm.ext_id in existing_id:
                cm.id = existing_id[cm.ext_id]

        data = []
        for cm in cms:
            d = cm.model_dump(mode="json")
            d["data"] = cm.model_dump_json()
            d["provider"] = cm.type.value
            data.append(d)

        query = """
        INSERT INTO accounting_cashoutmethod (
            id, last_updated, is_live, provider, ext_id, name, data
        ) VALUES (
            %(id)s, %(last_updated)s, %(is_live)s, %(provider)s,
            %(ext_id)s, %(name)s, %(data)s
        ) ON CONFLICT (id) DO UPDATE
        SET last_updated = NOW(),
            name = %(name)s, is_live=TRUE,
            data = %(data)s
        """
        with self.pg_config.make_connection() as conn:
            with conn.cursor() as c:
                c.executemany(query, data)
            conn.commit()
