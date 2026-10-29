from __future__ import annotations

import logging
import threading
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
    GiftronautCashoutMethodData,
    GiftronautCashoutMethodRequestData,
)
from generalresearch.models.thl.wallet.definitions import Currency, PayoutType
from generalresearch.pg_helper import PostgresConfig
from generalresearch.redis_helper import RedisConfig

if TYPE_CHECKING:
    from generalresearch.managers.thl.wallet.user_payout import UserPayoutEventManager

logging.basicConfig()
logger = logging.getLogger()
logger.setLevel("INFO")

BASE_URL = "https://api.giftronaut.com"

locales = Localelator()


class GiftronautError(RuntimeError):
    pass


class GiftronautClient:
    def __init__(
        self,
        *,
        client_id: str,
        secret: str,
    ) -> None:
        self.base_url = BASE_URL.rstrip("/")
        self.timeout = 5
        self.client_id = client_id
        self.secret = secret

        self.access_token = None
        self.session = None
        self.expires_at = None

        threading.Thread(target=self.get_access_token, daemon=True).start()

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json: dict[str, Any] | None = None,
    ) -> Any:
        assert self.session is not None, "get_access_token didn't finish or failed"
        # Refresh access token if it is expired (production tokens expire after 1 hour)
        if datetime.now(tz=UTC) > self.expires_at:
            self.get_access_token()

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

        raise GiftronautError(
            f"Giftronaut API error {response.status_code}" + f": {body}"
        )

    def get_access_token(self):
        data = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.secret,
            "scope": "orders.read orders.write orders.cancel catalog.read balance.read balance.write webhooks.read webhooks.write",
        }
        res = requests.post(
            "https://api.giftronaut.com/oauth2/token", data=data, timeout=5
        )
        res.raise_for_status()
        d = res.json()
        self.access_token = d["access_token"]
        self.expires_at = datetime.now(tz=UTC) + timedelta(seconds=d["expires_in"] - 30)
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Accept": "application/json",
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            }
        )

    @staticmethod
    def format_card_to_cashout_method(c: dict):
        if c["currencyCode"] not in Currency:
            logger.info(f"unsupported currency {c['currencyCode']}")
            return None
        assert c["priceType"] in {"RANGE", "FIXED"}, c["priceType"]
        if not c["prices"]:
            logger.info(f"invalid prices: {c['prices']}")
            return None
        if c["priceType"] == "RANGE":
            if c["priceStep"] > 1:
                logger.info(f"unsupported priceStep {c['priceStep']}")
                return None
            min_value = round(c["prices"][0] * 100)  # cents
            max_value = round(c["prices"][1] * 100)  # cents
        else:
            min_value = round(min(c["prices"]) * 100)
            max_value = round(min(c["prices"]) * 100)
        country_iso = locales.get_country_iso(c["countryCode"].lower())

        return CashoutMethod(
            name=c["name"],
            description=c["name"],
            id=uuid4().hex,
            currency=Currency.USD,
            original_currency=Currency(c["currencyCode"]),
            image_url=c["imageUrl"],
            min_value=min_value,
            max_value=max_value,
            data=GiftronautCashoutMethodData(
                productId=c["productId"],
                terms=c["terms"] or "",
                redemptionInstructions=c["redemptionInstructions"] or "",
                country_iso=country_iso,
                priceStep=c["priceStep"],
            ),
            type=PayoutType.GIFTRONAUT,
            user=None,
            ext_id=c["productId"],
        )

    def list_brands_page(self, page: int = 1, size: int = 20) -> dict[str, Any]:
        params = {"page": page, "pageSize": size}
        _res = self._request("GET", "/api/v1/catalog/branded-cards", params=params)
        return _res

    def list_brands(self) -> list[dict[str, Any]]:
        page = 1
        size = 100
        res = []
        while True:
            _res = self.list_brands_page(page=page, size=size)
            logger.info(f"list_brands {page=} {size=} total={_res['totalCount']}")
            res.extend(_res["data"])
            if _res["totalCount"] == len(res):
                break
            page += 1
        return res

    def list_supported_brands(self) -> list[CashoutMethod]:
        res = self.list_brands()
        cms = [self.format_card_to_cashout_method(b) for b in res]
        return [x for x in cms if x is not None]

    def get_order(self, order_id: str):
        res = self._request("GET", f"/api/v1/orders/{order_id}")
        d = res["data"]
        assert len(d["recipients"]) == 1
        d.update(d.pop("recipients")[0])
        return d

    def create_order(
        self, cashout_request_id: str, card_product_id: str, amount: Decimal
    ):
        # amazon.com: G00000020361
        data = {
            "idempotencyKey": cashout_request_id,
            "cardType": "BRANDED_CARD",
            "productId": card_product_id,
            "quantity": 1,
            "amountPerLink": str(amount),
            "refundOption": True,
            "refundOptionDays": 90,
        }
        res = self._request("POST", "/api/v1/orders/reward-links", json=data)
        """
        {'data': {'orderId': 'LO20261030149296',
          'idempotencyKey': 'test',
          'cardType': 'BRANDED_CARD',
          'productId': 'G00000020361',
          'quantity': 1,
          'amountPerLink': {'amount': 5.0, 'currency': 'USD'},
          'totalCost': {'amount': 5.0, 'currency': 'USD'},
          'status': 'PENDING',
          'createdAt': '2026-10-29T18:59:05.781813622Z'}}
        """
        return res

    def get_balance(self):
        return self._request("GET", "/api/v1/balance")['data']


class GiftronautManager(PostgresManagerWithRedis):
    def __init__(
        self,
        *,
        client_id: str,
        secret: str,
        pg_config: PostgresConfig,
        redis_config: RedisConfig,
        permissions: Collection[Permission] | None = None,
        cache_prefix: str | None = None,
    ):
        self.client = GiftronautClient(client_id=client_id, secret=secret)
        self.cashout_method_manager = CashoutMethodManager(pg_config=pg_config)
        super().__init__(
            pg_config=pg_config,
            permissions=permissions,
            redis_config=redis_config,
            cache_prefix=cache_prefix,
        )

    def make_request(
        self, amount: USDCent, cashout_method: CashoutMethod, payout_event_id: str
    ) -> GiftronautCashoutMethodRequestData:
        """Build the data needed to place an order."""
        assert type(amount) is USDCent
        brand_code = cashout_method.data.brand_code
        currency = cashout_method.original_currency
        assert currency == Currency.USD
        amount_in_currency = Decimal(float(amount) / 100).quantize(Decimal("0.01"))
        return GiftronautCashoutMethodRequestData.model_validate(
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
        request = GiftronautCashoutMethodRequestData.model_validate(
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
            status=order["status"],
            ext_ref_id=order["number"],
            order_data=order,
        )

        ledger_manager.create_tx_user_payout_complete(
            user,
            payout_event=payout_event,
        )

        return payout_event

    def update_cashout_method_db(self):
        # This only should be run once, or when brands/cards offered change.
        cms = self.client.list_supported_brands()

        existing = self.cashout_method_manager.filter(
            payout_types=[PayoutType.GIFTRONAUT]
        )
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
