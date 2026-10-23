from __future__ import annotations

import base64
from collections.abc import Collection
from decimal import Decimal
from typing import TYPE_CHECKING, Any
from uuid import uuid4

import requests

from generalresearch.currency import USDCent
from generalresearch.managers.base import Permission, PostgresManagerWithRedis
from generalresearch.managers.thl.cashout_method import CashoutMethodManager
from generalresearch.managers.thl.ledger_manager.thl_ledger import ThlLedgerManager
from generalresearch.models.thl.definitions import PayoutStatus
from generalresearch.models.thl.payout import UserPayoutEvent
from generalresearch.models.thl.user import User
from generalresearch.models.thl.wallet.cashout_method import (
    CashoutMethod,
    VICashoutMethodData,
    VICashoutMethodRequestData,
)
from generalresearch.models.thl.wallet.definitions import Currency, PayoutType
from generalresearch.pg_helper import PostgresConfig
from generalresearch.redis_helper import RedisConfig

if TYPE_CHECKING:
    from generalresearch.managers.thl.wallet.user_payout import UserPayoutEventManager

BASE_URL = "https://rest.virtualincentives.com/v6/json"


class VirtualIncentivesError(RuntimeError):
    pass


class VirtualIncentivesClient:
    PROGRAM_ID = 61929

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
                "Authorization": f"Basic {token}",
            }
        )

    @staticmethod
    def generate_token_from_login(username: str, password: str) -> str:
        s = username + ":" + password
        return base64.urlsafe_b64encode(s.encode()).decode()

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

        raise VirtualIncentivesError(
            f"VirtualIncentives API error {response.status_code}" + f": {body}"
        )

    def get_program_brands(self) -> list[dict[str, Any]]:
        return self._request(
            "GET",
            f"/programs/{self.PROGRAM_ID}/products",
        )["products"]

    def get_brand_marketing(self, sku: str) -> Any:
        return self._request(
            "GET",
            f"/products/{sku}/marketing",
        )

    def get_brand_terms(self, sku: str) -> Any:
        return self._request(
            "GET",
            f"/products/{sku}/terms",
        )

    # def get_all_brands(self):
    #     return self._request(
    #         "GET",
    #         "/products",
    #     )
    #
    # def get_brand(self, sku: str):
    #     return self._request(
    #         "GET",
    #         f"/products/{sku}",
    #     )

    @staticmethod
    def format_brand_to_cashout_method(c: dict):
        assert c["currency"] == "USD"
        assert c["country"] == "US"
        assert not c["denomination"]["fixed"]
        denomination_range = c["denomination"]["range"]
        assert len(denomination_range) == 1
        range = denomination_range[0]
        return CashoutMethod(
            name=c["name"],
            description=c["name"],
            id=uuid4().hex,
            currency=Currency.USD,
            original_currency=Currency.USD,
            image_url=c["faceplate"],
            min_value=round(range["min"] * 100),  # $1.00
            max_value=round(min(250.00, range["max"]) * 100),  # $250.00
            data=VICashoutMethodData(
                sku=c["sku"],
                value_type="variable",
                marketing=c["marketing"],
                terms=c["terms"],
            ),
            type=PayoutType.VIRTUAL_INCENTIVES,
            user=None,
            ext_id=c["sku"],
        )

    def get_order(self, cashout_request_id: str) -> dict[str, Any]:
        params = {"clientid": cashout_request_id}
        res = self._request("GET", "/orders", params=params)
        order = res["orders"]["order"]
        if not order:
            raise VirtualIncentivesError("Order not found")
        return order[0]

    def get_order_if_exists(self, cashout_request_id: str) -> dict[str, Any] | None:
        try:
            return self.get_order(cashout_request_id)
        except VirtualIncentivesError as e:
            if "Order not found" not in e.args[0]:
                raise
            return None

    def create_order(self, cashout_request_id: str, sku: str, amount: Decimal):
        """

        :param cashout_request_id: unique identifier of the cashout request
        :param sku: sku of the brand (from VI)
        :param amount: amount of the cashout (in the noted currency)
        :return:
        """
        assert type(amount) is Decimal
        data = {
            "programid": self.PROGRAM_ID,
            "clientid": cashout_request_id,
            "fulfillment": "bulk",
            "currency": "USD",
            "accounts": [
                {
                    "firstname": "Recipient",
                    "lastname": "Recipient",
                    "email": "support@g-r-l.com",
                    "sku": sku,
                    "amount": str(amount),
                }
            ],
        }

        return self._request("POST", "/orders", json={"order": data})


class VirtualIncentivesManager(PostgresManagerWithRedis):
    def __init__(
        self,
        *,
        virtual_incentives_token: str,
        pg_config: PostgresConfig,
        redis_config: RedisConfig,
        permissions: Collection[Permission] | None = None,
        cache_prefix: str | None = None,
    ):
        self.client = VirtualIncentivesClient(token=virtual_incentives_token)
        self.cashout_method_manager = CashoutMethodManager(pg_config=pg_config)
        super().__init__(
            pg_config=pg_config,
            permissions=permissions,
            redis_config=redis_config,
            cache_prefix=cache_prefix,
        )

    def make_request(
        self, amount: USDCent, cashout_method: CashoutMethod, payout_event_id: str
    ) -> VICashoutMethodRequestData:
        """Build the data needed to place a VI order."""
        assert type(amount) is USDCent
        sku = cashout_method.data.sku
        currency = cashout_method.original_currency
        assert currency == Currency.USD
        amount_in_currency = Decimal(float(amount) / 100).quantize(Decimal("0.01"))
        return VICashoutMethodRequestData.model_validate(
            {
                "sku": sku,
                "amount": amount_in_currency,
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
        We approved the VI card redemption. Actually request the card.
        """
        assert payout_event.status in {
            PayoutStatus.PENDING,
            PayoutStatus.FAILED,
        }, (
            "attempting to manage payout that is not pending (you can also retry a failed order)"
        )
        request = VICashoutMethodRequestData.model_validate(payout_event.request_data)

        # Note: VI uses the cashout_request_id to uniquify orders
        try:
            order = self.client.create_order(
                cashout_request_id=request.cashout_request_id,
                sku=request.sku,
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
        # This only should be run once, or when brands/cards offered by VI change.
        cards = self.client.get_program_brands()
        cms = []
        for c in cards:
            cms.append(self.client.format_brand_to_cashout_method(c))

        existing = self.cashout_method_manager.filter(
            payout_types=[PayoutType.VIRTUAL_INCENTIVES]
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
