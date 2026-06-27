from decimal import Decimal
from typing import Any, Dict, List

import requests
from pydantic import BaseModel

TANGO_SANDBOX_URL = "https://integration-api.tangocard.com/raas/v2"
TANGO_PROD_URL = "https://api.tangocard.com/raas/v2"


class TangoError(RuntimeError):
    pass


class TangoOrderRequest(BaseModel):
    externalRefID: str

    customerIdentifier: str
    accountIdentifier: str

    utid: str
    sendEmail: bool
    campaign: str
    # In USD (e.g. '0.24' is 24 cents)
    amount: Decimal


class TangoClient:
    def __init__(
        self,
        *,
        platform_name: str,
        platform_key: str,
        base_url: str = TANGO_SANDBOX_URL,
        timeout: float = 30.0,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

        self.session = requests.Session()
        self.session.auth = (platform_name, platform_key)
        self.session.headers.update(
            {
                "Accept": "application/json",
                "Content-Type": "application/json",
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

        request_id = response.headers.get("X-REQUEST-ID")
        try:
            body = response.json()
        except ValueError:
            body = response.text

        raise TangoError(
            f"Tango API error {response.status_code}"
            + (f" X-REQUEST-ID={request_id}" if request_id else "")
            + f": {body}"
        )

    def get_exchange_rates(self, reward_currency: str = "USD") -> Any:
        return self._request(
            "GET",
            "/exchangerates",
            params={"rewardCurrency": reward_currency},
        )

    def get_customers(self) -> Any:
        return self._request("GET", "/customers")

    def get_accounts(self, customer_identifier: str) -> Any:
        return self._request(
            "GET",
            f"/customers/{customer_identifier}/accounts",
        )

    def get_account(
        self,
        account_identifier: str,
    ) -> Any:
        return self._request(
            "GET",
            f"/accounts/{account_identifier}",
        )

    def get_catalog(self) -> Dict[str, List[Dict[str, Any]]]:
        """
        Replacement for:
            api_client.catalog.get_catalog()
        """
        return self._request("GET", "/catalogs")

    def list_orders(self, **filters: Any) -> list[dict[str, Any]]:
        return self._request("GET", "/orders", params=filters or None)

    def get_order(self, reference_order_id: str) -> dict[str, Any]:
        return self._request("GET", f"/orders/{reference_order_id}")

    def get_order_if_exists(self, reference_order_id: str) -> dict[str, Any] | None:
        try:
            return self.get_order(reference_order_id)
        except TangoError as e:
            if "The order you requested cannot be found" not in e.args[0]:
                raise e
            return None

    def create_order(self, order: TangoOrderRequest) -> dict[str, Any]:
        payload = order.model_dump(mode="json", exclude_none=True)

        # Pydantic serializes Decimal as string in JSON mode. Tango docs say `amount`
        # is a double, so convert for the outgoing payload.
        payload["amount"] = float(order.amount)

        return self._request("POST", "/orders", json=payload)
