from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Mapping

import requests

from generalresearch.currency import USDCent
from generalresearch.managers.thl.ledger_manager.thl_ledger import (
    ThlLedgerManager,
)
from generalresearch.managers.thl.payout import UserPayoutEventManager
from generalresearch.managers.thl.user_manager.user_manager import UserManager
from generalresearch.models.thl.definitions import PayoutStatus
from generalresearch.models.thl.payout import UserPayoutEvent
from generalresearch.models.thl.user import User
from generalresearch.models.thl.wallet import PayoutType
from generalresearch.models.thl.wallet.cashout_method import (
    CashoutMethod,
    PaypalCashoutMethodData,
)

PAYPAL_SANDBOX_URL = "https://api-m.sandbox.paypal.com"
PAYPAL_PROD_URL = "https://api-m.paypal.com"


class PayPalError(RuntimeError):
    """Raised when PayPal returns an unsuccessful API response."""


class PayPalPayoutManager:
    """Small client for PayPal's Payouts REST API.

    Credentials are supplied by the caller so they can come from the
    application's secret store.  ``sender_batch_id`` and ``sender_item_id``
    should be stable application identifiers; PayPal uses them to prevent
    duplicate payouts.
    """

    def __init__(
        self,
        *,
        client_id: str,
        client_secret: str,
        base_url: str = PAYPAL_SANDBOX_URL,
        timeout: float = 30.0,
        session: requests.Session | None = None,
    ) -> None:
        if not client_id or not client_secret:
            raise ValueError("PayPal client_id and client_secret are required")

        self.client_id = client_id
        self.client_secret = client_secret
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.session = session or requests.Session()
        self._access_token: str | None = None
        self._access_token_expires_at: datetime | None = None

    def _get_access_token(self) -> str:
        now = datetime.now(tz=timezone.utc)
        if (
            self._access_token
            and self._access_token_expires_at
            and now < self._access_token_expires_at
        ):
            return self._access_token

        response = self.session.post(
            f"{self.base_url}/v1/oauth2/token",
            auth=(self.client_id, self.client_secret),
            data={"grant_type": "client_credentials"},
            headers={"Accept": "application/json"},
            timeout=self.timeout,
        )
        data = self._response_json(response)
        token = data.get("access_token")
        if not token:
            raise PayPalError("PayPal token response did not include access_token")

        # Refresh one minute early to avoid using a token while it expires.
        expires_in = max(int(data.get("expires_in", 300)) - 60, 0)
        self._access_token = token
        self._access_token_expires_at = now + timedelta(seconds=expires_in)
        return token

    def _request(
        self,
        method: str,
        path: str,
        *,
        json: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        response = self.session.request(
            method,
            f"{self.base_url}{path}",
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self._get_access_token()}",
            },
            json=json,
            params=params,
            timeout=self.timeout,
        )
        return self._response_json(response)

    @staticmethod
    def _response_json(response: requests.Response) -> dict[str, Any]:
        try:
            data = response.json()
        except ValueError:
            data = {"message": response.text}

        if not response.ok:
            debug_id = response.headers.get("PayPal-Debug-Id")
            raise PayPalError(
                f"PayPal API error {response.status_code}"
                + (f" PayPal-Debug-Id={debug_id}" if debug_id else "")
                + f": {data}"
            )
        if not isinstance(data, dict):
            raise PayPalError(f"Unexpected PayPal response: {data!r}")
        return data

    def send_payment(
        self,
        *,
        recipient_email: str,
        amount_cents: USDCent,
        sender_batch_id: str,
        sender_item_id: str | None = None,
        note: str | None = None,
        email_subject: str | None = None,
    ) -> dict[str, Any]:
        """Create a one-recipient USD payout and return PayPal's response."""
        assert isinstance(amount_cents, USDCent)
        if amount_cents <= 0:
            raise ValueError("amount_cents must be positive")
        if not recipient_email:
            raise ValueError("recipient_email is required")
        if not sender_batch_id:
            raise ValueError("sender_batch_id is required")

        amount = (Decimal(amount_cents) / 100).quantize(Decimal("0.01"))
        batch_header: dict[str, Any] = {
            "sender_batch_id": sender_batch_id,
            "recipient_type": "EMAIL",
        }
        if email_subject:
            batch_header["email_subject"] = email_subject

        item: dict[str, Any] = {
            "recipient_type": "EMAIL",
            "receiver": recipient_email,
            "amount": {"value": f"{amount:.2f}", "currency": "USD"},
            "sender_item_id": sender_item_id or sender_batch_id,
        }
        if note:
            item["note"] = note

        return self._request(
            "POST",
            "/v1/payments/payouts",
            json={"sender_batch_header": batch_header, "items": [item]},
        )

    def get_payout(self, payout_batch_id: str) -> dict[str, Any]:
        """Return the latest batch and item status for a payout."""
        if not payout_batch_id:
            raise ValueError("payout_batch_id is required")
        return self._request("GET", f"/v1/payments/payouts/{payout_batch_id}")

    def verify_webhook_signature(
        self,
        *,
        headers: Mapping[str, str],
        webhook_event: dict[str, Any],
        webhook_id: str,
    ) -> bool:
        """Ask PayPal to verify a webhook notification's signature."""
        normalized_headers = {key.lower(): value for key, value in headers.items()}

        def required_header(name: str) -> str:
            value = normalized_headers.get(name.lower())
            if not value:
                raise ValueError(f"Missing PayPal webhook header: {name}")
            return value

        result = self._request(
            "POST",
            "/v1/notifications/verify-webhook-signature",
            json={
                "auth_algo": required_header("PAYPAL-AUTH-ALGO"),
                "cert_url": required_header("PAYPAL-CERT-URL"),
                "transmission_id": required_header("PAYPAL-TRANSMISSION-ID"),
                "transmission_sig": required_header("PAYPAL-TRANSMISSION-SIG"),
                "transmission_time": required_header("PAYPAL-TRANSMISSION-TIME"),
                "webhook_id": webhook_id,
                "webhook_event": webhook_event,
            },
        )
        return result.get("verification_status") == "SUCCESS"


def create_paypal_payout(
    *,
    user: User,
    cashout_method: CashoutMethod,
    amount_cents: USDCent,
    user_payout_event_manager: UserPayoutEventManager,
    ledger_manager: ThlLedgerManager,
    paypal: PayPalPayoutManager,
    note: str | None = None,
    email_subject: str | None = None,
) -> UserPayoutEvent:
    """Create, reserve funds for, and submit a one-item PayPal payout.

    The payout-event UUID is used as both sender IDs, making a retry at
    PayPal idempotent for 30 days. The PayPal-generated batch ID is persisted
    in ``ext_ref_id`` and the initial API response is kept in ``order_data``.

    If submission raises, the payout event and its ledger reservation remain
    in place. This is intentional: a timeout or 5xx response is ambiguous and
    rolling back could allow the same money to be paid twice. The exception
    includes the payout-event UUID for reconciliation or a same-ID retry.
    """
    if cashout_method.type != PayoutType.PAYPAL:
        raise ValueError("cashout_method must be a PayPal cashout method")
    if not isinstance(cashout_method.data, PaypalCashoutMethodData):
        raise ValueError("cashout_method does not contain PayPal data")
    if (
        user.user_id is None
        or cashout_method.user is None
        or cashout_method.user.user_id != user.user_id
    ):
        raise ValueError("cashout_method does not belong to user")

    cashout_method.validate_requested_amount(amount_cents)
    user_account = ledger_manager.get_account_or_create_user_wallet(user=user)
    payout_event = user_payout_event_manager.create(
        debit_account_uuid=user_account.uuid,
        cashout_method_uuid=cashout_method.id,
        payout_type=PayoutType.PAYPAL,
        amount=amount_cents,
        status=PayoutStatus.PENDING,
        account_reference_type="user",
        account_reference_uuid=user.uuid,
        description=cashout_method.name,
        request_data={
            "interface": "api",
            "recipient_email": str(cashout_method.data.email),
        },
    )
    ledger_manager.create_tx_user_payout_request(
        user=user,
        payout_event=payout_event,
    )

    try:
        paypal_response = paypal.send_payment(
            recipient_email=str(cashout_method.data.email),
            amount_cents=amount_cents,
            sender_batch_id=payout_event.uuid,
            sender_item_id=payout_event.uuid,
            note=note,
            email_subject=email_subject,
        )
        payout_batch_id = paypal_response["batch_header"]["payout_batch_id"]
    except Exception as exc:
        raise PayPalError(
            f"PayPal submission failed for payout event {payout_event.uuid}"
        ) from exc

    user_payout_event_manager.update(
        payout_event=payout_event,
        status=PayoutStatus.PENDING,
        ext_ref_id=payout_batch_id,
        order_data=paypal_response,
    )
    return payout_event


def handle_paypal_payout_webhook(
    *,
    headers: Mapping[str, str],
    webhook_event: dict[str, Any],
    webhook_id: str,
    paypal: PayPalPayoutManager,
    user_payout_event_manager: UserPayoutEventManager,
    user_manager: UserManager,
    ledger_manager: ThlLedgerManager,
) -> UserPayoutEvent | None:
    """Verify and apply a PayPal Payouts webhook idempotently."""
    if not paypal.verify_webhook_signature(
        headers=headers,
        webhook_event=webhook_event,
        webhook_id=webhook_id,
    ):
        raise PayPalError("PayPal webhook signature verification failed")

    event_type = webhook_event.get("event_type", "")
    if not event_type.startswith(("PAYMENT.PAYOUTSBATCH.", "PAYMENT.PAYOUTS-ITEM.")):
        return None

    resource = webhook_event.get("resource")
    if not isinstance(resource, dict):
        raise PayPalError("PayPal payout webhook has no resource")

    sender_batch_header = resource.get("sender_batch_header") or {}
    payout_item = resource.get("payout_item") or {}
    payout_event_uuid = (
        resource.get("sender_batch_id")
        or sender_batch_header.get("sender_batch_id")
        or payout_item.get("sender_item_id")
    )
    if not payout_event_uuid:
        raise PayPalError("PayPal payout webhook has no sender payout ID")

    payout_event = user_payout_event_manager.get_by_uuid(payout_event_uuid)
    if payout_event.payout_type != PayoutType.PAYPAL:
        raise PayPalError(f"Payout event {payout_event.uuid} is not a PayPal payout")
    if payout_event.status == PayoutStatus.COMPLETE:
        return payout_event

    payout_batch_id = resource.get("payout_batch_id") or payout_event.ext_ref_id
    if not payout_batch_id:
        raise PayPalError("PayPal payout webhook has no payout_batch_id")
    if payout_event.ext_ref_id and payout_event.ext_ref_id != payout_batch_id:
        raise PayPalError("PayPal payout_batch_id does not match the payout event")

    details = paypal.get_payout(payout_batch_id)
    items = details.get("items") or []
    if len(items) != 1:
        raise PayPalError(
            f"Expected one PayPal payout item for {payout_event.uuid}, got {len(items)}"
        )
    item = items[0]
    item_sender_id = (item.get("payout_item") or {}).get("sender_item_id")
    if item_sender_id != payout_event.uuid:
        raise PayPalError("PayPal sender_item_id does not match the payout event")

    amount = (item.get("payout_item") or {}).get("amount") or {}
    if amount.get("currency") != "USD":
        raise PayPalError("Only USD PayPal payouts are supported")
    amount_cents = int(Decimal(amount["value"]) * 100)
    if amount_cents != payout_event.amount:
        raise PayPalError("PayPal payout amount does not match the payout event")

    # get_payout_detail() expects transaction_id at the top level.
    details["transaction_id"] = item.get("transaction_id")
    transaction_status = item.get("transaction_status")
    if transaction_status == "SUCCESS":
        fee = item.get("payout_item_fee") or {}
        if fee.get("currency") != "USD" or "value" not in fee:
            raise PayPalError("Successful PayPal payout has no USD fee")

        user = user_manager.get_user(user_uuid=payout_event.account_reference_uuid)
        user.prefetch_product(pg_config=ledger_manager.pg_config)
        complete_tag = (
            f"{ledger_manager.currency.value}:user_payout:"
            f"{payout_event.uuid}:complete"
        )
        complete_transactions = ledger_manager.get_tx_ids_by_tag(complete_tag)
        if len(complete_transactions) > 1:
            raise PayPalError(f"Multiple ledger transactions found for {complete_tag}")
        if not complete_transactions:
            ledger_manager.create_tx_user_payout_complete(
                user=user,
                payout_event=payout_event,
                fee_amount=Decimal(fee["value"]),
            )
        user_payout_event_manager.update(
            payout_event=payout_event,
            status=PayoutStatus.COMPLETE,
            ext_ref_id=payout_batch_id,
            order_data=details,
        )
    elif transaction_status in {
        "FAILED",
        "BLOCKED",
        "RETURNED",
        "REFUNDED",
        "REVERSED",
        "CANCELED",
    }:
        user_payout_event_manager.update(
            payout_event=payout_event,
            status=PayoutStatus.FAILED,
            ext_ref_id=payout_batch_id,
            order_data=details,
        )
    elif payout_event.status == PayoutStatus.PENDING:
        user_payout_event_manager.update(
            payout_event=payout_event,
            status=PayoutStatus.APPROVED,
            ext_ref_id=payout_batch_id,
            order_data=details,
        )

    return payout_event
