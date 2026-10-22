from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from generalresearch.currency import USDCent
from generalresearch.managers.thl.virtual_incentives_api import (
    VirtualIncentivesClient,
    VirtualIncentivesManager,
)
from generalresearch.models.thl.definitions import PayoutStatus


@pytest.fixture
def client() -> VirtualIncentivesClient:
    return VirtualIncentivesClient(token="secret-token")


@pytest.fixture
def variable_brand() -> dict:
    return {
        "sku": "AMAZON-US",
        "name": "Amazon.com Gift Card",
        "currency": "USD",
        "country": "US",
        "faceplate": "https://example.com/amazon.png",
        "marketing": "Amazon marketing copy",
        "terms": "Amazon terms",
        "denomination": {
            "fixed": False,
            "range": [{"min": 5.0, "max": 500.0}],
        },
    }


class TestVirtualIncentivesClient:

    def test_create_order_builds_vi_payload(self, client):
        client._request = Mock(return_value={"number": "order-1"})

        result = client.create_order("request-1", "AMAZON-US", Decimal("12.34"))

        assert result == {"number": "order-1"}
        client._request.assert_called_once_with(
            "POST",
            "/orders",
            json={
                "order": {
                    "programid": client.PROGRAM_ID,
                    "clientid": "request-1",
                    "fulfillment": "bulk",
                    "currency": "USD",
                    "accounts": [
                        {
                            "firstname": "Recipient",
                            "lastname": "Recipient",
                            "email": "support@g-r-l.com",
                            "sku": "AMAZON-US",
                            "amount": "12.34",
                        }
                    ],
                }
            },
        )



class TestVirtualIncentivesManager:
    @pytest.fixture
    def manager(self):
        manager = VirtualIncentivesManager.__new__(VirtualIncentivesManager)
        manager.client = Mock(spec=VirtualIncentivesClient)
        return manager

    def test_make_request(
        self, manager, client, variable_brand
    ):
        cashout_method = client.format_brand_to_cashout_method(variable_brand)

        request = manager.make_request(USDCent(1_234), cashout_method, "payout-1")

        assert request.sku == "AMAZON-US"
        assert request.amount == Decimal("12.34")
        assert request.cashout_request_id == "payout-1"

    def test_complete_order_updates_payout_and_ledger(self, manager):
        payout_event = SimpleNamespace(
            status=PayoutStatus.PENDING,
            request_data={
                "sku": "AMAZON-US",
                "amount": "12.34",
                "cashout_request_id": "payout-1",
            },
        )
        order = {"status": PayoutStatus.COMPLETE, "number": "order-1"}
        manager.client.create_order.return_value = order
        payout_manager = Mock()
        ledger_manager = Mock()
        user = Mock()

        result = manager.complete_order(
            user, payout_event, payout_manager, ledger_manager
        )

        assert result is payout_event
        manager.client.create_order.assert_called_once_with(
            cashout_request_id="payout-1",
            sku="AMAZON-US",
            amount=Decimal("12.34"),
        )
        payout_manager.update.assert_called_once_with(
            payout_event,
            status=PayoutStatus.COMPLETE,
            ext_ref_id="order-1",
            order_data=order,
        )
        ledger_manager.create_tx_user_payout_complete.assert_called_once_with(
            user, payout_event=payout_event
        )

    def test_complete_order_marks_assertion_failure_as_failed(self, manager):
        payout_event = SimpleNamespace(
            status=PayoutStatus.FAILED,
            request_data={
                "sku": "AMAZON-US",
                "amount": "12.34",
                "cashout_request_id": "payout-1",
            },
        )
        manager.client.create_order.side_effect = AssertionError("bad order")
        payout_manager = Mock()
        ledger_manager = Mock()

        result = manager.complete_order(
            Mock(), payout_event, payout_manager, ledger_manager
        )

        assert result is payout_event
        payout_manager.update.assert_called_once_with(
            payout_event, status=PayoutStatus.FAILED
        )
        ledger_manager.create_tx_user_payout_complete.assert_not_called()

    def test_complete_order_rejects_final_payout(self, manager):
        payout_event = SimpleNamespace(
            status=PayoutStatus.COMPLETE,
            request_data={
                "sku": "AMAZON-US",
                "amount": "12.34",
                "cashout_request_id": "payout-1",
            },
        )

        with pytest.raises(AssertionError, match="not pending"):
            manager.complete_order(Mock(), payout_event, Mock(), Mock())

        manager.client.create_order.assert_not_called()
