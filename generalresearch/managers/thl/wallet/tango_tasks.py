from __future__ import annotations

from typing import TYPE_CHECKING, Any

from generalresearch.config import (
    is_debug,
)
from generalresearch.managers.thl.tango_api import TangoOrderRequest
from generalresearch.models.thl.definitions import PayoutStatus

if TYPE_CHECKING:
    from generalresearch.managers.thl.ledger_manager.thl_ledger import (
        ThlLedgerManager,
    )
    from generalresearch.managers.thl.payout import PayoutEventManager
    from generalresearch.managers.thl.tango_api import TangoClient
    from generalresearch.models.thl.payout import UserPayoutEvent
    from generalresearch.models.thl.user import User


def complete_tango_order(
    user: User,
    payout_event: UserPayoutEvent,
    payout_event_manager: PayoutEventManager,
    ledger_manager: ThlLedgerManager,
    tango_client: TangoClient,
):
    """
    We approved the Tango card redemption. Actually request the card.

    (Note: we're skipping the PENDING -> APPROVED -> COMPLETE order for tango.
        When a tango request gets APPROVED, we COMPLETE it (or FAIL!) in the
        same step)
    """
    assert payout_event.status in {
        PayoutStatus.PENDING,
        PayoutStatus.FAILED,
    }, "attempting to manage payout that is not pending (or you can retry a failed order)"
    request = payout_event.request_data
    ref_id = request["externalRefID"]
    # amount_usd = Decimal(payout_event.request_data["amount_usd"])

    # Note: tango uses the ref_id to uniquify orders, so locking is not actually needed as long
    #   as the ref_id is the same.
    try:
        order = create_tango_order(
            request_data=payout_event.request_data,
            ref_id=ref_id,
            tango_client=tango_client,
        )

    except AssertionError:
        # todo: its possible the order went through, but something else was wrong
        # we should try to retrieve the order by its ref_id and confirm it really
        # failed...
        payout_event_manager.update(payout_event, status=PayoutStatus.FAILED)
        return payout_event

    # update TangoPayoutEvent with the order data
    payout_event_manager.update(
        payout_event,
        status=order["status"],
        ext_ref_id=order["referenceOrderID"],
        order_data=order,
    )

    ledger_manager.create_tx_user_payout_complete(user, payout_event=payout_event)

    return payout_event


def create_tango_order(
    request_data: dict[str, Any], ref_id: str, tango_client: TangoClient
) -> dict[str, Any]:
    """
    Create a tango gift card order.
    Throws exception if anything is not right.
    - https://integration-www.tangocard.com/raas_api_console/v2/
    - https://www.apimatic.io/apidocs/tangocard/v/2_3_4#/python

    :param utid: Card identifier
    :param amount: requested card value in USD
    :param ref_id: TangoPayoutEvent.uuid
    :return:
    """
    # make sure we don't create more than one tango order for a single PayoutEvent
    assert tango_client.get_order_if_exists(ref_id) is None
    amount = request_data["amount"]
    request_data.pop("amount_usd", None)
    request_data.pop("description", None)

    if is_debug():
        return {
            "status": "COMPLETE",
            "referenceOrderID": "test",
            "reward": {
                "credentials": {
                    "Security Code": "XXXX-XXXX",
                    "Redemption URL": "https://codes.rewardcodes.com/r2/1/XXXX",
                },
                "credentialList": [
                    {
                        "type": "text",
                        "label": "Security Code",
                        "value": "XXXX-XXXX",
                    },
                    {
                        "type": "url",
                        "label": "Redemption URL",
                        "value": "https://codes.rewardcodes.com/r2/1/XXXX",
                    },
                ],
                "redemptionInstructions": "do your thang fam",
            },
        }

    request = TangoOrderRequest.model_validate(request_data)
    order = tango_client.create_order(request)

    amount_f: float = float(amount)
    assert order["status"] == "COMPLETE"
    assert abs(order["amountCharged"]["total"] - amount_f) < 0.0200001
    assert order["amountCharged"]["currencyCode"] == "USD"
    if order["denomination"]["currencyCode"] == "USD":
        assert order["denomination"]["value"] == amount_f

    return order
