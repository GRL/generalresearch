from generalresearch.managers.thl.ledger_manager.thl_ledger import (
    ThlLedgerManager,
)
from generalresearch.managers.thl.payout import PayoutEventManager
from generalresearch.models.thl.definitions import PayoutStatus
from generalresearch.models.thl.payout import UserPayoutEvent
from generalresearch.models.thl.user import User


def approve_paypal_order(
    payout_event: UserPayoutEvent, payout_event_manager: PayoutEventManager
):
    """
    The order has been approved, but it hasn't actually been sent.
    """
    assert payout_event.status in {
        PayoutStatus.PENDING,
        PayoutStatus.FAILED,
    }, "attempting to manage payout that is not pending (or you can retry a failed order)"

    payout_event_manager.update(payout_event, status=PayoutStatus.APPROVED)

    interface = payout_event.request_data.get("interface")
    if interface == "api":
        # todo: Use the Payouts API to sent this payout, and then update the DB
        pass

    else:
        # Flow monitoring for payouts where the type is paypal, the status is
        # approved, and the interface is web, and then it'll send the payout and
        # update the status to complete (and create a ledger item for the fee)
        pass

    return payout_event


