from decimal import Decimal
from typing import Optional, Dict

from generalresearch.managers.thl.ledger_manager.thl_ledger import (
    ThlLedgerManager,
)
from generalresearch.managers.thl.payout import (
    UserPayoutEventManager,
    PayoutEventManager,
)
from generalresearch.managers.thl.user_manager.user_manager import (
    UserManager,
)
from generalresearch.managers.thl.userhealth import UserIpHistoryManager
from generalresearch.managers.thl.wallet.approve import (
    approve_paypal_order,
    approve_amt_cashout,
)
from generalresearch.models.thl.definitions import PayoutStatus
from generalresearch.models.thl.payout import UserPayoutEvent
from generalresearch.models.thl.wallet import PayoutType
from generalresearch.models.thl.wallet.cashout_method import (
    CashMailOrderData,
)


def manage_pending_cashout(
    payout_id: str,
    new_status: PayoutStatus,
    user_payout_event_manager: UserPayoutEventManager,
    user_ip_history_manager: UserIpHistoryManager,
    user_manager: UserManager,
    ledger_manager: ThlLedgerManager,
    order_data: Optional[Dict | CashMailOrderData] = None,
) -> UserPayoutEvent:
    """
    Called by a UI actions performed by Todd. This rejects/approves/cancels
    a payout event. We're calling this "cashout" because that is the
    terminology used in generalresearch, even though the cashouts are stored
    in the payoutevent table

    :param payout_id: the payoutevent pk hex
    :param new_status:
    :param user_payout_event_manager
    :param user_ip_history_manager
    :param user_manager
    :param ledger_manager
    :param order_data: For Cash_in_mail, pass this in.

    :returns: PayoutEvent object
    """
    pe: UserPayoutEvent = user_payout_event_manager.get_by_uuid(payout_id)
    pe.check_status_change_allowed(status=new_status)
    assert pe.account_reference_type == "user"
    user = user_manager.get_user(user_uuid=pe.account_reference_uuid)
    user.prefetch_product(user_manager.mysql_user_manager.pg_config)

    assert (
        user.product.user_wallet_enabled
    ), "manage_pending_cashout called on user without managed wallet"
    assert not user.blocked, "manage_pending_cashout: Blocked user"
    assert not user_ip_history_manager.is_user_anonymous(
        user
    ), "manage_pending_cashout: Anonymous user"

    # Just assign it with direct casting/type annotation
    payout_event_manager: PayoutEventManager = user_payout_event_manager

    if new_status == PayoutStatus.APPROVED:
        if pe.payout_type == PayoutType.TANGO:
            from generalresearch.managers.thl.wallet.tango import (
                complete_tango_order,
            )

            complete_tango_order(
                user=user,
                payout_event=pe,
                payout_event_manager=payout_event_manager,
                ledger_manager=ledger_manager,
            )

        elif pe.payout_type == PayoutType.PAYPAL:
            approve_paypal_order(
                payout_event=pe, payout_event_manager=payout_event_manager
            )

        elif pe.payout_type in {PayoutType.AMT_BONUS, PayoutType.AMT_HIT}:
            approve_amt_cashout(
                user=user,
                payout_event=pe,
                payout_event_manager=payout_event_manager,
                ledger_manager=ledger_manager,
            )

        elif pe.payout_type == PayoutType.CASH_IN_MAIL:
            assert order_data, "must pass order_data"
            payout_event_manager.update(
                pe, status=PayoutStatus.APPROVED, order_data=order_data
            )
            ledger_manager.create_tx_user_payout_complete(
                user,
                payout_event=pe,
                fee_amount=Decimal(order_data.shipping_cost) / 100,
            )

        else:
            raise ValueError(f"unsupported payout_type: {pe.payout_type}")

        return pe

    elif new_status == PayoutStatus.COMPLETE:
        # Used only for AMT/dummy cashouts that are actually paid out not
        # by us. They are informing us that the cashout was successfully
        # sent to the user
        if pe.payout_type in {PayoutType.AMT_BONUS, PayoutType.AMT_HIT}:
            # We already do this under approve_amt_cashout()
            pass

        elif pe.payout_type == PayoutType.PAYPAL:
            # This is an issue here in that we actually don't know what the
            # fee is until it is sent and we read it back from paypal's csv
            # result. We have to just run this with a custom script, which
            # uses manual_complete_paypal_order()
            raise ValueError("user custom paypal script for this")

        payout_event_manager.update(pe, status=new_status)

        return pe

    elif new_status == PayoutStatus.REJECTED:
        # They lose the money in their wallet at this point, no ledger txs occur.
        payout_event_manager.update(pe, status=new_status)
        return pe

    elif new_status == PayoutStatus.CANCELLED:
        # create another ledger item putting the money back into their wallet.
        payout_event_manager.update(pe, status=new_status)
        ledger_manager.create_tx_user_payout_cancelled(user, payout_event=pe)
        return pe

    elif new_status == PayoutStatus.FAILED:
        # We just update the status (like in PayoutStatus.REJECTED). No ledger xs
        payout_event_manager.update(pe, status=new_status)
        return pe

    else:
        raise ValueError(f"unsupported status: {new_status}")
