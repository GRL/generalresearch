import logging
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Callable, Optional, Tuple

from generalresearch.config import JAMES_BILLINGS_BPID, JAMES_BILLINGS_TX_CUTOFF
from generalresearch.currency import USDCent
from generalresearch.models.custom_types import UUIDStr
from generalresearch.models.thl.product import Product
from generalresearch.models.thl.session import Session, Wall
from generalresearch.models.thl.user import User

logging.basicConfig()
logger = logging.getLogger("LedgerManager")
logger.setLevel(logging.INFO)

if TYPE_CHECKING:
    from generalresearch.managers.thl.ledger_manager.ledger import (
        LedgerManager,
    )
    from generalresearch.managers.thl.ledger_manager.thl_ledger import (
        ThlLedgerManager,
    )


def generate_condition_mp_payment(wall: "Wall") -> Callable[..., bool]:
    """This returns a function that checks if the payment for this wall event
    exists already. This function gets run after we acquire a lock. It
    should return True if we want to continue (create a tx).
    """
    wall_uuid = wall.uuid

    def _condition(lm: "LedgerManager") -> bool:
        tag = f"{lm.currency.value}:mp_payment:{wall_uuid}"
        txs = lm.get_tx_ids_by_tag(tag=tag)
        return len(txs) == 0

    return _condition


def generate_condition_bp_payment(session: "Session") -> Callable[..., bool]:
    """This returns a function that checks if the payment for this Session
    exists already. This function gets run after we acquire a lock. It
    should return True if we want to continue (create a tx).
    """
    session_uuid = session.uuid

    def _condition(lm: "LedgerManager") -> bool:
        tag = f"{lm.currency.value}:bp_payment:{session_uuid}"
        txs_ids = lm.get_tx_ids_by_tag(tag=tag)
        return len(txs_ids) == 0

    return _condition


def generate_condition_tag_exists(tag: str) -> Callable[..., bool]:
    """This returns a function that checks if a tx with this tag already
    exists. It should return True if we want to continue (create a tx).
    """

    def _condition(lm: "LedgerManager") -> bool:
        txs_ids = lm.get_tx_ids_by_tag(tag=tag)
        return len(txs_ids) == 0

    return _condition


def generate_condition_bp_payout(
    product: "Product",
    amount: USDCent,
    payoutevent_uuid: UUIDStr,
    skip_one_per_day_check: bool = False,
    skip_wallet_balance_check: bool = False,
) -> Callable[..., Tuple[bool, str]]:
    created = datetime.now(tz=timezone.utc)

    def _condition(
        lm: "ThlLedgerManager",
    ) -> Tuple[bool, str]:
        bp_wallet_account = lm.get_account_or_create_bp_wallet(product=product)
        tag = f"{lm.currency.value}:bp_payout:{payoutevent_uuid}"
        txs_ids = lm.get_tx_ids_by_tag(tag=tag)

        if len(txs_ids) != 0:
            logger.info(f"{tag} failed condition check: already paid out payoutevent")
            return False, "duplicate tag"

        if not skip_one_per_day_check:
            txs = lm.get_tx_filtered_by_account(
                account_uuid=bp_wallet_account.uuid,
                time_start=created - timedelta(days=1),
                time_end=created,
            )
            txs = [tx for tx in txs if tx.metadata.get("tx_type") == "bp_payout"]
            if len(txs) != 0:
                logger.info(f"{tag} failed condition check >1 tx per day")
                return False, ">1 tx per day"

        if not skip_wallet_balance_check:
            balance: int = lm.get_account_balance(account=bp_wallet_account)
            if balance < amount:
                logger.info(
                    f"{tag} failed condition check balance: {balance} < requested amount: {amount}"
                )
                return False, "insufficient balance"

        return True, ""

    return _condition


def generate_condition_user_payout_request(
    user: User, payoutevent_uuid: UUIDStr, min_balance: Optional[int] = None
) -> Callable[..., bool]:
    """This returns a function that checks if `user` has at least
    `min_balance` in their wallet and that a payout request hasn't already
    been issued with this payoutevent_uuid.

    min_balance is an Optional[int] and not a USDCent because I believe
    that it could be negative. - Max 2024-07-18
    """

    if min_balance is not None:
        assert isinstance(min_balance, int)

    def _condition(lm: "ThlLedgerManager") -> bool:
        tag = f"{lm.currency.value}:user_payout:{payoutevent_uuid}:request"
        txs_ids = lm.get_tx_ids_by_tag(tag)

        if len(txs_ids) != 0:
            logger.info(f"{tag} failed condition check duplicate transaction")
            return False

        if min_balance is not None:
            user_wallet_account = lm.get_account_or_create_user_wallet(user)
            if user.product_id == JAMES_BILLINGS_BPID:
                balance = lm.get_account_balance_timerange(
                    user_wallet_account, time_start=JAMES_BILLINGS_TX_CUTOFF
                )
            else:
                balance = lm.get_account_balance(user_wallet_account)
            if balance < min_balance:
                logger.info(
                    f"{tag} failed condition check balance: {balance} < requested amount: {min_balance}"
                )
                return False
        return True

    return _condition


def generate_condition_enter_contest(
    user: User, tag: str, min_balance: USDCent
) -> Callable[..., Tuple[bool, str]]:
    """This returns a function that checks if `user` has at least
    `min_balance` in their wallet and that a tx doesn't already exist
    with this tag
    """
    assert isinstance(min_balance, USDCent), "balance must be USDCent"

    def _condition(lm: "ThlLedgerManager") -> Tuple[bool, str]:
        txs_ids = lm.get_tx_ids_by_tag(tag)
        if len(txs_ids) != 0:
            logger.info(f"{tag} failed condition check duplicate transaction")
            return False, "duplicate transaction"

        user_wallet_account = lm.get_account_or_create_user_wallet(user)
        balance = lm.get_account_balance(user_wallet_account)
        if balance < min_balance:
            logger.info(
                f"{tag} failed condition check balance: {balance} < requested amount: {min_balance}"
            )
            return False, "insufficient balance"
        return True, ""

    return _condition


def generate_condition_user_payout_action(
    payoutevent_uuid: UUIDStr, action: str
) -> Callable[..., bool]:
    """The balance has already been taken from the user's wallet, so there
    is no balance check. We only just check that the ledger transaction
    doesn't already exist.

    If the action is complete, we check if it hasn't already been cancelled.
    If canceled, we check it hasn't been completed.

    :param action: should be in {'complete', 'cancel'}
    """

    def _condition(lm: "ThlLedgerManager") -> bool:
        tag = f"{lm.currency.value}:user_payout:{payoutevent_uuid}:{action}"
        txs_ids = lm.get_tx_ids_by_tag(tag)
        if len(txs_ids) != 0:
            logger.info(f"{tag} failed condition check duplicate transaction")
            return False

        if action == "complete":
            tag = f"{lm.currency.value}:user_payout:{payoutevent_uuid}:cancel"
            txs = lm.get_tx_ids_by_tag(tag)
            if len(txs) != 0:
                logger.warning(
                    f"{tag} failed condition: trying to complete payout that was already cancelled"
                )
                return False

        if action == "cancel":
            tag = f"{lm.currency.value}:user_payout:{payoutevent_uuid}:complete"
            txs = lm.get_tx_ids_by_tag(tag)
            if len(txs) != 0:
                logger.warning(
                    f"{tag} failed condition: trying to cancel payout that was already completed"
                )
                return False
        return True

    return _condition
