from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import TYPE_CHECKING, Callable
from uuid import uuid4

from generalresearch.managers.thl.ledger_manager.thl_ledger import ThlLedgerManager
from generalresearch.managers.thl.user_compensate import user_compensate
from generalresearch.models.thl.definitions import (
    Status,
    WallAdjustedStatus,
)
from generalresearch.models.thl.ledger import (
    TransactionType,
    UserLedgerTransactionTypesSummary,
    UserLedgerTransactionTypeSummary,
)

if TYPE_CHECKING:
    from generalresearch.config import GRLSettings
    from generalresearch.models.thl.product import Product
    from generalresearch.models.thl.session import Session
    from generalresearch.models.thl.user import User
    from generalresearch.models.thl.wallet import PayoutType


def test_user_txs(
    user_factory: Callable[..., "User"],
    product_amt_true: "Product",
    create_main_accounts: Callable[..., None],
    thl_lm: ThlLedgerManager,
    lm,
    delete_ledger_db: Callable[..., None],
    session_with_tx_factory,
    adj_to_fail_with_tx_factory,
    adj_to_complete_with_tx_factory,
    session_factory,
    user_payout_event_manager,
    utc_now: datetime,
    settings: "GRLSettings",
):
    delete_ledger_db()
    create_main_accounts()

    user: User = user_factory(product=product_amt_true)
    account = thl_lm.get_account_or_create_user_wallet(user)
    print(f"{account.uuid=}")

    s: Session = session_with_tx_factory(user=user, wall_req_cpi=Decimal("1.00"))

    bribe_uuid = user_compensate(
        ledger_manager=thl_lm,
        user=user,
        amount_int=100,
    )

    pe = user_payout_event_manager.create(
        uuid=uuid4().hex,
        debit_account_uuid=account.uuid,
        cashout_method_uuid=settings.amt_assignment_cashout_method_id,
        amount=5,
        created=utc_now,
        payout_type=PayoutType.AMT_HIT,
        request_data=dict(),
    )
    thl_lm.create_tx_user_payout_request(
        user=user,
        payout_event=pe,
    )
    pe = user_payout_event_manager.create(
        uuid=uuid4().hex,
        debit_account_uuid=account.uuid,
        cashout_method_uuid=settings.amt_bonus_cashout_method_id,
        amount=127,
        created=utc_now,
        payout_type=PayoutType.AMT_BONUS,
        request_data=dict(),
    )
    thl_lm.create_tx_user_payout_request(
        user=user,
        payout_event=pe,
    )

    wall = s.wall_events[-1]
    adj_to_fail_with_tx_factory(session=s, created=wall.finished)

    # And a fail -> complete adjustment
    s_fail: Session = session_factory(
        user=user,
        wall_count=1,
        final_status=Status.FAIL,
        wall_req_cpi=Decimal("2.00"),
    )
    adj_to_complete_with_tx_factory(session=s_fail, created=utc_now)

    # txs = thl_lm.get_tx_filtered_by_account(account.uuid)
    # print(len(txs), txs)
    txs = thl_lm.get_user_txs(user)
    assert len(txs.transactions) == 6
    assert txs.total == 6
    assert txs.page == 1
    assert txs.size == 50

    # print(len(txs.transactions), txs)
    d = txs.model_dump_json()
    # print(d)

    descriptions = {x.description for x in txs.transactions}
    assert descriptions == {
        "Compensation Bonus",
        "HIT Bonus",
        "HIT Reward",
        "Task Adjustment",
        "Task Complete",
    }
    amounts = {x.amount for x in txs.transactions}
    assert amounts == {-127, 100, 38, -38, -5, 76}

    assert txs.summary == UserLedgerTransactionTypesSummary(
        bp_adjustment=UserLedgerTransactionTypeSummary(
            entry_count=2, min_amount=-38, max_amount=76, total_amount=76 - 38
        ),
        bp_payment=UserLedgerTransactionTypeSummary(
            entry_count=1, min_amount=38, max_amount=38, total_amount=38
        ),
        user_bonus=UserLedgerTransactionTypeSummary(
            entry_count=1, min_amount=100, max_amount=100, total_amount=100
        ),
        user_payout_request=UserLedgerTransactionTypeSummary(
            entry_count=2, min_amount=-127, max_amount=-5, total_amount=-132
        ),
    )
    tx_adj_c = [
        tx for tx in txs.transactions if tx.tx_type == TransactionType.BP_ADJUSTMENT
    ]
    assert sorted([tx.amount for tx in tx_adj_c]) == [-38, 76]


def test_user_txs_pagination(
    user_factory: Callable[..., "User"],
    product_amt_true: "Product",
    create_main_accounts: Callable[..., None],
    thl_lm: "ThlLedgerManager",
    lm: "LedgerManager",
    delete_ledger_db: Callable[..., None],
    session_with_tx_factory: Callable[..., "Session"],
    adj_to_fail_with_tx_factory,
    user_payout_event_manager,
    utc_now: datetime,
):
    delete_ledger_db()
    create_main_accounts()

    user: User = user_factory(product=product_amt_true)
    account = thl_lm.get_account_or_create_user_wallet(user)
    print(f"{account.uuid=}")

    for _ in range(12):
        user_compensate(
            ledger_manager=thl_lm,
            user=user,
            amount_int=100,
            skip_flag_check=True,
        )

    txs = thl_lm.get_user_txs(user, page=1, size=5)
    assert len(txs.transactions) == 5
    assert txs.total == 12
    assert txs.page == 1
    assert txs.size == 5
    assert txs.summary.user_bonus.total_amount == 1200
    assert txs.summary.user_bonus.entry_count == 12

    # Skip to the 3rd page. We made 12, so there are 2 left
    txs = thl_lm.get_user_txs(user, page=3, size=5)
    assert len(txs.transactions) == 2
    assert txs.total == 12
    assert txs.page == 3
    assert txs.summary.user_bonus.total_amount == 1200
    assert txs.summary.user_bonus.entry_count == 12

    # Should be empty, not fail
    txs = thl_lm.get_user_txs(user, page=4, size=5)
    assert len(txs.transactions) == 0
    assert txs.total == 12
    assert txs.page == 4
    assert txs.summary.user_bonus.total_amount == 1200
    assert txs.summary.user_bonus.entry_count == 12

    # Test filtering. We should pull back only this one
    now = datetime.now(tz=timezone.utc)
    user_compensate(
        ledger_manager=thl_lm,
        user=user,
        amount_int=100,
        skip_flag_check=True,
    )
    txs = thl_lm.get_user_txs(user, page=1, size=5, time_start=now)
    assert len(txs.transactions) == 1
    assert txs.total == 1
    assert txs.page == 1
    # And the summary is restricted to this time range also!
    assert txs.summary.user_bonus.total_amount == 100
    assert txs.summary.user_bonus.entry_count == 1

    # And filtering with 0 results
    now = datetime.now(tz=timezone.utc)
    txs = thl_lm.get_user_txs(user, page=1, size=5, time_start=now)
    assert len(txs.transactions) == 0
    assert txs.total == 0
    assert txs.page == 1
    assert txs.pages == 0
    # And the summary is restricted to this time range also!
    assert txs.summary.user_bonus.total_amount == None
    assert txs.summary.user_bonus.entry_count == 0


def test_user_txs_rolling_balance(
    user_factory: Callable[..., "User"],
    product_amt_true: "Product",
    create_main_accounts,
    thl_lm,
    lm,
    delete_ledger_db: Callable[..., None],
    session_with_tx_factory,
    adj_to_fail_with_tx_factory,
    user_payout_event_manager,
    settings: "GRLSettings",
):
    """
    Creates 3 $1.00 bonuses (postive),
    then 1 cashout (negative), $1.50
    then 3 more $1.00 bonuses.
    Note: pagination + rolling balance will BREAK if txs have
    identical timestamps. In practice, they do not.
    """
    delete_ledger_db()
    create_main_accounts()

    user: User = user_factory(product=product_amt_true)
    account = thl_lm.get_account_or_create_user_wallet(user)

    for _ in range(3):
        user_compensate(
            ledger_manager=thl_lm,
            user=user,
            amount_int=100,
            skip_flag_check=True,
        )

    pe = user_payout_event_manager.create(
        uuid=uuid4().hex,
        debit_account_uuid=account.uuid,
        cashout_method_uuid=settings.amt_bonus_cashout_method_id,
        amount=150,
        payout_type=PayoutType.AMT_BONUS,
        request_data=dict(),
    )
    thl_lm.create_tx_user_payout_request(
        user=user,
        payout_event=pe,
    )
    for _ in range(3):
        user_compensate(
            ledger_manager=thl_lm,
            user=user,
            amount_int=100,
            skip_flag_check=True,
        )

    txs = thl_lm.get_user_txs(user, page=1, size=10)
    assert txs.transactions[0].balance_after == 100
    assert txs.transactions[1].balance_after == 200
    assert txs.transactions[2].balance_after == 300
    assert txs.transactions[3].balance_after == 150
    assert txs.transactions[4].balance_after == 250
    assert txs.transactions[5].balance_after == 350
    assert txs.transactions[6].balance_after == 450

    # Ascending order, get 2nd page, make sure the balances include
    # the previous txs. (will return last 3 txs)
    txs = thl_lm.get_user_txs(user, page=2, size=4)
    assert len(txs.transactions) == 3
    assert txs.transactions[0].balance_after == 250
    assert txs.transactions[1].balance_after == 350
    assert txs.transactions[2].balance_after == 450

    # Descending order, get 1st page. Will
    # return most recent 3 txs in desc order
    txs = thl_lm.get_user_txs(user, page=1, size=3, order_by="-created")
    assert len(txs.transactions) == 3
    assert txs.transactions[0].balance_after == 450
    assert txs.transactions[1].balance_after == 350
    assert txs.transactions[2].balance_after == 250
