import logging
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from uuid import uuid4

import pytest

from generalresearch.managers.thl.ledger_manager.exceptions import (
    LedgerTransactionFlagAlreadyExistsError,
    LedgerTransactionConditionFailedError,
)
from generalresearch.models.thl.user import User
from generalresearch.models.thl.wallet import PayoutType
from generalresearch.models.thl.payout import UserPayoutEvent
from test_utils.managers.ledger.conftest import create_main_accounts


class TestLedgerManagerAMT:

    def test_create_transaction_amt_ass_request(
        self,
        user_factory,
        product_amt_true,
        create_main_accounts,
        thl_lm,
        lm,
        delete_ledger_db,
    ):
        delete_ledger_db()
        create_main_accounts()
        user: User = user_factory(product=product_amt_true)

        # debit_account_uuid nothing checks they match the ledger ... todo?
        pe = UserPayoutEvent(
            uuid=uuid4().hex,
            payout_type=PayoutType.AMT_HIT,
            amount=5,
            cashout_method_uuid=uuid4().hex,
            debit_account_uuid=uuid4().hex,
        )
        flag_key = f"test:user_payout:{pe.uuid}:request"
        flag_name = f"ledger-manager:transaction_flag:{flag_key}"
        lm.redis_client.delete(flag_name)

        # User has $0 in their wallet. They are allowed amt_assignment payouts until -$1.00
        thl_lm.create_tx_user_payout_request(user=user, payout_event=pe)
        with pytest.raises(expected_exception=LedgerTransactionFlagAlreadyExistsError):
            thl_lm.create_tx_user_payout_request(
                user=user, payout_event=pe, skip_flag_check=False
            )
        with pytest.raises(expected_exception=LedgerTransactionConditionFailedError):
            thl_lm.create_tx_user_payout_request(
                user=user, payout_event=pe, skip_flag_check=True
            )
        pe2 = UserPayoutEvent(
            uuid=uuid4().hex,
            payout_type=PayoutType.AMT_HIT,
            amount=96,
            cashout_method_uuid=uuid4().hex,
            debit_account_uuid=uuid4().hex,
        )

        flag_key = f"test:user_payout:{pe2.uuid}:request"
        flag_name = f"ledger-manager:transaction_flag:{flag_key}"
        lm.redis_client.delete(flag_name)
        # 96 cents would put them over the -$1.00 limit
        with pytest.raises(expected_exception=LedgerTransactionConditionFailedError):
            thl_lm.create_tx_user_payout_request(user, payout_event=pe2)

        # But they could do 0.95 cents
        pe2.amount = 95
        thl_lm.create_tx_user_payout_request(
            user, payout_event=pe2, skip_flag_check=True
        )

        cash = thl_lm.get_account_cash()
        bp_wallet_account = thl_lm.get_account_or_create_bp_wallet(user.product)
        bp_pending_account = thl_lm.get_or_create_bp_pending_payout_account(
            product=user.product
        )
        user_wallet_account = thl_lm.get_account_or_create_user_wallet(user=user)

        assert 0 == lm.get_account_balance(account=bp_wallet_account)
        assert 0 == lm.get_account_balance(account=cash)
        assert 100 == lm.get_account_balance(account=bp_pending_account)
        assert -100 == lm.get_account_balance(account=user_wallet_account)
        assert thl_lm.check_ledger_balanced()
        assert -5 == thl_lm.get_account_filtered_balance(
            account=user_wallet_account,
            metadata_key="payoutevent",
            metadata_value=pe.uuid,
        )

        assert -95 == thl_lm.get_account_filtered_balance(
            account=user_wallet_account,
            metadata_key="payoutevent",
            metadata_value=pe2.uuid,
        )

    def test_create_transaction_amt_ass_complete(
        self,
        user_factory,
        product_amt_true,
        create_main_accounts,
        thl_lm,
        lm,
        delete_ledger_db,
    ):
        delete_ledger_db()
        create_main_accounts()
        user: User = user_factory(product=product_amt_true)

        pe = UserPayoutEvent(
            uuid=uuid4().hex,
            payout_type=PayoutType.AMT_HIT,
            amount=5,
            cashout_method_uuid=uuid4().hex,
            debit_account_uuid=uuid4().hex,
        )
        flag = f"ledger-manager:transaction_flag:test:user_payout:{pe.uuid}:request"
        lm.redis_client.delete(flag)
        flag = f"ledger-manager:transaction_flag:test:user_payout:{pe.uuid}:complete"
        lm.redis_client.delete(flag)

        # User has $0 in their wallet. They are allowed amt_assignment payouts until -$1.00
        thl_lm.create_tx_user_payout_request(user, payout_event=pe)
        thl_lm.create_tx_user_payout_complete(user, payout_event=pe)

        cash = thl_lm.get_account_cash()
        bp_wallet_account = thl_lm.get_account_or_create_bp_wallet(user.product)
        bp_pending_account = thl_lm.get_or_create_bp_pending_payout_account(
            user.product
        )
        bp_amt_expense_account = thl_lm.get_account_or_create_bp_expense(
            user.product, expense_name="amt"
        )
        user_wallet_account = thl_lm.get_account_or_create_user_wallet(user)

        # BP wallet pays the 1cent fee
        assert -1 == thl_lm.get_account_balance(bp_wallet_account)
        assert -5 == thl_lm.get_account_balance(cash)
        assert -1 == thl_lm.get_account_balance(bp_amt_expense_account)
        assert 0 == thl_lm.get_account_balance(bp_pending_account)
        assert -5 == lm.get_account_balance(user_wallet_account)
        assert thl_lm.check_ledger_balanced()

    def test_create_transaction_amt_bonus(
        self,
        user_factory,
        product_amt_true,
        create_main_accounts,
        thl_lm,
        lm,
        delete_ledger_db,
    ):
        delete_ledger_db()
        create_main_accounts()

        user: User = user_factory(product=product_amt_true)

        pe = UserPayoutEvent(
            uuid=uuid4().hex,
            payout_type=PayoutType.AMT_BONUS,
            amount=34,
            cashout_method_uuid=uuid4().hex,
            debit_account_uuid=uuid4().hex,
        )
        flag = f"ledger-manager:transaction_flag:test:user_payout:{pe.uuid}:request"
        lm.redis_client.delete(flag)
        flag = f"ledger-manager:transaction_flag:test:user_payout:{pe.uuid}:complete"
        lm.redis_client.delete(flag)

        with pytest.raises(expected_exception=LedgerTransactionConditionFailedError):
            # User has $0 in their wallet. No amt bonus allowed
            thl_lm.create_tx_user_payout_request(user, payout_event=pe)

        thl_lm.create_tx_user_bonus(
            user,
            amount=Decimal(5),
            ref_uuid="e703830dec124f17abed2d697d8d7701",
            description="Bribe",
            skip_flag_check=True,
        )
        pe.amount = 101
        thl_lm.create_tx_user_payout_request(
            user, payout_event=pe, skip_flag_check=False
        )
        thl_lm.create_tx_user_payout_complete(
            user, payout_event=pe, skip_flag_check=False
        )
        with pytest.raises(expected_exception=LedgerTransactionFlagAlreadyExistsError):
            # duplicate, even if amount changed
            pe.amount = 200
            thl_lm.create_tx_user_payout_complete(
                user, payout_event=pe, skip_flag_check=False
            )
        with pytest.raises(expected_exception=LedgerTransactionConditionFailedError):
            # duplicate
            thl_lm.create_tx_user_payout_complete(
                user, payout_event=pe, skip_flag_check=True
            )
        pe.uuid = "533364150de4451198e5774e221a2acb"
        pe.amount = 9900
        with pytest.raises(expected_exception=ValueError):
            # Trying to complete payout with no pending tx
            thl_lm.create_tx_user_payout_complete(
                user, payout_event=pe, skip_flag_check=True
            )
        with pytest.raises(expected_exception=LedgerTransactionConditionFailedError):
            # trying to payout $99 with only a $5 balance
            thl_lm.create_tx_user_payout_request(
                user, payout_event=pe, skip_flag_check=True
            )

        cash = thl_lm.get_account_cash()
        bp_wallet_account = thl_lm.get_account_or_create_bp_wallet(user.product)
        bp_pending_account = thl_lm.get_or_create_bp_pending_payout_account(
            user.product
        )
        bp_amt_expense_account = thl_lm.get_account_or_create_bp_expense(
            user.product, expense_name="amt"
        )
        user_wallet_account = thl_lm.get_account_or_create_user_wallet(user)
        assert -500 + round(-101 * 0.20) == thl_lm.get_account_balance(
            bp_wallet_account
        )
        assert -101 == lm.get_account_balance(cash)
        assert -20 == lm.get_account_balance(bp_amt_expense_account)
        assert 0 == lm.get_account_balance(bp_pending_account)
        assert 500 - 101 == lm.get_account_balance(user_wallet_account)
        assert lm.check_ledger_balanced() is True

    def test_create_transaction_amt_bonus_cancel(
        self,
        user_factory,
        product_amt_true,
        create_main_accounts,
        caplog,
        thl_lm,
        lm,
        delete_ledger_db,
    ):
        delete_ledger_db()
        create_main_accounts()

        now = datetime.now(timezone.utc) - timedelta(hours=1)
        user: User = user_factory(product=product_amt_true)

        pe = UserPayoutEvent(
            uuid=uuid4().hex,
            payout_type=PayoutType.AMT_BONUS,
            amount=101,
            cashout_method_uuid=uuid4().hex,
            debit_account_uuid=uuid4().hex,
        )

        thl_lm.create_tx_user_bonus(
            user,
            amount=Decimal(5),
            ref_uuid="c44f4da2db1d421ebc6a5e5241ca4ce6",
            description="Bribe",
            skip_flag_check=True,
        )
        thl_lm.create_tx_user_payout_request(
            user, payout_event=pe, skip_flag_check=True
        )
        thl_lm.create_tx_user_payout_cancelled(
            user, payout_event=pe, skip_flag_check=True
        )
        with pytest.raises(expected_exception=LedgerTransactionConditionFailedError):
            with caplog.at_level(logging.WARNING):
                thl_lm.create_tx_user_payout_complete(
                    user, payout_event=pe, skip_flag_check=True
                )
        assert "trying to complete payout that was already cancelled" in caplog.text

        cash = thl_lm.get_account_cash()
        bp_wallet_account = thl_lm.get_account_or_create_bp_wallet(user.product)
        bp_pending_account = thl_lm.get_or_create_bp_pending_payout_account(
            user.product
        )
        bp_amt_expense_account = thl_lm.get_account_or_create_bp_expense(
            user.product, expense_name="amt"
        )
        user_wallet_account = thl_lm.get_account_or_create_user_wallet(user)
        assert -500 == thl_lm.get_account_balance(account=bp_wallet_account)
        assert 0 == thl_lm.get_account_balance(account=cash)
        assert 0 == thl_lm.get_account_balance(account=bp_amt_expense_account)
        assert 0 == thl_lm.get_account_balance(account=bp_pending_account)
        assert 500 == thl_lm.get_account_balance(account=user_wallet_account)
        assert thl_lm.check_ledger_balanced()

        pe2 = UserPayoutEvent(
            uuid=uuid4().hex,
            payout_type=PayoutType.AMT_BONUS,
            amount=200,
            cashout_method_uuid=uuid4().hex,
            debit_account_uuid=uuid4().hex,
        )
        thl_lm.create_tx_user_payout_request(
            user, payout_event=pe2, skip_flag_check=True
        )
        thl_lm.create_tx_user_payout_complete(
            user, payout_event=pe2, skip_flag_check=True
        )
        with pytest.raises(expected_exception=LedgerTransactionConditionFailedError):
            with caplog.at_level(logging.WARNING):
                thl_lm.create_tx_user_payout_cancelled(
                    user, payout_event=pe2, skip_flag_check=True
                )
        assert "trying to cancel payout that was already completed" in caplog.text


class TestLedgerManagerTango:

    def test_create_transaction_tango_request(
        self,
        user_factory,
        product_amt_true,
        create_main_accounts,
        thl_lm,
        lm,
        delete_ledger_db,
    ):
        delete_ledger_db()
        create_main_accounts()

        user: User = user_factory(product=product_amt_true)

        # debit_account_uuid nothing checks they match the ledger ... todo?
        pe = UserPayoutEvent(
            uuid=uuid4().hex,
            payout_type=PayoutType.TANGO,
            amount=500,
            cashout_method_uuid=uuid4().hex,
            debit_account_uuid=uuid4().hex,
        )
        flag_key = f"test:user_payout:{pe.uuid}:request"
        flag_name = f"ledger-manager:transaction_flag:{flag_key}"
        lm.redis_client.delete(flag_name)
        thl_lm.create_tx_user_bonus(
            user,
            amount=Decimal(6),
            ref_uuid="e703830dec124f17abed2d697d8d7701",
            description="Bribe",
            skip_flag_check=True,
        )
        thl_lm.create_tx_user_payout_request(
            user, payout_event=pe, skip_flag_check=True
        )

        cash = thl_lm.get_account_cash()
        bp_wallet_account = thl_lm.get_account_or_create_bp_wallet(user.product)
        bp_pending_account = thl_lm.get_or_create_bp_pending_payout_account(
            user.product
        )
        bp_tango_expense_account = thl_lm.get_account_or_create_bp_expense(
            user.product, expense_name="tango"
        )
        user_wallet_account = thl_lm.get_account_or_create_user_wallet(user)
        assert -600 == thl_lm.get_account_balance(bp_wallet_account)
        assert 0 == thl_lm.get_account_balance(cash)
        assert 0 == thl_lm.get_account_balance(bp_tango_expense_account)
        assert 500 == thl_lm.get_account_balance(bp_pending_account)
        assert 600 - 500 == thl_lm.get_account_balance(user_wallet_account)
        assert thl_lm.check_ledger_balanced()

        thl_lm.create_tx_user_payout_complete(
            user, payout_event=pe, skip_flag_check=True
        )
        assert -600 - round(500 * 0.035) == thl_lm.get_account_balance(
            bp_wallet_account
        )
        assert -500, thl_lm.get_account_balance(cash)
        assert round(-500 * 0.035) == thl_lm.get_account_balance(
            bp_tango_expense_account
        )
        assert 0 == lm.get_account_balance(bp_pending_account)
        assert 100 == lm.get_account_balance(user_wallet_account)
        assert lm.check_ledger_balanced()


class TestLedgerManagerPaypal:

    def test_create_transaction_paypal_request(
        self,
        user_factory,
        product_amt_true,
        create_main_accounts,
        thl_lm,
        lm,
        delete_ledger_db,
    ):
        delete_ledger_db()
        create_main_accounts()

        now = datetime.now(tz=timezone.utc) - timedelta(hours=1)
        user: User = user_factory(product=product_amt_true)

        # debit_account_uuid nothing checks they match the ledger ... todo?
        pe = UserPayoutEvent(
            uuid=uuid4().hex,
            payout_type=PayoutType.PAYPAL,
            amount=500,
            cashout_method_uuid=uuid4().hex,
            debit_account_uuid=uuid4().hex,
        )
        flag_key = f"test:user_payout:{pe.uuid}:request"
        flag_name = f"ledger-manager:transaction_flag:{flag_key}"
        lm.redis_client.delete(flag_name)
        thl_lm.create_tx_user_bonus(
            user=user,
            amount=Decimal(6),
            ref_uuid="e703830dec124f17abed2d697d8d7701",
            description="Bribe",
            skip_flag_check=True,
        )

        thl_lm.create_tx_user_payout_request(
            user, payout_event=pe, skip_flag_check=True
        )

        cash = thl_lm.get_account_cash()
        bp_wallet_account = thl_lm.get_account_or_create_bp_wallet(user.product)
        bp_pending_account = thl_lm.get_or_create_bp_pending_payout_account(
            product=user.product
        )
        bp_paypal_expense_account = thl_lm.get_account_or_create_bp_expense(
            product=user.product, expense_name="paypal"
        )
        user_wallet_account = thl_lm.get_account_or_create_user_wallet(user=user)
        assert -600 == lm.get_account_balance(account=bp_wallet_account)
        assert 0 == lm.get_account_balance(account=cash)
        assert 0 == lm.get_account_balance(account=bp_paypal_expense_account)
        assert 500 == lm.get_account_balance(account=bp_pending_account)
        assert 600 - 500 == lm.get_account_balance(account=user_wallet_account)
        assert thl_lm.check_ledger_balanced()

        thl_lm.create_tx_user_payout_complete(
            user=user, payout_event=pe, skip_flag_check=True, fee_amount=Decimal("0.50")
        )
        assert -600 - 50 == thl_lm.get_account_balance(bp_wallet_account)
        assert -500 == thl_lm.get_account_balance(cash)
        assert -50 == thl_lm.get_account_balance(bp_paypal_expense_account)
        assert 0 == thl_lm.get_account_balance(bp_pending_account)
        assert 100 == thl_lm.get_account_balance(user_wallet_account)
        assert thl_lm.check_ledger_balanced()


class TestLedgerManagerBonus:

    def test_create_transaction_bonus(
        self,
        user_factory,
        product_user_wallet_yes,
        create_main_accounts,
        thl_lm,
        lm,
        delete_ledger_db,
    ):
        delete_ledger_db()
        create_main_accounts()

        user: User = user_factory(product=product_user_wallet_yes)

        thl_lm.create_tx_user_bonus(
            user=user,
            amount=Decimal(5),
            ref_uuid="8d0aaf612462448a9ebdd57fab0fc660",
            description="Bribe",
            skip_flag_check=True,
        )
        cash = thl_lm.get_account_cash()
        bp_wallet_account = thl_lm.get_account_or_create_bp_wallet(user.product)
        bp_pending_account = thl_lm.get_or_create_bp_pending_payout_account(
            product=user.product
        )
        bp_amt_expense_account = thl_lm.get_account_or_create_bp_expense(
            user.product, expense_name="amt"
        )
        user_wallet_account = thl_lm.get_account_or_create_user_wallet(user=user)

        assert -500 == lm.get_account_balance(account=bp_wallet_account)
        assert 0 == lm.get_account_balance(account=cash)
        assert 0 == lm.get_account_balance(account=bp_amt_expense_account)
        assert 0 == lm.get_account_balance(account=bp_pending_account)
        assert 500 == lm.get_account_balance(account=user_wallet_account)
        assert thl_lm.check_ledger_balanced()

        with pytest.raises(expected_exception=LedgerTransactionFlagAlreadyExistsError):
            thl_lm.create_tx_user_bonus(
                user=user,
                amount=Decimal(5),
                ref_uuid="8d0aaf612462448a9ebdd57fab0fc660",
                description="Bribe",
                skip_flag_check=False,
            )
        with pytest.raises(expected_exception=LedgerTransactionConditionFailedError):
            thl_lm.create_tx_user_bonus(
                user=user,
                amount=Decimal(5),
                ref_uuid="8d0aaf612462448a9ebdd57fab0fc660",
                description="Bribe",
                skip_flag_check=True,
            )
