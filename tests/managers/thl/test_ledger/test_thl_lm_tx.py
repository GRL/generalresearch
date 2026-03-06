import logging
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from random import randint
from uuid import uuid4

import pytest

from generalresearch.currency import USDCent
from generalresearch.managers.thl.ledger_manager.ledger import (
    LedgerTransaction,
)
from generalresearch.models import Source
from generalresearch.models.thl.definitions import (
    WALL_ALLOWED_STATUS_STATUS_CODE,
)
from generalresearch.models.thl.ledger import Direction
from generalresearch.models.thl.ledger import TransactionType
from generalresearch.models.thl.product import (
    PayoutConfig,
    PayoutTransformation,
    UserWalletConfig,
)
from generalresearch.models.thl.session import (
    Wall,
    Status,
    StatusCode1,
    Session,
    WallAdjustedStatus,
)
from generalresearch.models.thl.user import User
from generalresearch.models.thl.wallet import PayoutType
from generalresearch.models.thl.payout import UserPayoutEvent

logger = logging.getLogger("LedgerManager")


class TestThlLedgerTxManager:

    def test_create_tx_task_complete(
        self,
        wall,
        user,
        account_revenue_task_complete,
        create_main_accounts,
        thl_lm,
        lm,
    ):
        create_main_accounts()
        tx = thl_lm.create_tx_task_complete(wall=wall, user=user)
        assert isinstance(tx, LedgerTransaction)

        res = lm.get_tx_by_id(transaction_id=tx.id)
        assert res.created == tx.created

    def test_create_tx_task_complete_(
        self, wall, user, account_revenue_task_complete, thl_lm, lm
    ):
        tx = thl_lm.create_tx_task_complete_(wall=wall, user=user)
        assert isinstance(tx, LedgerTransaction)

        res = lm.get_tx_by_id(transaction_id=tx.id)
        assert res.created == tx.created

    def test_create_tx_bp_payment(
        self,
        session_factory,
        user,
        create_main_accounts,
        delete_ledger_db,
        thl_lm,
        lm,
        session_manager,
    ):
        delete_ledger_db()
        create_main_accounts()
        s1 = session_factory(user=user)

        status, status_code_1 = s1.determine_session_status()
        thl_net, commission_amount, bp_pay, user_pay = s1.determine_payments()
        session_manager.finish_with_status(
            session=s1,
            status=Status.COMPLETE,
            status_code_1=status_code_1,
            finished=datetime.now(tz=timezone.utc) + timedelta(minutes=10),
            payout=bp_pay,
            user_payout=user_pay,
        )

        tx = thl_lm.create_tx_bp_payment(session=s1)
        assert isinstance(tx, LedgerTransaction)

        res = lm.get_tx_by_id(transaction_id=tx.id)
        assert res.created == tx.created

    def test_create_tx_bp_payment_amt(
        self,
        session_factory,
        user_factory,
        product_manager,
        create_main_accounts,
        delete_ledger_db,
        thl_lm,
        lm,
        session_manager,
    ):
        delete_ledger_db()
        create_main_accounts()
        product = product_manager.create_dummy(
            payout_config=PayoutConfig(
                payout_transformation=PayoutTransformation(
                    f="payout_transformation_amt"
                )
            ),
            user_wallet_config=UserWalletConfig(amt=True, enabled=True),
        )
        user = user_factory(product=product)
        s1 = session_factory(user=user, wall_req_cpi=Decimal("1"))

        status, status_code_1 = s1.determine_session_status()
        assert status == Status.COMPLETE
        thl_net, commission_amount, bp_pay, user_pay = s1.determine_payments(
            thl_ledger_manager=thl_lm
        )
        print(thl_net, commission_amount, bp_pay, user_pay)
        session_manager.finish_with_status(
            session=s1,
            status=Status.COMPLETE,
            status_code_1=status_code_1,
            finished=datetime.now(tz=timezone.utc) + timedelta(minutes=10),
            payout=bp_pay,
            user_payout=user_pay,
        )

        tx = thl_lm.create_tx_bp_payment(session=s1)
        assert isinstance(tx, LedgerTransaction)

        res = lm.get_tx_by_id(transaction_id=tx.id)
        assert res.created == tx.created

    def test_create_tx_bp_payment_(
        self,
        session_factory,
        user,
        create_main_accounts,
        thl_lm,
        lm,
        session_manager,
        utc_hour_ago,
    ):
        s1 = session_factory(user=user)
        status, status_code_1 = s1.determine_session_status()
        thl_net, commission_amount, bp_pay, user_pay = s1.determine_payments()
        session_manager.finish_with_status(
            session=s1,
            status=status,
            status_code_1=status_code_1,
            finished=utc_hour_ago + timedelta(minutes=10),
            payout=bp_pay,
            user_payout=user_pay,
        )

        s1.determine_payments()
        tx = thl_lm.create_tx_bp_payment_(session=s1)
        assert isinstance(tx, LedgerTransaction)

        res = lm.get_tx_by_id(transaction_id=tx.id)
        assert res.created == tx.created

    def test_create_tx_task_adjustment(
        self, wall_factory, session, user, create_main_accounts, thl_lm, lm
    ):
        """Create Wall event Complete, and Create a Tx Task Adjustment

        - I don't know what this does exactly... but we can confirm
            the transaction comes back with balanced amounts, and that
            the name of the Source is in the Tx description
        """

        wall_status = Status.COMPLETE
        wall: Wall = wall_factory(session=session, wall_status=wall_status)

        tx = thl_lm.create_tx_task_adjustment(wall=wall, user=user)
        assert isinstance(tx, LedgerTransaction)
        res = lm.get_tx_by_id(transaction_id=tx.id)

        assert res.entries[0].amount == int(wall.cpi * 100)
        assert res.entries[1].amount == int(wall.cpi * 100)
        assert wall.source.name in res.ext_description
        assert res.created == tx.created

    def test_create_tx_bp_adjustment(self, session, user, caplog, thl_lm, lm):
        status, status_code_1 = session.determine_session_status()
        thl_net, commission_amount, bp_pay, user_pay = session.determine_payments()

        # The default session fixture is just an unfinished wall event
        assert len(session.wall_events) == 1
        assert session.finished is None
        assert status == Status.TIMEOUT
        assert status_code_1 in list(
            WALL_ALLOWED_STATUS_STATUS_CODE.get(Status.TIMEOUT, {})
        )
        assert thl_net == Decimal(0)
        assert commission_amount == Decimal(0)
        assert bp_pay == Decimal(0)
        assert user_pay is None

        # Update the finished timestamp, but nothing else. This means that
        #   there is no financial changes needed
        session.update(
            **{
                "finished": datetime.now(tz=timezone.utc) + timedelta(minutes=10),
            }
        )
        assert session.finished
        with caplog.at_level(logging.INFO):
            tx = thl_lm.create_tx_bp_adjustment(session=session)
            assert tx is None
        assert "No transactions needed." in caplog.text

    def test_create_tx_bp_payout(self, product, caplog, thl_lm, currency):
        rand_amount: USDCent = USDCent(randint(100, 1_000))
        payoutevent_uuid = uuid4().hex

        # Create a BP Payout for a Product without any activity. By issuing,
        #   the skip_* checks, we should be able to force it to work, and will
        #   then ultimately result in a negative balance
        tx = thl_lm.create_tx_bp_payout(
            product=product,
            amount=rand_amount,
            payoutevent_uuid=payoutevent_uuid,
            created=datetime.now(tz=timezone.utc),
            skip_wallet_balance_check=True,
            skip_one_per_day_check=True,
            skip_flag_check=True,
        )

        # Check the basic attributes
        assert isinstance(tx, LedgerTransaction)
        assert tx.ext_description == "BP Payout"
        assert (
            tx.tag
            == f"{thl_lm.currency.value}:{TransactionType.BP_PAYOUT.value}:{payoutevent_uuid}"
        )
        assert tx.entries[0].amount == rand_amount
        assert tx.entries[1].amount == rand_amount

        # Check the Product's balance, it should be negative the amount that was
        #   paid out. That's because the Product earned nothing.. and then was
        #   sent something.
        balance = thl_lm.get_account_balance(
            account=thl_lm.get_account_or_create_bp_wallet(product=product)
        )
        assert balance == int(rand_amount) * -1

        # Test some basic assertions
        with caplog.at_level(logging.INFO):
            with pytest.raises(expected_exception=Exception):
                thl_lm.create_tx_bp_payout(
                    product=product,
                    amount=rand_amount,
                    payoutevent_uuid=uuid4().hex,
                    created=datetime.now(tz=timezone.utc),
                    skip_wallet_balance_check=False,
                    skip_one_per_day_check=False,
                    skip_flag_check=False,
                )
        assert "failed condition check >1 tx per day" in caplog.text

    def test_create_tx_bp_payout_(self, product, thl_lm, lm, currency):
        rand_amount: USDCent = USDCent(randint(100, 1_000))
        payoutevent_uuid = uuid4().hex

        # Create a BP Payout for a Product without any activity.
        tx = thl_lm.create_tx_bp_payout_(
            product=product,
            amount=rand_amount,
            payoutevent_uuid=payoutevent_uuid,
            created=datetime.now(tz=timezone.utc),
        )

        # Check the basic attributes
        assert isinstance(tx, LedgerTransaction)
        assert tx.ext_description == "BP Payout"
        assert (
            tx.tag
            == f"{currency.value}:{TransactionType.BP_PAYOUT.value}:{payoutevent_uuid}"
        )
        assert tx.entries[0].amount == rand_amount
        assert tx.entries[1].amount == rand_amount

    def test_create_tx_plug_bp_wallet(
        self, product, create_main_accounts, thl_lm, lm, currency
    ):
        """A BP Wallet "plug" is a way to makeup discrepancies and simply
        add or remove money
        """
        rand_amount: USDCent = USDCent(randint(100, 1_000))

        tx = thl_lm.create_tx_plug_bp_wallet(
            product=product,
            amount=rand_amount,
            created=datetime.now(tz=timezone.utc),
            direction=Direction.DEBIT,
            skip_flag_check=False,
        )

        assert isinstance(tx, LedgerTransaction)

        # We issued the BP money they didn't earn, so now they have a
        #   negative balance
        balance = thl_lm.get_account_balance(
            account=thl_lm.get_account_or_create_bp_wallet(product=product)
        )
        assert balance == int(rand_amount) * -1

    def test_create_tx_plug_bp_wallet_(
        self, product, create_main_accounts, thl_lm, lm, currency
    ):
        """A BP Wallet "plug" is a way to fix discrepancies and simply
        add or remove money.

        Similar to above, but because it's unprotected, we can immediately
            issue another to see if the balance changes
        """
        rand_amount: USDCent = USDCent(randint(100, 1_000))

        tx = thl_lm.create_tx_plug_bp_wallet_(
            product=product,
            amount=rand_amount,
            created=datetime.now(tz=timezone.utc),
            direction=Direction.DEBIT,
        )

        assert isinstance(tx, LedgerTransaction)

        # We issued the BP money they didn't earn, so now they have a
        #   negative balance
        balance = thl_lm.get_account_balance(
            account=thl_lm.get_account_or_create_bp_wallet(product=product)
        )
        assert balance == int(rand_amount) * -1

        # Issue a positive one now, and confirm the balance goes positive
        thl_lm.create_tx_plug_bp_wallet_(
            product=product,
            amount=rand_amount + rand_amount,
            created=datetime.now(tz=timezone.utc),
            direction=Direction.CREDIT,
        )
        balance = thl_lm.get_account_balance(
            account=thl_lm.get_account_or_create_bp_wallet(product=product)
        )
        assert balance == int(rand_amount)

    def test_create_tx_user_payout_request(
        self,
        user,
        product_user_wallet_yes,
        user_factory,
        delete_df_collection,
        thl_lm,
        lm,
        currency,
    ):
        pe = UserPayoutEvent(
            uuid=uuid4().hex,
            payout_type=PayoutType.PAYPAL,
            amount=500,
            cashout_method_uuid=uuid4().hex,
            debit_account_uuid=uuid4().hex,
        )

        # The default user fixture uses a product that doesn't have wallet
        #   mode enabled
        with pytest.raises(expected_exception=AssertionError):
            thl_lm.create_tx_user_payout_request(
                user=user,
                payout_event=pe,
                skip_flag_check=True,
                skip_wallet_balance_check=True,
            )

        # Now try it for a user on a product with wallet mode
        u2 = user_factory(product=product_user_wallet_yes)

        # User's pre-balance is 0 because no activity has occurred yet
        pre_balance = lm.get_account_balance(
            account=thl_lm.get_account_or_create_user_wallet(user=u2)
        )
        assert pre_balance == 0

        tx = thl_lm.create_tx_user_payout_request(
            user=u2,
            payout_event=pe,
            skip_flag_check=True,
            skip_wallet_balance_check=True,
        )
        assert isinstance(tx, LedgerTransaction)
        assert tx.entries[0].amount == pe.amount
        assert tx.entries[1].amount == pe.amount
        assert tx.ext_description == "User Payout Paypal Request $5.00"

        #
        # (TODO): This key ":user_payout:" is NOT part of the TransactionType
        #   enum and was manually set. It should be based off the
        #   TransactionType names.
        #

        assert tx.tag == f"{currency.value}:user_payout:{pe.uuid}:request"

        # Post balance is -$5.00 because it comes out of the wallet before
        #   it's Approved or Completed
        post_balance = lm.get_account_balance(
            account=thl_lm.get_account_or_create_user_wallet(user=u2)
        )
        assert post_balance == -500

    def test_create_tx_user_payout_request_(
        self,
        user,
        product_user_wallet_yes,
        user_factory,
        delete_ledger_db,
        thl_lm,
        lm,
    ):
        delete_ledger_db()

        pe = UserPayoutEvent(
            uuid=uuid4().hex,
            payout_type=PayoutType.PAYPAL,
            amount=500,
            cashout_method_uuid=uuid4().hex,
            debit_account_uuid=uuid4().hex,
        )

        rand_description = uuid4().hex
        tx = thl_lm.create_tx_user_payout_request_(
            user=user, payout_event=pe, description=rand_description
        )

        assert tx.ext_description == rand_description

        post_balance = lm.get_account_balance(
            account=thl_lm.get_account_or_create_user_wallet(user=user)
        )
        assert post_balance == -500

    def test_create_tx_user_payout_complete(
        self,
        user_factory,
        product_user_wallet_yes,
        create_main_accounts,
        delete_ledger_db,
        thl_lm,
        lm,
        currency,
    ):
        delete_ledger_db()
        create_main_accounts()

        user: User = user_factory(product=product_user_wallet_yes)
        user_account = thl_lm.get_account_or_create_user_wallet(user=user)
        rand_amount = randint(100, 1_000)

        # Ensure the user starts out with nothing...
        assert lm.get_account_balance(account=user_account) == 0

        pe = UserPayoutEvent(
            uuid=uuid4().hex,
            payout_type=PayoutType.PAYPAL,
            amount=rand_amount,
            cashout_method_uuid=uuid4().hex,
            debit_account_uuid=uuid4().hex,
        )

        # Confirm it's not possible unless a request occurred happen
        with pytest.raises(expected_exception=ValueError):
            thl_lm.create_tx_user_payout_complete(
                user=user,
                payout_event=pe,
                fee_amount=None,
                skip_flag_check=False,
            )

        # (1) Make a request first
        thl_lm.create_tx_user_payout_request(
            user=user,
            payout_event=pe,
            skip_flag_check=True,
            skip_wallet_balance_check=True,
        )
        # Assert the balance came out of their user wallet
        assert lm.get_account_balance(account=user_account) == rand_amount * -1

        # (2) Complete the request
        tx = thl_lm.create_tx_user_payout_complete(
            user=user,
            payout_event=pe,
            fee_amount=Decimal(0),
            skip_flag_check=False,
        )
        assert tx.entries[0].amount == rand_amount
        assert tx.entries[1].amount == rand_amount
        assert tx.tag == f"{currency.value}:user_payout:{pe.uuid}:complete"
        assert isinstance(tx, LedgerTransaction)

        # The amount that comes out of the user wallet doesn't change after
        #   it's approved becuase it's already been withdrawn
        assert lm.get_account_balance(account=user_account) == rand_amount * -1

    def test_create_tx_user_payout_complete_(
        self,
        user_factory,
        product_user_wallet_yes,
        create_main_accounts,
        thl_lm,
        lm,
    ):
        user: User = user_factory(product=product_user_wallet_yes)
        user_account = thl_lm.get_account_or_create_user_wallet(user=user)
        rand_amount = randint(100, 1_000)

        pe = UserPayoutEvent(
            uuid=uuid4().hex,
            payout_type=PayoutType.PAYPAL,
            amount=rand_amount,
            cashout_method_uuid=uuid4().hex,
            debit_account_uuid=uuid4().hex,
        )

        # (1) Make a request first
        thl_lm.create_tx_user_payout_request(
            user=user,
            payout_event=pe,
            skip_flag_check=True,
            skip_wallet_balance_check=True,
        )

        # (2) Complete the request
        rand_desc = uuid4().hex

        bp_expense_account = thl_lm.get_account_or_create_bp_expense(
            product=user.product, expense_name="paypal"
        )
        bp_wallet_account = thl_lm.get_account_or_create_bp_wallet(product=user.product)

        tx = thl_lm.create_tx_user_payout_complete_(
            user=user,
            payout_event=pe,
            fee_amount=Decimal("0.00"),
            fee_expense_account=bp_expense_account,
            fee_payer_account=bp_wallet_account,
            description=rand_desc,
        )
        assert tx.ext_description == rand_desc
        assert lm.get_account_balance(account=user_account) == rand_amount * -1

    def test_create_tx_user_payout_cancelled(
        self,
        user_factory,
        product_user_wallet_yes,
        create_main_accounts,
        thl_lm,
        lm,
        currency,
    ):
        user: User = user_factory(product=product_user_wallet_yes)
        user_account = thl_lm.get_account_or_create_user_wallet(user=user)
        rand_amount = randint(100, 1_000)

        pe = UserPayoutEvent(
            uuid=uuid4().hex,
            payout_type=PayoutType.PAYPAL,
            amount=rand_amount,
            cashout_method_uuid=uuid4().hex,
            debit_account_uuid=uuid4().hex,
        )

        # (1) Make a request first
        thl_lm.create_tx_user_payout_request(
            user=user,
            payout_event=pe,
            skip_flag_check=True,
            skip_wallet_balance_check=True,
        )
        # Assert the balance came out of their user wallet
        assert lm.get_account_balance(account=user_account) == rand_amount * -1

        # (2) Cancel the request
        tx = thl_lm.create_tx_user_payout_cancelled(
            user=user,
            payout_event=pe,
            skip_flag_check=False,
        )
        assert tx.entries[0].amount == rand_amount
        assert tx.entries[1].amount == rand_amount
        assert tx.tag == f"{currency.value}:user_payout:{pe.uuid}:cancel"
        assert isinstance(tx, LedgerTransaction)

        # Assert the balance comes back to 0 after it was cancelled
        assert lm.get_account_balance(account=user_account) == 0

    def test_create_tx_user_payout_cancelled_(
        self,
        user_factory,
        product_user_wallet_yes,
        create_main_accounts,
        thl_lm,
        lm,
        currency,
    ):
        user: User = user_factory(product=product_user_wallet_yes)
        user_account = thl_lm.get_account_or_create_user_wallet(user=user)
        rand_amount = randint(100, 1_000)

        pe = UserPayoutEvent(
            uuid=uuid4().hex,
            payout_type=PayoutType.PAYPAL,
            amount=rand_amount,
            cashout_method_uuid=uuid4().hex,
            debit_account_uuid=uuid4().hex,
        )

        # (1) Make a request first
        thl_lm.create_tx_user_payout_request(
            user=user,
            payout_event=pe,
            skip_flag_check=True,
            skip_wallet_balance_check=True,
        )
        # Assert the balance came out of their user wallet
        assert lm.get_account_balance(account=user_account) == rand_amount * -1

        # (2) Cancel the request
        rand_desc = uuid4().hex
        tx = thl_lm.create_tx_user_payout_cancelled_(
            user=user, payout_event=pe, description=rand_desc
        )
        assert isinstance(tx, LedgerTransaction)
        assert tx.ext_description == rand_desc
        assert lm.get_account_balance(account=user_account) == 0

    def test_create_tx_user_bonus(
        self,
        user_factory,
        product_user_wallet_yes,
        create_main_accounts,
        thl_lm,
        lm,
        currency,
    ):
        user: User = user_factory(product=product_user_wallet_yes)
        user_account = thl_lm.get_account_or_create_user_wallet(user=user)
        rand_amount = randint(100, 1_000)
        rand_ref_uuid = uuid4().hex
        rand_desc = uuid4().hex

        # Assert the balance came out of their user wallet
        assert lm.get_account_balance(account=user_account) == 0

        tx = thl_lm.create_tx_user_bonus(
            user=user,
            amount=Decimal(rand_amount / 100),
            ref_uuid=rand_ref_uuid,
            description=rand_desc,
            skip_flag_check=True,
        )
        assert tx.ext_description == rand_desc
        assert tx.tag == f"{thl_lm.currency.value}:user_bonus:{rand_ref_uuid}"
        assert tx.entries[0].amount == rand_amount
        assert tx.entries[1].amount == rand_amount

        # Assert the balance came out of their user wallet
        assert lm.get_account_balance(account=user_account) == rand_amount

    def test_create_tx_user_bonus_(
        self,
        user_factory,
        product_user_wallet_yes,
        create_main_accounts,
        thl_lm,
        lm,
        currency,
    ):
        user: User = user_factory(product=product_user_wallet_yes)
        user_account = thl_lm.get_account_or_create_user_wallet(user=user)
        rand_amount = randint(100, 1_000)
        rand_ref_uuid = uuid4().hex
        rand_desc = uuid4().hex

        # Assert the balance came out of their user wallet
        assert lm.get_account_balance(account=user_account) == 0

        tx = thl_lm.create_tx_user_bonus_(
            user=user,
            amount=Decimal(rand_amount / 100),
            ref_uuid=rand_ref_uuid,
            description=rand_desc,
        )
        assert tx.ext_description == rand_desc
        assert tx.tag == f"{thl_lm.currency.value}:user_bonus:{rand_ref_uuid}"
        assert tx.entries[0].amount == rand_amount
        assert tx.entries[1].amount == rand_amount

        # Assert the balance came out of their user wallet
        assert lm.get_account_balance(account=user_account) == rand_amount


class TestThlLedgerTxManagerFlows:
    """Combine the various THL_LM methods to create actual "real world"
    examples
    """

    def test_create_tx_task_complete(
        self, user, create_main_accounts, thl_lm, lm, currency, delete_ledger_db
    ):
        delete_ledger_db()
        create_main_accounts()

        wall1 = Wall(
            user_id=1,
            source=Source.DYNATA,
            req_survey_id="xxx",
            req_cpi=Decimal("1.23"),
            session_id=1,
            status=Status.COMPLETE,
            status_code_1=StatusCode1.COMPLETE,
            started=datetime.now(timezone.utc),
            finished=datetime.now(timezone.utc) + timedelta(seconds=1),
        )
        thl_lm.create_tx_task_complete(wall=wall1, user=user, created=wall1.started)

        wall2 = Wall(
            user_id=1,
            source=Source.FULL_CIRCLE,
            req_survey_id="yyy",
            req_cpi=Decimal("3.21"),
            session_id=1,
            status=Status.COMPLETE,
            status_code_1=StatusCode1.COMPLETE,
            started=datetime.now(timezone.utc),
            finished=datetime.now(timezone.utc) + timedelta(seconds=1),
        )
        thl_lm.create_tx_task_complete(wall=wall2, user=user, created=wall2.started)

        cash = thl_lm.get_account_cash()
        revenue = thl_lm.get_account_task_complete_revenue()

        assert lm.get_account_balance(cash) == 123 + 321
        assert lm.get_account_balance(revenue) == 123 + 321
        assert lm.check_ledger_balanced()

        assert (
            lm.get_account_filtered_balance(
                account=revenue, metadata_key="source", metadata_value="d"
            )
            == 123
        )

        assert (
            lm.get_account_filtered_balance(
                account=revenue, metadata_key="source", metadata_value="f"
            )
            == 321
        )

        assert (
            lm.get_account_filtered_balance(
                account=revenue, metadata_key="source", metadata_value="x"
            )
            == 0
        )

        assert (
            thl_lm.get_account_filtered_balance(
                account=revenue,
                metadata_key="thl_wall",
                metadata_value=wall1.uuid,
            )
            == 123
        )

    def test_create_transaction_task_complete_1_cent(
        self, user, create_main_accounts, thl_lm, lm, currency
    ):
        wall1 = Wall(
            user_id=1,
            source=Source.DYNATA,
            req_survey_id="xxx",
            req_cpi=Decimal("0.007"),
            session_id=1,
            status=Status.COMPLETE,
            status_code_1=StatusCode1.COMPLETE,
            started=datetime.now(timezone.utc),
            finished=datetime.now(timezone.utc) + timedelta(seconds=1),
        )
        tx = thl_lm.create_tx_task_complete(
            wall=wall1, user=user, created=wall1.started
        )

        assert isinstance(tx, LedgerTransaction)

    def test_create_transaction_bp_payment(
        self,
        user,
        create_main_accounts,
        thl_lm,
        lm,
        currency,
        delete_ledger_db,
        session_factory,
        utc_hour_ago,
    ):
        delete_ledger_db()
        create_main_accounts()

        s1: Session = session_factory(
            user=user,
            wall_count=1,
            started=utc_hour_ago,
            wall_source=Source.TESTING,
        )
        w1: Wall = s1.wall_events[0]

        tx = thl_lm.create_tx_task_complete(wall=w1, user=user, created=w1.started)
        assert isinstance(tx, LedgerTransaction)

        status, status_code_1 = s1.determine_session_status()
        thl_net, commission_amount, bp_pay, user_pay = s1.determine_payments()
        s1.update(
            **{
                "status": status,
                "status_code_1": status_code_1,
                "finished": s1.started + timedelta(minutes=10),
                "payout": bp_pay,
                "user_payout": user_pay,
            }
        )
        print(thl_net, commission_amount, bp_pay, user_pay)
        thl_lm.create_tx_bp_payment(session=s1, created=w1.started)

        revenue = thl_lm.get_account_task_complete_revenue()
        bp_wallet = thl_lm.get_account_or_create_bp_wallet(product=user.product)
        bp_commission = thl_lm.get_account_or_create_bp_commission(product=user.product)

        assert 0 == lm.get_account_balance(account=revenue)
        assert 50 == lm.get_account_filtered_balance(
            account=revenue,
            metadata_key="source",
            metadata_value=Source.TESTING,
        )
        assert 48 == lm.get_account_balance(account=bp_wallet)
        assert 48 == lm.get_account_filtered_balance(
            account=bp_wallet,
            metadata_key="thl_session",
            metadata_value=s1.uuid,
        )
        assert 2 == thl_lm.get_account_balance(account=bp_commission)
        assert thl_lm.check_ledger_balanced()

    def test_create_transaction_bp_payment_round(
        self,
        user_factory,
        product_user_wallet_no,
        create_main_accounts,
        thl_lm,
        lm,
        currency,
    ):
        product_user_wallet_no.commission_pct = Decimal("0.085")
        user: User = user_factory(product=product_user_wallet_no)

        wall1 = Wall(
            user_id=user.user_id,
            source=Source.SAGO,
            req_survey_id="xxx",
            req_cpi=Decimal("0.287"),
            session_id=3,
            status=Status.COMPLETE,
            status_code_1=StatusCode1.COMPLETE,
            started=datetime.now(timezone.utc),
            finished=datetime.now(timezone.utc) + timedelta(seconds=1),
        )

        tx = thl_lm.create_tx_task_complete(
            wall=wall1, user=user, created=wall1.started
        )
        assert isinstance(tx, LedgerTransaction)

        session = Session(started=wall1.started, user=user, wall_events=[wall1])
        status, status_code_1 = session.determine_session_status()
        thl_net, commission_amount, bp_pay, user_pay = session.determine_payments()
        session.update(
            **{
                "status": status,
                "status_code_1": status_code_1,
                "finished": session.started + timedelta(minutes=10),
                "payout": bp_pay,
                "user_payout": user_pay,
            }
        )

        print(thl_net, commission_amount, bp_pay, user_pay)
        tx = thl_lm.create_tx_bp_payment(session=session, created=wall1.started)
        assert isinstance(tx, LedgerTransaction)

    def test_create_transaction_bp_payment_round2(
        self, delete_ledger_db, user, create_main_accounts, thl_lm, lm, currency
    ):
        delete_ledger_db()
        create_main_accounts()
        # user must be no user wallet
        # e.g. session 869b5bfa47f44b4f81cd095ed01df2ff this fails if you dont round properly

        wall1 = Wall(
            user_id=user.user_id,
            source=Source.SAGO,
            req_survey_id="xxx",
            req_cpi=Decimal("1.64500"),
            session_id=3,
            status=Status.COMPLETE,
            status_code_1=StatusCode1.COMPLETE,
            started=datetime.now(timezone.utc),
            finished=datetime.now(timezone.utc) + timedelta(seconds=1),
        )

        thl_lm.create_tx_task_complete(wall=wall1, user=user, created=wall1.started)
        session = Session(started=wall1.started, user=user, wall_events=[wall1])
        status, status_code_1 = session.determine_session_status()
        # thl_net, commission_amount, bp_pay, user_pay = session.determine_payments()
        session.update(
            **{
                "status": status,
                "status_code_1": status_code_1,
                "finished": session.started + timedelta(minutes=10),
                "payout": Decimal("1.53"),
                "user_payout": Decimal("1.53"),
            }
        )

        thl_lm.create_tx_bp_payment(session=session, created=wall1.started)

    def test_create_transaction_bp_payment_round3(
        self,
        user_factory,
        product_user_wallet_yes,
        create_main_accounts,
        thl_lm,
        lm,
        currency,
    ):
        # e.g. session ___ fails b/c we rounded incorrectly
        #   before, and now we are off by a penny...
        user: User = user_factory(product=product_user_wallet_yes)

        wall1 = Wall(
            user_id=user.user_id,
            source=Source.SAGO,
            req_survey_id="xxx",
            req_cpi=Decimal("0.385"),
            session_id=3,
            status=Status.COMPLETE,
            status_code_1=StatusCode1.COMPLETE,
            started=datetime.now(timezone.utc),
            finished=datetime.now(timezone.utc) + timedelta(seconds=1),
        )
        thl_lm.create_tx_task_complete(wall=wall1, user=user, created=wall1.started)

        session = Session(started=wall1.started, user=user, wall_events=[wall1])
        status, status_code_1 = session.determine_session_status()
        # thl_net, commission_amount, bp_pay, user_pay = session.determine_payments()
        session.update(
            **{
                "status": status,
                "status_code_1": status_code_1,
                "finished": session.started + timedelta(minutes=10),
                "payout": Decimal("0.39"),
                "user_payout": Decimal("0.26"),
            }
        )
        # with pytest.logs(logger, level=logging.WARNING) as cm:
        #     tx = thl_lm.create_transaction_bp_payment(session, created=wall1.started)
        # assert "Capping bp_pay to thl_net" in cm.output[0]

    def test_create_transaction_bp_payment_user_wallet(
        self,
        user_factory,
        product_user_wallet_yes,
        create_main_accounts,
        delete_ledger_db,
        thl_lm,
        session_manager,
        wall_manager,
        lm,
        session_factory,
        currency,
        utc_hour_ago,
    ):
        delete_ledger_db()
        create_main_accounts()

        user: User = user_factory(product=product_user_wallet_yes)
        assert user.product.user_wallet_enabled

        s1: Session = session_factory(
            user=user,
            wall_count=1,
            started=utc_hour_ago,
            wall_req_cpi=Decimal(".50"),
            wall_source=Source.TESTING,
        )
        w1: Wall = s1.wall_events[0]

        thl_lm.create_tx_task_complete(wall=w1, user=user, created=w1.started)

        status, status_code_1 = s1.determine_session_status()
        thl_net, commission_amount, bp_pay, user_pay = s1.determine_payments()
        session_manager.finish_with_status(
            session=s1,
            status=status,
            status_code_1=status_code_1,
            finished=s1.started + timedelta(minutes=10),
            payout=bp_pay,
            user_payout=user_pay,
        )
        thl_lm.create_tx_bp_payment(session=s1, created=w1.started)

        revenue = thl_lm.get_account_task_complete_revenue()
        bp_wallet = thl_lm.get_account_or_create_bp_wallet(product=user.product)
        bp_commission = thl_lm.get_account_or_create_bp_commission(product=user.product)
        user_wallet = thl_lm.get_account_or_create_user_wallet(user=user)

        assert 0 == thl_lm.get_account_balance(account=revenue)
        assert 50 == thl_lm.get_account_filtered_balance(
            account=revenue,
            metadata_key="source",
            metadata_value=Source.TESTING,
        )

        assert 48 - 19 == thl_lm.get_account_balance(account=bp_wallet)
        assert 48 - 19 == thl_lm.get_account_filtered_balance(
            account=bp_wallet,
            metadata_key="thl_session",
            metadata_value=s1.uuid,
        )
        assert 2 == thl_lm.get_account_balance(bp_commission)
        assert 19 == thl_lm.get_account_balance(user_wallet)
        assert 19 == thl_lm.get_account_filtered_balance(
            account=user_wallet,
            metadata_key="thl_session",
            metadata_value=s1.uuid,
        )

        assert 0 == thl_lm.get_account_filtered_balance(
            account=user_wallet, metadata_key="thl_session", metadata_value="x"
        )
        assert thl_lm.check_ledger_balanced()


class TestThlLedgerManagerAdj:

    def test_create_tx_task_adjustment(
        self,
        user_factory,
        product_user_wallet_no,
        create_main_accounts,
        delete_ledger_db,
        thl_lm,
        lm,
        utc_hour_ago,
        currency,
    ):
        delete_ledger_db()
        create_main_accounts()

        user: User = user_factory(product=product_user_wallet_no)

        wall1 = Wall(
            user_id=user.user_id,
            source=Source.DYNATA,
            req_survey_id="xxx",
            req_cpi=Decimal("1.23"),
            session_id=1,
            status=Status.COMPLETE,
            status_code_1=StatusCode1.COMPLETE,
            started=utc_hour_ago,
            finished=utc_hour_ago + timedelta(seconds=1),
        )

        thl_lm.create_tx_task_complete(wall1, user, created=wall1.started)

        wall2 = Wall(
            user_id=1,
            source=Source.FULL_CIRCLE,
            req_survey_id="yyy",
            req_cpi=Decimal("3.21"),
            session_id=1,
            status=Status.COMPLETE,
            status_code_1=StatusCode1.COMPLETE,
            started=utc_hour_ago,
            finished=utc_hour_ago + timedelta(seconds=1),
        )
        thl_lm.create_tx_task_complete(wall2, user, created=wall2.started)

        wall1.update(
            adjusted_status=WallAdjustedStatus.ADJUSTED_TO_FAIL,
            adjusted_cpi=0,
            adjusted_timestamp=utc_hour_ago + timedelta(hours=1),
        )
        print(wall1.get_cpi_after_adjustment())
        thl_lm.create_tx_task_adjustment(wall1, user)

        cash = thl_lm.get_account_cash()
        revenue = thl_lm.get_account_task_complete_revenue()

        assert 123 + 321 - 123 == thl_lm.get_account_balance(account=cash)
        assert 123 + 321 - 123 == thl_lm.get_account_balance(account=revenue)
        assert thl_lm.check_ledger_balanced()
        assert 0 == thl_lm.get_account_filtered_balance(
            revenue, metadata_key="source", metadata_value="d"
        )
        assert 321 == thl_lm.get_account_filtered_balance(
            revenue, metadata_key="source", metadata_value="f"
        )
        assert 0 == thl_lm.get_account_filtered_balance(
            revenue, metadata_key="source", metadata_value="x"
        )
        assert 123 - 123 == thl_lm.get_account_filtered_balance(
            account=revenue, metadata_key="thl_wall", metadata_value=wall1.uuid
        )

        # un-reconcile it
        wall1.update(
            adjusted_status=None,
            adjusted_cpi=None,
            adjusted_timestamp=utc_hour_ago + timedelta(minutes=45),
        )
        print(wall1.get_cpi_after_adjustment())
        thl_lm.create_tx_task_adjustment(wall1, user)
        # and then run it again to make sure it does nothing
        thl_lm.create_tx_task_adjustment(wall1, user)

        cash = thl_lm.get_account_cash()
        revenue = thl_lm.get_account_task_complete_revenue()

        assert 123 + 321 - 123 + 123 == thl_lm.get_account_balance(cash)
        assert 123 + 321 - 123 + 123 == thl_lm.get_account_balance(revenue)
        assert thl_lm.check_ledger_balanced()
        assert 123 == thl_lm.get_account_filtered_balance(
            account=revenue, metadata_key="source", metadata_value="d"
        )
        assert 321 == thl_lm.get_account_filtered_balance(
            account=revenue, metadata_key="source", metadata_value="f"
        )
        assert 0 == thl_lm.get_account_filtered_balance(
            account=revenue, metadata_key="source", metadata_value="x"
        )
        assert 123 - 123 + 123 == thl_lm.get_account_filtered_balance(
            account=revenue, metadata_key="thl_wall", metadata_value=wall1.uuid
        )

    def test_create_tx_bp_adjustment(
        self,
        user,
        product_user_wallet_no,
        create_main_accounts,
        caplog,
        thl_lm,
        lm,
        currency,
        session_manager,
        wall_manager,
        session_factory,
        utc_hour_ago,
        delete_ledger_db,
    ):
        delete_ledger_db()
        create_main_accounts()

        s1 = session_factory(
            user=user,
            wall_count=2,
            wall_req_cpis=[Decimal(1), Decimal(3)],
            wall_statuses=[Status.COMPLETE, Status.COMPLETE],
            started=utc_hour_ago,
        )

        w1: Wall = s1.wall_events[0]
        w2: Wall = s1.wall_events[1]

        thl_lm.create_tx_task_complete(wall=w1, user=user, created=w1.started)
        thl_lm.create_tx_task_complete(wall=w2, user=user, created=w2.started)

        status, status_code_1 = s1.determine_session_status()
        thl_net, commission_amount, bp_pay, user_pay = s1.determine_payments()
        session_manager.finish_with_status(
            session=s1,
            status=status,
            status_code_1=status_code_1,
            finished=utc_hour_ago + timedelta(minutes=10),
            payout=bp_pay,
            user_payout=user_pay,
        )
        thl_lm.create_tx_bp_payment(session=s1, created=w1.started)
        revenue = thl_lm.get_account_task_complete_revenue()
        bp_wallet_account = thl_lm.get_account_or_create_bp_wallet(product=user.product)
        bp_commission_account = thl_lm.get_account_or_create_bp_commission(
            product=user.product
        )
        assert 380 == thl_lm.get_account_balance(account=bp_wallet_account)
        assert 0 == thl_lm.get_account_balance(account=revenue)
        assert 20 == thl_lm.get_account_balance(account=bp_commission_account)
        thl_lm.check_ledger_balanced()

        # This should do nothing (since we haven't adjusted any wall events)
        s1.adjust_status()
        with caplog.at_level(logging.INFO):
            thl_lm.create_tx_bp_adjustment(session=s1)

        assert (
            "create_transaction_bp_adjustment. No transactions needed." in caplog.text
        )

        # self.assertEqual(380, ledger_manager.get_account_balance(bp_wallet_account))
        # self.assertEqual(0, ledger_manager.get_account_balance(revenue))
        # self.assertEqual(20, ledger_manager.get_account_balance(bp_commission_account))
        # self.assertTrue(ledger_manager.check_ledger_balanced())

        # recon $1 survey.
        wall_manager.adjust_status(
            wall=w1,
            adjusted_status=WallAdjustedStatus.ADJUSTED_TO_FAIL,
            adjusted_cpi=Decimal(0),
            adjusted_timestamp=utc_hour_ago + timedelta(hours=1),
        )
        thl_lm.create_tx_task_adjustment(wall=w1, user=user)
        # -$1.00 b/c the MP took the $1 back, but we haven't yet taken the BP payment back
        assert -100 == thl_lm.get_account_balance(revenue)
        s1.adjust_status()
        thl_lm.create_tx_bp_adjustment(session=s1)

        with caplog.at_level(logging.INFO):
            thl_lm.create_tx_bp_adjustment(session=s1)
        assert (
            "create_transaction_bp_adjustment. No transactions needed." in caplog.text
        )

        assert 380 - 95 == thl_lm.get_account_balance(bp_wallet_account)
        assert 0 == thl_lm.get_account_balance(revenue)
        assert 20 - 5 == thl_lm.get_account_balance(bp_commission_account)
        assert thl_lm.check_ledger_balanced()

        # unrecon the $1 survey
        wall_manager.adjust_status(
            wall=w1,
            adjusted_status=None,
            adjusted_cpi=None,
            adjusted_timestamp=utc_hour_ago + timedelta(minutes=45),
        )
        thl_lm.create_tx_task_adjustment(
            wall=w1,
            user=user,
            created=utc_hour_ago + timedelta(minutes=45),
        )
        new_status, new_payout, new_user_payout = s1.determine_new_status_and_payouts()
        s1.adjust_status()
        thl_lm.create_tx_bp_adjustment(session=s1)
        assert 380 == thl_lm.get_account_balance(bp_wallet_account)
        assert 0 == thl_lm.get_account_balance(revenue)
        assert 20, thl_lm.get_account_balance(bp_commission_account)
        assert thl_lm.check_ledger_balanced()

    def test_create_tx_bp_adjustment_small(
        self,
        user_factory,
        product_user_wallet_no,
        create_main_accounts,
        delete_ledger_db,
        thl_lm,
        lm,
        utc_hour_ago,
        currency,
    ):
        delete_ledger_db()
        create_main_accounts()

        # This failed when I didn't check that `change_commission` > 0 in
        #   create_transaction_bp_adjustment
        user: User = user_factory(product=product_user_wallet_no)

        wall1 = Wall(
            user_id=user.user_id,
            source=Source.DYNATA,
            req_survey_id="xxx",
            req_cpi=Decimal("0.10"),
            session_id=1,
            status=Status.COMPLETE,
            status_code_1=StatusCode1.COMPLETE,
            started=utc_hour_ago,
            finished=utc_hour_ago + timedelta(seconds=1),
        )

        tx = thl_lm.create_tx_task_complete(
            wall=wall1, user=user, created=wall1.started
        )
        assert isinstance(tx, LedgerTransaction)

        session = Session(started=wall1.started, user=user, wall_events=[wall1])
        status, status_code_1 = session.determine_session_status()
        thl_net, commission_amount, bp_pay, user_pay = session.determine_payments()
        session.update(
            **{
                "status": status,
                "status_code_1": status_code_1,
                "finished": utc_hour_ago + timedelta(minutes=10),
                "payout": bp_pay,
                "user_payout": user_pay,
            }
        )
        thl_lm.create_tx_bp_payment(session, created=wall1.started)

        wall1.update(
            adjusted_status=WallAdjustedStatus.ADJUSTED_TO_FAIL,
            adjusted_cpi=0,
            adjusted_timestamp=utc_hour_ago + timedelta(hours=1),
        )
        thl_lm.create_tx_task_adjustment(wall1, user)
        session.adjust_status()
        thl_lm.create_tx_bp_adjustment(session)

    def test_create_tx_bp_adjustment_abandon(
        self,
        user_factory,
        product_user_wallet_no,
        delete_ledger_db,
        session_factory,
        create_main_accounts,
        caplog,
        thl_lm,
        lm,
        currency,
        utc_hour_ago,
        session_manager,
        wall_manager,
    ):
        delete_ledger_db()
        create_main_accounts()
        user: User = user_factory(product=product_user_wallet_no)
        s1: Session = session_factory(
            user=user, final_status=Status.ABANDON, wall_req_cpi=Decimal(1)
        )
        w1 = s1.wall_events[-1]

        # Adjust to complete.
        wall_manager.adjust_status(
            wall=w1,
            adjusted_status=WallAdjustedStatus.ADJUSTED_TO_COMPLETE,
            adjusted_cpi=w1.cpi,
            adjusted_timestamp=utc_hour_ago + timedelta(hours=1),
        )
        thl_lm.create_tx_task_adjustment(wall=w1, user=user)
        s1.adjust_status()
        thl_lm.create_tx_bp_adjustment(session=s1)
        # And then adjust it back (it was abandon before, but now it should be
        # fail (?) or back to abandon?)
        wall_manager.adjust_status(
            wall=w1,
            adjusted_status=None,
            adjusted_cpi=None,
            adjusted_timestamp=utc_hour_ago + timedelta(hours=1),
        )
        thl_lm.create_tx_task_adjustment(wall=w1, user=user)
        s1.adjust_status()
        thl_lm.create_tx_bp_adjustment(session=s1)

        revenue = thl_lm.get_account_task_complete_revenue()
        bp_wallet_account = thl_lm.get_account_or_create_bp_wallet(product=user.product)
        bp_commission_account = thl_lm.get_account_or_create_bp_commission(
            product=user.product
        )
        assert 0 == thl_lm.get_account_balance(bp_wallet_account)
        assert 0 == thl_lm.get_account_balance(revenue)
        assert 0 == thl_lm.get_account_balance(bp_commission_account)
        assert thl_lm.check_ledger_balanced()

        # This should do nothing
        s1.adjust_status()
        with caplog.at_level(logging.INFO):
            thl_lm.create_tx_bp_adjustment(session=s1)
        assert "No transactions needed" in caplog.text

        # Now back to complete again
        wall_manager.adjust_status(
            wall=w1,
            adjusted_status=WallAdjustedStatus.ADJUSTED_TO_COMPLETE,
            adjusted_cpi=w1.cpi,
            adjusted_timestamp=utc_hour_ago + timedelta(hours=1),
        )
        s1.adjust_status()
        thl_lm.create_tx_bp_adjustment(session=s1)
        assert 95 == thl_lm.get_account_balance(bp_wallet_account)

    def test_create_tx_bp_adjustment_user_wallet(
        self,
        user_factory,
        product_user_wallet_yes,
        create_main_accounts,
        delete_ledger_db,
        caplog,
        thl_lm,
        lm,
        currency,
    ):
        delete_ledger_db()
        create_main_accounts()

        now = datetime.now(timezone.utc) - timedelta(days=1)
        user: User = user_factory(product=product_user_wallet_yes)

        # Create 2 Wall completes and create the respective transaction for
        #   them. We then create a 3rd wall event which is a failure but we
        #   do NOT create a transaction for it

        wall3 = Wall(
            user_id=8,
            source=Source.CINT,
            req_survey_id="zzz",
            req_cpi=Decimal("2.00"),
            session_id=1,
            status=Status.FAIL,
            status_code_1=StatusCode1.BUYER_FAIL,
            started=now,
            finished=now + timedelta(minutes=1),
        )

        now_w1 = now + timedelta(minutes=1, milliseconds=1)
        wall1 = Wall(
            user_id=user.user_id,
            source=Source.DYNATA,
            req_survey_id="xxx",
            req_cpi=Decimal("1.00"),
            session_id=1,
            status=Status.COMPLETE,
            status_code_1=StatusCode1.COMPLETE,
            started=now_w1,
            finished=now_w1 + timedelta(minutes=1),
        )
        tx = thl_lm.create_tx_task_complete(
            wall=wall1, user=user, created=wall1.started
        )
        assert isinstance(tx, LedgerTransaction)

        now_w2 = now + timedelta(minutes=2, milliseconds=1)
        wall2 = Wall(
            user_id=user.user_id,
            source=Source.FULL_CIRCLE,
            req_survey_id="yyy",
            req_cpi=Decimal("3.00"),
            session_id=1,
            status=Status.COMPLETE,
            status_code_1=StatusCode1.COMPLETE,
            started=now_w2,
            finished=now_w2 + timedelta(minutes=1),
        )
        tx = thl_lm.create_tx_task_complete(
            wall=wall2, user=user, created=wall2.started
        )
        assert isinstance(tx, LedgerTransaction)

        # It doesn't matter what order these wall events go in as because
        #   we have a pydantic valiator that sorts them
        wall_events = [wall3, wall1, wall2]
        # shuffle(wall_events)
        session = Session(started=wall1.started, user=user, wall_events=wall_events)
        status, status_code_1 = session.determine_session_status()
        assert status == Status.COMPLETE
        assert status_code_1 == StatusCode1.COMPLETE

        thl_net, commission_amount, bp_pay, user_pay = session.determine_payments()
        assert thl_net == Decimal("4.00")
        assert commission_amount == Decimal("0.20")
        assert bp_pay == Decimal("3.80")
        assert user_pay == Decimal("1.52")

        session.update(
            **{
                "status": status,
                "status_code_1": status_code_1,
                "finished": now + timedelta(minutes=10),
                "payout": bp_pay,
                "user_payout": user_pay,
            }
        )

        tx = thl_lm.create_tx_bp_adjustment(session=session, created=wall1.started)
        assert isinstance(tx, LedgerTransaction)

        bp_wallet_account = thl_lm.get_account_or_create_bp_wallet(product=user.product)
        assert 228 == thl_lm.get_account_balance(account=bp_wallet_account)

        user_account = thl_lm.get_account_or_create_user_wallet(user=user)
        assert 152 == thl_lm.get_account_balance(account=user_account)

        revenue = thl_lm.get_account_task_complete_revenue()
        assert 0 == thl_lm.get_account_balance(account=revenue)

        bp_commission_account = thl_lm.get_account_or_create_bp_commission(
            product=user.product
        )
        assert 20 == thl_lm.get_account_balance(account=bp_commission_account)

        # the total (4.00) = 2.28 + 1.52 + .20
        assert thl_lm.check_ledger_balanced()

        # This should do nothing (since we haven't adjusted any wall events)
        session.adjust_status()
        print(
            session.get_status_after_adjustment(),
            session.get_payout_after_adjustment(),
            session.get_user_payout_after_adjustment(),
        )
        with caplog.at_level(logging.INFO):
            thl_lm.create_tx_bp_adjustment(session)
        assert (
            "create_transaction_bp_adjustment. No transactions needed." in caplog.text
        )

        # recon $1 survey.
        wall1.update(
            adjusted_status=WallAdjustedStatus.ADJUSTED_TO_FAIL,
            adjusted_cpi=0,
            adjusted_timestamp=now + timedelta(hours=1),
        )
        thl_lm.create_tx_task_adjustment(wall1, user)
        # -$1.00 b/c the MP took the $1 back, but we haven't yet taken the BP payment back
        assert -100 == thl_lm.get_account_balance(revenue)
        session.adjust_status()
        print(
            session.get_status_after_adjustment(),
            session.get_payout_after_adjustment(),
            session.get_user_payout_after_adjustment(),
        )
        thl_lm.create_tx_bp_adjustment(session)

        # running this twice b/c it should do nothing the 2nd time
        print(
            session.get_status_after_adjustment(),
            session.get_payout_after_adjustment(),
            session.get_user_payout_after_adjustment(),
        )
        with caplog.at_level(logging.INFO):
            thl_lm.create_tx_bp_adjustment(session)
        assert (
            "create_transaction_bp_adjustment. No transactions needed." in caplog.text
        )

        assert 228 - 57 == thl_lm.get_account_balance(bp_wallet_account)
        assert 152 - 38 == thl_lm.get_account_balance(user_account)
        assert 0 == thl_lm.get_account_balance(revenue)
        assert 20 - 5 == thl_lm.get_account_balance(bp_commission_account)
        assert thl_lm.check_ledger_balanced()

        # unrecon the $1 survey
        wall1.update(
            adjusted_status=None,
            adjusted_cpi=None,
            adjusted_timestamp=now + timedelta(hours=2),
        )
        tx = thl_lm.create_tx_task_adjustment(wall=wall1, user=user)
        assert isinstance(tx, LedgerTransaction)

        new_status, new_payout, new_user_payout = (
            session.determine_new_status_and_payouts()
        )
        print(new_status, new_payout, new_user_payout, session.adjusted_payout)
        session.adjust_status()
        print(
            session.get_status_after_adjustment(),
            session.get_payout_after_adjustment(),
            session.get_user_payout_after_adjustment(),
        )
        thl_lm.create_tx_bp_adjustment(session)

        assert 228 - 57 + 57 == thl_lm.get_account_balance(bp_wallet_account)
        assert 152 - 38 + 38 == thl_lm.get_account_balance(user_account)
        assert 0 == thl_lm.get_account_balance(revenue)
        assert 20 - 5 + 5 == thl_lm.get_account_balance(bp_commission_account)
        assert thl_lm.check_ledger_balanced()

        # make the $2 failure into a complete also
        wall3.update(
            adjusted_status=WallAdjustedStatus.ADJUSTED_TO_COMPLETE,
            adjusted_cpi=wall3.cpi,
            adjusted_timestamp=now + timedelta(hours=2),
        )
        thl_lm.create_tx_task_adjustment(wall3, user)
        new_status, new_payout, new_user_payout = (
            session.determine_new_status_and_payouts()
        )
        print(new_status, new_payout, new_user_payout, session.adjusted_payout)
        session.adjust_status()
        print(
            session.get_status_after_adjustment(),
            session.get_payout_after_adjustment(),
            session.get_user_payout_after_adjustment(),
        )
        thl_lm.create_tx_bp_adjustment(session)
        assert 228 - 57 + 57 + 114 == thl_lm.get_account_balance(bp_wallet_account)
        assert 152 - 38 + 38 + 76 == thl_lm.get_account_balance(user_account)
        assert 0 == thl_lm.get_account_balance(revenue)
        assert 20 - 5 + 5 + 10 == thl_lm.get_account_balance(bp_commission_account)
        assert thl_lm.check_ledger_balanced()

    def test_create_transaction_bp_adjustment_cpi_adjustment(
        self,
        user_factory,
        product_user_wallet_no,
        create_main_accounts,
        delete_ledger_db,
        caplog,
        thl_lm,
        lm,
        utc_hour_ago,
        currency,
    ):
        delete_ledger_db()
        create_main_accounts()
        user: User = user_factory(product=product_user_wallet_no)

        wall1 = Wall(
            user_id=user.user_id,
            source=Source.DYNATA,
            req_survey_id="xxx",
            req_cpi=Decimal("1.00"),
            session_id=1,
            status=Status.COMPLETE,
            status_code_1=StatusCode1.COMPLETE,
            started=utc_hour_ago,
            finished=utc_hour_ago + timedelta(seconds=1),
        )
        tx = thl_lm.create_tx_task_complete(
            wall=wall1, user=user, created=wall1.started
        )
        assert isinstance(tx, LedgerTransaction)

        wall2 = Wall(
            user_id=user.user_id,
            source=Source.FULL_CIRCLE,
            req_survey_id="yyy",
            req_cpi=Decimal("3.00"),
            session_id=1,
            status=Status.COMPLETE,
            status_code_1=StatusCode1.COMPLETE,
            started=utc_hour_ago,
            finished=utc_hour_ago + timedelta(seconds=1),
        )
        tx = thl_lm.create_tx_task_complete(
            wall=wall2, user=user, created=wall2.started
        )
        assert isinstance(tx, LedgerTransaction)

        session = Session(started=wall1.started, user=user, wall_events=[wall1, wall2])
        status, status_code_1 = session.determine_session_status()
        thl_net, commission_amount, bp_pay, user_pay = session.determine_payments()
        session.update(
            **{
                "status": status,
                "status_code_1": status_code_1,
                "finished": utc_hour_ago + timedelta(minutes=10),
                "payout": bp_pay,
                "user_payout": user_pay,
            }
        )
        thl_lm.create_tx_bp_payment(session, created=wall1.started)

        revenue = thl_lm.get_account_task_complete_revenue()
        bp_wallet_account = thl_lm.get_account_or_create_bp_wallet(user.product)
        bp_commission_account = thl_lm.get_account_or_create_bp_commission(user.product)
        assert 380 == thl_lm.get_account_balance(bp_wallet_account)
        assert 0 == thl_lm.get_account_balance(revenue)
        assert 20 == thl_lm.get_account_balance(bp_commission_account)
        assert thl_lm.check_ledger_balanced()

        # cpi adjustment $1 -> $.60.
        wall1.update(
            adjusted_status=WallAdjustedStatus.CPI_ADJUSTMENT,
            adjusted_cpi=Decimal("0.60"),
            adjusted_timestamp=utc_hour_ago + timedelta(minutes=30),
        )
        thl_lm.create_tx_task_adjustment(wall1, user)

        # -$0.40 b/c the MP took $0.40 back, but we haven't yet taken the BP payment back
        assert -40 == thl_lm.get_account_balance(revenue)
        session.adjust_status()
        print(
            session.get_status_after_adjustment(),
            session.get_payout_after_adjustment(),
            session.get_user_payout_after_adjustment(),
        )
        thl_lm.create_tx_bp_adjustment(session)

        # running this twice b/c it should do nothing the 2nd time
        print(
            session.get_status_after_adjustment(),
            session.get_payout_after_adjustment(),
            session.get_user_payout_after_adjustment(),
        )
        with caplog.at_level(logging.INFO):
            thl_lm.create_tx_bp_adjustment(session)
        assert "create_transaction_bp_adjustment." in caplog.text
        assert "No transactions needed." in caplog.text

        assert 380 - 38 == thl_lm.get_account_balance(bp_wallet_account)
        assert 0 == thl_lm.get_account_balance(revenue)
        assert 20 - 2 == thl_lm.get_account_balance(bp_commission_account)
        assert thl_lm.check_ledger_balanced()

        # adjust it to failure
        wall1.update(
            adjusted_status=WallAdjustedStatus.ADJUSTED_TO_FAIL,
            adjusted_cpi=0,
            adjusted_timestamp=utc_hour_ago + timedelta(minutes=45),
        )
        thl_lm.create_tx_task_adjustment(wall1, user)
        session.adjust_status()
        thl_lm.create_tx_bp_adjustment(session)
        assert 300 - (300 * 0.05) == thl_lm.get_account_balance(bp_wallet_account)
        assert 0 == thl_lm.get_account_balance(revenue)
        assert 300 * 0.05 == thl_lm.get_account_balance(bp_commission_account)
        assert thl_lm.check_ledger_balanced()

        # and then back to cpi adj again, but this time for more than the orig amount
        wall1.update(
            adjusted_status=WallAdjustedStatus.CPI_ADJUSTMENT,
            adjusted_cpi=Decimal("2.00"),
            adjusted_timestamp=utc_hour_ago + timedelta(minutes=45),
        )
        thl_lm.create_tx_task_adjustment(wall1, user)
        session.adjust_status()
        thl_lm.create_tx_bp_adjustment(session)
        assert 500 - (500 * 0.05) == thl_lm.get_account_balance(bp_wallet_account)
        assert 0 == thl_lm.get_account_balance(revenue)
        assert 500 * 0.05 == thl_lm.get_account_balance(bp_commission_account)
        assert thl_lm.check_ledger_balanced()

        # And adjust again
        wall1.update(
            adjusted_status=WallAdjustedStatus.CPI_ADJUSTMENT,
            adjusted_cpi=Decimal("3.00"),
            adjusted_timestamp=utc_hour_ago + timedelta(minutes=45),
        )
        thl_lm.create_tx_task_adjustment(wall=wall1, user=user)
        session.adjust_status()
        thl_lm.create_tx_bp_adjustment(session=session)
        assert 600 - (600 * 0.05) == thl_lm.get_account_balance(
            account=bp_wallet_account
        )
        assert 0 == thl_lm.get_account_balance(account=revenue)
        assert 600 * 0.05 == thl_lm.get_account_balance(account=bp_commission_account)
        assert thl_lm.check_ledger_balanced()
