import logging
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from typing import Callable

import pytest

from generalresearch.managers.thl.ledger_manager.conditions import (
    generate_condition_mp_payment,
)
from generalresearch.managers.thl.ledger_manager.exceptions import (
    LedgerTransactionCreateLockError,
    LedgerTransactionFlagAlreadyExistsError,
    LedgerTransactionCreateError,
)
from generalresearch.models import Source
from generalresearch.models.thl.ledger import LedgerTransaction
from generalresearch.models.thl.session import (
    Wall,
    Status,
    StatusCode1,
    Session,
    WallAdjustedStatus,
)
from generalresearch.models.thl.user import User
from test_utils.models.conftest import user_factory, session, product_user_wallet_no

logger = logging.getLogger("LedgerManager")


class TestLedgerLocks:

    def test_a(
        self,
        user_factory,
        session_factory,
        product_user_wallet_no,
        create_main_accounts,
        caplog,
        thl_lm,
        lm,
        utc_hour_ago,
        currency,
        wall_factory,
        delete_ledger_db,
    ):
        """
        TODO: This whole test is confusing a I don't really understand.
            It needs to be better documented and explained what we want
            it to do and evaluate...
        """
        delete_ledger_db()
        create_main_accounts()

        user: User = user_factory(product=product_user_wallet_no)
        s1 = session_factory(
            user=user,
            wall_count=3,
            wall_req_cpis=[Decimal("1.23"), Decimal("3.21"), Decimal("4")],
            wall_statuses=[Status.COMPLETE, Status.COMPLETE, Status.COMPLETE],
        )

        # A User does a Wall Completion in Session=1
        w1 = s1.wall_events[0]
        tx = thl_lm.create_tx_task_complete(wall=w1, user=user, created=w1.started)
        assert isinstance(tx, LedgerTransaction)

        # A User does another Wall Completion in Session=1
        w2 = s1.wall_events[1]
        tx = thl_lm.create_tx_task_complete(wall=w2, user=user, created=w2.started)
        assert isinstance(tx, LedgerTransaction)

        # That first Wall Complete was "adjusted" to instead be marked
        #   as a Failure
        w1.update(
            adjusted_status=WallAdjustedStatus.ADJUSTED_TO_FAIL,
            adjusted_cpi=0,
            adjusted_timestamp=utc_hour_ago + timedelta(hours=1),
        )
        tx = thl_lm.create_tx_task_adjustment(wall=w1, user=user)
        assert isinstance(tx, LedgerTransaction)

        # A User does another! Wall Completion in Session=1; however, we
        #   don't create a transaction for it
        w3 = s1.wall_events[2]

        # Make sure we clear any flags/locks first
        lock_key = f"{currency.value}:thl_wall:{w3.uuid}"
        lock_name = f"{lm.cache_prefix}:transaction_lock:{lock_key}"
        flag_name = f"{lm.cache_prefix}:transaction_flag:{lock_key}"
        lm.redis_client.delete(lock_name)
        lm.redis_client.delete(flag_name)

        # Despite the
        f1 = generate_condition_mp_payment(wall=w1)
        f2 = generate_condition_mp_payment(wall=w2)
        f3 = generate_condition_mp_payment(wall=w3)
        assert f1(lm=lm) is False
        assert f2(lm=lm) is False
        assert f3(lm=lm) is True

        condition = f3
        create_tx_func = lambda: thl_lm.create_tx_task_complete_(wall=w3, user=user)
        assert isinstance(create_tx_func, Callable)
        assert f3(lm) is True

        lm.redis_client.delete(flag_name)
        lm.redis_client.delete(lock_name)

        tx = thl_lm.create_tx_protected(
            lock_key=lock_key, condition=condition, create_tx_func=create_tx_func
        )
        assert f3(lm) is False

        # purposely hold the lock open
        tx = None
        lm.redis_client.set(lock_name, "1")
        with caplog.at_level(logging.ERROR):
            with pytest.raises(expected_exception=LedgerTransactionCreateLockError):
                tx = thl_lm.create_tx_protected(
                    lock_key=lock_key,
                    condition=condition,
                    create_tx_func=create_tx_func,
                )
                assert tx is None
        assert "Unable to acquire lock within the time specified" in caplog.text
        lm.redis_client.delete(lock_name)

    def test_locking(
        self,
        user_factory,
        product_user_wallet_no,
        create_main_accounts,
        delete_ledger_db,
        caplog,
        thl_lm,
        lm,
    ):
        delete_ledger_db()
        create_main_accounts()

        now = datetime.now(timezone.utc) - timedelta(hours=1)
        user: User = user_factory(product=product_user_wallet_no)

        # A User does a Wall complete on Session.id=1 and the transaction is
        #   logged to the ledger
        wall1 = Wall(
            user_id=user.user_id,
            source=Source.DYNATA,
            req_survey_id="xxx",
            req_cpi=Decimal("1.23"),
            session_id=1,
            status=Status.COMPLETE,
            status_code_1=StatusCode1.COMPLETE,
            started=now,
            finished=now + timedelta(seconds=1),
        )
        thl_lm.create_tx_task_complete(wall=wall1, user=user, created=wall1.started)

        # A User does a Wall complete on Session.id=1 and the transaction is
        #   logged to the ledger
        wall2 = Wall(
            user_id=user.user_id,
            source=Source.FULL_CIRCLE,
            req_survey_id="yyy",
            req_cpi=Decimal("3.21"),
            session_id=1,
            status=Status.COMPLETE,
            status_code_1=StatusCode1.COMPLETE,
            started=now,
            finished=now + timedelta(seconds=1),
        )
        thl_lm.create_tx_task_complete(wall=wall2, user=user, created=wall2.started)

        # An hour later, the first wall complete is adjusted to a Failure and
        #   it's tracked in the ledger
        wall1.update(
            adjusted_status=WallAdjustedStatus.ADJUSTED_TO_FAIL,
            adjusted_cpi=0,
            adjusted_timestamp=now + timedelta(hours=1),
        )
        thl_lm.create_tx_task_adjustment(wall=wall1, user=user)

        # A User does a Wall complete on Session.id=1 and the transaction
        #   IS NOT logged to the ledger
        wall3 = Wall(
            user_id=user.user_id,
            source=Source.DYNATA,
            req_survey_id="xxx",
            req_cpi=Decimal("4"),
            session_id=1,
            status=Status.COMPLETE,
            status_code_1=StatusCode1.COMPLETE,
            started=now,
            finished=now + timedelta(seconds=1),
            uuid="867a282d8b4d40d2a2093d75b802b629",
        )

        revenue_account = thl_lm.get_account_task_complete_revenue()
        assert 0 == thl_lm.get_account_filtered_balance(
            account=revenue_account,
            metadata_key="thl_wall",
            metadata_value=wall3.uuid,
        )
        # Make sure we clear any flags/locks first
        lock_key = f"test:thl_wall:{wall3.uuid}"
        lock_name = f"{lm.cache_prefix}:transaction_lock:{lock_key}"
        flag_name = f"{lm.cache_prefix}:transaction_flag:{lock_key}"
        lm.redis_client.delete(lock_name)
        lm.redis_client.delete(flag_name)

        # Purposely hold the lock open
        lm.redis_client.set(name=lock_name, value="1")
        with caplog.at_level(logging.DEBUG):
            with pytest.raises(expected_exception=LedgerTransactionCreateLockError):
                tx = thl_lm.create_tx_task_complete(
                    wall=wall3, user=user, created=wall3.started
                )
                assert isinstance(tx, LedgerTransaction)
        assert "Unable to acquire lock within the time specified" in caplog.text

        # Release the lock
        lm.redis_client.delete(lock_name)

        # Set the redis flag to indicate it has been run
        lm.redis_client.set(flag_name, "1")
        # with self.assertLogs(logger=logger, level=logging.DEBUG) as cm2:
        with pytest.raises(expected_exception=LedgerTransactionFlagAlreadyExistsError):
            tx = thl_lm.create_tx_task_complete(
                wall=wall3, user=user, created=wall3.started
            )
        # self.assertIn("entered_lock: True, flag_set: True", cm2.output[0])

        # Unset the flag
        lm.redis_client.delete(flag_name)

        assert 0 == lm.get_account_filtered_balance(
            account=revenue_account,
            metadata_key="thl_wall",
            metadata_value=wall3.uuid,
        )

        # Now actually run it
        tx = thl_lm.create_tx_task_complete(
            wall=wall3, user=user, created=wall3.started
        )
        assert tx is not None

        # Run it again, should return None
        # Confirm the Exception inheritance works
        tx = None
        with pytest.raises(expected_exception=LedgerTransactionCreateError):
            tx = thl_lm.create_tx_task_complete(
                wall=wall3, user=user, created=wall3.started
            )
        assert tx is None

        # clear the redis flag, it should query the db
        assert lm.redis_client.get(flag_name) is not None
        lm.redis_client.delete(flag_name)
        assert lm.redis_client.get(flag_name) is None

        with pytest.raises(expected_exception=LedgerTransactionCreateError):
            tx = thl_lm.create_tx_task_complete(
                wall=wall3, user=user, created=wall3.started
            )

        assert 400 == thl_lm.get_account_filtered_balance(
            account=revenue_account,
            metadata_key="thl_wall",
            metadata_value=wall3.uuid,
        )

    def test_bp_payment_without_locks(
        self, user_factory, product_user_wallet_no, create_main_accounts, thl_lm, lm
    ):
        user: User = user_factory(product=product_user_wallet_no)
        wall1 = Wall(
            user_id=user.user_id,
            source=Source.SAGO,
            req_survey_id="xxx",
            req_cpi=Decimal("0.50"),
            session_id=3,
            status=Status.COMPLETE,
            status_code_1=StatusCode1.COMPLETE,
            started=datetime.now(timezone.utc),
            finished=datetime.now(timezone.utc) + timedelta(seconds=1),
        )

        thl_lm.create_tx_task_complete(wall=wall1, user=user, created=wall1.started)
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

        # Run it 3 times without any checks, and it gets made three times!
        thl_lm.create_tx_bp_payment(session=session, created=wall1.started)
        thl_lm.create_tx_bp_payment_(session=session, created=wall1.started)
        thl_lm.create_tx_bp_payment_(session=session, created=wall1.started)

        bp_wallet = thl_lm.get_account_or_create_bp_wallet(product=user.product)
        assert 48 * 3 == lm.get_account_balance(account=bp_wallet)
        assert 48 * 3 == thl_lm.get_account_filtered_balance(
            account=bp_wallet, metadata_key="thl_session", metadata_value=session.uuid
        )
        assert lm.check_ledger_balanced()

    def test_bp_payment_with_locks(
        self, user_factory, product_user_wallet_no, create_main_accounts, thl_lm, lm
    ):
        user: User = user_factory(product=product_user_wallet_no)

        wall1 = Wall(
            user_id=user.user_id,
            source=Source.SAGO,
            req_survey_id="xxx",
            req_cpi=Decimal("0.50"),
            session_id=3,
            status=Status.COMPLETE,
            status_code_1=StatusCode1.COMPLETE,
            started=datetime.now(timezone.utc),
            finished=datetime.now(timezone.utc) + timedelta(seconds=1),
        )

        thl_lm.create_tx_task_complete(wall1, user, created=wall1.started)
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

        # Make sure we clear any flags/locks first
        lock_key = f"test:thl_wall:{wall1.uuid}"
        lock_name = f"{lm.cache_prefix}:transaction_lock:{lock_key}"
        flag_name = f"{lm.cache_prefix}:transaction_flag:{lock_key}"
        lm.redis_client.delete(lock_name)
        lm.redis_client.delete(flag_name)

        # Run it 3 times with check, and it gets made once!
        thl_lm.create_tx_bp_payment(session=session, created=wall1.started)
        with pytest.raises(expected_exception=LedgerTransactionCreateError):
            thl_lm.create_tx_bp_payment(session=session, created=wall1.started)

        with pytest.raises(expected_exception=LedgerTransactionCreateError):
            thl_lm.create_tx_bp_payment(session=session, created=wall1.started)

        bp_wallet = thl_lm.get_account_or_create_bp_wallet(product=user.product)
        assert 48 == thl_lm.get_account_balance(bp_wallet)
        assert 48 == thl_lm.get_account_filtered_balance(
            account=bp_wallet,
            metadata_key="thl_session",
            metadata_value=session.uuid,
        )
        assert lm.check_ledger_balanced()
