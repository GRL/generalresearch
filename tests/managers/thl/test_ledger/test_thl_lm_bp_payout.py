import logging
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from random import randint
from uuid import uuid4

import pytest
import redis
from pydantic import RedisDsn
from redis.lock import Lock

from generalresearch.currency import USDCent
from generalresearch.managers.base import Permission
from generalresearch.managers.thl.ledger_manager.thl_ledger import ThlLedgerManager
from generalresearch.managers.thl.ledger_manager.exceptions import (
    LedgerTransactionFlagAlreadyExistsError,
    LedgerTransactionConditionFailedError,
    LedgerTransactionReleaseLockError,
    LedgerTransactionCreateError,
)
from generalresearch.managers.thl.ledger_manager.ledger import LedgerTransaction
from generalresearch.models import Source
from generalresearch.models.thl.definitions import PayoutStatus
from generalresearch.models.thl.ledger import Direction, TransactionType
from generalresearch.models.thl.session import (
    Wall,
    Status,
    StatusCode1,
    Session,
)
from generalresearch.models.thl.user import User
from generalresearch.models.thl.wallet import PayoutType
from generalresearch.redis_helper import RedisConfig


def broken_acquire(self, *args, **kwargs):
    raise redis.exceptions.TimeoutError("Simulated timeout during acquire")


def broken_release(self, *args, **kwargs):
    raise redis.exceptions.TimeoutError("Simulated timeout during release")


class TestThlLedgerManagerBPPayout:

    def test_create_tx_with_bp_payment(
        self,
        user_factory,
        product_user_wallet_no,
        create_main_accounts,
        caplog,
        thl_lm,
        delete_ledger_db,
    ):
        delete_ledger_db()
        create_main_accounts()

        now = datetime.now(timezone.utc) - timedelta(hours=1)
        user: User = user_factory(product=product_user_wallet_no)

        wall1 = Wall(
            user_id=user.user_id,
            source=Source.DYNATA,
            req_survey_id="xxx",
            req_cpi=Decimal("6.00"),
            session_id=1,
            status=Status.COMPLETE,
            status_code_1=StatusCode1.COMPLETE,
            started=now,
            finished=now + timedelta(seconds=1),
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
                "finished": now + timedelta(minutes=10),
                "payout": bp_pay,
                "user_payout": user_pay,
            }
        )
        thl_lm.create_tx_bp_payment(session=session, created=wall1.started)

        lock_key = f"test:bp_payout:{user.product.id}"
        flag_name = f"{thl_lm.cache_prefix}:transaction_flag:{lock_key}"
        thl_lm.redis_client.delete(flag_name)

        payoutevent_uuid = uuid4().hex
        thl_lm.create_tx_bp_payout(
            product=user.product,
            amount=USDCent(200),
            created=now,
            payoutevent_uuid=payoutevent_uuid,
        )

        payoutevent_uuid = uuid4().hex
        thl_lm.create_tx_bp_payout(
            product=user.product,
            amount=USDCent(200),
            created=now + timedelta(minutes=2),
            skip_one_per_day_check=True,
            payoutevent_uuid=payoutevent_uuid,
        )

        cash = thl_lm.get_account_cash()
        bp_wallet_account = thl_lm.get_account_or_create_bp_wallet(user.product)
        assert 170 == thl_lm.get_account_balance(bp_wallet_account)
        assert 200 == thl_lm.get_account_balance(cash)

        with pytest.raises(expected_exception=LedgerTransactionFlagAlreadyExistsError):
            thl_lm.create_tx_bp_payout(
                user.product,
                amount=USDCent(200),
                created=now + timedelta(minutes=2),
                skip_one_per_day_check=False,
                skip_wallet_balance_check=False,
                payoutevent_uuid=payoutevent_uuid,
            )

        payoutevent_uuid = uuid4().hex
        with caplog.at_level(logging.INFO):
            with pytest.raises(LedgerTransactionConditionFailedError):
                thl_lm.create_tx_bp_payout(
                    user.product,
                    amount=USDCent(10_000),
                    created=now + timedelta(minutes=2),
                    skip_one_per_day_check=True,
                    skip_wallet_balance_check=False,
                    payoutevent_uuid=payoutevent_uuid,
                )
        assert "failed condition check balance:" in caplog.text

        thl_lm.create_tx_bp_payout(
            product=user.product,
            amount=USDCent(10_00),
            created=now + timedelta(minutes=2),
            skip_one_per_day_check=True,
            skip_wallet_balance_check=True,
            payoutevent_uuid=payoutevent_uuid,
        )
        assert 170 - 1000 == thl_lm.get_account_balance(bp_wallet_account)

    def test_create_tx(self, product, caplog, thl_lm, currency):
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
            == f"{currency.value}:{TransactionType.BP_PAYOUT.value}:{payoutevent_uuid}"
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

    def test_create_tx_redis_failure(self, product, thl_web_rw, thl_lm):
        rand_amount: USDCent = USDCent(randint(100, 1_000))
        payoutevent_uuid = uuid4().hex
        now = datetime.now(tz=timezone.utc)

        thl_lm.create_tx_plug_bp_wallet(
            product, rand_amount, now, direction=Direction.CREDIT
        )

        # Non routable IP address. Redis will fail
        thl_lm_redis_0 = ThlLedgerManager(
            pg_config=thl_web_rw,
            permissions=[
                Permission.CREATE,
                Permission.READ,
                Permission.UPDATE,
                Permission.DELETE,
            ],
            testing=True,
            redis_config=RedisConfig(
                dsn=RedisDsn("redis://10.255.255.1:6379"),
                socket_connect_timeout=0.1,
            ),
        )

        with pytest.raises(expected_exception=Exception) as e:
            tx = thl_lm_redis_0.create_tx_bp_payout(
                product=product,
                amount=rand_amount,
                payoutevent_uuid=payoutevent_uuid,
                created=datetime.now(tz=timezone.utc),
            )
        assert e.type is redis.exceptions.TimeoutError
        # No txs were created
        bp_wallet_account = thl_lm.get_account_or_create_bp_wallet(product=product)
        txs = thl_lm.get_tx_filtered_by_account(account_uuid=bp_wallet_account.uuid)
        txs = [tx for tx in txs if tx.metadata["tx_type"] != "plug"]
        assert len(txs) == 0

    def test_create_tx_multiple_per_day(self, product, thl_lm):
        rand_amount: USDCent = USDCent(randint(100, 1_000))
        payoutevent_uuid = uuid4().hex
        now = datetime.now(tz=timezone.utc)

        thl_lm.create_tx_plug_bp_wallet(
            product, rand_amount * USDCent(2), now, direction=Direction.CREDIT
        )

        tx = thl_lm.create_tx_bp_payout(
            product=product,
            amount=rand_amount,
            payoutevent_uuid=payoutevent_uuid,
            created=datetime.now(tz=timezone.utc),
        )

        # Try to create another
        # Will fail b/c it has the same payout event uuid
        with pytest.raises(expected_exception=Exception) as e:
            tx = thl_lm.create_tx_bp_payout(
                product=product,
                amount=rand_amount,
                payoutevent_uuid=payoutevent_uuid,
                created=datetime.now(tz=timezone.utc),
            )
        assert e.type is LedgerTransactionFlagAlreadyExistsError

        # Try to create another
        # Will fail due to multiple per day
        payoutevent_uuid2 = uuid4().hex
        with pytest.raises(expected_exception=Exception) as e:
            tx = thl_lm.create_tx_bp_payout(
                product=product,
                amount=rand_amount,
                payoutevent_uuid=payoutevent_uuid2,
                created=datetime.now(tz=timezone.utc),
            )
        assert e.type is LedgerTransactionConditionFailedError
        assert str(e.value) == ">1 tx per day"

        # Make it run by skipping one per day check
        tx = thl_lm.create_tx_bp_payout(
            product=product,
            amount=rand_amount,
            payoutevent_uuid=payoutevent_uuid2,
            created=datetime.now(tz=timezone.utc),
            skip_one_per_day_check=True,
        )

    def test_create_tx_redis_lock_release_error(self, product, thl_lm):
        rand_amount: USDCent = USDCent(randint(100, 1_000))
        payoutevent_uuid = uuid4().hex
        now = datetime.now(tz=timezone.utc)
        bp_wallet_account = thl_lm.get_account_or_create_bp_wallet(product=product)

        thl_lm.create_tx_plug_bp_wallet(
            product, rand_amount * USDCent(2), now, direction=Direction.CREDIT
        )

        original_acquire = Lock.acquire
        original_release = Lock.release
        Lock.acquire = broken_acquire

        # Create TX will fail on lock enter, no tx will actually get created
        with pytest.raises(expected_exception=Exception) as e:
            tx = thl_lm.create_tx_bp_payout(
                product=product,
                amount=rand_amount,
                payoutevent_uuid=payoutevent_uuid,
                created=datetime.now(tz=timezone.utc),
            )
        assert e.type is LedgerTransactionCreateError
        assert str(e.value) == "Redis error: Simulated timeout during acquire"
        txs = thl_lm.get_tx_filtered_by_account(account_uuid=bp_wallet_account.uuid)
        txs = [tx for tx in txs if tx.metadata["tx_type"] != "plug"]
        assert len(txs) == 0

        Lock.acquire = original_acquire
        Lock.release = broken_release

        # Create TX will fail on lock exit, after the tx was created!
        with pytest.raises(expected_exception=Exception) as e:
            tx = thl_lm.create_tx_bp_payout(
                product=product,
                amount=rand_amount,
                payoutevent_uuid=payoutevent_uuid,
                created=datetime.now(tz=timezone.utc),
            )
        assert e.type is LedgerTransactionReleaseLockError
        assert str(e.value) == "Redis error: Simulated timeout during release"

        # Transaction was still created!
        txs = thl_lm.get_tx_filtered_by_account(account_uuid=bp_wallet_account.uuid)
        txs = [tx for tx in txs if tx.metadata["tx_type"] != "plug"]
        assert len(txs) == 1
        Lock.release = original_release


class TestPayoutEventManagerBPPayout:

    def test_create(self, product, thl_lm, brokerage_product_payout_event_manager):
        rand_amount: USDCent = USDCent(randint(100, 1_000))
        now = datetime.now(tz=timezone.utc)
        bp_wallet_account = thl_lm.get_account_or_create_bp_wallet(product=product)
        assert thl_lm.get_account_balance(bp_wallet_account) == 0
        thl_lm.create_tx_plug_bp_wallet(
            product, rand_amount, now, direction=Direction.CREDIT
        )
        assert thl_lm.get_account_balance(bp_wallet_account) == rand_amount
        brokerage_product_payout_event_manager.set_account_lookup_table(thl_lm=thl_lm)

        pe = brokerage_product_payout_event_manager.create_bp_payout_event(
            thl_ledger_manager=thl_lm,
            product=product,
            created=now,
            amount=rand_amount,
            payout_type=PayoutType.ACH,
        )
        assert brokerage_product_payout_event_manager.check_for_ledger_tx(
            thl_ledger_manager=thl_lm,
            product_id=product.id,
            amount=rand_amount,
            payout_event=pe,
        )
        assert thl_lm.get_account_balance(bp_wallet_account) == 0

    def test_create_with_redis_error(
        self, product, caplog, thl_lm, brokerage_product_payout_event_manager
    ):
        caplog.set_level("WARNING")
        original_acquire = Lock.acquire
        original_release = Lock.release

        rand_amount: USDCent = USDCent(randint(100, 1_000))
        now = datetime.now(tz=timezone.utc)
        bp_wallet_account = thl_lm.get_account_or_create_bp_wallet(product=product)
        assert thl_lm.get_account_balance(bp_wallet_account) == 0
        thl_lm.create_tx_plug_bp_wallet(
            product, rand_amount, now, direction=Direction.CREDIT
        )
        assert thl_lm.get_account_balance(bp_wallet_account) == rand_amount
        brokerage_product_payout_event_manager.set_account_lookup_table(thl_lm=thl_lm)

        # Will fail on lock enter, no tx will actually get created
        Lock.acquire = broken_acquire
        with pytest.raises(expected_exception=Exception) as e:
            pe = brokerage_product_payout_event_manager.create_bp_payout_event(
                thl_ledger_manager=thl_lm,
                product=product,
                created=now,
                amount=rand_amount,
                payout_type=PayoutType.ACH,
            )
        assert e.type is LedgerTransactionCreateError
        assert str(e.value) == "Redis error: Simulated timeout during acquire"
        assert any(
            "Simulated timeout during acquire. No ledger tx was created" in m
            for m in caplog.messages
        )

        txs = thl_lm.get_tx_filtered_by_account(account_uuid=bp_wallet_account.uuid)
        txs = [tx for tx in txs if tx.metadata["tx_type"] != "plug"]
        # One payout event is created, status is failed, and no ledger txs exist
        assert len(txs) == 0
        pes = (
            brokerage_product_payout_event_manager.get_bp_bp_payout_events_for_products(
                thl_ledger_manager=thl_lm, product_uuids=[product.id]
            )
        )
        assert len(pes) == 1
        assert pes[0].status == PayoutStatus.FAILED
        pe = pes[0]

        # Fix the redis method
        Lock.acquire = original_acquire

        # Try to fix the failed payout, by trying ledger tx again
        brokerage_product_payout_event_manager.retry_create_bp_payout_event_tx(
            product=product, thl_ledger_manager=thl_lm, payout_event_uuid=pe.uuid
        )
        txs = thl_lm.get_tx_filtered_by_account(account_uuid=bp_wallet_account.uuid)
        txs = [tx for tx in txs if tx.metadata["tx_type"] != "plug"]
        assert len(txs) == 1
        assert thl_lm.get_account_balance(bp_wallet_account) == 0

        # And then try to run it again, it'll fail because a payout event with the same info exists
        with pytest.raises(expected_exception=Exception) as e:
            pe = brokerage_product_payout_event_manager.create_bp_payout_event(
                thl_ledger_manager=thl_lm,
                product=product,
                created=now,
                amount=rand_amount,
                payout_type=PayoutType.ACH,
            )
        assert e.type is ValueError
        assert "Payout event already exists!" in str(e.value)

        # We wouldn't do this in practice, because this is paying out the BP again, but
        #   we can if want to.
        # Change the timestamp so it'll create a new payout event
        now = datetime.now(tz=timezone.utc)
        with pytest.raises(LedgerTransactionConditionFailedError) as e:
            pe = brokerage_product_payout_event_manager.create_bp_payout_event(
                thl_ledger_manager=thl_lm,
                product=product,
                created=now,
                amount=rand_amount,
                payout_type=PayoutType.ACH,
            )
        # But it will fail due to 1 per day check
        assert str(e.value) == ">1 tx per day"
        pe = brokerage_product_payout_event_manager.get_by_uuid(e.value.pe_uuid)
        assert pe.status == PayoutStatus.FAILED

        # And if we really want to, we can make it again
        now = datetime.now(tz=timezone.utc)
        pe = brokerage_product_payout_event_manager.create_bp_payout_event(
            thl_ledger_manager=thl_lm,
            product=product,
            created=now,
            amount=rand_amount,
            payout_type=PayoutType.ACH,
            skip_one_per_day_check=True,
            skip_wallet_balance_check=True,
        )

        txs = thl_lm.get_tx_filtered_by_account(account_uuid=bp_wallet_account.uuid)
        txs = [tx for tx in txs if tx.metadata["tx_type"] != "plug"]
        assert len(txs) == 2
        # since they were paid twice
        assert thl_lm.get_account_balance(bp_wallet_account) == 0 - rand_amount

        Lock.release = original_release
        Lock.acquire = original_acquire

    def test_create_with_redis_error_release(
        self, product, caplog, thl_lm, brokerage_product_payout_event_manager
    ):
        caplog.set_level("WARNING")

        original_release = Lock.release

        rand_amount: USDCent = USDCent(randint(100, 1_000))
        now = datetime.now(tz=timezone.utc)
        bp_wallet_account = thl_lm.get_account_or_create_bp_wallet(product=product)
        brokerage_product_payout_event_manager.set_account_lookup_table(thl_lm=thl_lm)

        assert thl_lm.get_account_balance(bp_wallet_account) == 0
        thl_lm.create_tx_plug_bp_wallet(
            product, rand_amount, now, direction=Direction.CREDIT
        )
        assert thl_lm.get_account_balance(bp_wallet_account) == rand_amount

        # Will fail on lock exit, after the tx was created!
        # But it'll see that the tx was created and so everything will be fine
        Lock.release = broken_release
        pe = brokerage_product_payout_event_manager.create_bp_payout_event(
            thl_ledger_manager=thl_lm,
            product=product,
            created=now,
            amount=rand_amount,
            payout_type=PayoutType.ACH,
        )
        assert any(
            "Simulated timeout during release but ledger tx exists" in m
            for m in caplog.messages
        )

        txs = thl_lm.get_tx_filtered_by_account(account_uuid=bp_wallet_account.uuid)
        txs = [tx for tx in txs if tx.metadata["tx_type"] != "plug"]
        assert len(txs) == 1
        pes = (
            brokerage_product_payout_event_manager.get_bp_bp_payout_events_for_products(
                thl_ledger_manager=thl_lm, product_uuids=[product.uuid]
            )
        )
        assert len(pes) == 1
        assert pes[0].status == PayoutStatus.COMPLETE
        Lock.release = original_release
