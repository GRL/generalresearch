import logging
import os
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from random import choice as rand_choice, randint
from typing import Optional
from uuid import uuid4

import pandas as pd
import pytest

from generalresearch.currency import USDCent
from generalresearch.managers.thl.ledger_manager.exceptions import (
    LedgerTransactionConditionFailedError,
)
from generalresearch.managers.thl.payout import UserPayoutEventManager
from generalresearch.models.thl.definitions import PayoutStatus
from generalresearch.models.thl.ledger import LedgerEntry, Direction
from generalresearch.models.thl.payout import BusinessPayoutEvent
from generalresearch.models.thl.payout import UserPayoutEvent
from generalresearch.models.thl.wallet import PayoutType
from generalresearch.models.thl.ledger import LedgerAccount

logger = logging.getLogger()

cashout_method_uuid = uuid4().hex


class TestPayout:

    def test_get_by_uuid_and_create(
        self,
        user,
        user_payout_event_manager: UserPayoutEventManager,
        thl_lm,
        utc_now,
    ):

        user_account: LedgerAccount = thl_lm.get_account_or_create_user_wallet(
            user=user
        )

        pe1: UserPayoutEvent = user_payout_event_manager.create(
            debit_account_uuid=user_account.uuid,
            payout_type=PayoutType.PAYPAL,
            cashout_method_uuid=cashout_method_uuid,
            amount=100,
            created=utc_now,
        )

        # these get added by the query
        pe1.account_reference_type = "user"
        pe1.account_reference_uuid = user.uuid
        # pe1.description = "PayPal"

        pe2 = user_payout_event_manager.get_by_uuid(pe_uuid=pe1.uuid)

        assert pe1 == pe2

    def test_update(self, user, user_payout_event_manager, lm, thl_lm, utc_now):
        from generalresearch.models.thl.definitions import PayoutStatus
        from generalresearch.models.thl.wallet import PayoutType

        user_account = thl_lm.get_account_or_create_user_wallet(user=user)

        pe1 = user_payout_event_manager.create(
            status=PayoutStatus.PENDING,
            debit_account_uuid=user_account.uuid,
            payout_type=PayoutType.PAYPAL,
            cashout_method_uuid=cashout_method_uuid,
            amount=100,
            created=utc_now,
        )
        user_payout_event_manager.update(
            payout_event=pe1,
            status=PayoutStatus.APPROVED,
            order_data={"foo": "bar"},
            ext_ref_id="abc",
        )
        pe = user_payout_event_manager.get_by_uuid(pe_uuid=pe1.uuid)
        assert pe.status == PayoutStatus.APPROVED
        assert pe.order_data == {"foo": "bar"}

        with pytest.raises(expected_exception=AssertionError) as cm:
            user_payout_event_manager.update(
                payout_event=pe, status=PayoutStatus.PENDING
            )
        assert "status APPROVED can only be" in str(cm.value)

    def test_create_bp_payout(
        self,
        user,
        thl_web_rr,
        user_payout_event_manager,
        lm,
        thl_lm,
        product,
        brokerage_product_payout_event_manager,
        delete_ledger_db,
        create_main_accounts,
    ):
        delete_ledger_db()
        create_main_accounts()
        from generalresearch.models.thl.ledger import LedgerAccount

        thl_lm.get_account_or_create_bp_wallet(product=product)
        brokerage_product_payout_event_manager.set_account_lookup_table(thl_lm=thl_lm)

        with pytest.raises(expected_exception=LedgerTransactionConditionFailedError):
            # wallet balance failure
            brokerage_product_payout_event_manager.create_bp_payout_event(
                thl_ledger_manager=thl_lm,
                product=product,
                amount=USDCent(100),
                skip_wallet_balance_check=False,
                skip_one_per_day_check=False,
            )

        # (we don't have a special method for this) Put money in the BP's account
        amount_cents = 100
        cash_account: LedgerAccount = thl_lm.get_account_cash()
        bp_wallet: LedgerAccount = thl_lm.get_account_or_create_bp_wallet(
            product=product
        )

        entries = [
            LedgerEntry(
                direction=Direction.DEBIT,
                account_uuid=cash_account.uuid,
                amount=amount_cents,
            ),
            LedgerEntry(
                direction=Direction.CREDIT,
                account_uuid=bp_wallet.uuid,
                amount=amount_cents,
            ),
        ]

        lm.create_tx(entries=entries)
        assert 100 == lm.get_account_balance(account=bp_wallet)

        # Then run it again for $1.00
        brokerage_product_payout_event_manager.create_bp_payout_event(
            thl_ledger_manager=thl_lm,
            product=product,
            amount=USDCent(100),
            skip_wallet_balance_check=False,
            skip_one_per_day_check=False,
        )
        assert 0 == lm.get_account_balance(account=bp_wallet)

        # Run again should without balance check, should still fail due to day check
        with pytest.raises(LedgerTransactionConditionFailedError):
            brokerage_product_payout_event_manager.create_bp_payout_event(
                thl_ledger_manager=thl_lm,
                product=product,
                amount=USDCent(100),
                skip_wallet_balance_check=True,
                skip_one_per_day_check=False,
            )

        # And then we can run again skip both checks
        pe = brokerage_product_payout_event_manager.create_bp_payout_event(
            thl_ledger_manager=thl_lm,
            product=product,
            amount=USDCent(100),
            skip_wallet_balance_check=True,
            skip_one_per_day_check=True,
        )
        assert -100 == lm.get_account_balance(account=bp_wallet)

        pe = brokerage_product_payout_event_manager.get_by_uuid(pe.uuid)
        txs = lm.get_tx_filtered_by_metadata(
            metadata_key="event_payout", metadata_value=pe.uuid
        )

        assert 1 == len(txs)

    def test_create_bp_payout_quick_dupe(
        self,
        user,
        product,
        thl_web_rw,
        brokerage_product_payout_event_manager,
        thl_lm,
        lm,
        utc_now,
        create_main_accounts,
    ):
        thl_lm.get_account_or_create_bp_wallet(product=product)
        brokerage_product_payout_event_manager.set_account_lookup_table(thl_lm=thl_lm)

        brokerage_product_payout_event_manager.create_bp_payout_event(
            thl_ledger_manager=thl_lm,
            product=product,
            amount=USDCent(100),
            skip_wallet_balance_check=True,
            skip_one_per_day_check=True,
            created=utc_now,
        )

        with pytest.raises(ValueError) as cm:
            brokerage_product_payout_event_manager.create_bp_payout_event(
                thl_ledger_manager=thl_lm,
                product=product,
                amount=USDCent(100),
                skip_wallet_balance_check=True,
                skip_one_per_day_check=True,
                created=utc_now,
            )
        assert "Payout event already exists!" in str(cm.value)

    def test_filter(
        self,
        thl_web_rw,
        thl_lm,
        lm,
        product,
        user,
        user_payout_event_manager,
        utc_now,
    ):
        from generalresearch.models.thl.definitions import PayoutStatus
        from generalresearch.models.thl.wallet import PayoutType

        user_account = thl_lm.get_account_or_create_user_wallet(user=user)
        bp_account = thl_lm.get_account_or_create_bp_wallet(product=product)

        user_payout_event_manager.create(
            status=PayoutStatus.PENDING,
            debit_account_uuid=user_account.uuid,
            payout_type=PayoutType.PAYPAL,
            cashout_method_uuid=cashout_method_uuid,
            amount=100,
            created=utc_now,
        )

        user_payout_event_manager.create(
            status=PayoutStatus.PENDING,
            debit_account_uuid=bp_account.uuid,
            payout_type=PayoutType.PAYPAL,
            cashout_method_uuid=cashout_method_uuid,
            amount=200,
            created=utc_now,
        )

        pes = user_payout_event_manager.filter_by(
            reference_uuid=user.uuid, created=utc_now
        )
        assert 1 == len(pes)

        pes = user_payout_event_manager.filter_by(
            debit_account_uuids=[bp_account.uuid], created=utc_now
        )
        assert 1 == len(pes)

        pes = user_payout_event_manager.filter_by(
            debit_account_uuids=[bp_account.uuid], amount=123
        )
        assert 0 == len(pes)

        pes = user_payout_event_manager.filter_by(
            product_ids=[user.product_id], bp_user_ids=["x"]
        )
        assert 0 == len(pes)

        pes = user_payout_event_manager.filter_by(product_ids=[user.product_id])
        assert 1 == len(pes)

        pes = user_payout_event_manager.filter_by(
            cashout_types=[PayoutType.PAYPAL],
            bp_user_ids=[user.product_user_id],
        )
        assert 1 == len(pes)

        pes = user_payout_event_manager.filter_by(
            statuses=[PayoutStatus.FAILED],
            cashout_types=[PayoutType.PAYPAL],
            bp_user_ids=[user.product_user_id],
        )
        assert 0 == len(pes)

        pes = user_payout_event_manager.filter_by(
            statuses=[PayoutStatus.PENDING],
            cashout_types=[PayoutType.PAYPAL],
            bp_user_ids=[user.product_user_id],
        )
        assert 1 == len(pes)


class TestPayoutEventManager:

    def test_set_account_lookup_table(
        self, payout_event_manager, thl_redis_config, thl_lm, delete_ledger_db
    ):
        delete_ledger_db()
        rc = thl_redis_config.create_redis_client()
        rc.delete("pem:account_to_product")
        rc.delete("pem:product_to_account")
        N = 5

        for idx in range(N):
            thl_lm.get_account_or_create_bp_wallet_by_uuid(product_uuid=uuid4().hex)

        res = rc.hgetall(name="pem:account_to_product")
        assert len(res.items()) == 0

        res = rc.hgetall(name="pem:product_to_account")
        assert len(res.items()) == 0

        payout_event_manager.set_account_lookup_table(
            thl_lm=thl_lm,
        )

        res = rc.hgetall(name="pem:account_to_product")
        assert len(res.items()) == N

        res = rc.hgetall(name="pem:product_to_account")
        assert len(res.items()) == N

        thl_lm.get_account_or_create_bp_wallet_by_uuid(product_uuid=uuid4().hex)
        payout_event_manager.set_account_lookup_table(
            thl_lm=thl_lm,
        )

        res = rc.hgetall(name="pem:account_to_product")
        assert len(res.items()) == N + 1

        res = rc.hgetall(name="pem:product_to_account")
        assert len(res.items()) == N + 1


class TestBusinessPayoutEventManager:

    @pytest.fixture
    def start(self) -> "datetime":
        return datetime(year=2018, month=3, day=14, hour=0, tzinfo=timezone.utc)

    @pytest.fixture
    def offset(self) -> str:
        return "5d"

    @pytest.fixture
    def duration(self) -> Optional["timedelta"]:
        return timedelta(days=10)

    def test_base(
        self,
        brokerage_product_payout_event_manager,
        business_payout_event_manager,
        delete_ledger_db,
        create_main_accounts,
        thl_lm,
        thl_web_rr,
        product_factory,
        bp_payout_factory,
        business,
    ):
        delete_ledger_db()
        create_main_accounts()

        from generalresearch.models.thl.product import Product

        p1: Product = product_factory(business=business)
        thl_lm.get_account_or_create_bp_wallet(product=p1)
        business_payout_event_manager.set_account_lookup_table(thl_lm=thl_lm)

        ach_id1 = uuid4().hex
        ach_id2 = uuid4().hex

        bp_payout_factory(
            product=p1,
            amount=USDCent(1),
            ext_ref_id=None,
            skip_wallet_balance_check=True,
            skip_one_per_day_check=True,
        )

        bp_payout_factory(
            product=p1,
            amount=USDCent(1),
            ext_ref_id=ach_id1,
            skip_wallet_balance_check=True,
            skip_one_per_day_check=True,
        )

        bp_payout_factory(
            product=p1,
            amount=USDCent(25),
            ext_ref_id=ach_id1,
            skip_wallet_balance_check=True,
            skip_one_per_day_check=True,
        )

        bp_payout_factory(
            product=p1,
            amount=USDCent(50),
            ext_ref_id=ach_id2,
            skip_wallet_balance_check=True,
            skip_one_per_day_check=True,
        )

        business.prebuild_payouts(
            thl_pg_config=thl_web_rr,
            thl_lm=thl_lm,
            bpem=business_payout_event_manager,
        )

        assert len(business.payouts) == 3
        assert business.payouts_total == sum([pe.amount for pe in business.payouts])
        assert business.payouts[0].created > business.payouts[1].created
        assert len(business.payouts[0].bp_payouts) == 1
        assert len(business.payouts[1].bp_payouts) == 2

        assert business.payouts[0].ext_ref_id == ach_id2
        assert business.payouts[1].ext_ref_id == ach_id1
        assert business.payouts[2].ext_ref_id is None

    def test_update_ext_reference_ids(
        self,
        brokerage_product_payout_event_manager,
        business_payout_event_manager,
        delete_ledger_db,
        create_main_accounts,
        thl_lm,
        thl_web_rr,
        product_factory,
        bp_payout_factory,
        delete_df_collection,
        user_factory,
        ledger_collection,
        session_with_tx_factory,
        pop_ledger_merge,
        client_no_amm,
        mnt_filepath,
        lm,
        product_manager,
        start,
        business,
    ):
        delete_ledger_db()
        create_main_accounts()
        delete_df_collection(coll=ledger_collection)

        from generalresearch.models.thl.product import Product
        from generalresearch.models.thl.user import User

        p1: Product = product_factory(business=business)
        u1: User = user_factory(product=p1)
        thl_lm.get_account_or_create_bp_wallet(product=p1)
        business_payout_event_manager.set_account_lookup_table(thl_lm=thl_lm)

        # $250.00 to work with
        for idx in range(1, 10):
            session_with_tx_factory(
                user=u1,
                wall_req_cpi=Decimal("25.00"),
                started=start + timedelta(days=1, minutes=idx),
            )

        ach_id1 = uuid4().hex
        ach_id2 = uuid4().hex

        with pytest.raises(expected_exception=Warning) as cm:
            business_payout_event_manager.update_ext_reference_ids(
                new_value=ach_id2,
                current_value=ach_id1,
            )
        assert "No event_payouts found to UPDATE" in str(cm)

        # We must build the balance to issue ACH/Wire
        ledger_collection.initial_load(client=None, sync=True)
        pop_ledger_merge.build(client=client_no_amm, ledger_coll=ledger_collection)
        business.prebuild_balance(
            thl_pg_config=thl_web_rr,
            lm=lm,
            ds=mnt_filepath,
            client=client_no_amm,
            pop_ledger=pop_ledger_merge,
        )

        res = business_payout_event_manager.create_from_ach_or_wire(
            business=business,
            amount=USDCent(100_01),
            pm=product_manager,
            thl_lm=thl_lm,
            transaction_id=ach_id1,
        )
        assert isinstance(res, BusinessPayoutEvent)

        # Okay, now that there is a payout_event, let's try to update the
        # ext_reference_id
        business_payout_event_manager.update_ext_reference_ids(
            new_value=ach_id2,
            current_value=ach_id1,
        )

        res = business_payout_event_manager.filter_by(ext_ref_id=ach_id1)
        assert len(res) == 0

        res = business_payout_event_manager.filter_by(ext_ref_id=ach_id2)
        assert len(res) == 1

    def test_delete_failed_business_payout(
        self,
        brokerage_product_payout_event_manager,
        business_payout_event_manager,
        delete_ledger_db,
        create_main_accounts,
        thl_lm,
        thl_web_rr,
        product_factory,
        bp_payout_factory,
        currency,
        delete_df_collection,
        user_factory,
        ledger_collection,
        session_with_tx_factory,
        pop_ledger_merge,
        client_no_amm,
        mnt_filepath,
        lm,
        product_manager,
        start,
        business,
    ):
        delete_ledger_db()
        create_main_accounts()
        delete_df_collection(coll=ledger_collection)

        from generalresearch.models.thl.product import Product
        from generalresearch.models.thl.user import User

        p1: Product = product_factory(business=business)
        u1: User = user_factory(product=p1)
        thl_lm.get_account_or_create_bp_wallet(product=p1)
        business_payout_event_manager.set_account_lookup_table(thl_lm=thl_lm)

        # $250.00 to work with
        for idx in range(1, 10):
            session_with_tx_factory(
                user=u1,
                wall_req_cpi=Decimal("25.00"),
                started=start + timedelta(days=1, minutes=idx),
            )

        # We must build the balance to issue ACH/Wire
        ledger_collection.initial_load(client=None, sync=True)
        pop_ledger_merge.build(client=client_no_amm, ledger_coll=ledger_collection)
        business.prebuild_balance(
            thl_pg_config=thl_web_rr,
            lm=lm,
            ds=mnt_filepath,
            client=client_no_amm,
            pop_ledger=pop_ledger_merge,
        )

        ach_id1 = uuid4().hex

        res = business_payout_event_manager.create_from_ach_or_wire(
            business=business,
            amount=USDCent(100_01),
            pm=product_manager,
            thl_lm=thl_lm,
            transaction_id=ach_id1,
        )
        assert isinstance(res, BusinessPayoutEvent)

        # (1) Confirm the initial Event Payout, Tx, TxMeta, TxEntry all exist
        event_payouts = business_payout_event_manager.filter_by(ext_ref_id=ach_id1)
        event_payout_uuids = [i.uuid for i in event_payouts]
        assert len(event_payout_uuids) == 1
        tags = [f"{currency.value}:bp_payout:{x}" for x in event_payout_uuids]
        transactions = thl_lm.get_txs_by_tags(tags=tags)
        assert len(transactions) == 1
        tx_metadata_ids = thl_lm.get_tx_metadata_ids_by_txs(transactions=transactions)
        assert len(tx_metadata_ids) == 2
        tx_entries = thl_lm.get_tx_entries_by_txs(transactions=transactions)
        assert len(tx_entries) == 2

        # (2) Delete!
        business_payout_event_manager.delete_failed_business_payout(
            ext_ref_id=ach_id1, thl_lm=thl_lm
        )

        # (3) Confirm the initial Event Payout, Tx, TxMeta, TxEntry have
        #   all been deleted
        res = business_payout_event_manager.filter_by(ext_ref_id=ach_id1)
        assert len(res) == 0

        # Note: b/c the event_payout shouldn't exist anymore, we are taking
        #    the tag strings and transactions from when they did..
        res = thl_lm.get_txs_by_tags(tags=tags)
        assert len(res) == 0

        tx_metadata_ids = thl_lm.get_tx_metadata_ids_by_txs(transactions=transactions)
        assert len(tx_metadata_ids) == 0
        tx_entries = thl_lm.get_tx_entries_by_txs(transactions=transactions)
        assert len(tx_entries) == 0

    def test_recoup_empty(self, business_payout_event_manager):
        res = {uuid4().hex: USDCent(0) for i in range(100)}
        df = pd.DataFrame.from_dict(res, orient="index").reset_index()
        df.columns = ["product_id", "available_balance"]

        with pytest.raises(expected_exception=ValueError) as cm:
            business_payout_event_manager.recoup_proportional(
                df=df, target_amount=USDCent(1)
            )
        assert "Total available amount is empty, cannot recoup" in str(cm)

    def test_recoup_exceeds(self, business_payout_event_manager):
        from random import randint

        res = {uuid4().hex: USDCent(randint(a=0, b=1_000_00)) for i in range(100)}
        df = pd.DataFrame.from_dict(res, orient="index").reset_index()
        df.columns = ["product_id", "available_balance"]

        avail_balance = USDCent(int(df.available_balance.sum()))

        with pytest.raises(expected_exception=ValueError) as cm:
            business_payout_event_manager.recoup_proportional(
                df=df, target_amount=avail_balance + USDCent(1)
            )
        assert " exceeds total available " in str(cm)

    def test_recoup(self, business_payout_event_manager):
        from random import randint, random

        res = {uuid4().hex: USDCent(randint(a=0, b=1_000_00)) for i in range(100)}
        df = pd.DataFrame.from_dict(res, orient="index").reset_index()
        df.columns = ["product_id", "available_balance"]

        avail_balance = USDCent(int(df.available_balance.sum()))
        random_recoup_amount = USDCent(1 + int(int(avail_balance) * random() * 0.5))

        res = business_payout_event_manager.recoup_proportional(
            df=df, target_amount=random_recoup_amount
        )

        assert isinstance(res, pd.DataFrame)
        assert res.weight.sum() == pytest.approx(1)
        assert res.deduction.sum() == random_recoup_amount
        assert res.remaining_balance.sum() == avail_balance - random_recoup_amount

    def test_recoup_loop(self, business_payout_event_manager, request):
        # TODO: Generate this file at random
        fp = os.path.join(
            request.config.rootpath, "data/pytest_recoup_proportional.csv"
        )
        df = pd.read_csv(fp, index_col=0)
        res = business_payout_event_manager.recoup_proportional(
            df=df, target_amount=USDCent(1416089)
        )

        res = res[res["remaining_balance"] > 0]

        assert int(res.deduction.sum()) == 1416089

    def test_recoup_loop_single_profitable_account(self, business_payout_event_manager):
        res = [{"product_id": uuid4().hex, "available_balance": 0} for i in range(1000)]
        for x in range(100):
            item = rand_choice(res)
            item["available_balance"] = randint(8, 12)

        df = pd.DataFrame(res)
        res = business_payout_event_manager.recoup_proportional(
            df=df, target_amount=USDCent(500)
        )
        # res = res[res["remaining_balance"] > 0]
        assert int(res.deduction.sum()) == 500

    def test_recoup_loop_assertions(self, business_payout_event_manager):
        df = pd.DataFrame(
            [
                {
                    "product_id": uuid4().hex,
                    "available_balance": randint(0, 999_999),
                }
                for i in range(10_000)
            ]
        )
        available_balance = int(df.available_balance.sum())

        # Exact amount
        res = business_payout_event_manager.recoup_proportional(
            df=df, target_amount=available_balance
        )
        assert res.remaining_balance.sum() == 0
        assert int(res.deduction.sum()) == available_balance

        # Slightly less
        res = business_payout_event_manager.recoup_proportional(
            df=df, target_amount=available_balance - 1
        )
        assert res.remaining_balance.sum() == 1
        assert int(res.deduction.sum()) == available_balance - 1

        # Slightly less
        with pytest.raises(expected_exception=Exception) as cm:
            res = business_payout_event_manager.recoup_proportional(
                df=df, target_amount=available_balance + 1
            )

        # Don't pull anything
        res = business_payout_event_manager.recoup_proportional(df=df, target_amount=0)
        assert res.remaining_balance.sum() == available_balance
        assert int(res.deduction.sum()) == 0

    def test_distribute_amount(self, business_payout_event_manager):
        import io

        df = pd.read_csv(
            io.StringIO(
                "product_id,available_balance,weight,raw_deduction,deduction,remainder,remaining_balance\n0,15faf,11768,0.0019298788807489663,222.3374860933269,223,0.33748609332690194,11545\n5,793e3,202,3.312674489388946e-05,3.8164660257352168,3,0.8164660257352168,199\n6,c703b,22257,0.0036500097084321667,420.5103184890531,421,0.510318489053077,21836\n13,72a70,1424,0.00023352715212326036,26.90419614181658,27,0.9041961418165805,1397\n14,86173,45634,0.007483692457860156,862.1812406851528,863,0.18124068515282943,44771\n17,4f230,143676,0.02356197128403199,2714.5275876907576,2715,0.5275876907576276,140961\n18,e1ee6,129,2.1155198471840298e-05,2.437248105543777,2,0.43724810554377713,127\n22,4524a,85613,0.014040000052478012,1617.5203260458868,1618,0.5203260458868044,83995\n25,4f5e2,30282,0.004966059845924558,572.1298227292765,573,0.12982272927649774,29709\n28,0f3b3,135,2.213916119146078e-05,2.5506084825458135,2,0.5506084825458135,133\n29,15c6f,1226,0.00020105638237578454,23.163303700749385,23,0.16330370074938472,1203\n31,c0c04,376,6.166166376288335e-05,7.103916958794265,7,0.10391695879426521,369\n33,2934c,37649,0.006174202071831903,711.3174722916099,712,0.31747229160987445,36937\n38,5585d,16471,0.0027011416591448184,311.1931282667562,312,0.19312826675621864,16159\n42,0a749,663,0.00010872788051806293,12.526321658724994,12,0.5263216587249939,651\n43,9e336,322,5.280599928629904e-05,6.08367356577594,6,0.08367356577593998,316\n46,043a5,11656,0.001911511576649384,220.22142572262223,221,0.2214257226222287,11435\n47,d6f4e,39,6.3957576775331136e-06,0.7368424505132349,0,0.7368424505132349,39\n48,3e123,3012,0.0004939492852494804,56.90690925502214,57,0.9069092550221427,2955\n49,76ccc,76,1.24635277818594e-05,1.4358981086924578,1,0.4358981086924578,75\n51,8acb9,7710,0.0012643920947123155,145.66808444761645,146,0.6680844476164509,7564\n56,fef3e,212,3.476668275992359e-05,4.005399987405277,4,0.005399987405277251,208\n57,6d7c2,455709,0.07473344449925481,8609.890673870148,8610,0.8906738701480208,447099\n58,f51f6,257,4.2146403157077186e-05,4.855602814920548,4,0.8556028149205481,253\n61,06acf,84310,0.013826316148533765,1592.902230840278,1593,0.90223084027798,82717\n62,6eca7,40,6.559751464136527e-06,0.755735846680241,0,0.755735846680241,40\n68,4a415,1955,0.00032060785280967275,36.93658950649678,37,0.9365895064967802,1918\n69,57a16,409,6.707345872079598e-05,7.727399032305463,7,0.7273990323054633,402\n70,c3ef6,593,9.724831545582401e-05,11.203783927034571,11,0.20378392703457138,582\n71,385da,825,0.00013529487394781586,15.587051837779969,15,0.5870518377799687,810\n72,a8435,748,0.00012266735237935304,14.132260332920506,14,0.13226033292050587,734\n75,c9374,263383,0.043193175496966774,4976.199362654548,4977,0.19936265454816748,258406\n76,7fcc7,136,2.230315497806419e-05,2.569501878712819,2,0.5695018787128192,134\n77,26aec,356,5.838178803081509e-05,6.726049035454145,6,0.7260490354541451,350\n78,76cc5,413,6.772943386720964e-05,7.802972616973489,7,0.8029726169734888,406\n82,9476a,13973,0.002291485180209492,263.99742464157515,264,0.9974246415751509,13709\n85,ee099,2397,0.0003930931064883814,45.28747061231344,46,0.2874706123134416,2351\n87,24bd5,122295,0.020055620132664414,2310.567884244002,2311,0.5678842440020162,119984\n91,1fa8f,4,6.559751464136527e-07,0.0755735846680241,0,0.0755735846680241,4\n92,53b5a,1,1.6399378660341317e-07,0.018893396167006023,0,0.018893396167006023,1\n93,f4f9e,201,3.296275110728605e-05,3.797572629568211,3,0.7975726295682111,198\n95,ff5d7,21317,0.0034958555490249587,402.75052609206745,403,0.7505260920674459,20914\n96,6290d,80,1.3119502928273054e-05,1.511471693360482,1,0.5114716933604819,79\n100,9c34b,1870,0.0003066683809483826,35.33065083230127,36,0.33065083230126646,1834\n101,d32a9,11577,0.0018985560675077143,218.72884742542874,219,0.7288474254287394,11358\n102,a6001,8981,0.0014728281974852537,169.6815909758811,170,0.6815909758811074,8811\n106,0a1ee,9,1.4759440794307186e-06,0.17004056550305424,0,0.17004056550305424,9\n108,8ad36,51,8.363683116774072e-06,0.9635632045173074,0,0.9635632045173074,51\n111,389ab,75,1.2299533995255988e-05,1.417004712525452,1,0.4170047125254519,74\n114,be86b,4831,0.000792253983081089,91.2739968828061,92,0.27399688280610235,4739\n118,99e96,271,4.444231616952497e-05,5.120110361258632,5,0.12011036125863228,266\n121,3b729,12417,0.0020363108482545815,234.59930020571383,235,0.5993002057138312,12182\n122,f16e2,2697,0.0004422912424694053,50.95548946241525,51,0.955489462415251,2646\n125,241c5,2,3.2798757320682633e-07,0.03778679233401205,0,0.03778679233401205,2\n127,523af,569,9.33124645773421e-05,10.750342419026428,10,0.7503424190264276,559\n130,dce2e,31217,0.005119394036398749,589.7951481454271,590,0.7951481454271061,30627\n133,214b8,37360,0.006126807867503516,705.8572807993451,706,0.8572807993450624,36654\n136,03c88,35,5.739782531119461e-06,0.6612688658452109,0,0.6612688658452109,35\n137,08021,44828,0.007351513465857806,846.9531633745461,847,0.9531633745460795,43981\n144,bc3d9,1174,0.00019252870547240705,22.180847100065073,22,0.18084710006507265,1152\n148,bac2f,3745,0.0006141567308297824,70.75576864543757,71,0.7557686454375698,3674\n150,d9e69,8755,0.0014357656017128823,165.41168344213776,166,0.41168344213775754,8589\n151,36b18,1,1.6399378660341317e-07,0.018893396167006023,0,0.018893396167006023,1\n152,e15d7,5259,0.0008624433237473498,99.36037044228468,100,0.3603704422846761,5159\n155,bad23,16504,0.002706553454102731,311.81661034026746,312,0.8166103402674594,16192\n159,9f77f,2220,0.00036406620625957726,41.943339490753374,42,0.9433394907533739,2178\n160,945ab,131,2.1483186045047126e-05,2.4750348978777894,2,0.4750348978777894,129\n161,3e2dc,354,5.8053800457608265e-05,6.688262243120133,6,0.6882622431201328,348\n162,62811,1608,0.0002637020088582884,30.38058103654569,31,0.38058103654568853,1577\n164,59a3d,2541,0.0004167082117592729,48.00811966036231,49,0.008119660362311265,2492\n165,871ca,8,1.3119502928273053e-06,0.1511471693360482,0,0.1511471693360482,8\n167,55d38,683,0.00011200775625013119,12.904189582065115,12,0.9041895820651149,671\n169,1e5af,6,9.83962719620479e-07,0.11336037700203613,0,0.11336037700203613,6\n170,b2901,15994,0.0026229166229349904,302.18097829509435,303,0.1809782950943486,15691\n174,dc880,2435,0.00039932487037931105,46.00541966665967,47,0.005419666659669531,2388\n176,d189e,34579,0.005670741146959424,653.3147460589013,654,0.31474605890127805,33925\n177,d35f5,41070,0.006735224815802179,775.9517805789375,776,0.9517805789374734,40294\n178,bd7a6,12563,0.0020602539410986796,237.35773604609668,238,0.35773604609667586,12325\n180,662cd,9675,0.0015866398853880224,182.79360791578327,183,0.7936079157832694,9492\n181,e995f,1011,0.00016579771825605072,19.101223524843093,19,0.1012235248430926,992\n189,0faf8,626,0.00010266011041373665,11.827266000545771,11,0.8272660005457713,615\n190,1fd84,213,3.4930676546527005e-05,4.024293383572283,4,0.02429338357228339,209\n193,c9b44,12,1.967925439240958e-06,0.22672075400407227,0,0.22672075400407227,12\n194,bbd8d,16686,0.0027364003232645522,315.25520844266254,316,0.2552084426625356,16370\n196,d4945,2654,0.00043523950964545856,50.14307342723399,51,0.1430734272339933,2603\n197,bbde8,3043,0.0004990330926341863,57.49260453619934,58,0.4926045361993374,2985\n200,579ad,20833,0.0034164825563089067,393.6061223472365,394,0.6061223472365214,20439\n202,2f932,15237,0.0024987733264762065,287.8786773966708,288,0.8786773966708097,14949\n208,b649d,103551,0.01698172059657004,1956.430066489641,1957,0.43006648964092165,101594\n209,0f939,26025,0.0042679382963538275,491.7006352463318,492,0.7006352463317853,25533\n211,638d8,6218,0.0010197133651000231,117.47913736644347,118,0.47913736644346727,6100\n215,d2a81,8301,0.0013613124225949327,156.834081582317,157,0.8340815823169976,8144\n216,62293,4,6.559751464136527e-07,0.0755735846680241,0,0.0755735846680241,4\n218,c8ae9,2829,0.00046393842230105583,53.44941775646004,54,0.44941775646004345,2775\n219,83a9f,6556,0.0010751432649719768,123.8651052708915,124,0.8651052708915046,6432\n221,d256a,72,1.1807552635445749e-05,1.360324524024434,1,0.36032452402443393,71\n222,6fdc2,7,1.1479565062238922e-06,0.1322537731690422,0,0.1322537731690422,7\n224,56b88,146928,0.02409527907806629,2775.9689120258613,2776,0.9689120258613002,144152\n230,f50f8,5798,0.0009508359747265895,109.54391097630092,110,0.5439109763009213,5688\n231,fa3be,2,3.2798757320682633e-07,0.03778679233401205,0,0.03778679233401205,2\n232,94934,537381,0.08812714503872877,10152.952125621865,10153,0.9521256218649796,527228\n234,4e20c,5,8.199689330170659e-07,0.09446698083503012,0,0.09446698083503012,5\n235,a5d68,31101,0.005100370757152753,587.6035141900543,588,0.603514190054284,30513\n236,e5a29,3208,0.0005260920674237494,60.61001490375533,61,0.610014903755328,3147\n237,0ce0f,294,4.821417326140347e-05,5.554658473099771,5,0.5546584730997708,289\n240,66d2b,18633,0.0030556962257813976,352.04065077982324,353,0.04065077982323828,18280\n244,1bd17,1815,0.0002976487226851949,34.29151404311594,35,0.29151404311593865,1780\n245,32aca,224,3.673460819916455e-05,4.23212074140935,4,0.23212074140935002,220\n247,cbf8e,4747,0.0007784785050064023,89.6869516047776,90,0.6869516047776045,4657\n249,8f24b,807633,0.13244679385587438,15258.930226547576,15259,0.9302265475762397,792374\n251,c97c3,20526,0.0033661364638216586,387.80584972396565,388,0.8058497239656504,20138\n252,88fee,13821,0.0022665581246457734,261.12562842419027,262,0.12562842419026765,13559\n253,a9ad3,178,2.9190894015407545e-05,3.3630245177270726,3,0.36302451772707256,175\n254,83738,104,1.705535380675497e-05,1.9649132013686266,1,0.9649132013686266,103\n255,21f6c,6288,0.001031192930162262,118.8016750981339,119,0.8016750981338987,6169\n256,97e28,6,9.83962719620479e-07,0.11336037700203613,0,0.11336037700203613,6\n257,7f689,39,6.3957576775331136e-06,0.7368424505132349,0,0.7368424505132349,39\n258,e7a50,28031,0.004596909832280275,529.6007879573459,530,0.600787957345915,27501\n259,2eb98,1,1.6399378660341317e-07,0.018893396167006023,0,0.018893396167006023,1\n260,349ba,19518,0.0032008307269254183,368.76130638762356,369,0.7613063876235628,19149\n261,a3d04,235,3.853853985180209e-05,4.439948099246416,4,0.43994809924641576,231\n262,40971,2249,0.0003688220260710762,42.49124797959655,43,0.491247979596551,2206\n264,4f588,6105,0.0010011820672138373,115.34418359957178,116,0.34418359957177813,5989\n269,f182e,1020,0.00016727366233548145,19.271264090346147,19,0.27126409034614696,1001\n270,3798e,5168,0.0008475198891664393,97.64107139108714,98,0.641071391087138,5070\n274,81dc3,274,4.493429752933521e-05,5.176790549759651,5,0.1767905497596507,269\n285,8520b,2,3.2798757320682633e-07,0.03778679233401205,0,0.03778679233401205,2\n287,37b89,3742,0.0006136647494699721,70.69908845693654,71,0.6990884569365363,3671\n291,63706,4740,0.0007773305485001784,89.55469783160856,90,0.5546978316085642,4650\n293,21c9d,241,3.9522502571422574e-05,4.553308476248452,4,0.5533084762484517,237\n296,3a42a,610,0.00010003620982808204,11.524971661873675,11,0.5249716618736748,599\n302,31a8f,148533,0.02435848910556477,2806.292812873906,2807,0.2928128739058593,145726\n303,38467,2399,0.0003934210940615882,45.32525740464745,46,0.3252574046474521,2353\n304,a49a6,26573,0.004357806891412498,502.0542163458511,503,0.05421634585110269,26070\n305,3c4e7,2286,0.0003748897961754025,43.19030363777577,44,0.19030363777577008,2242\n306,63986,34132,0.005597435924347699,644.8693979722497,645,0.8693979722496579,33487\n307,640f4,50,8.199689330170658e-06,0.9446698083503012,0,0.9446698083503012,50\n309,ba81a,3015,0.0004944412666092907,56.96358944352316,57,0.963589443523162,2958\n310,b7e8e,11409,0.0018710051113583408,215.55475686937172,216,0.5547568693717153,11193\n311,98301,5694,0.0009337806209198346,107.5789977749323,108,0.5789977749323043,5586\n312,e70bd,19,3.11588194546485e-06,0.35897452717311445,0,0.35897452717311445,19\n314,adf9c,5023,0.0008237407901089443,94.90152894687125,95,0.9015289468712524,4928\n316,ab5f4,55,9.019658263187724e-06,1.0391367891853314,1,0.039136789185331367,54\n318,d4a4c,2242,0.0003676740695648523,42.358994206427504,43,0.35899420642750357,2199\n319,b4062,222,3.640662062595773e-05,4.194333949075338,4,0.19433394907533774,218\n321,cb691,12,1.967925439240958e-06,0.22672075400407227,0,0.22672075400407227,12\n322,b83ed,3716,0.0006094009110182833,70.20786015659439,71,0.20786015659439272,3645\n323,8759e,5964,0.0009780589433027562,112.68021474002393,113,0.6802147400239278,5851\n325,2f217,3625,0.0005944774764373728,68.48856110539684,69,0.4885611053968404,3556\n326,d683f,28156,0.004617409055605701,531.9624624782216,532,0.962462478221596,27624\n327,97cf7,928,0.00015218623396796743,17.533071642981593,17,0.5330716429815929,911\n328,75135,2841,0.0004659063477402968,53.67613851046411,54,0.6761385104641136,2787\n329,d7c10,29913,0.0049055461386678986,565.1581595436512,566,0.15815954365120888,29347\n330,8598e,66,1.082358991582527e-05,1.2469641470223976,1,0.24696414702239755,65\n331,5e72b,10825,0.0017752327399819475,204.52101350784022,205,0.5210135078402232,10620\n332,30bff,975,0.00015989394193832783,18.42106126283087,18,0.42106126283087164,957\n333,f79d7,696,0.00011413967547597557,13.149803732236194,13,0.1498037322361938,683\n334,d0a0d,2121,0.0003478308213858393,40.07289327021978,41,0.07289327021977954,2080\n335,22ec5,3942,0.0006464635067906547,74.47776769033774,75,0.4777676903377426,3867\n336,efeac,515,8.445680010075779e-05,9.730099026008103,9,0.7300990260081033,506\n337,c3854,105882,0.017363990113142592,2000.4705729549316,2001,0.4705729549316402,103881\n338,cd2a3,42924,0.007039269296164907,810.9801370725665,811,0.9801370725665493,42113\n339,d7333,5089,0.0008345643800247697,96.14849309389366,97,0.14849309389366283,4992\n340,7d48c,33,5.411794957912635e-06,0.6234820735111988,0,0.6234820735111988,33\n341,6a148,100,1.6399378660341316e-05,1.8893396167006025,1,0.8893396167006025,99\n342,ba8ff,400,6.559751464136527e-05,7.55735846680241,7,0.5573584668024099,393\n343,38810,3194,0.0005237961544113017,60.34550735741724,61,0.34550735741724026,3133\n344,04d6e,43,7.051732823946766e-06,0.812416035181259,0,0.812416035181259,43\n345,d4f05,1394,0.00022860733852515797,26.337394256806398,27,0.3373942568063981,1367\n346,53f71,1325,0.00021729176724952245,25.033749921282983,26,0.0337499212829826,1299\n347,e4d06,60,9.83962719620479e-06,1.1336037700203614,1,0.1336037700203614,59\n348,8ab28,5,8.199689330170659e-07,0.09446698083503012,0,0.09446698083503012,5\n349,f21e0,65,1.0659596129221857e-05,1.2280707508553916,1,0.22807075085539164,64\n350,02fc2,2202,0.0003611143181007158,41.603258359747265,42,0.6032583597472652,2160\n351,310c0,18370,0.0030125658599047,347.07168758790067,348,0.07168758790066931,18022\n352,fceae,169,2.7714949935976826e-05,3.192983952224018,3,0.1929839522240182,166\n353,25c52,256,4.198240937047377e-05,4.836709418753542,4,0.836709418753542,252\n354,2e0cc,7029,0.0011527123260353911,132.80168165788535,133,0.8016816578853536,6896\n355,4baa9,405,6.641748357438233e-05,7.65182544763744,7,0.6518254476374397,398\n356,f2cb1,2236,0.00036669010684523185,42.24563382942547,43,0.24563382942547207,2193\n357,c4f9f,69,1.1315571275635508e-05,1.3036443355234157,1,0.30364433552341574,68\n358,0fe2b,33,5.411794957912635e-06,0.6234820735111988,0,0.6234820735111988,33\n359,ba2cc,911,0.0001493983395957094,17.21188390814249,17,0.21188390814248947,894\n360,22c5c,84,1.3775478074686707e-05,1.587045278028506,1,0.587045278028506,83\n361,b0369,42,6.8877390373433535e-06,0.793522639014253,0,0.793522639014253,42\n362,a7d03,12344,0.002024339301832532,233.22008228552235,234,0.2200822855223521,12110\n363,f6d7e,11703,0.0019192192846197442,221.1094153424715,222,0.1094153424714932,11481\n364,c281d,50376,0.008261350993933542,951.7737253090955,952,0.7737253090955392,49424\n365,dfbd6,3932,0.0006448235689246206,74.2888337286677,75,0.2888337286676972,3857\n366,a34ba,3909,0.0006410517118327421,73.85428561682654,74,0.8542856168265445,3835\n367,157ee,253,4.149042801066353e-05,4.780029230252524,4,0.7800292302525236,249\n369,33a29,6954,0.0011404127920401352,131.3846769453599,132,0.3846769453598995,6822\n370,2bd95,11279,0.001849685919099897,213.09861536766095,214,0.09861536766095469,11065\n371,138b7,2094,0.00034340298914754716,39.56277157371061,40,0.5627715737106129,2054\n372,eea6f,3884,0.0006369518671676568,73.3819507126514,74,0.38195071265140257,3810\n373,8f39b,1046,0.00017153750078717018,19.7624923906883,19,0.7624923906883012,1027\n374,bacfb,773,0.00012676719704443837,14.604595237095657,14,0.6045952370956567,759\n375,23403,855,0.00014021468754591825,16.15385372279015,16,0.1538537227901493,839\n376,0ad32,1630,0.00026730987216356345,30.79623575221982,31,0.7962357522198182,1599\n377,6be35,21,3.4438695186716767e-06,0.3967613195071265,0,0.3967613195071265,21\n378,12402,2994,0.000490997397090619,56.566828124016034,57,0.566828124016034,2937\n379,bae61,40980,0.006720465375007872,774.251374923907,775,0.25137492390695115,40205\n380,384f7,23545,0.003861233705577363,444.84501275215683,445,0.8450127521568334,23100\n381,03ed6,115,1.8859285459392513e-05,2.1727405592056925,2,0.17274055920569253,113\n382,cc4c8,3529,0.0005787340729234451,66.67479507336427,67,0.6747950733642654,3462\n383,263fa,4019,0.0006590910283591176,75.93255919519721,76,0.9325591951972143,3943\n384,704a1,11504,0.001886584521085665,217.3496295052373,218,0.34962950523728864,11286\n385,a0efb,7960,0.0013053905413631687,150.39143348936796,151,0.3914334893679552,7809\n386,b3197,981,0.00016087790465794832,18.53442163983291,18,0.5344216398329102,963\n387,1c91d,174,2.8534918868993892e-05,3.2874509330590485,3,0.28745093305904845,171\n388,1afff,797,0.0001307030479229203,15.058036745103802,15,0.05803674510380219,782\n389,20304,10605,0.0017391541069291968,200.3644663510989,201,0.3644663510989119,10404\n390,638fd,79,1.295550914166964e-05,1.4925782971934758,1,0.4925782971934758,78\n391,8c258,11,1.8039316526375448e-06,0.20782735783706627,0,0.20782735783706627,11\n392,c1847,20,3.2798757320682634e-06,0.3778679233401205,0,0.3778679233401205,20\n393,d72ec,2445,0.0004009648082453452,46.19435362832973,47,0.19435362832972913,2398\n394,d83e3,60,9.83962719620479e-06,1.1336037700203614,1,0.1336037700203614,59\n395,04a4f,60,9.83962719620479e-06,1.1336037700203614,1,0.1336037700203614,59\n396,94c2e,52,8.527676903377485e-06,0.9824566006843133,0,0.9824566006843133,52\n397,2fc45,8,1.3119502928273053e-06,0.1511471693360482,0,0.1511471693360482,8\n398,e7986,30,4.919813598102395e-06,0.5668018850101807,0,0.5668018850101807,30\n399,612a5,20340,0.003335633619513424,384.2916780369025,385,0.2916780369025105,19955\n400,f9e3a,12,1.967925439240958e-06,0.22672075400407227,0,0.22672075400407227,12\n401,75d14,667,0.00010938385566447659,12.601895243393018,12,0.6018952433930185,655\n402,e02b6,86,1.4103465647893532e-05,1.624832070362518,1,0.6248320703625181,85\n403,b4c46,479,7.855302378303491e-05,9.049936763995886,9,0.04993676399588587,470\n404,f9fb4,299,4.9034142194420536e-05,5.6491254539348015,5,0.6491254539348015,294\n405,08461,34,5.575788744516048e-06,0.6423754696782048,0,0.6423754696782048,34\n406,42032,124,2.0335229538823232e-05,2.342781124708747,2,0.3427811247087469,122\n407,b7ea6,2861,0.0004691862234723651,54.05400643380424,55,0.05400643380423986,2806\n408,cf91f,12,1.967925439240958e-06,0.22672075400407227,0,0.22672075400407227,12\n409,64f3d,11,1.8039316526375448e-06,0.20782735783706627,0,0.20782735783706627,11\n410,49d78,408,6.690946493419258e-05,7.708505636138459,7,0.708505636138459,401\n411,aa802,3907,0.0006407237242595352,73.81649882449253,74,0.8164988244925269,3833\n412,7feff,450,7.379720397153592e-05,8.50202827515271,8,0.5020282751527105,442\n413,08bc7,12,1.967925439240958e-06,0.22672075400407227,0,0.22672075400407227,12\n414,c7cad,16,2.6239005856546107e-06,0.3022943386720964,0,0.3022943386720964,16\n415,ec21a,84,1.3775478074686707e-05,1.587045278028506,1,0.587045278028506,83\n416,a1ba3,5,8.199689330170659e-07,0.09446698083503012,0,0.09446698083503012,5\n417,7f276,240,3.935850878481916e-05,4.534415080081446,4,0.5344150800814456,236\n419,e86cf,188,3.0830831881441676e-05,3.5519584793971326,3,0.5519584793971326,185\n420,4f7b0,14,2.2959130124477845e-06,0.2645075463380844,0,0.2645075463380844,14\n421,c61f3,363,5.952974453703898e-05,6.858302808623187,6,0.8583028086231872,357\n422,0c672,32,5.247801171309221e-06,0.6045886773441927,0,0.6045886773441927,32\n423,9f766,14028,0.00230050483847268,265.0365614307605,266,0.03656143076051421,13762\n424,2f990,4092,0.0006710625747811667,77.31177711538865,78,0.3117771153886508,4014\n425,660cb,1043,0.00017104551942735994,19.705812202187285,19,0.7058122021872855,1024\n426,0045c,81,1.3283496714876466e-05,1.5303650895274878,1,0.5303650895274878,80\n427,934e9,818,0.00013414691744159196,15.454798064610927,15,0.45479806461092664,803\n428,820eb,21,3.4438695186716767e-06,0.3967613195071265,0,0.3967613195071265,21\n429,8736b,14,2.2959130124477845e-06,0.2645075463380844,0,0.2645075463380844,14\n430,3de8b,18,2.9518881588614373e-06,0.3400811310061085,0,0.3400811310061085,18\n431,16140,21927,0.0035958917588530407,414.2754977539411,415,0.2754977539411243,21512\n432,44e80,17490,0.0028682513276936964,330.44549896093537,331,0.44549896093536745,17159\n434,c10c0,58575,0.009605936050294927,1106.680680482378,1107,0.6806804823779657,57468\n435,05214,129705,0.021270814091395706,2450.5679498415166,2451,0.5679498415165654,127254\n436,d70bb,130766,0.021444811498981926,2470.61384317471,2471,0.6138431747099276,128295\n437,a7eba,155,2.5419036923529042e-05,2.928476405885934,2,0.928476405885934,153\n438,2afa7,603,9.888825332185814e-05,11.392717888704633,11,0.39271788870463276,592\n439,ff2e5,1855,0.00030420847414933144,35.04724988979618,36,0.04724988979617706,1819\n440,b784f,148,2.4271080417305148e-05,2.7962226327168915,2,0.7962226327168915,146\n441,c2ce4,16469,0.0027008136715716115,311.15534147442224,312,0.15534147442224366,16157\n442,02ab5,491,8.052094922227586e-05,9.276657517999958,9,0.27665751799995775,482\n443,56013,65781,0.010787675276559121,1242.8264932618233,1243,0.8264932618233161,64538\n444,b4b2c,2429,0.0003983409076596906,45.89205928965763,46,0.8920592896576309,2383\n445,298a3,231,3.788256470538844e-05,4.364374514578392,4,0.3643745145783921,227\n446,3df28,3918,0.0006425276559121728,74.0243261823296,75,0.024326182329602375,3843\n447,99b56,594,9.741230924242742e-05,11.222677323201578,11,0.2226773232015784,583\n448,7c7e3,285,4.6738229181972755e-05,5.384617907596717,5,0.3846179075967173,280\n449,230a9,29334,0.004810593736224522,554.2188831629547,555,0.21888316295473942,28779\n450,11677,332788,0.05457516425617666,6287.4955236256,6288,0.4955236256000717,326500\n451,2e832,12,1.967925439240958e-06,0.22672075400407227,0,0.22672075400407227,12\n452,b961f,105,1.7219347593358382e-05,1.9838065975356325,1,0.9838065975356325,104\n453,caff0,2624,0.00043031969604735617,49.57627154222381,50,0.5762715422238074,2574\n454,c0a9c,76675,0.012574223587816704,1448.6511511051867,1449,0.6511511051867274,75226\n455,91d53,2145,0.00035176667226432124,40.52633477822792,41,0.5263347782279197,2104\n456,77494,1404,0.0002302472763911921,26.526328218476458,27,0.5263282184764577,1377\n457,9346a,160758,0.026363313146791495,3037.2645810155545,3038,0.2645810155545405,157720\n458,48d36,690,0.00011315571275635509,13.036443355234157,13,0.03644335523415698,677\n459,de96c,20110,0.003297915048594639,379.9461969184912,380,0.9461969184911823,19730\n460,1eca8,1577,0.00025861820147358256,29.7948857553685,30,0.7948857553685009,1547\n461,29179,1443,0.0002366430340687252,27.26317066898969,28,0.2631706689896909,1415\n462,f6416,12243,0.0020077759293855874,231.31184927265477,232,0.3118492726547686,12011\n463,eb5ab,24654,0.004043102814920548,465.79778910136656,466,0.7977891013665612,24188\n464,bb24e,275,4.5098291315938624e-05,5.195683945926657,5,0.19568394592665683,270\n465,089a1,1595,0.000261570089632444,30.13496688637461,31,0.13496688637460963,1564\n466,8f9ad,8142,0.00133523741052499,153.83003159176306,154,0.8300315917630599,7988\n467,01aa2,67287,0.011034649919183862,1271.2799478893344,1272,0.2799478893343803,66015\n468,b8427,76545,0.012552904395558262,1446.1950096034761,1447,0.19500960347613727,75098\n469,4ca48,20684,0.003392047482104998,390.7910063183526,391,0.7910063183525722,20293\n470,a6fe0,673,0.00011036781838409706,12.715255620395054,12,0.7152556203950535,661\n471,8131b,524,8.59327441801885e-05,9.900139591511158,9,0.9001395915111576,515\n472,9ed2f,2084,0.000341763051281513,39.37383761204055,40,0.3738376120405533,2044\n473,62851,47540,0.007796264615126262,898.1920537794664,899,0.19205377946639146,46641\n474,c8364,85303,0.013989161978630954,1611.663373234115,1612,0.6633732341149425,83691\n475,63dcc,5712,0.000936732509078696,107.9190789059384,108,0.9190789059384059,5604\n476,53490,53415,0.008759728111421314,1009.1907562606268,1010,0.19075626062681295,52405\n477,ea1ca,9513,0.0015600728919582694,179.7328777367283,180,0.7328777367283124,9333\n478,b98cd,86047,0.014111173355863893,1625.7200599823675,1626,0.720059982367502,84421\n479,5cc45,458,7.510915426436323e-05,8.65317544448876,8,0.6531754444887596,450\n480,935e4,1124,0.0001843290161422364,21.23617729171477,21,0.2361772917147711,1103\n481,25830,51,8.363683116774072e-06,0.9635632045173074,0,0.9635632045173074,51\n482,e1bce,401,6.576150842796868e-05,7.576251862969416,7,0.576251862969416,394\n483,522f3,25806,0.00423202365708768,487.5629814857574,488,0.5629814857574047,25318\n484,a091d,297,4.870615462121371e-05,5.611338661600789,5,0.6113386616007892,292\n485,5d15a,3101,0.0005085447322571842,58.588421513885685,59,0.5884215138856845,3042\n486,3807a,606,9.938023468166839e-05,11.449398077205652,11,0.44939807720565206,595\n487,6fc74,14406,0.00236249448980877,272.1782651818888,273,0.17826518188877571,14133\n488,c4b61,186,3.0502844308234848e-05,3.5141716870631203,3,0.5141716870631203,183\n489,1d557,935,0.0001533341904741913,17.665325416150633,17,0.6653254161506332,918\n490,4810f,267,4.378634102311132e-05,5.044536776590609,5,0.04453677659060862,262\n492,20171,1303,0.00021368390394424736,24.61809520560885,24,0.6180952056088493,1279\n493,645a8,507,8.314484980793048e-05,9.578951856672054,9,0.5789518566720542,498\n494,32855,5841,0.0009578877075505363,110.3563270114822,111,0.35632701148219326,5730\n495,9f0b8,1978,0.00032437970990155125,37.37113761833792,38,0.3711376183379187,1940\n496,bfa97,171,2.804293750918365e-05,3.23077074455803,3,0.23077074455803004,168\n497,c7ae8,39,6.3957576775331136e-06,0.7368424505132349,0,0.7368424505132349,39\n498,0443b,107,1.754733516656521e-05,2.0215933898696448,2,0.021593389869644763,105\n499,8b6d1,36,5.9037763177228745e-06,0.680162262012217,0,0.680162262012217,36\n500,0df28,803,0.00013168701064254076,15.171397122105837,15,0.17139712210583724,788\n501,b8da7,1772,0.00029059698986124815,33.479098007934674,34,0.47909800793467383,1738\n502,be111,30,4.919813598102395e-06,0.5668018850101807,0,0.5668018850101807,30\n503,fff89,160,2.6239005856546107e-05,3.022943386720964,3,0.022943386720963854,157\n504,69f64,6756,0.0011079420222926595,127.64378450429271,128,0.6437845042927108,6628\n505,cd02f,1582,0.00025943817040659966,29.889352736203534,30,0.8893527362035343,1552\n506,49604,1205,0.00019761251285711287,22.76654238124226,22,0.7665423812422603,1183\n507,f5741,2674,0.0004385193853775268,50.52094135057411,51,0.5209413505741125,2623\n508,35c93,984,0.00016136988601775856,18.59110182833393,18,0.5911018283339295,966\n509,7af3c,15579,0.0025548592014945737,294.34021888578684,295,0.34021888578683956,15284\n510,b061f,733,0.00012020744558030186,13.848859390415416,13,0.8488593904154165,720\n511,10963,255,4.181841558387036e-05,4.817816022586537,4,0.8178160225865367,251\n512,7429f,596,9.774029681563425e-05,11.26046411553559,11,0.2604641155355907,585\n513,8e06f,114,1.86952916727891e-05,2.153847163038687,2,0.15384716303868684,112\n514,adfd4,10604,0.0017389901131425933,200.3455729549319,201,0.345572954931896,10403\n515,f5fd5,4215,0.0006912338105333865,79.63566484393039,80,0.6356648439303854,4135\n516,565d2,285,4.6738229181972755e-05,5.384617907596717,5,0.3846179075967173,280\n517,8f6cf,119,1.9515260605806166e-05,2.2483141438737166,2,0.24831414387371664,117\n518,4f72e,3024,0.0004959172106887215,57.13363000902623,58,0.13363000902622701,2966\n519,d9867,1147,0.00018810087323411492,21.670725403555913,21,0.6707254035559131,1126\n520,2c9f1,82,1.344749050147988e-05,1.549258485694494,1,0.549258485694494,81\n521,dccb0,2004,0.00032864354835324,37.86236591868007,38,0.8623659186800694,1966\n522,6e283,2322,0.00038079357249312537,43.87046589978799,44,0.8704658997879875,2278\n523,43593,923,0.00015136626503495037,17.438604662146563,17,0.43860466214656313,906\n524,17b6e,903,0.0001480863893028821,17.06073673880644,17,0.06073673880644037,886\n525,72ae9,357,5.85457818174185e-05,6.74494243162115,6,0.7449424316211504,351\n526,d902d,873,0.0001431665757047797,16.49393485379626,16,0.49393485379626156,857\n527,64690,318,5.215002413988539e-05,6.008099981107916,6,0.00809998110791632,312\n528,f476f,25,4.099844665085329e-06,0.4723349041751506,0,0.4723349041751506,25\n529,82153,127,2.0827210898633473e-05,2.3994613132097653,2,0.3994613132097653,125\n530,de876,128,2.0991204685236885e-05,2.418354709376771,2,0.418354709376771,126\n531,0f108,641,0.00010512001721278785,12.110666943050862,12,0.11066694305086244,629\n532,c4fd2,1020,0.00016727366233548145,19.271264090346147,19,0.27126409034614696,1001\n533,5a3a4,147,2.4107086630701735e-05,2.7773292365498854,2,0.7773292365498854,145\n534,59b0c,157,2.5747024496735867e-05,2.966263198219946,2,0.9662631982199459,155\n535,1b5c6,24,3.935850878481916e-06,0.45344150800814453,0,0.45344150800814453,24\n536,433d0,129,2.1155198471840298e-05,2.437248105543777,2,0.43724810554377713,127\n537,69d01,1266,0.00020761613383992107,23.919039547429627,23,0.9190395474296267,1243\n538,d05da,277,4.542627888914545e-05,5.233470738260669,5,0.2334707382606691,272\n539,c552a,924,0.00015153025882155377,17.45749805831357,17,0.4574980583135684,907\n540,ede77,196,3.214278217426898e-05,3.703105648733181,3,0.7031056487331808,193\n541,5fb34,336,5.510191229874683e-05,6.348181112114024,6,0.34818111211402414,330\n542,3c8b0,303,4.969011734083419e-05,5.724699038602826,5,0.724699038602826,298\n543,69faf,48,7.871701756963832e-06,0.9068830160162891,0,0.9068830160162891,48\n544,4282e,40,6.559751464136527e-06,0.755735846680241,0,0.755735846680241,40\n545,c8c9d,21,3.4438695186716767e-06,0.3967613195071265,0,0.3967613195071265,21\n"
            )
        )

        for x in range(100_01, df.remaining_balance.sum(), 12345):
            amount = USDCent(x)
            res = business_payout_event_manager.distribute_amount(df=df, amount=amount)
            assert isinstance(res, pd.Series)
            assert res.sum() == amount

    def test_ach_payment_min_amount(
        self,
        product,
        mnt_filepath,
        thl_lm,
        client_no_amm,
        thl_redis_config,
        payout_event_manager,
        brokerage_product_payout_event_manager,
        business_payout_event_manager,
        delete_ledger_db,
        create_main_accounts,
        delete_df_collection,
        ledger_collection,
        business,
        user_factory,
        product_factory,
        session_with_tx_factory,
        pop_ledger_merge,
        start,
        bp_payout_factory,
        adj_to_fail_with_tx_factory,
        thl_web_rr,
        lm,
        product_manager,
    ):
        """Test having a Business with three products.. one that lost money
        and two that gained money. Ensure that the Business balance
        reflects that to compensate for the Product in the negative and only
        assigns Brokerage Product payments from the 2 accounts that have
        a positive balance.
        """
        # Now let's load it up and actually test some things
        delete_ledger_db()
        create_main_accounts()
        delete_df_collection(coll=ledger_collection)

        from generalresearch.models.thl.product import Product
        from generalresearch.models.thl.user import User

        p1: Product = product_factory(business=business)
        u1: User = user_factory(product=p1)
        thl_lm.get_account_or_create_bp_wallet(product=p1)

        session_with_tx_factory(
            user=u1,
            wall_req_cpi=Decimal("5.00"),
            started=start + timedelta(days=1),
        )
        session_with_tx_factory(
            user=u1,
            wall_req_cpi=Decimal("5.00"),
            started=start + timedelta(days=6),
        )
        payout_event_manager.set_account_lookup_table(thl_lm=thl_lm)
        bp_payout_factory(
            product=u1.product,
            amount=USDCent(475),  # 95% of $5.00
            created=start + timedelta(days=1, minutes=1),
        )

        ledger_collection.initial_load(client=None, sync=True)
        pop_ledger_merge.build(client=client_no_amm, ledger_coll=ledger_collection)
        business.prebuild_balance(
            thl_pg_config=thl_web_rr,
            lm=lm,
            ds=mnt_filepath,
            client=client_no_amm,
            pop_ledger=pop_ledger_merge,
        )

        with pytest.raises(expected_exception=AssertionError) as cm:
            business_payout_event_manager.create_from_ach_or_wire(
                business=business,
                amount=USDCent(500),
                pm=product_manager,
                thl_lm=thl_lm,
            )
        assert "Must issue Supplier Payouts at least $100 minimum." in str(cm)

    def test_ach_payment(
        self,
        product,
        mnt_filepath,
        thl_lm,
        client_no_amm,
        thl_redis_config,
        payout_event_manager,
        brokerage_product_payout_event_manager,
        business_payout_event_manager,
        delete_ledger_db,
        create_main_accounts,
        delete_df_collection,
        ledger_collection,
        business,
        user_factory,
        product_factory,
        session_with_tx_factory,
        pop_ledger_merge,
        start,
        bp_payout_factory,
        adj_to_fail_with_tx_factory,
        thl_web_rr,
        lm,
        product_manager,
        rm_ledger_collection,
        rm_pop_ledger_merge,
    ):
        """Test having a Business with three products.. one that lost money
        and two that gained money. Ensure that the Business balance
        reflects that to compensate for the Product in the negative and only
        assigns Brokerage Product payments from the 2 accounts that have
        a positive balance.
        """
        # Now let's load it up and actually test some things
        delete_ledger_db()
        create_main_accounts()
        delete_df_collection(coll=ledger_collection)

        from generalresearch.models.thl.product import Product
        from generalresearch.models.thl.user import User

        p1: Product = product_factory(business=business)
        p2: Product = product_factory(business=business)
        p3: Product = product_factory(business=business)
        u1: User = user_factory(product=p1)
        u2: User = user_factory(product=p2)
        u3: User = user_factory(product=p3)
        thl_lm.get_account_or_create_bp_wallet(product=p1)
        thl_lm.get_account_or_create_bp_wallet(product=p2)
        thl_lm.get_account_or_create_bp_wallet(product=p3)

        ach_id1 = uuid4().hex
        ach_id2 = uuid4().hex

        # Product 1: Complete, Payout, Recon..
        s1 = session_with_tx_factory(
            user=u1,
            wall_req_cpi=Decimal("5.00"),
            started=start + timedelta(days=1),
        )
        payout_event_manager.set_account_lookup_table(thl_lm=thl_lm)
        bp_payout_factory(
            product=u1.product,
            amount=USDCent(475),  # 95% of $5.00
            ext_ref_id=ach_id1,
            created=start + timedelta(days=1, minutes=1),
            skip_wallet_balance_check=True,
            skip_one_per_day_check=True,
        )
        adj_to_fail_with_tx_factory(
            session=s1,
            created=start + timedelta(days=1, minutes=2),
        )

        # Product 2: Complete x10
        for idx in range(15):
            session_with_tx_factory(
                user=u2,
                wall_req_cpi=Decimal("7.50"),
                started=start + timedelta(days=1, hours=2, minutes=1 + idx),
            )

        # Product 3: Complete x5
        for idx in range(10):
            session_with_tx_factory(
                user=u3,
                wall_req_cpi=Decimal("7.50"),
                started=start + timedelta(days=1, hours=3, minutes=1 + idx),
            )

        # Now that we paid out the business, let's confirm the updated balances
        ledger_collection.initial_load(client=None, sync=True)
        pop_ledger_merge.build(client=client_no_amm, ledger_coll=ledger_collection)
        business.prebuild_balance(
            thl_pg_config=thl_web_rr,
            lm=lm,
            ds=mnt_filepath,
            client=client_no_amm,
            pop_ledger=pop_ledger_merge,
        )

        bb1 = business.balance
        pb1 = bb1.product_balances[0]
        pb2 = bb1.product_balances[1]
        pb3 = bb1.product_balances[2]
        assert bb1.payout == 25 * 712 + 475  # $7.50 * .95% = $7.125 = $7.12
        assert bb1.adjustment == -475
        assert bb1.net == ((25 * 7.12 + 4.75) - 4.75) * 100
        # The balance is lower because the single $4.75 payout
        assert bb1.balance == bb1.net - 475
        assert (
            bb1.available_balance
            == (pb2.available_balance + pb3.available_balance) - 475
        )

        assert pb1.available_balance == 0
        assert pb2.available_balance == 8010
        assert pb3.available_balance == 5340

        assert bb1.recoup_usd_str == "$4.75"
        assert pb1.recoup_usd_str == "$4.75"
        assert pb2.recoup_usd_str == "$0.00"
        assert pb3.recoup_usd_str == "$0.00"

        assert business.payouts is None
        business.prebuild_payouts(
            thl_pg_config=thl_web_rr,
            thl_lm=thl_lm,
            bpem=business_payout_event_manager,
        )
        assert len(business.payouts) == 1
        assert business.payouts[0].ext_ref_id == ach_id1

        bp1 = business_payout_event_manager.create_from_ach_or_wire(
            business=business,
            amount=USDCent(bb1.available_balance),
            pm=product_manager,
            thl_lm=thl_lm,
            created=start + timedelta(days=1, hours=5),
        )
        assert isinstance(bp1, BusinessPayoutEvent)
        assert len(bp1.bp_payouts) == 2
        assert bp1.bp_payouts[0].status == PayoutStatus.COMPLETE
        assert bp1.bp_payouts[1].status == PayoutStatus.COMPLETE
        bp1_tx = brokerage_product_payout_event_manager.check_for_ledger_tx(
            thl_ledger_manager=thl_lm,
            payout_event=bp1.bp_payouts[0],
            product_id=bp1.bp_payouts[0].product_id,
            amount=bp1.bp_payouts[0].amount,
        )
        assert bp1_tx

        bp2_tx = brokerage_product_payout_event_manager.check_for_ledger_tx(
            thl_ledger_manager=thl_lm,
            payout_event=bp1.bp_payouts[1],
            product_id=bp1.bp_payouts[1].product_id,
            amount=bp1.bp_payouts[1].amount,
        )
        assert bp2_tx

        # Now that we paid out the business, let's confirm the updated balances
        rm_ledger_collection()
        rm_pop_ledger_merge()
        ledger_collection.initial_load(client=None, sync=True)
        pop_ledger_merge.build(client=client_no_amm, ledger_coll=ledger_collection)

        business.prebuild_balance(
            thl_pg_config=thl_web_rr,
            lm=lm,
            ds=mnt_filepath,
            client=client_no_amm,
            pop_ledger=pop_ledger_merge,
        )
        business.prebuild_payouts(
            thl_pg_config=thl_web_rr,
            thl_lm=thl_lm,
            bpem=business_payout_event_manager,
        )
        assert len(business.payouts) == 2
        assert len(business.payouts[0].bp_payouts) == 2
        assert len(business.payouts[1].bp_payouts) == 1

        bb2 = business.balance

        # Okay os we have the balance before, and after the Business Payout
        #    of bb1.available_balance worth..
        assert bb1.payout == bb2.payout
        assert bb1.adjustment == bb2.adjustment
        assert bb1.net == bb2.net
        assert bb1.available_balance > bb2.available_balance

        # This is the ultimate test. Confirm that the second time we get the
        #   Business balance, it is equal to the first time we Business balance
        #   minus the amount that was just paid out across any children
        #   Brokerage Products.
        #
        # This accounts for all the net positive and net negative children
        #   Brokerage Products under the Business in thi
        assert bb2.balance == bb1.balance - bb1.available_balance

    def test_ach_payment_partial_amount(
        self,
        product,
        mnt_filepath,
        thl_lm,
        client_no_amm,
        thl_redis_config,
        payout_event_manager,
        brokerage_product_payout_event_manager,
        business_payout_event_manager,
        delete_ledger_db,
        create_main_accounts,
        delete_df_collection,
        ledger_collection,
        business,
        user_factory,
        product_factory,
        session_with_tx_factory,
        pop_ledger_merge,
        start,
        bp_payout_factory,
        adj_to_fail_with_tx_factory,
        thl_web_rr,
        lm,
        product_manager,
        rm_ledger_collection,
        rm_pop_ledger_merge,
    ):
        """There are valid instances when we want issue a ACH or Wire to a
        Business, but not for the full Available Balance amount in their
        account.

        To test this, we'll create a Business with multiple Products, and
        a cumulative Available Balance of $100 (for example), but then only
        issue a payout of $75 (for example). We want to confirm the sum
        of the Product payouts equals the $75 number and isn't greedy and
        takes the full $100 amount that is available to the Business.

        """
        # Now let's load it up and actually test some things
        delete_ledger_db()
        create_main_accounts()
        delete_df_collection(coll=ledger_collection)

        from generalresearch.models.thl.product import Product
        from generalresearch.models.thl.user import User

        p1: Product = product_factory(business=business)
        p2: Product = product_factory(business=business)
        p3: Product = product_factory(business=business)
        u1: User = user_factory(product=p1)
        u2: User = user_factory(product=p2)
        u3: User = user_factory(product=p3)
        thl_lm.get_account_or_create_bp_wallet(product=p1)
        thl_lm.get_account_or_create_bp_wallet(product=p2)
        thl_lm.get_account_or_create_bp_wallet(product=p3)

        # Product 1, 2, 3: Complete, and Payout multiple times.
        for idx in range(5):
            for u in [u1, u2, u3]:
                session_with_tx_factory(
                    user=u,
                    wall_req_cpi=Decimal("50.00"),
                    started=start + timedelta(days=1, hours=2, minutes=1 + idx),
                )
        payout_event_manager.set_account_lookup_table(thl_lm=thl_lm)

        # Now that we paid out the business, let's confirm the updated balances
        ledger_collection.initial_load(client=None, sync=True)
        pop_ledger_merge.build(client=client_no_amm, ledger_coll=ledger_collection)
        business.prebuild_balance(
            thl_pg_config=thl_web_rr,
            lm=lm,
            ds=mnt_filepath,
            client=client_no_amm,
            pop_ledger=pop_ledger_merge,
        )
        business.prebuild_payouts(
            thl_pg_config=thl_web_rr,
            thl_lm=thl_lm,
            bpem=business_payout_event_manager,
        )

        # Confirm the initial amounts.
        assert len(business.payouts) == 0
        bb1 = business.balance
        assert bb1.payout == 3 * 5 * 4750
        assert bb1.adjustment == 0
        assert bb1.payout == bb1.net

        assert bb1.balance_usd_str == "$712.50"
        assert bb1.available_balance_usd_str == "$534.39"

        for x in range(2):
            assert bb1.product_balances[x].balance == 5 * 4750
            assert bb1.product_balances[x].available_balance_usd_str == "$178.13"

        assert business.payouts_total_str == "$0.00"
        assert business.balance.payment_usd_str == "$0.00"
        assert business.balance.available_balance_usd_str == "$534.39"

        # This is the important part, even those the Business has $534.39
        # available to it, we are only trying to issue out a $250.00 ACH or
        # Wire to the Business
        bp1 = business_payout_event_manager.create_from_ach_or_wire(
            business=business,
            amount=USDCent(250_00),
            pm=product_manager,
            thl_lm=thl_lm,
            created=start + timedelta(days=1, hours=3),
        )
        assert isinstance(bp1, BusinessPayoutEvent)
        assert len(bp1.bp_payouts) == 3

        # Now that we paid out the business, let's confirm the updated
        # balances. Clear and rebuild the parquet files.
        rm_ledger_collection()
        rm_pop_ledger_merge()
        ledger_collection.initial_load(client=None, sync=True)
        pop_ledger_merge.build(client=client_no_amm, ledger_coll=ledger_collection)

        # Now rebuild and confirm the payouts, balance.payment, and the
        #   balance.available_balance are reflective of having a $250 ACH/Wire
        #   sent to the Business
        business.prebuild_balance(
            thl_pg_config=thl_web_rr,
            lm=lm,
            ds=mnt_filepath,
            client=client_no_amm,
            pop_ledger=pop_ledger_merge,
        )
        business.prebuild_payouts(
            thl_pg_config=thl_web_rr,
            thl_lm=thl_lm,
            bpem=business_payout_event_manager,
        )
        assert len(business.payouts) == 1
        assert len(business.payouts[0].bp_payouts) == 3
        assert business.payouts_total_str == "$250.00"
        assert business.balance.payment_usd_str == "$250.00"
        assert business.balance.available_balance_usd_str == "$346.88"

    def test_ach_tx_id_reference(
        self,
        product,
        mnt_filepath,
        thl_lm,
        client_no_amm,
        thl_redis_config,
        payout_event_manager,
        brokerage_product_payout_event_manager,
        business_payout_event_manager,
        delete_ledger_db,
        create_main_accounts,
        delete_df_collection,
        ledger_collection,
        business,
        user_factory,
        product_factory,
        session_with_tx_factory,
        pop_ledger_merge,
        start,
        bp_payout_factory,
        adj_to_fail_with_tx_factory,
        thl_web_rr,
        lm,
        product_manager,
        rm_ledger_collection,
        rm_pop_ledger_merge,
    ):

        # Now let's load it up and actually test some things
        delete_ledger_db()
        create_main_accounts()
        delete_df_collection(coll=ledger_collection)

        from generalresearch.models.thl.product import Product
        from generalresearch.models.thl.user import User

        p1: Product = product_factory(business=business)
        p2: Product = product_factory(business=business)
        p3: Product = product_factory(business=business)
        u1: User = user_factory(product=p1)
        u2: User = user_factory(product=p2)
        u3: User = user_factory(product=p3)
        thl_lm.get_account_or_create_bp_wallet(product=p1)
        thl_lm.get_account_or_create_bp_wallet(product=p2)
        thl_lm.get_account_or_create_bp_wallet(product=p3)

        ach_id1 = uuid4().hex
        ach_id2 = uuid4().hex

        for idx in range(15):
            for iidx, u in enumerate([u1, u2, u3]):
                session_with_tx_factory(
                    user=u,
                    wall_req_cpi=Decimal("7.50"),
                    started=start + timedelta(days=1, hours=1 + iidx, minutes=1 + idx),
                )
        payout_event_manager.set_account_lookup_table(thl_lm=thl_lm)

        rm_ledger_collection()
        rm_pop_ledger_merge()
        ledger_collection.initial_load(client=None, sync=True)
        pop_ledger_merge.build(client=client_no_amm, ledger_coll=ledger_collection)
        business.prebuild_balance(
            thl_pg_config=thl_web_rr,
            lm=lm,
            ds=mnt_filepath,
            client=client_no_amm,
            pop_ledger=pop_ledger_merge,
        )

        bp1 = business_payout_event_manager.create_from_ach_or_wire(
            business=business,
            amount=USDCent(100_01),
            transaction_id=ach_id1,
            pm=product_manager,
            thl_lm=thl_lm,
            created=start + timedelta(days=2, hours=1),
        )

        rm_ledger_collection()
        rm_pop_ledger_merge()
        ledger_collection.initial_load(client=None, sync=True)
        pop_ledger_merge.build(client=client_no_amm, ledger_coll=ledger_collection)
        business.prebuild_balance(
            thl_pg_config=thl_web_rr,
            lm=lm,
            ds=mnt_filepath,
            client=client_no_amm,
            pop_ledger=pop_ledger_merge,
        )

        bp2 = business_payout_event_manager.create_from_ach_or_wire(
            business=business,
            amount=USDCent(100_02),
            transaction_id=ach_id2,
            pm=product_manager,
            thl_lm=thl_lm,
            created=start + timedelta(days=4, hours=1),
        )

        assert isinstance(bp1, BusinessPayoutEvent)
        assert isinstance(bp2, BusinessPayoutEvent)

        rm_ledger_collection()
        rm_pop_ledger_merge()
        ledger_collection.initial_load(client=None, sync=True)
        pop_ledger_merge.build(client=client_no_amm, ledger_coll=ledger_collection)
        business.prebuild_payouts(
            thl_pg_config=thl_web_rr,
            thl_lm=thl_lm,
            bpem=business_payout_event_manager,
        )
        business.prebuild_balance(
            thl_pg_config=thl_web_rr,
            lm=lm,
            ds=mnt_filepath,
            client=client_no_amm,
            pop_ledger=pop_ledger_merge,
        )
        assert business.payouts[0].ext_ref_id == ach_id2
        assert business.payouts[1].ext_ref_id == ach_id1
