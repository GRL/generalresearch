import os
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Optional
from uuid import uuid4

import pandas as pd
import pytest

# noinspection PyUnresolvedReferences
from distributed.utils_test import (
    gen_cluster,
    client_no_amm,
    loop,
    loop_in_thread,
    cleanup,
    cluster_fixture,
    client,
)
from pytest import approx

from generalresearch.currency import USDCent
from generalresearch.models.thl.finance import (
    ProductBalances,
    BusinessBalances,
)

# from test_utils.incite.conftest import mnt_filepath
from test_utils.managers.conftest import (
    business_bank_account_manager,
    lm,
    thl_lm,
)


class TestBusinessBankAccount:

    def test_init(self, business, business_bank_account_manager):
        from generalresearch.models.gr.business import (
            BusinessBankAccount,
            TransferMethod,
        )

        instance = business_bank_account_manager.create(
            business_id=business.id,
            uuid=uuid4().hex,
            transfer_method=TransferMethod.ACH,
        )
        assert isinstance(instance, BusinessBankAccount)

    def test_business(self, business_bank_account, business, gr_db, gr_redis_config):
        from generalresearch.models.gr.business import Business

        assert business_bank_account.business is None

        business_bank_account.prefetch_business(
            pg_config=gr_db, redis_config=gr_redis_config
        )
        assert isinstance(business_bank_account.business, Business)
        assert business_bank_account.business.uuid == business.uuid


class TestBusinessAddress:

    def test_init(self, business_address):
        from generalresearch.models.gr.business import BusinessAddress

        assert isinstance(business_address, BusinessAddress)


class TestBusinessContact:

    def test_init(self):
        from generalresearch.models.gr.business import BusinessContact

        bc = BusinessContact(name="abc", email="test@abc.com")
        assert isinstance(bc, BusinessContact)


class TestBusiness:
    @pytest.fixture
    def start(self) -> "datetime":
        return datetime(year=2018, month=3, day=14, hour=0, tzinfo=timezone.utc)

    @pytest.fixture
    def offset(self) -> str:
        return "30d"

    @pytest.fixture
    def duration(self) -> Optional["timedelta"]:
        return None

    def test_init(self, business):
        from generalresearch.models.gr.business import Business

        assert isinstance(business, Business)
        assert isinstance(business.id, int)
        assert isinstance(business.uuid, str)

    def test_str_and_repr(
        self,
        business,
        product_factory,
        thl_web_rr,
        lm,
        thl_lm,
        business_payout_event_manager,
        bp_payout_factory,
        start,
        user_factory,
        session_with_tx_factory,
        pop_ledger_merge,
        client_no_amm,
        ledger_collection,
        mnt_filepath,
        create_main_accounts,
    ):
        create_main_accounts()
        p1 = product_factory(business=business)
        u1 = user_factory(product=p1)
        p2 = product_factory(business=business)
        thl_lm.get_account_or_create_bp_wallet(product=p1)
        thl_lm.get_account_or_create_bp_wallet(product=p2)

        res1 = repr(business)

        assert business.uuid in res1
        assert "<Business: " in res1

        res2 = str(business)

        assert business.uuid in res2
        assert "Name:" in res2
        assert "Not Loaded" in res2

        business.prefetch_products(thl_pg_config=thl_web_rr)
        business.prefetch_bp_accounts(lm=lm, thl_pg_config=thl_web_rr)
        res3 = str(business)
        assert "Products: 2" in res3
        assert "Ledger Accounts: 2" in res3

        # -- need some tx to make these interesting
        business_payout_event_manager.set_account_lookup_table(thl_lm=thl_lm)
        session_with_tx_factory(
            user=u1,
            wall_req_cpi=Decimal("2.50"),
            started=start + timedelta(days=5),
        )
        bp_payout_factory(
            product=p1,
            amount=USDCent(50),
            created=start + timedelta(days=4),
            skip_wallet_balance_check=True,
            skip_one_per_day_check=True,
        )

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
        res4 = str(business)
        assert "Payouts: 1" in res4
        assert "Available Balance: 141" in res4

    def test_addresses(self, business, business_address, gr_db):
        from generalresearch.models.gr.business import BusinessAddress

        assert business.addresses is None

        business.prefetch_addresses(pg_config=gr_db)
        assert isinstance(business.addresses, list)
        assert len(business.addresses) == 1
        assert isinstance(business.addresses[0], BusinessAddress)

    def test_teams(self, business, team, team_manager, gr_db):
        assert business.teams is None

        business.prefetch_teams(pg_config=gr_db)
        assert isinstance(business.teams, list)
        assert len(business.teams) == 0

        team_manager.add_business(team=team, business=business)
        assert len(business.teams) == 0
        business.prefetch_teams(pg_config=gr_db)
        assert len(business.teams) == 1

    def test_products(self, business, product_factory, thl_web_rr):
        from generalresearch.models.thl.product import Product

        p1 = product_factory(business=business)
        assert business.products is None

        business.prefetch_products(thl_pg_config=thl_web_rr)
        assert isinstance(business.products, list)
        assert len(business.products) == 1
        assert isinstance(business.products[0], Product)

        assert business.products[0].uuid == p1.uuid

        # Add two more, but list is still one until we prefetch
        p2 = product_factory(business=business)
        p3 = product_factory(business=business)
        assert len(business.products) == 1

        business.prefetch_products(thl_pg_config=thl_web_rr)
        assert len(business.products) == 3

    def test_bank_accounts(self, business, business_bank_account, gr_db):
        assert business.products is None

        # It's an empty list after prefetch
        business.prefetch_bank_accounts(pg_config=gr_db)
        assert isinstance(business.bank_accounts, list)
        assert len(business.bank_accounts) == 1

    def test_balance(
        self,
        business,
        mnt_filepath,
        client_no_amm,
        thl_web_rr,
        lm,
        pop_ledger_merge,
    ):
        assert business.balance is None

        with pytest.raises(expected_exception=AssertionError) as cm:
            business.prebuild_balance(
                thl_pg_config=thl_web_rr,
                lm=lm,
                ds=mnt_filepath,
                client=client_no_amm,
                pop_ledger=pop_ledger_merge,
            )
        assert "Cannot build Business Balance" in str(cm.value)
        assert business.balance is None

        # TODO: Add parquet building so that this doesn't fail and we can
        #    properly assign a business.balance

    def test_payouts_no_accounts(
        self,
        business,
        product_factory,
        thl_web_rr,
        thl_lm,
        business_payout_event_manager,
    ):
        assert business.payouts is None

        with pytest.raises(expected_exception=AssertionError) as cm:
            business.prebuild_payouts(
                thl_pg_config=thl_web_rr,
                thl_lm=thl_lm,
                bpem=business_payout_event_manager,
            )
        assert "Must provide product_uuids" in str(cm.value)

        p = product_factory(business=business)
        thl_lm.get_account_or_create_bp_wallet(product=p)

        business.prebuild_payouts(
            thl_pg_config=thl_web_rr,
            thl_lm=thl_lm,
            bpem=business_payout_event_manager,
        )
        assert isinstance(business.payouts, list)
        assert len(business.payouts) == 0

    def test_payouts(
        self,
        business,
        product_factory,
        bp_payout_factory,
        thl_lm,
        thl_web_rr,
        business_payout_event_manager,
        create_main_accounts,
    ):
        create_main_accounts()
        p = product_factory(business=business)
        thl_lm.get_account_or_create_bp_wallet(product=p)
        business_payout_event_manager.set_account_lookup_table(thl_lm=thl_lm)

        bp_payout_factory(
            product=p, amount=USDCent(123), skip_wallet_balance_check=True
        )

        business.prebuild_payouts(
            thl_pg_config=thl_web_rr,
            thl_lm=thl_lm,
            bpem=business_payout_event_manager,
        )
        assert len(business.payouts) == 1
        assert sum([p.amount for p in business.payouts]) == 123

        # Add another!
        bp_payout_factory(
            product=p,
            amount=USDCent(123),
            skip_wallet_balance_check=True,
            skip_one_per_day_check=True,
        )
        business_payout_event_manager.set_account_lookup_table(thl_lm=thl_lm)
        business.prebuild_payouts(
            thl_pg_config=thl_web_rr,
            thl_lm=thl_lm,
            bpem=business_payout_event_manager,
        )
        assert len(business.payouts) == 1
        assert len(business.payouts[0].bp_payouts) == 2
        assert sum([p.amount for p in business.payouts]) == 246

    def test_payouts_totals(
        self,
        business,
        product_factory,
        bp_payout_factory,
        thl_lm,
        thl_web_rr,
        business_payout_event_manager,
        create_main_accounts,
    ):
        from generalresearch.models.thl.product import Product

        create_main_accounts()

        p1: Product = product_factory(business=business)
        thl_lm.get_account_or_create_bp_wallet(product=p1)
        business_payout_event_manager.set_account_lookup_table(thl_lm=thl_lm)

        bp_payout_factory(
            product=p1,
            amount=USDCent(1),
            skip_wallet_balance_check=True,
            skip_one_per_day_check=True,
        )

        bp_payout_factory(
            product=p1,
            amount=USDCent(25),
            skip_wallet_balance_check=True,
            skip_one_per_day_check=True,
        )

        bp_payout_factory(
            product=p1,
            amount=USDCent(50),
            skip_wallet_balance_check=True,
            skip_one_per_day_check=True,
        )

        business.prebuild_payouts(
            thl_pg_config=thl_web_rr,
            thl_lm=thl_lm,
            bpem=business_payout_event_manager,
        )

        assert len(business.payouts) == 1
        assert len(business.payouts[0].bp_payouts) == 3
        assert business.payouts_total == USDCent(76)
        assert business.payouts_total_str == "$0.76"

    def test_pop_financial(
        self,
        business,
        thl_web_rr,
        lm,
        mnt_filepath,
        client_no_amm,
        pop_ledger_merge,
    ):
        assert business.pop_financial is None
        business.prebuild_pop_financial(
            thl_pg_config=thl_web_rr,
            lm=lm,
            ds=mnt_filepath,
            client=client_no_amm,
            pop_ledger=pop_ledger_merge,
        )
        assert business.pop_financial == []

    def test_bp_accounts(self, business, lm, thl_web_rr, product_factory, thl_lm):
        assert business.bp_accounts is None
        business.prefetch_bp_accounts(lm=lm, thl_pg_config=thl_web_rr)
        assert business.bp_accounts == []

        from generalresearch.models.thl.product import Product

        p1: Product = product_factory(business=business)
        thl_lm.get_account_or_create_bp_wallet(product=p1)

        business.prefetch_bp_accounts(lm=lm, thl_pg_config=thl_web_rr)
        assert len(business.bp_accounts) == 1


class TestBusinessBalance:

    @pytest.fixture
    def start(self) -> "datetime":
        return datetime(year=2018, month=3, day=14, hour=0, tzinfo=timezone.utc)

    @pytest.fixture
    def offset(self) -> str:
        return "30d"

    @pytest.fixture
    def duration(self) -> Optional["timedelta"]:
        return None

    @pytest.mark.skip
    def test_product_ordering(self):
        # Assert that the order of business.balance.product_balances is always
        #   consistent and in the same order based off product.created ASC
        pass

    def test_single_product(
        self,
        business,
        product_factory,
        user_factory,
        mnt_filepath,
        bp_payout_factory,
        thl_lm,
        lm,
        duration,
        offset,
        start,
        thl_web_rr,
        payout_event_manager,
        session_with_tx_factory,
        delete_ledger_db,
        create_main_accounts,
        client_no_amm,
        ledger_collection,
        pop_ledger_merge,
        delete_df_collection,
    ):
        delete_ledger_db()
        create_main_accounts()
        delete_df_collection(coll=ledger_collection)

        from generalresearch.models.thl.product import Product
        from generalresearch.models.thl.user import User

        p1: Product = product_factory(business=business)
        u1: User = user_factory(product=p1)
        u2: User = user_factory(product=p1)

        session_with_tx_factory(
            user=u1,
            wall_req_cpi=Decimal(".75"),
            started=start + timedelta(days=1),
        )

        session_with_tx_factory(
            user=u2,
            wall_req_cpi=Decimal("1.25"),
            started=start + timedelta(days=2),
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
        assert isinstance(business.balance, BusinessBalances)
        assert business.balance.payout == 190
        assert business.balance.adjustment == 0
        assert business.balance.net == 190
        assert business.balance.retainer == 47
        assert business.balance.available_balance == 143

        assert len(business.balance.product_balances) == 1
        pb = business.balance.product_balances[0]
        assert isinstance(pb, ProductBalances)
        assert pb.balance == business.balance.balance
        assert pb.available_balance == business.balance.available_balance
        assert pb.adjustment_percent == 0.0

    def test_multi_product(
        self,
        business,
        product_factory,
        user_factory,
        mnt_filepath,
        bp_payout_factory,
        thl_lm,
        lm,
        duration,
        offset,
        start,
        thl_web_rr,
        payout_event_manager,
        session_with_tx_factory,
        delete_ledger_db,
        create_main_accounts,
        client_no_amm,
        ledger_collection,
        pop_ledger_merge,
        delete_df_collection,
    ):
        delete_ledger_db()
        create_main_accounts()
        delete_df_collection(coll=ledger_collection)

        from generalresearch.models.thl.user import User

        u1: User = user_factory(product=product_factory(business=business))
        u2: User = user_factory(product=product_factory(business=business))

        session_with_tx_factory(
            user=u1,
            wall_req_cpi=Decimal(".75"),
            started=start + timedelta(days=1),
        )

        session_with_tx_factory(
            user=u2,
            wall_req_cpi=Decimal("1.25"),
            started=start + timedelta(days=2),
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
        assert isinstance(business.balance, BusinessBalances)
        assert business.balance.payout == 190
        assert business.balance.balance == 190
        assert business.balance.adjustment == 0
        assert business.balance.net == 190
        assert business.balance.retainer == 46
        assert business.balance.available_balance == 144

        assert len(business.balance.product_balances) == 2

        pb1 = business.balance.product_balances[0]
        pb2 = business.balance.product_balances[1]
        assert isinstance(pb1, ProductBalances)
        assert pb1.product_id == u1.product_id
        assert isinstance(pb2, ProductBalances)
        assert pb2.product_id == u2.product_id

        for pb in [pb1, pb2]:
            assert pb.balance != business.balance.balance
            assert pb.available_balance != business.balance.available_balance
            assert pb.adjustment_percent == 0.0

        assert pb1.product_id in [u1.product_id, u2.product_id]
        assert pb1.payout == 71
        assert pb1.adjustment == 0
        assert pb1.expense == 0
        assert pb1.net == 71
        assert pb1.retainer == 17
        assert pb1.available_balance == 54

        assert pb2.product_id in [u1.product_id, u2.product_id]
        assert pb2.payout == 119
        assert pb2.adjustment == 0
        assert pb2.expense == 0
        assert pb2.net == 119
        assert pb2.retainer == 29
        assert pb2.available_balance == 90

    def test_multi_product_multi_payout(
        self,
        business,
        product_factory,
        user_factory,
        mnt_filepath,
        bp_payout_factory,
        thl_lm,
        lm,
        duration,
        offset,
        start,
        thl_web_rr,
        payout_event_manager,
        session_with_tx_factory,
        delete_ledger_db,
        create_main_accounts,
        client_no_amm,
        ledger_collection,
        pop_ledger_merge,
        delete_df_collection,
    ):
        delete_ledger_db()
        create_main_accounts()
        delete_df_collection(coll=ledger_collection)

        from generalresearch.models.thl.user import User

        u1: User = user_factory(product=product_factory(business=business))
        u2: User = user_factory(product=product_factory(business=business))

        session_with_tx_factory(
            user=u1,
            wall_req_cpi=Decimal(".75"),
            started=start + timedelta(days=1),
        )

        session_with_tx_factory(
            user=u2,
            wall_req_cpi=Decimal("1.25"),
            started=start + timedelta(days=2),
        )

        payout_event_manager.set_account_lookup_table(thl_lm=thl_lm)

        bp_payout_factory(
            product=u1.product,
            amount=USDCent(5),
            created=start + timedelta(days=4),
            skip_wallet_balance_check=True,
            skip_one_per_day_check=True,
        )

        bp_payout_factory(
            product=u2.product,
            amount=USDCent(50),
            created=start + timedelta(days=4),
            skip_wallet_balance_check=True,
            skip_one_per_day_check=True,
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

        assert business.balance.payout == 190
        assert business.balance.net == 190

        assert business.balance.balance == 135

    def test_multi_product_multi_payout_adjustment(
        self,
        business,
        product_factory,
        user_factory,
        mnt_filepath,
        bp_payout_factory,
        thl_lm,
        lm,
        duration,
        offset,
        start,
        thl_web_rr,
        payout_event_manager,
        session_with_tx_factory,
        delete_ledger_db,
        create_main_accounts,
        client_no_amm,
        ledger_collection,
        task_adj_collection,
        pop_ledger_merge,
        wall_manager,
        session_manager,
        adj_to_fail_with_tx_factory,
        delete_df_collection,
    ):
        """
        - Product 1 $2.50 Complete
        - Product 2 $2.50 Complete
        - $2.50 Payout on Product 1
        - $0.50 Payout on Product 2
        - Product 3 $2.50 Complete
        - Complete -> Failure $2.50 Adjustment on Product 1
        ====
        - Net: $7.50 * .95 = $7.125
            - $2.50 = $2.375 = $2.38
            - $2.50 = $2.375 = $2.38
            - $2.50 = $2.375 = $2.38
            ====
                             - $7.14
        - Balance: $2
        """

        delete_ledger_db()
        create_main_accounts()
        delete_df_collection(coll=ledger_collection)
        delete_df_collection(coll=task_adj_collection)

        from generalresearch.models.thl.user import User

        u1: User = user_factory(product=product_factory(business=business))
        u2: User = user_factory(product=product_factory(business=business))
        u3: User = user_factory(product=product_factory(business=business))

        s1 = session_with_tx_factory(
            user=u1,
            wall_req_cpi=Decimal("2.50"),
            started=start + timedelta(days=1),
        )

        session_with_tx_factory(
            user=u2,
            wall_req_cpi=Decimal("2.50"),
            started=start + timedelta(days=2),
        )
        payout_event_manager.set_account_lookup_table(thl_lm=thl_lm)

        bp_payout_factory(
            product=u1.product,
            amount=USDCent(250),
            created=start + timedelta(days=3),
            skip_wallet_balance_check=True,
            skip_one_per_day_check=True,
        )

        bp_payout_factory(
            product=u2.product,
            amount=USDCent(50),
            created=start + timedelta(days=4),
            skip_wallet_balance_check=True,
            skip_one_per_day_check=True,
        )

        adj_to_fail_with_tx_factory(session=s1, created=start + timedelta(days=5))

        session_with_tx_factory(
            user=u3,
            wall_req_cpi=Decimal("2.50"),
            started=start + timedelta(days=6),
        )

        # Build and prepare the Business with the db transactions now in place

        # This isn't needed for Business Balance... but good to also check
        # task_adj_collection.initial_load(client=None, sync=True)
        # These are the only two that are needed for Business Balance
        ledger_collection.initial_load(client=None, sync=True)
        pop_ledger_merge.build(client=client_no_amm, ledger_coll=ledger_collection)

        df = client_no_amm.compute(ledger_collection.ddf(), sync=True)
        assert df.shape == (24, 24)

        df = client_no_amm.compute(pop_ledger_merge.ddf(), sync=True)
        assert df.shape == (20, 28)

        business.prebuild_balance(
            thl_pg_config=thl_web_rr,
            lm=lm,
            ds=mnt_filepath,
            client=client_no_amm,
            pop_ledger=pop_ledger_merge,
        )

        assert business.balance.payout == 714
        assert business.balance.adjustment == -238

        assert business.balance.product_balances[0].adjustment == -238
        assert business.balance.product_balances[1].adjustment == 0
        assert business.balance.product_balances[2].adjustment == 0

        assert business.balance.expense == 0
        assert business.balance.net == 714 - 238
        assert business.balance.balance == business.balance.payout - (250 + 50 + 238)

        predicted_retainer = sum(
            [
                pb.balance * 0.25
                for pb in business.balance.product_balances
                if pb.balance > 0
            ]
        )
        assert business.balance.retainer == approx(predicted_retainer, rel=0.01)

    def test_neg_balance_cache(
        self,
        product,
        mnt_filepath,
        thl_lm,
        client_no_amm,
        thl_redis_config,
        brokerage_product_payout_event_manager,
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
        payout_event_manager,
        adj_to_fail_with_tx_factory,
        thl_web_rr,
        lm,
    ):
        """Test having a Business with two products.. one that lost money
        and one that gained money. Ensure that the Business balance
        reflects that to compensate for the Product in the negative.
        """
        # Now let's load it up and actually test some things
        delete_ledger_db()
        create_main_accounts()
        delete_df_collection(coll=ledger_collection)

        from generalresearch.models.thl.product import Product
        from generalresearch.models.thl.user import User

        p1: Product = product_factory(business=business)
        p2: Product = product_factory(business=business)
        u1: User = user_factory(product=p1)
        u2: User = user_factory(product=p2)
        thl_lm.get_account_or_create_bp_wallet(product=p1)
        thl_lm.get_account_or_create_bp_wallet(product=p2)

        # Product 1: Complete, Payout, Recon..
        s1 = session_with_tx_factory(
            user=u1,
            wall_req_cpi=Decimal(".75"),
            started=start + timedelta(days=1),
        )
        payout_event_manager.set_account_lookup_table(thl_lm=thl_lm)
        bp_payout_factory(
            product=u1.product,
            amount=USDCent(71),
            ext_ref_id=uuid4().hex,
            created=start + timedelta(days=1, minutes=1),
            skip_wallet_balance_check=True,
            skip_one_per_day_check=True,
        )
        adj_to_fail_with_tx_factory(
            session=s1,
            created=start + timedelta(days=1, minutes=2),
        )

        # Product 2: Complete, Complete.
        s2 = session_with_tx_factory(
            user=u2,
            wall_req_cpi=Decimal(".75"),
            started=start + timedelta(days=1, minutes=3),
        )
        s3 = session_with_tx_factory(
            user=u2,
            wall_req_cpi=Decimal(".75"),
            started=start + timedelta(days=1, minutes=4),
        )

        # Finally, process everything:
        ledger_collection.initial_load(client=None, sync=True)
        pop_ledger_merge.build(client=client_no_amm, ledger_coll=ledger_collection)

        business.prebuild_balance(
            thl_pg_config=thl_web_rr,
            lm=lm,
            ds=mnt_filepath,
            client=client_no_amm,
            pop_ledger=pop_ledger_merge,
        )

        # Check Product 1
        pb1 = business.balance.product_balances[0]
        assert pb1.product_id == p1.uuid
        assert pb1.payout == 71
        assert pb1.adjustment == -71
        assert pb1.net == 0
        assert pb1.balance == 71 - (71 * 2)
        assert pb1.retainer == 0
        assert pb1.available_balance == 0

        # Check Product 2
        pb2 = business.balance.product_balances[1]
        assert pb2.product_id == p2.uuid
        assert pb2.payout == 71 * 2
        assert pb2.adjustment == 0
        assert pb2.net == 71 * 2
        assert pb2.balance == (71 * 2)
        assert pb2.retainer == pytest.approx((71 * 2) * 0.25, rel=1)
        assert pb2.available_balance == 107

        # Check Business
        bb1 = business.balance
        assert bb1.payout == (71 * 3)  # Raw total of completes
        assert bb1.adjustment == -71  # 1 Complete >> Failure
        assert bb1.expense == 0
        assert bb1.net == (71 * 3) - 71  # How much the Business actually earned
        assert (
            bb1.balance == (71 * 3) - 71 - 71
        )  # 3 completes, but 1 payout and 1 recon leaves only one complete
        # worth of activity on the account
        assert bb1.retainer == pytest.approx((71 * 2) * 0.25, rel=1)
        assert bb1.available_balance_usd_str == "$0.36"

        # Confirm that the debt from the pb1 in the red is covered when
        #    calculating the Business balance by the profit of pb2
        assert pb2.available_balance + pb1.balance == bb1.available_balance

    def test_multi_product_multi_payout_adjustment_at_timestamp(
        self,
        business,
        product_factory,
        user_factory,
        mnt_filepath,
        bp_payout_factory,
        thl_lm,
        lm,
        duration,
        offset,
        start,
        thl_web_rr,
        payout_event_manager,
        session_with_tx_factory,
        delete_ledger_db,
        create_main_accounts,
        client_no_amm,
        ledger_collection,
        task_adj_collection,
        pop_ledger_merge,
        wall_manager,
        session_manager,
        adj_to_fail_with_tx_factory,
        delete_df_collection,
    ):
        """
            This test measures a complex Business situation, but then makes
            various assertions based off the query which uses an at_timestamp.

            The goal here is a feature that allows us to look back and see
            what the balance was of an account at any specific point in time.

        - Day 1: Product 1 $2.50 Complete
            - Total Payout: $2.38
            - Smart Retainer: $0.59
            - Available Balance: $1.79
        - Day 2: Product 2 $2.50 Complete
            - Total Payout: $4.76
            - Smart Retainer: $1.18
            - Available Balance: $3.58
        - Day 3: $2.50 Payout on Product 1
            - Total Payout: $4.76
            - Smart Retainer: $0.59
            - Available Balance: $1.67
        - Day 4: $0.50 Payout on Product 2
            - Total Payout: $4.76
            - Smart Retainer: $0.47
            - Available Balance: $1.29
        - Day 5: Product 3 $2.50 Complete
            - Total Payout: $7.14
            - Smart Retainer: $1.06
            - Available Balance: $3.08
        - Day 6: Complete -> Failure $2.50 Adjustment on Product 1
            - Total Payout: $7.18
            - Smart Retainer: $1.06
            - Available Balance: $0.70
        """

        delete_ledger_db()
        create_main_accounts()
        delete_df_collection(coll=ledger_collection)
        delete_df_collection(coll=task_adj_collection)

        from generalresearch.models.thl.user import User

        u1: User = user_factory(product=product_factory(business=business))
        u2: User = user_factory(product=product_factory(business=business))
        u3: User = user_factory(product=product_factory(business=business))

        s1 = session_with_tx_factory(
            user=u1,
            wall_req_cpi=Decimal("2.50"),
            started=start + timedelta(days=1),
        )

        session_with_tx_factory(
            user=u2,
            wall_req_cpi=Decimal("2.50"),
            started=start + timedelta(days=2),
        )
        payout_event_manager.set_account_lookup_table(thl_lm=thl_lm)

        bp_payout_factory(
            product=u1.product,
            amount=USDCent(250),
            created=start + timedelta(days=3),
            skip_wallet_balance_check=True,
            skip_one_per_day_check=True,
        )

        bp_payout_factory(
            product=u2.product,
            amount=USDCent(50),
            created=start + timedelta(days=4),
            skip_wallet_balance_check=True,
            skip_one_per_day_check=True,
        )

        session_with_tx_factory(
            user=u3,
            wall_req_cpi=Decimal("2.50"),
            started=start + timedelta(days=5),
        )

        adj_to_fail_with_tx_factory(session=s1, created=start + timedelta(days=6))

        # Build and prepare the Business with the db transactions now in place

        # This isn't needed for Business Balance... but good to also check
        # task_adj_collection.initial_load(client=None, sync=True)
        # These are the only two that are needed for Business Balance
        ledger_collection.initial_load(client=None, sync=True)
        pop_ledger_merge.build(client=client_no_amm, ledger_coll=ledger_collection)

        df = client_no_amm.compute(ledger_collection.ddf(), sync=True)
        assert df.shape == (24, 24)

        df = client_no_amm.compute(pop_ledger_merge.ddf(), sync=True)
        assert df.shape == (20, 28)

        business.prebuild_balance(
            thl_pg_config=thl_web_rr,
            lm=lm,
            ds=mnt_filepath,
            client=client_no_amm,
            pop_ledger=pop_ledger_merge,
        )

        business.prebuild_balance(
            thl_pg_config=thl_web_rr,
            lm=lm,
            ds=mnt_filepath,
            client=client_no_amm,
            pop_ledger=pop_ledger_merge,
            at_timestamp=start + timedelta(days=1, hours=1),
        )
        day1_bal = business.balance

        business.prebuild_balance(
            thl_pg_config=thl_web_rr,
            lm=lm,
            ds=mnt_filepath,
            client=client_no_amm,
            pop_ledger=pop_ledger_merge,
            at_timestamp=start + timedelta(days=2, hours=1),
        )
        day2_bal = business.balance

        business.prebuild_balance(
            thl_pg_config=thl_web_rr,
            lm=lm,
            ds=mnt_filepath,
            client=client_no_amm,
            pop_ledger=pop_ledger_merge,
            at_timestamp=start + timedelta(days=3, hours=1),
        )
        day3_bal = business.balance

        business.prebuild_balance(
            thl_pg_config=thl_web_rr,
            lm=lm,
            ds=mnt_filepath,
            client=client_no_amm,
            pop_ledger=pop_ledger_merge,
            at_timestamp=start + timedelta(days=4, hours=1),
        )
        day4_bal = business.balance

        business.prebuild_balance(
            thl_pg_config=thl_web_rr,
            lm=lm,
            ds=mnt_filepath,
            client=client_no_amm,
            pop_ledger=pop_ledger_merge,
            at_timestamp=start + timedelta(days=5, hours=1),
        )
        day5_bal = business.balance

        business.prebuild_balance(
            thl_pg_config=thl_web_rr,
            lm=lm,
            ds=mnt_filepath,
            client=client_no_amm,
            pop_ledger=pop_ledger_merge,
            at_timestamp=start + timedelta(days=6, hours=1),
        )
        day6_bal = business.balance

        assert day1_bal.payout == 238
        assert day1_bal.retainer == 59
        assert day1_bal.available_balance == 179

        assert day2_bal.payout == 476
        assert day2_bal.retainer == 118
        assert day2_bal.available_balance == 358

        assert day3_bal.payout == 476
        assert day3_bal.retainer == 59
        assert day3_bal.available_balance == 167

        assert day4_bal.payout == 476
        assert day4_bal.retainer == 47
        assert day4_bal.available_balance == 129

        assert day5_bal.payout == 714
        assert day5_bal.retainer == 106
        assert day5_bal.available_balance == 308

        assert day6_bal.payout == 714
        assert day6_bal.retainer == 106
        assert day6_bal.available_balance == 70


class TestBusinessMethods:

    @pytest.fixture(scope="function")
    def start(self, utc_90days_ago) -> "datetime":
        s = utc_90days_ago.replace(microsecond=0)
        return s

    @pytest.fixture(scope="function")
    def offset(self) -> str:
        return "15d"

    @pytest.fixture(scope="function")
    def duration(
        self,
    ) -> Optional["timedelta"]:
        return None

    def test_cache_key(self, business, gr_redis):
        assert isinstance(business.cache_key, str)
        assert ":" in business.cache_key
        assert str(business.uuid) in business.cache_key

    def test_set_cache(
        self,
        business,
        gr_redis,
        gr_db,
        thl_web_rr,
        client_no_amm,
        mnt_filepath,
        lm,
        thl_lm,
        business_payout_event_manager,
        product_factory,
        membership_factory,
        team,
        session_with_tx_factory,
        user_factory,
        ledger_collection,
        pop_ledger_merge,
        utc_60days_ago,
        delete_ledger_db,
        create_main_accounts,
        gr_redis_config,
        mnt_gr_api_dir,
    ):
        assert gr_redis.get(name=business.cache_key) is None

        p1 = product_factory(team=team, business=business)
        u1 = user_factory(product=p1)

        # Business needs tx & incite to build balance
        delete_ledger_db()
        create_main_accounts()
        thl_lm.get_account_or_create_bp_wallet(product=p1)
        session_with_tx_factory(user=u1, started=utc_60days_ago)
        ledger_collection.initial_load(client=None, sync=True)
        pop_ledger_merge.build(client=client_no_amm, ledger_coll=ledger_collection)

        business.set_cache(
            pg_config=gr_db,
            thl_web_rr=thl_web_rr,
            redis_config=gr_redis_config,
            client=client_no_amm,
            ds=mnt_filepath,
            lm=lm,
            thl_lm=thl_lm,
            bpem=business_payout_event_manager,
            pop_ledger=pop_ledger_merge,
            mnt_gr_api=mnt_gr_api_dir,
        )

        assert gr_redis.hgetall(name=business.cache_key) is not None
        from generalresearch.models.gr.business import Business

        # We're going to pull only a specific year, but make sure that
        # it's being assigned to the field regardless
        year = datetime.now(tz=timezone.utc).year
        res = Business.from_redis(
            uuid=business.uuid,
            fields=[f"pop_financial:{year}"],
            gr_redis_config=gr_redis_config,
        )
        assert len(res.pop_financial) > 0

    def test_set_cache_business(
        self,
        gr_user,
        business,
        gr_user_token,
        gr_redis,
        gr_db,
        thl_web_rr,
        product_factory,
        team,
        membership_factory,
        client_no_amm,
        mnt_filepath,
        lm,
        thl_lm,
        business_payout_event_manager,
        user_factory,
        delete_ledger_db,
        create_main_accounts,
        session_with_tx_factory,
        ledger_collection,
        team_manager,
        pop_ledger_merge,
        gr_redis_config,
        utc_60days_ago,
        mnt_gr_api_dir,
    ):
        from generalresearch.models.gr.business import Business

        p1 = product_factory(team=team, business=business)
        u1 = user_factory(product=p1)
        team_manager.add_business(team=team, business=business)

        # Business needs tx & incite to build balance
        delete_ledger_db()
        create_main_accounts()
        thl_lm.get_account_or_create_bp_wallet(product=p1)
        session_with_tx_factory(user=u1, started=utc_60days_ago)
        ledger_collection.initial_load(client=None, sync=True)
        pop_ledger_merge.build(client=client_no_amm, ledger_coll=ledger_collection)

        business.set_cache(
            pg_config=gr_db,
            thl_web_rr=thl_web_rr,
            redis_config=gr_redis_config,
            client=client_no_amm,
            ds=mnt_filepath,
            lm=lm,
            thl_lm=thl_lm,
            bpem=business_payout_event_manager,
            pop_ledger=pop_ledger_merge,
            mnt_gr_api=mnt_gr_api_dir,
        )

        # keys: List = Business.required_fields() + ["products", "bp_accounts"]
        business2 = Business.from_redis(
            uuid=business.uuid,
            fields=[
                "id",
                "tax_number",
                "contact",
                "addresses",
                "teams",
                "products",
                "bank_accounts",
                "balance",
                "payouts_total_str",
                "payouts_total",
                "payouts",
                "pop_financial",
                "bp_accounts",
            ],
            gr_redis_config=gr_redis_config,
        )

        assert business.model_dump_json() == business2.model_dump_json()
        assert p1.uuid in [p.uuid for p in business2.products]
        assert len(business2.teams) == 1
        assert team.uuid in [t.uuid for t in business2.teams]

        assert business2.balance.payout == 48
        assert business2.balance.balance == 48
        assert business2.balance.net == 48
        assert business2.balance.retainer == 12
        assert business2.balance.available_balance == 36
        assert len(business2.balance.product_balances) == 1

        assert len(business2.payouts) == 0

        assert len(business2.bp_accounts) == 1
        assert len(business2.bp_accounts) == len(business2.product_uuids)

        assert len(business2.pop_financial) == 1
        assert business2.pop_financial[0].payout == business2.balance.payout
        assert business2.pop_financial[0].net == business2.balance.net

    def test_prebuild_enriched_session_parquet(
        self,
        event_report_request,
        enriched_session_merge,
        client_no_amm,
        wall_collection,
        session_collection,
        thl_web_rr,
        session_report_request,
        user_factory,
        start,
        session_factory,
        product_factory,
        delete_df_collection,
        business,
        mnt_filepath,
        mnt_gr_api_dir,
    ):

        delete_df_collection(coll=wall_collection)
        delete_df_collection(coll=session_collection)

        p1 = product_factory(business=business)
        p2 = product_factory(business=business)

        for p in [p1, p2]:
            u = user_factory(product=p)
            for i in range(50):
                s = session_factory(
                    user=u,
                    wall_count=1,
                    wall_req_cpi=Decimal("1.00"),
                    started=start + timedelta(minutes=i, seconds=1),
                )
        wall_collection.initial_load(client=None, sync=True)
        session_collection.initial_load(client=None, sync=True)

        enriched_session_merge.build(
            client=client_no_amm,
            session_coll=session_collection,
            wall_coll=wall_collection,
            pg_config=thl_web_rr,
        )

        business.prebuild_enriched_session_parquet(
            thl_pg_config=thl_web_rr,
            ds=mnt_filepath,
            client=client_no_amm,
            mnt_gr_api=mnt_gr_api_dir,
            enriched_session=enriched_session_merge,
        )

        # Now try to read from path
        df = pd.read_parquet(
            os.path.join(mnt_gr_api_dir, "pop_session", f"{business.file_key}.parquet")
        )
        assert isinstance(df, pd.DataFrame)

    def test_prebuild_enriched_wall_parquet(
        self,
        event_report_request,
        enriched_session_merge,
        enriched_wall_merge,
        client_no_amm,
        wall_collection,
        session_collection,
        thl_web_rr,
        session_report_request,
        user_factory,
        start,
        session_factory,
        product_factory,
        delete_df_collection,
        business,
        mnt_filepath,
        mnt_gr_api_dir,
    ):

        delete_df_collection(coll=wall_collection)
        delete_df_collection(coll=session_collection)

        p1 = product_factory(business=business)
        p2 = product_factory(business=business)

        for p in [p1, p2]:
            u = user_factory(product=p)
            for i in range(50):
                s = session_factory(
                    user=u,
                    wall_count=1,
                    wall_req_cpi=Decimal("1.00"),
                    started=start + timedelta(minutes=i, seconds=1),
                )
        wall_collection.initial_load(client=None, sync=True)
        session_collection.initial_load(client=None, sync=True)

        enriched_wall_merge.build(
            client=client_no_amm,
            session_coll=session_collection,
            wall_coll=wall_collection,
            pg_config=thl_web_rr,
        )

        business.prebuild_enriched_wall_parquet(
            thl_pg_config=thl_web_rr,
            ds=mnt_filepath,
            client=client_no_amm,
            mnt_gr_api=mnt_gr_api_dir,
            enriched_wall=enriched_wall_merge,
        )

        # Now try to read from path
        df = pd.read_parquet(
            os.path.join(mnt_gr_api_dir, "pop_event", f"{business.file_key}.parquet")
        )
        assert isinstance(df, pd.DataFrame)
