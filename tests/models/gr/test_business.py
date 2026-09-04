from __future__ import annotations

import os
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING
from uuid import uuid4

import pandas as pd
import pytest
from dask.distributed import Client as DaskClient

# noinspection PyUnresolvedReferences
from distributed.utils_test import (
    client_no_amm,
)
from pytest import approx

from generalresearch.currency import USDCent
from generalresearch.models.gr.business import (
    Business,
    BusinessAddress,
    BusinessContact,
)
from generalresearch.models.thl.finance import (
    BusinessBalances,
    ProductBalances,
)
from generalresearch.models.thl.product import Product

if TYPE_CHECKING:
    from generalresearch.incite.base import GRLDatasets
    from generalresearch.incite.collections.thl_web import (
        SessionDFCollection,
        WallDFCollection,
    )
    from generalresearch.incite.mergers.pop_ledger import PopLedgerMerge
    from generalresearch.managers.gr.business import BusinessBankAccountManager
    from generalresearch.managers.gr.team import TeamManager
    from generalresearch.managers.thl.ledger_manager.ledger import LedgerManager
    from generalresearch.managers.thl.ledger_manager.thl_ledger import ThlLedgerManager
    from generalresearch.managers.thl.payout import (
        BusinessPayoutEventManager,
        PayoutEventManager,
    )
    from generalresearch.managers.thl.product import ProductManager
    from generalresearch.models.gr.business import (
        BusinessBankAccount,
    )
    from generalresearch.models.gr.team import Team
    from generalresearch.models.thl.product import BrokerageProductPayoutEvent
    from generalresearch.models.thl.session import Session
    from generalresearch.models.thl.user import User
    from generalresearch.pg_helper import PostgresConfig
    from generalresearch.redis_helper import RedisConfig


class TestBusinessBankAccount:

    def test_init(
        self,
        gr_business: Business,
        gr_business_bank_account_manager: BusinessBankAccountManager,
    ):
        from generalresearch.models.gr.business import BusinessBankAccount
        from generalresearch.models.gr.definitions import TransferMethod

        instance = gr_business_bank_account_manager.create(
            business_id=gr_business.id,
            uuid=uuid4().hex,
            transfer_method=TransferMethod.ACH,
        )
        assert isinstance(instance, BusinessBankAccount)

    def test_business(
        self,
        gr_business_bank_account: BusinessBankAccount,
        gr_business: Business,
        gr_db: PostgresConfig,
        gr_redis_config: RedisConfig,
    ):
        from generalresearch.models.gr.business import Business

        assert gr_business_bank_account.business is None

        gr_business_bank_account.prefetch_business(
            pg_config=gr_db, redis_config=gr_redis_config
        )
        assert isinstance(gr_business_bank_account.business, Business)
        assert gr_business_bank_account.business.uuid == gr_business.uuid


class TestBusinessAddress:

    def test_init(self, business_address: BusinessAddress):
        assert isinstance(business_address, BusinessAddress)


class TestBusinessContact:

    def test_init(self):

        bc = BusinessContact(name="abc", email="test@abc.com")
        assert isinstance(bc, BusinessContact)


class TestBusiness:
    @pytest.fixture
    def start(self) -> datetime:
        return datetime(year=2018, month=3, day=14, hour=0, tzinfo=UTC)

    @pytest.fixture
    def offset(self) -> str:
        return "30D"

    @pytest.fixture
    def duration(self) -> timedelta | None:
        return None

    def test_init(self, gr_business: Business):

        assert isinstance(gr_business, Business)
        assert isinstance(gr_business.id, int)
        assert isinstance(gr_business.uuid, str)

    def test_str_and_repr(
        self,
        gr_business: Business,
        product_factory: Callable[..., Product],
        thl_web_rr: PostgresConfig,
        ledger_manager: LedgerManager,
        thl_ledger_manager: ThlLedgerManager,
        product_manager: ProductManager,
        business_payout_event_manager: BusinessPayoutEventManager,
        brokerage_product_payout_event_factory: Callable[
            ..., BusinessPayoutEventManager
        ],
        start: datetime,
        user_factory: Callable[..., User],
        session_with_tx_factory: Callable[..., Session],
        pop_ledger_merge: PopLedgerMerge,
        client_no_amm: DaskClient,
        ledger_collection,
        mnt_filepath: GRLDatasets,
        create_main_accounts: Callable[..., None],
    ):
        create_main_accounts()
        p1 = product_factory(business=gr_business)
        u1 = user_factory(product=p1)
        p2 = product_factory(business=gr_business)
        thl_ledger_manager.get_account_or_create_bp_wallet(product=p1)
        thl_ledger_manager.get_account_or_create_bp_wallet(product=p2)

        res1 = repr(gr_business)

        assert gr_business.uuid in res1
        assert "<Business: " in res1

        res2 = str(gr_business)

        assert gr_business.uuid in res2
        assert "Name:" in res2
        assert "Not Loaded" in res2

        gr_business.prefetch_products(product_manager=product_manager)
        gr_business.prefetch_bp_accounts(
            thl_lm=thl_ledger_manager, product_manager=product_manager
        )
        res3 = str(gr_business)
        assert "Products: 2" in res3
        assert "Ledger Accounts: 2" in res3

        # -- need some tx to make these interesting
        business_payout_event_manager.set_account_lookup_table(
            thl_lm=thl_ledger_manager
        )
        session_with_tx_factory(
            user=u1,
            wall_req_cpi=Decimal("2.50"),
            started=start + timedelta(days=5),
        )
        brokerage_product_payout_event_factory(
            product=p1,
            amount=USDCent(50),
            created=start + timedelta(days=4),
            skip_wallet_balance_check=True,
            skip_one_per_day_check=True,
        )

        ledger_collection.initial_load(client=None, sync=True)
        pop_ledger_merge.build(client=client_no_amm, ledger_coll=ledger_collection)

        gr_business.prebuild_payouts(
            bpem=business_payout_event_manager,
        )
        gr_business.prebuild_balance(
            product_manager=product_manager,
            lm=ledger_manager,
            ds=mnt_filepath,
            client=client_no_amm,
            pop_ledger=pop_ledger_merge,
        )
        res4 = str(gr_business)
        assert "Payouts: 1" in res4
        assert "Available Balance: 141" in res4

    def test_addresses(
        self,
        gr_business: Business,
        gr_db: PostgresConfig,
    ):
        from generalresearch.models.gr.business import BusinessAddress

        assert gr_business.addresses is None

        gr_business.prefetch_addresses(pg_config=gr_db)
        assert isinstance(gr_business.addresses, list)
        assert len(gr_business.addresses) == 1
        assert isinstance(gr_business.addresses[0], BusinessAddress)

    def test_teams(
        self,
        gr_business: Business,
        team: Team,
        team_manager: TeamManager,
        gr_db: PostgresConfig,
    ):
        assert gr_business.teams is None

        gr_business.prefetch_teams(pg_config=gr_db)
        assert isinstance(gr_business.teams, list)
        assert len(gr_business.teams) == 0

        team_manager.add_business(team=team, business=gr_business)
        assert len(gr_business.teams) == 0
        gr_business.prefetch_teams(pg_config=gr_db)
        assert len(gr_business.teams) == 1

    def test_products(
        self,
        gr_business: Business,
        product_factory: Callable[..., Product],
        product_manager: ProductManager,
    ):

        p1 = product_factory(business=gr_business)
        assert gr_business.products is None

        gr_business.prefetch_products(product_manager=product_manager)
        assert isinstance(gr_business.products, list)
        assert len(gr_business.products) == 1
        assert isinstance(gr_business.products[0], Product)

        assert gr_business.products[0].uuid == p1.uuid

        # Add two more, but list is still one until we prefetch
        product_factory(business=gr_business)
        product_factory(business=gr_business)
        assert len(gr_business.products) == 1

        gr_business.prefetch_products(product_manager=product_manager)
        assert len(gr_business.products) == 3

    def test_bank_accounts(
        self,
        gr_business: Business,
        gr_business_bank_account_manager: BusinessBankAccountManager,
    ):
        assert gr_business.products is None

        # It's an empty list after prefetch
        gr_business.prefetch_bank_accounts(
            business_bank_account_manager=gr_business_bank_account_manager
        )
        assert isinstance(gr_business.bank_accounts, list)
        assert len(gr_business.bank_accounts) == 1

    def test_balance(
        self,
        gr_business: Business,
        mnt_filepath: GRLDatasets,
        client_no_amm: DaskClient,
        thl_web_rr: PostgresConfig,
        ledger_manager: LedgerManager,
        pop_ledger_merge: PopLedgerMerge,
        product_manager: ProductManager,
    ):
        assert gr_business.balance is None

        with pytest.raises(expected_exception=AssertionError) as cm:
            gr_business.prebuild_balance(
                product_manager=product_manager,
                lm=ledger_manager,
                ds=mnt_filepath,
                client=client_no_amm,
                pop_ledger=pop_ledger_merge,
            )
        assert "Cannot build Business Balance" in str(cm.value)
        assert gr_business.balance is None

        # TODO: Add parquet building so that this doesn't fail and we can
        #    properly assign a business.balance

    def test_payouts_no_accounts(
        self,
        gr_business: Business,
        product_factory: Callable[..., Product],
        thl_ledger_manager: ThlLedgerManager,
        business_payout_event_manager: BusinessPayoutEventManager,
    ):
        assert gr_business.payouts is None

        with pytest.raises(expected_exception=AssertionError) as cm:
            gr_business.prebuild_payouts(
                bpem=business_payout_event_manager,
            )
        assert "Must provide product_uuids" in str(cm.value)

        p = product_factory(business=gr_business)
        thl_ledger_manager.get_account_or_create_bp_wallet(product=p)

        gr_business.prebuild_payouts(
            bpem=business_payout_event_manager,
        )
        assert isinstance(gr_business.payouts, list)
        assert len(gr_business.payouts) == 0

    def test_payouts(
        self,
        gr_business: Business,
        product_factory: Callable[..., Product],
        brokerage_product_payout_event_factory: Callable[
            ..., BrokerageProductPayoutEvent
        ],
        thl_ledger_manager: ThlLedgerManager,
        business_payout_event_manager: BusinessPayoutEventManager,
        create_main_accounts: Callable[..., None],
    ):
        create_main_accounts()
        p = product_factory(business=gr_business)
        thl_ledger_manager.get_account_or_create_bp_wallet(product=p)
        business_payout_event_manager.set_account_lookup_table(
            thl_lm=thl_ledger_manager
        )

        brokerage_product_payout_event_factory(
            product=p, amount=USDCent(123), skip_wallet_balance_check=True
        )

        gr_business.prebuild_payouts(
            bpem=business_payout_event_manager,
        )
        assert len(gr_business.payouts) == 1
        assert sum([p.amount for p in gr_business.payouts]) == 123

        # Add another!
        brokerage_product_payout_event_factory(
            product=p,
            amount=USDCent(123),
            skip_wallet_balance_check=True,
            skip_one_per_day_check=True,
        )
        business_payout_event_manager.set_account_lookup_table(
            thl_lm=thl_ledger_manager
        )
        gr_business.prebuild_payouts(
            bpem=business_payout_event_manager,
        )
        assert isinstance(gr_business.payouts, list)
        assert len(gr_business.payouts) == 1
        assert len(gr_business.payouts[0].bp_payouts) == 2
        assert sum([p.amount for p in gr_business.payouts]) == 246

    def test_payouts_totals(
        self,
        gr_business: Business,
        product_factory: Callable[..., Product],
        brokerage_product_payout_event_factory: Callable[
            ..., BrokerageProductPayoutEvent
        ],
        thl_ledger_manager: ThlLedgerManager,
        thl_web_rr: PostgresConfig,
        business_payout_event_manager: BusinessPayoutEventManager,
        create_main_accounts: Callable[..., None],
    ):

        create_main_accounts()

        p1: Product = product_factory(business=gr_business)
        thl_ledger_manager.get_account_or_create_bp_wallet(product=p1)
        business_payout_event_manager.set_account_lookup_table(
            thl_lm=thl_ledger_manager
        )

        brokerage_product_payout_event_factory(
            product=p1,
            amount=USDCent(1),
            skip_wallet_balance_check=True,
            skip_one_per_day_check=True,
        )

        brokerage_product_payout_event_factory(
            product=p1,
            amount=USDCent(25),
            skip_wallet_balance_check=True,
            skip_one_per_day_check=True,
        )

        brokerage_product_payout_event_factory(
            product=p1,
            amount=USDCent(50),
            skip_wallet_balance_check=True,
            skip_one_per_day_check=True,
        )

        gr_business.prebuild_payouts(
            bpem=business_payout_event_manager,
        )

        assert isinstance(gr_business.payouts, list)
        assert len(gr_business.payouts) == 1
        assert len(gr_business.payouts[0].bp_payouts) == 3
        assert gr_business.payouts_total == USDCent(76)
        assert gr_business.payouts_total_str == "$0.76"

    def test_pop_financial(
        self,
        gr_business: Business,
        product_manager: ProductManager,
        thl_ledger_manager: ThlLedgerManager,
        mnt_filepath: GRLDatasets,
        client_no_amm: DaskClient,
        pop_ledger_merge: PopLedgerMerge,
    ):
        assert gr_business.pop_financial is None
        gr_business.prebuild_pop_financial(
            product_manager=product_manager,
            thl_lm=thl_ledger_manager,
            ds=mnt_filepath,
            client=client_no_amm,
            pop_ledger=pop_ledger_merge,
        )
        assert gr_business.pop_financial == []

    def test_bp_accounts(
        self,
        gr_business: Business,
        product_factory: Callable[..., Product],
        thl_ledger_manager: ThlLedgerManager,
        product_manager: ProductManager,
    ):
        assert gr_business.bp_accounts is None
        gr_business.prefetch_bp_accounts(
            thl_lm=thl_ledger_manager, product_manager=product_manager
        )
        assert gr_business.bp_accounts == []

        p1: Product = product_factory(business=gr_business)
        thl_ledger_manager.get_account_or_create_bp_wallet(product=p1)

        gr_business.prefetch_bp_accounts(
            thl_lm=thl_ledger_manager, product_manager=product_manager
        )
        assert len(gr_business.bp_accounts) == 1


class TestBusinessBalance:

    @pytest.fixture
    def start(self) -> datetime:
        return datetime(year=2018, month=3, day=14, hour=0, tzinfo=UTC)

    @pytest.fixture
    def offset(self) -> str:
        return "30D"

    @pytest.fixture
    def duration(self) -> timedelta | None:
        return None

    @pytest.mark.skip
    def test_product_ordering(self):
        # Assert that the order of business.balance.product_balances is always
        #   consistent and in the same order based off product.created ASC
        pass

    def test_single_product(
        self,
        gr_business: Business,
        product_factory: Callable[..., Product],
        user_factory: Callable[..., User],
        mnt_filepath,
        ledger_manager: LedgerManager,
        start: datetime,
        thl_web_rr: PostgresConfig,
        session_with_tx_factory: Callable[..., Session],
        delete_ledger_db: Callable[..., None],
        create_main_accounts: Callable[..., None],
        client_no_amm: DaskClient,
        ledger_collection,
        product_manager: ProductManager,
        pop_ledger_merge: PopLedgerMerge,
        delete_df_collection: Callable[..., None],
    ):
        delete_ledger_db()
        create_main_accounts()
        delete_df_collection(coll=ledger_collection)

        p1: Product = product_factory(business=gr_business)
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

        gr_business.prebuild_balance(
            product_manager=product_manager,
            lm=ledger_manager,
            ds=mnt_filepath,
            client=client_no_amm,
            pop_ledger=pop_ledger_merge,
        )
        assert isinstance(gr_business.balance, BusinessBalances)
        assert gr_business.balance.payout == 190
        assert gr_business.balance.adjustment == 0
        assert gr_business.balance.net == 190
        assert gr_business.balance.retainer == 47
        assert gr_business.balance.available_balance == 143

        assert len(gr_business.balance.product_balances) == 1
        pb = gr_business.balance.product_balances[0]
        assert isinstance(pb, ProductBalances)
        assert pb.balance == gr_business.balance.balance
        assert pb.available_balance == gr_business.balance.available_balance
        assert pb.adjustment_percent == 0.0

    def test_multi_product(
        self,
        gr_business: Business,
        product_factory: Callable[..., Product],
        user_factory: Callable[..., User],
        mnt_filepath: GRLDatasets,
        ledger_manager: LedgerManager,
        product_manager: ProductManager,
        start: datetime,
        session_with_tx_factory: Callable[..., Session],
        delete_ledger_db: Callable[..., None],
        create_main_accounts: Callable[..., None],
        client_no_amm: DaskClient,
        ledger_collection,
        pop_ledger_merge: PopLedgerMerge,
        delete_df_collection: Callable[..., None],
    ):
        delete_ledger_db()
        create_main_accounts()
        delete_df_collection(coll=ledger_collection)

        u1: User = user_factory(product=product_factory(business=gr_business))
        u2: User = user_factory(product=product_factory(business=gr_business))

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

        gr_business.prebuild_balance(
            product_manager=product_manager,
            lm=ledger_manager,
            ds=mnt_filepath,
            client=client_no_amm,
            pop_ledger=pop_ledger_merge,
        )
        assert isinstance(gr_business.balance, BusinessBalances)
        assert gr_business.balance.payout == 190
        assert gr_business.balance.balance == 190
        assert gr_business.balance.adjustment == 0
        assert gr_business.balance.net == 190
        assert gr_business.balance.retainer == 46
        assert gr_business.balance.available_balance == 144

        assert len(gr_business.balance.product_balances) == 2

        pb1 = gr_business.balance.product_balances[0]
        pb2 = gr_business.balance.product_balances[1]
        assert isinstance(pb1, ProductBalances)
        assert pb1.product_id == u1.product_id
        assert isinstance(pb2, ProductBalances)
        assert pb2.product_id == u2.product_id

        for pb in [pb1, pb2]:
            assert pb.balance != gr_business.balance.balance
            assert pb.available_balance != gr_business.balance.available_balance
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
        gr_business: Business,
        product_factory: Callable[..., Product],
        user_factory: Callable[..., User],
        product_manager: ProductManager,
        mnt_filepath: GRLDatasets,
        brokerage_product_payout_event_factory: Callable[
            ..., BrokerageProductPayoutEvent
        ],
        thl_ledger_manager: ThlLedgerManager,
        ledger_manager: LedgerManager,
        start: datetime,
        thl_web_rr: PostgresConfig,
        payout_event_manager: PayoutEventManager,
        session_with_tx_factory: Callable[..., None],
        delete_ledger_db: Callable[..., None],
        create_main_accounts: Callable[..., None],
        client_no_amm: DaskClient,
        ledger_collection,
        pop_ledger_merge: PopLedgerMerge,
        delete_df_collection: Callable[..., None],
    ):
        delete_ledger_db()
        create_main_accounts()
        delete_df_collection(coll=ledger_collection)

        u1: User = user_factory(product=product_factory(business=gr_business))
        u2: User = user_factory(product=product_factory(business=gr_business))

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

        brokerage_product_payout_event_factory(
            product=u1.product,
            amount=USDCent(5),
            created=start + timedelta(days=4),
            skip_wallet_balance_check=True,
            skip_one_per_day_check=True,
        )

        brokerage_product_payout_event_factory(
            product=u2.product,
            amount=USDCent(50),
            created=start + timedelta(days=4),
            skip_wallet_balance_check=True,
            skip_one_per_day_check=True,
        )

        ledger_collection.initial_load(client=None, sync=True)
        pop_ledger_merge.build(client=client_no_amm, ledger_coll=ledger_collection)

        gr_business.prebuild_balance(
            product_manager=product_manager,
            lm=ledger_manager,
            ds=mnt_filepath,
            client=client_no_amm,
            pop_ledger=pop_ledger_merge,
        )

        assert isinstance(gr_business.balance, BusinessBalances)
        assert gr_business.balance.payout == 190
        assert gr_business.balance.net == 190

        assert gr_business.balance.balance == 135

    def test_multi_product_multi_payout_adjustment(
        self,
        gr_business: Business,
        product_factory: Callable[..., Product],
        user_factory: Callable[..., User],
        mnt_filepath: GRLDatasets,
        brokerage_product_payout_event_factory: Callable[
            ..., BrokerageProductPayoutEvent
        ],
        ledger_manager: LedgerManager,
        thl_ledger_manager: ThlLedgerManager,
        start: datetime,
        thl_web_rr: PostgresConfig,
        payout_event_manager: PayoutEventManager,
        session_with_tx_factory: Callable[..., Session],
        delete_ledger_db: Callable[..., None],
        product_manager: ProductManager,
        create_main_accounts: Callable[..., None],
        ledger_collection,
        task_adj_collection,
        pop_ledger_merge: PopLedgerMerge,
        adj_to_fail_with_tx_factory: Callable[..., None],
        delete_df_collection: Callable[..., None],
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

        u1: User = user_factory(product=product_factory(business=gr_business))
        u2: User = user_factory(product=product_factory(business=gr_business))
        u3: User = user_factory(product=product_factory(business=gr_business))

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

        brokerage_product_payout_event_factory(
            product=u1.product,
            amount=USDCent(250),
            created=start + timedelta(days=3),
            skip_wallet_balance_check=True,
            skip_one_per_day_check=True,
        )

        brokerage_product_payout_event_factory(
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

        gr_business.prebuild_balance(
            product_manager=product_manager,
            lm=ledger_manager,
            ds=mnt_filepath,
            client=client_no_amm,
            pop_ledger=pop_ledger_merge,
        )

        assert isinstance(gr_business.balance, BusinessBalances)
        assert gr_business.balance.payout == 714
        assert gr_business.balance.adjustment == -238

        assert gr_business.balance.product_balances[0].adjustment == -238
        assert gr_business.balance.product_balances[1].adjustment == 0
        assert gr_business.balance.product_balances[2].adjustment == 0

        assert gr_business.balance.expense == 0
        assert gr_business.balance.net == 714 - 238
        assert gr_business.balance.balance == gr_business.balance.payout - (
            250 + 50 + 238
        )

        predicted_retainer = sum(
            [
                pb.balance * 0.25
                for pb in gr_business.balance.product_balances
                if pb.balance > 0
            ]
        )
        assert gr_business.balance.retainer == approx(predicted_retainer, rel=0.01)

    def test_neg_balance_cache(
        self,
        mnt_filepath: GRLDatasets,
        thl_ledger_manager: ThlLedgerManager,
        client_no_amm: DaskClient,
        delete_ledger_db: Callable[..., None],
        create_main_accounts: Callable[..., None],
        delete_df_collection: Callable[..., None],
        ledger_collection,
        gr_business: Business,
        user_factory: Callable[..., User],
        product_factory: Callable[..., Product],
        session_with_tx_factory: Callable[..., Session],
        pop_ledger_merge: PopLedgerMerge,
        start: datetime,
        brokerage_product_payout_event_factory: Callable[
            ..., BrokerageProductPayoutEvent
        ],
        payout_event_manager,
        product_manager: ProductManager,
        adj_to_fail_with_tx_factory: Callable[..., None],
        thl_web_rr: PostgresConfig,
        ledger_manager: LedgerManager,
    ):
        """Test having a Business with two products.. one that lost money
        and one that gained money. Ensure that the Business balance
        reflects that to compensate for the Product in the negative.
        """
        # Now let's load it up and actually test some things
        delete_ledger_db()
        create_main_accounts()
        delete_df_collection(coll=ledger_collection)

        p1: Product = product_factory(business=gr_business)
        p2: Product = product_factory(business=gr_business)
        u1: User = user_factory(product=p1)
        u2: User = user_factory(product=p2)
        thl_ledger_manager.get_account_or_create_bp_wallet(product=p1)
        thl_ledger_manager.get_account_or_create_bp_wallet(product=p2)

        # Product 1: Complete, Payout, Recon..
        s1 = session_with_tx_factory(
            user=u1,
            wall_req_cpi=Decimal(".75"),
            started=start + timedelta(days=1),
        )
        brokerage_product_payout_event_factory(
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
        session_with_tx_factory(
            user=u2,
            wall_req_cpi=Decimal(".75"),
            started=start + timedelta(days=1, minutes=3),
        )
        session_with_tx_factory(
            user=u2,
            wall_req_cpi=Decimal(".75"),
            started=start + timedelta(days=1, minutes=4),
        )

        # Finally, process everything:
        ledger_collection.initial_load(client=None, sync=True)
        pop_ledger_merge.build(client=client_no_amm, ledger_coll=ledger_collection)

        gr_business.prebuild_balance(
            product_manager=product_manager,
            lm=ledger_manager,
            ds=mnt_filepath,
            client=client_no_amm,
            pop_ledger=pop_ledger_merge,
        )

        # Check Product 1
        assert isinstance(gr_business.balance, BusinessBalances)
        pb1 = gr_business.balance.product_balances[0]
        assert pb1.product_id == p1.uuid
        assert pb1.payout == 71
        assert pb1.adjustment == -71
        assert pb1.net == 0
        assert pb1.balance == 71 - (71 * 2)
        assert pb1.retainer == 0
        assert pb1.available_balance == 0

        # Check Product 2
        pb2 = gr_business.balance.product_balances[1]
        assert pb2.product_id == p2.uuid
        assert pb2.payout == 71 * 2
        assert pb2.adjustment == 0
        assert pb2.net == 71 * 2
        assert pb2.balance == (71 * 2)
        assert pb2.retainer == pytest.approx((71 * 2) * 0.25, rel=1)
        assert pb2.available_balance == 107

        # Check Business
        bb1 = gr_business.balance
        assert isinstance(bb1, BusinessBalances)
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
        gr_business: Business,
        product_factory: Callable[..., Product],
        user_factory: Callable[..., User],
        mnt_filepath: GRLDatasets,
        brokerage_product_payout_event_factory: Callable[
            ..., BrokerageProductPayoutEvent
        ],
        thl_ledger_manager: ThlLedgerManager,
        ledger_manager: LedgerManager,
        product_manager: ProductManager,
        start: datetime,
        payout_event_manager,
        session_with_tx_factory: Callable[..., None],
        delete_ledger_db: Callable[..., None],
        create_main_accounts: Callable[..., None],
        client_no_amm: DaskClient,
        ledger_collection,
        task_adj_collection,
        pop_ledger_merge: PopLedgerMerge,
        adj_to_fail_with_tx_factory: Callable[..., None],
        delete_df_collection: Callable[..., None],
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

        u1: User = user_factory(product=product_factory(business=gr_business))
        u2: User = user_factory(product=product_factory(business=gr_business))
        u3: User = user_factory(product=product_factory(business=gr_business))

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

        brokerage_product_payout_event_factory(
            product=u1.product,
            amount=USDCent(250),
            created=start + timedelta(days=3),
            skip_wallet_balance_check=True,
            skip_one_per_day_check=True,
        )

        brokerage_product_payout_event_factory(
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

        gr_business.prebuild_balance(
            product_manager=product_manager,
            lm=ledger_manager,
            ds=mnt_filepath,
            client=client_no_amm,
            pop_ledger=pop_ledger_merge,
        )

        gr_business.prebuild_balance(
            product_manager=product_manager,
            lm=ledger_manager,
            ds=mnt_filepath,
            client=client_no_amm,
            pop_ledger=pop_ledger_merge,
            at_timestamp=start + timedelta(days=1, hours=1),
        )
        day1_bal = gr_business.balance

        gr_business.prebuild_balance(
            product_manager=product_manager,
            lm=ledger_manager,
            ds=mnt_filepath,
            client=client_no_amm,
            pop_ledger=pop_ledger_merge,
            at_timestamp=start + timedelta(days=2, hours=1),
        )
        day2_bal = gr_business.balance

        gr_business.prebuild_balance(
            product_manager=product_manager,
            lm=ledger_manager,
            ds=mnt_filepath,
            client=client_no_amm,
            pop_ledger=pop_ledger_merge,
            at_timestamp=start + timedelta(days=3, hours=1),
        )
        day3_bal = gr_business.balance

        gr_business.prebuild_balance(
            product_manager=product_manager,
            lm=ledger_manager,
            ds=mnt_filepath,
            client=client_no_amm,
            pop_ledger=pop_ledger_merge,
            at_timestamp=start + timedelta(days=4, hours=1),
        )
        day4_bal = gr_business.balance

        gr_business.prebuild_balance(
            product_manager=product_manager,
            lm=ledger_manager,
            ds=mnt_filepath,
            client=client_no_amm,
            pop_ledger=pop_ledger_merge,
            at_timestamp=start + timedelta(days=5, hours=1),
        )
        day5_bal = gr_business.balance

        gr_business.prebuild_balance(
            product_manager=product_manager,
            lm=ledger_manager,
            ds=mnt_filepath,
            client=client_no_amm,
            pop_ledger=pop_ledger_merge,
            at_timestamp=start + timedelta(days=6, hours=1),
        )
        day6_bal = gr_business.balance

        assert isinstance(day1_bal, BusinessBalances)
        assert isinstance(day2_bal, BusinessBalances)
        assert isinstance(day3_bal, BusinessBalances)
        assert isinstance(day4_bal, BusinessBalances)
        assert isinstance(day5_bal, BusinessBalances)
        assert isinstance(day6_bal, BusinessBalances)

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
    def start(self, utc_90days_ago: datetime) -> datetime:
        s = utc_90days_ago.replace(microsecond=0)
        return s

    @pytest.fixture(scope="function")
    def offset(self) -> str:
        return "15d"

    @pytest.fixture(scope="function")
    def duration(
        self,
    ) -> timedelta | None:
        return None

    def test_cache_key(self, gr_business: Business):
        assert isinstance(gr_business.cache_key, str)
        assert ":" in gr_business.cache_key
        assert str(gr_business.uuid) in gr_business.cache_key

    def test_set_cache(
        self,
        gr_business: Business,
        thl_web_rr: PostgresConfig,
        client_no_amm: DaskClient,
        mnt_filepath: GRLDatasets,
        ledger_manager: LedgerManager,
        thl_ledger_manager: ThlLedgerManager,
        business_payout_event_manager,
        gr_business_bank_account_manager: BusinessBankAccountManager,
        product_manager: ProductManager,
        product_factory: Callable[..., Product],
        gr_team: Team,
        session_with_tx_factory: Callable[..., Session],
        user_factory: Callable[..., User],
        ledger_collection,
        pop_ledger_merge: PopLedgerMerge,
        utc_60days_ago: datetime,
        delete_ledger_db: Callable[..., None],
        create_main_accounts: Callable[..., None],
        gr_redis_config: RedisConfig,
        mnt_gr_api_dir: Path,
    ):
        client = gr_redis_config.create_redis_client()
        assert client.get(name=gr_business.cache_key) is None

        p1 = product_factory(team=gr_team, business=gr_business)
        u1 = user_factory(product=p1)

        # Business needs tx & incite to build balance
        delete_ledger_db()
        create_main_accounts()
        thl_ledger_manager.get_account_or_create_bp_wallet(product=p1)
        session_with_tx_factory(user=u1, started=utc_60days_ago)
        ledger_collection.initial_load(client=None, sync=True)
        pop_ledger_merge.build(client=client_no_amm, ledger_coll=ledger_collection)

        gr_business.set_cache(
            product_manager=product_manager,
            business_bank_account_manager=gr_business_bank_account_manager,
            pg_config=thl_web_rr,
            thl_web_rr=thl_web_rr,
            redis_config=gr_redis_config,
            client=client_no_amm,
            ds=mnt_filepath,
            lm=ledger_manager,
            thl_lm=thl_ledger_manager,
            bpem=business_payout_event_manager,
            pop_ledger=pop_ledger_merge,
            mnt_gr_api=mnt_gr_api_dir,
        )

        assert client.hgetall(name=gr_business.cache_key) is not None
        from generalresearch.models.gr.business import Business

        # We're going to pull only a specific year, but make sure that
        # it's being assigned to the field regardless
        year = datetime.now(tz=UTC).year
        res = Business.from_redis(
            uuid=gr_business.uuid,
            fields=[f"pop_financial:{year}"],
            gr_redis_config=gr_redis_config,
        )
        assert len(res.pop_financial) > 0

    def test_set_cache_business(
        self,
        gr_business: Business,
        gr_db: PostgresConfig,
        thl_web_rr: PostgresConfig,
        product_factory: Callable[..., Product],
        team: Team,
        client_no_amm: DaskClient,
        mnt_filepath: GRLDatasets,
        ledger_manager: LedgerManager,
        thl_ledger_manager: ThlLedgerManager,
        business_payout_event_manager,
        product_manager: ProductManager,
        gr_business_bank_account_manager: BusinessBankAccountManager,
        user_factory: Callable[..., User],
        delete_ledger_db: Callable[..., None],
        create_main_accounts: Callable[..., None],
        session_with_tx_factory: Callable[..., Session],
        ledger_collection,
        team_manager: TeamManager,
        pop_ledger_merge: PopLedgerMerge,
        gr_redis_config: RedisConfig,
        utc_60days_ago: datetime,
        mnt_gr_api_dir: Path,
    ):
        from generalresearch.models.gr.business import Business

        p1 = product_factory(team=team, business=gr_business)
        u1 = user_factory(product=p1)
        team_manager.add_business(team=team, business=gr_business)

        # Business needs tx & incite to build balance
        delete_ledger_db()
        create_main_accounts()
        thl_ledger_manager.get_account_or_create_bp_wallet(product=p1)
        session_with_tx_factory(user=u1, started=utc_60days_ago)
        ledger_collection.initial_load(client=None, sync=True)
        pop_ledger_merge.build(client=client_no_amm, ledger_coll=ledger_collection)

        gr_business.set_cache(
            product_manager=product_manager,
            business_bank_account_manager=gr_business_bank_account_manager,
            pg_config=gr_db,
            thl_web_rr=thl_web_rr,
            redis_config=gr_redis_config,
            client=client_no_amm,
            ds=mnt_filepath,
            lm=ledger_manager,
            thl_lm=thl_ledger_manager,
            bpem=business_payout_event_manager,
            pop_ledger=pop_ledger_merge,
            mnt_gr_api=mnt_gr_api_dir,
        )

        # keys: List = Business.required_fields() + ["products", "bp_accounts"]
        business2 = Business.from_redis(
            uuid=gr_business.uuid,
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

        assert isinstance(business2, Business)
        assert gr_business.model_dump_json() == business2.model_dump_json()
        # assert isinstance(business2.balance, BusinessBalances)
        assert isinstance(business2.products, list)
        assert isinstance(business2.teams, list)
        assert p1.uuid in [p.uuid for p in business2.products]
        assert len(business2.teams) == 1
        assert team.uuid in [t.uuid for t in business2.teams]

        assert isinstance(business2.balance, BusinessBalances)
        assert business2.balance.payout == 48
        assert business2.balance.balance == 48
        assert business2.balance.net == 48
        assert business2.balance.retainer == 12
        assert business2.balance.available_balance == 36
        assert len(business2.balance.product_balances) == 1

        assert len(business2.payouts) == 0

        assert len(business2.bp_accounts) == 1
        assert len(business2.bp_accounts) == len(business2.product_uuids)

        assert isinstance(business2.pop_financial, list)
        assert len(business2.pop_financial) == 1
        assert business2.pop_financial[0].payout == business2.balance.payout
        assert business2.pop_financial[0].net == business2.balance.net

    def test_prebuild_enriched_session_parquet(
        self,
        enriched_session_merge,
        client_no_amm: DaskClient,
        wall_collection: WallDFCollection,
        product_manager: ProductManager,
        session_collection: SessionDFCollection,
        thl_web_rr: PostgresConfig,
        user_factory: Callable[..., User],
        start: datetime,
        session_factory: Callable[..., Session],
        product_factory: Callable[..., Product],
        delete_df_collection: Callable[..., None],
        gr_business: Business,
        mnt_filepath: GRLDatasets,
        mnt_gr_api_dir: Path,
    ):

        delete_df_collection(coll=wall_collection)
        delete_df_collection(coll=session_collection)

        p1 = product_factory(business=gr_business)
        p2 = product_factory(business=gr_business)

        for p in [p1, p2]:
            u = user_factory(product=p)
            for i in range(50):
                session_factory(
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

        gr_business.prebuild_enriched_session_parquet(
            product_manager=product_manager,
            ds=mnt_filepath,
            client=client_no_amm,
            mnt_gr_api=mnt_gr_api_dir,
            enriched_session=enriched_session_merge,
        )

        # Now try to read from path
        df = pd.read_parquet(
            os.path.join(
                mnt_gr_api_dir, "pop_session", f"{gr_business.file_key}.parquet"
            )
        )
        assert isinstance(df, pd.DataFrame)

    def test_prebuild_enriched_wall_parquet(
        self,
        enriched_wall_merge,
        client_no_amm: DaskClient,
        wall_collection: WallDFCollection,
        product_manager: ProductManager,
        session_collection: SessionDFCollection,
        thl_web_rr: PostgresConfig,
        user_factory: Callable[..., User],
        start: datetime,
        session_factory: Callable[..., Session],
        product_factory: Callable[..., Product],
        delete_df_collection: Callable[..., None],
        gr_business: Business,
        mnt_filepath: GRLDatasets,
        mnt_gr_api_dir: Path,
    ):

        delete_df_collection(coll=wall_collection)
        delete_df_collection(coll=session_collection)

        p1 = product_factory(business=gr_business)
        p2 = product_factory(business=gr_business)

        for p in [p1, p2]:
            u = user_factory(product=p)
            for i in range(50):
                session_factory(
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

        gr_business.prebuild_enriched_wall_parquet(
            product_manager=product_manager,
            ds=mnt_filepath,
            client=client_no_amm,
            mnt_gr_api=mnt_gr_api_dir,
            enriched_wall=enriched_wall_merge,
        )

        # Now try to read from path
        df = pd.read_parquet(
            os.path.join(mnt_gr_api_dir, "pop_event", f"{gr_business.file_key}.parquet")
        )
        assert isinstance(df, pd.DataFrame)
