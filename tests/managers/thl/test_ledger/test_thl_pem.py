import uuid
from random import randint
from uuid import uuid4, UUID

import pytest

from generalresearch.currency import USDCent
from generalresearch.models.thl.definitions import PayoutStatus
from generalresearch.models.thl.payout import BrokerageProductPayoutEvent
from generalresearch.models.thl.product import Product
from generalresearch.models.thl.wallet.cashout_method import (
    CashoutRequestInfo,
)


class TestThlPayoutEventManager:

    def test_get_by_uuid(self, brokerage_product_payout_event_manager, thl_lm):
        """This validates that the method raises an exception if it
        fails. There are plenty of other tests that use this method so
        it seems silly to duplicate it here again
        """

        with pytest.raises(expected_exception=AssertionError) as excinfo:
            brokerage_product_payout_event_manager.get_by_uuid(pe_uuid=uuid4().hex)
        assert "expected 1 result, got 0" in str(excinfo.value)

    def test_filter_by(
        self,
        product_factory,
        usd_cent,
        bp_payout_event_factory,
        thl_lm,
        brokerage_product_payout_event_manager,
    ):
        from generalresearch.models.thl.payout import UserPayoutEvent

        N_PRODUCTS = randint(3, 10)
        N_PAYOUT_EVENTS = randint(3, 10)
        amounts = []
        products = []

        for x_idx in range(N_PRODUCTS):
            product: Product = product_factory()
            thl_lm.get_account_or_create_bp_wallet(product=product)
            products.append(product)
            brokerage_product_payout_event_manager.set_account_lookup_table(
                thl_lm=thl_lm
            )

            for y_idx in range(N_PAYOUT_EVENTS):
                pe = bp_payout_event_factory(product=product, usd_cent=usd_cent)
                amounts.append(int(usd_cent))
                assert isinstance(pe, BrokerageProductPayoutEvent)

        # We just added Payout Events for Products, now go ahead and
        #   query for them
        accounts = thl_lm.get_accounts_bp_wallet_for_products(
            product_uuids=[i.uuid for i in products]
        )
        res = brokerage_product_payout_event_manager.filter_by(
            debit_account_uuids=[i.uuid for i in accounts]
        )

        assert len(res) == (N_PRODUCTS * N_PAYOUT_EVENTS)
        assert sum([i.amount for i in res]) == sum(amounts)

    def test_get_bp_payout_events_for_product(
        self,
        product_factory,
        usd_cent,
        bp_payout_event_factory,
        brokerage_product_payout_event_manager,
        thl_lm,
    ):
        from generalresearch.models.thl.payout import UserPayoutEvent

        N_PRODUCTS = randint(3, 10)
        N_PAYOUT_EVENTS = randint(3, 10)
        amounts = []
        products = []

        for x_idx in range(N_PRODUCTS):
            product: Product = product_factory()
            products.append(product)
            thl_lm.get_account_or_create_bp_wallet(product=product)
            brokerage_product_payout_event_manager.set_account_lookup_table(
                thl_lm=thl_lm
            )

            for y_idx in range(N_PAYOUT_EVENTS):
                pe = bp_payout_event_factory(product=product, usd_cent=usd_cent)
                amounts.append(usd_cent)
                assert isinstance(pe, BrokerageProductPayoutEvent)

            # We just added 5 Payouts for a specific Product, now go
            # ahead and query for them
            res = brokerage_product_payout_event_manager.get_bp_bp_payout_events_for_products(
                thl_ledger_manager=thl_lm, product_uuids=[product.id]
            )

            assert len(res) == N_PAYOUT_EVENTS

        # Now that all the Payouts for all the Products have been added, go
        #   ahead and query for them
        res = (
            brokerage_product_payout_event_manager.get_bp_bp_payout_events_for_products(
                thl_ledger_manager=thl_lm, product_uuids=[i.uuid for i in products]
            )
        )

        assert len(res) == (N_PRODUCTS * N_PAYOUT_EVENTS)
        assert sum([i.amount for i in res]) == sum(amounts)

    @pytest.mark.skip
    def test_get_payout_detail(self, user_payout_event_manager):
        """This fails because the description coming back is None, but then
        it tries to return a PayoutEvent which validates that the
        description can't be None
        """
        from generalresearch.models.thl.payout import (
            UserPayoutEvent,
            PayoutType,
        )

        rand_amount = randint(a=99, b=999)

        pe = user_payout_event_manager.create(
            debit_account_uuid=uuid4().hex,
            account_reference_type="str-type-random",
            account_reference_uuid=uuid4().hex,
            cashout_method_uuid=uuid4().hex,
            description="Best payout !",
            amount=rand_amount,
            status=PayoutStatus.PENDING,
            ext_ref_id="123",
            payout_type=PayoutType.CASH_IN_MAIL,
            request_data={"foo": 123},
            order_data={},
        )

        res = user_payout_event_manager.get_payout_detail(pe_uuid=pe.uuid)
        assert isinstance(res, CashoutRequestInfo)

    # def test_filter_by(self):
    #     raise NotImplementedError

    def test_create(self, user_payout_event_manager):
        from generalresearch.models.thl.payout import UserPayoutEvent

        # Confirm the creation method returns back an instance.
        pe = user_payout_event_manager.create_dummy()
        assert isinstance(pe, UserPayoutEvent)

        # Now query the DB for that PayoutEvent to confirm it was actually
        #   saved.
        res = user_payout_event_manager.get_by_uuid(pe_uuid=pe.uuid)
        assert isinstance(res, UserPayoutEvent)
        assert UUID(res.uuid)

        # Confirm they're the same
        # assert pe.model_dump_json() == res2.model_dump_json()
        assert res.description is None

    # def test_update(self):
    #     raise NotImplementedError

    def test_create_bp_payout(
        self,
        product,
        delete_ledger_db,
        create_main_accounts,
        thl_lm,
        brokerage_product_payout_event_manager,
        lm,
    ):
        from generalresearch.models.thl.payout import UserPayoutEvent

        delete_ledger_db()
        create_main_accounts()

        account_bp_wallet = thl_lm.get_account_or_create_bp_wallet(product=product)
        brokerage_product_payout_event_manager.set_account_lookup_table(thl_lm=thl_lm)

        rand_amount = randint(a=99, b=999)

        # Save a Brokerage Product Payout, so we have something in the
        # Payout Event table and the respective ledger TX and Entry rows for it
        pe = brokerage_product_payout_event_manager.create_bp_payout_event(
            thl_ledger_manager=thl_lm,
            product=product,
            amount=USDCent(rand_amount),
            skip_wallet_balance_check=True,
            skip_one_per_day_check=True,
        )
        assert isinstance(pe, BrokerageProductPayoutEvent)

        # Now try to query for it!
        res = thl_lm.get_tx_bp_payouts(account_uuids=[account_bp_wallet.uuid])
        assert len(res) == 1
        res = thl_lm.get_tx_bp_payouts(account_uuids=[uuid4().hex])
        assert len(res) == 0

        # Confirm it added to the users balance. The amount is negative because
        #   money was sent to the Brokerage Product, but they didn't have
        #   any activity that earned them money
        bal = lm.get_account_balance(account=account_bp_wallet)
        assert rand_amount == bal * -1


class TestBPPayoutEvent:

    def test_get_bp_bp_payout_events_for_products(
        self,
        product_factory,
        bp_payout_event_factory,
        usd_cent,
        delete_ledger_db,
        create_main_accounts,
        brokerage_product_payout_event_manager,
        thl_lm,
    ):
        delete_ledger_db()
        create_main_accounts()

        N_PAYOUT_EVENTS = randint(3, 10)
        amounts = []

        product: Product = product_factory()
        thl_lm.get_account_or_create_bp_wallet(product=product)
        brokerage_product_payout_event_manager.set_account_lookup_table(thl_lm=thl_lm)

        for y_idx in range(N_PAYOUT_EVENTS):
            bp_payout_event_factory(product=product, usd_cent=usd_cent)
            amounts.append(usd_cent)

        # Fetch using the _bp_bp_ approach, so we have an
        #   array of BPPayoutEvents
        bp_bp_res = (
            brokerage_product_payout_event_manager.get_bp_bp_payout_events_for_products(
                thl_ledger_manager=thl_lm, product_uuids=[product.uuid]
            )
        )
        assert isinstance(bp_bp_res, list)
        assert sum(amounts) == sum([i.amount for i in bp_bp_res])
        for i in bp_bp_res:
            assert isinstance(i, BrokerageProductPayoutEvent)
            assert isinstance(i.amount, int)
            assert isinstance(i.amount_usd, USDCent)
            assert isinstance(i.amount_usd_str, str)
            assert i.amount_usd_str[0] == "$"
