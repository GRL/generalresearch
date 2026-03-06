from datetime import timezone, timedelta
from itertools import product as iter_product
from random import randint
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
from faker import Faker

from generalresearch.incite.schemas.mergers.pop_ledger import (
    numerical_col_names,
)
from generalresearch.models.thl.finance import (
    POPFinancial,
    ProductBalances,
    BusinessBalances,
)
from test_utils.conftest import delete_df_collection
from test_utils.incite.collections.conftest import ledger_collection
from test_utils.incite.mergers.conftest import pop_ledger_merge
from test_utils.managers.ledger.conftest import (
    create_main_accounts,
    session_with_tx_factory,
)

fake = Faker()


class TestProductBalanceInitialize:

    def test_unknown_fields(self):
        with pytest.raises(expected_exception=ValueError):
            ProductBalances.model_validate(
                {
                    "bp_payment.DEBIT": 1,
                }
            )

    def test_payout(self):
        val = randint(1, 1_000)
        instance = ProductBalances.model_validate({"bp_payment.CREDIT": val})
        assert instance.payout == val

    def test_adjustment(self):
        instance = ProductBalances.model_validate(
            {"bp_adjustment.CREDIT": 90, "bp_adjustment.DEBIT": 147}
        )

        assert -57 == instance.adjustment

    def test_plug(self):
        instance = ProductBalances.model_validate(
            {
                "bp_adjustment.CREDIT": 1000,
                "bp_adjustment.DEBIT": 200,
                "plug.DEBIT": 50,
            }
        )
        assert 750 == instance.adjustment

        instance = ProductBalances.model_validate(
            {
                "bp_payment.CREDIT": 789,
                "bp_adjustment.CREDIT": 23,
                "bp_adjustment.DEBIT": 101,
                "plug.DEBIT": 17,
            }
        )
        assert 694 == instance.net
        assert 694 == instance.balance

    def test_expense(self):
        instance = ProductBalances.model_validate(
            {"user_bonus.CREDIT": 0, "user_bonus.DEBIT": 999}
        )

        assert -999 == instance.expense

    def test_payment(self):
        instance = ProductBalances.model_validate(
            {"bp_payout.CREDIT": 1, "bp_payout.DEBIT": 100}
        )

        assert 99 == instance.payment

    def test_balance(self):
        instance = ProductBalances.model_validate(
            {
                # Payouts from surveys: 1000
                "bp_payment.CREDIT": 1000,
                # Adjustments: -200
                "bp_adjustment.CREDIT": 100,
                "bp_adjustment.DEBIT": 300,
                # Expense: -50
                "user_bonus.CREDIT": 0,
                "user_bonus.DEBIT": 50,
                # Prior supplier Payouts = 99
                "bp_payout.CREDIT": 1,
                "bp_payout.DEBIT": 100,
            }
        )

        # Supplier payments aren't considered in the net
        assert 750 == instance.net

        # Confirm any Supplier payments are taken out of their balance
        assert 651 == instance.balance

    def test_retainer(self):
        instance = ProductBalances.model_validate(
            {
                "bp_payment.CREDIT": 1000,
            }
        )

        assert 1000 == instance.balance
        assert 250 == instance.retainer

        instance = ProductBalances.model_validate(
            {
                "bp_payment.CREDIT": 1000,
                # 1001 worth of adjustments, making it negative
                "bp_adjustment.DEBIT": 1001,
            }
        )

        assert -1 == instance.balance
        assert 0 == instance.retainer

    def test_available_balance(self):
        instance = ProductBalances.model_validate(
            {
                "bp_payment.CREDIT": 1000,
            }
        )

        assert 750 == instance.available_balance

        instance = ProductBalances.model_validate(
            {
                # Payouts from surveys: $188.37
                "bp_payment.CREDIT": 18_837,
                # Adjustments: -$7.53 + $.17
                "bp_adjustment.CREDIT": 17,
                "bp_adjustment.DEBIT": 753,
                # $.15 of those marketplace Failure >> Completes were never
                #   actually paid out, so plug those positive adjustments
                "plug.DEBIT": 15,
                # Expense: -$27.45
                "user_bonus.CREDIT": 0,
                "user_bonus.DEBIT": 2_745,
                # Prior supplier Payouts = $100
                "bp_payout.CREDIT": 1,
                "bp_payout.DEBIT": 10_001,
            }
        )

        assert 18837 == instance.payout
        assert -751 == instance.adjustment
        assert 15341 == instance.net

        # Confirm any Supplier payments are taken out of their balance
        assert 5341 == instance.balance
        assert 1335 == instance.retainer
        assert 4006 == instance.available_balance

    def test_json_schema(self):
        instance = ProductBalances.model_validate(
            {
                # Payouts from surveys: 1000
                "bp_payment.CREDIT": 1000,
                # Adjustments: -200
                "bp_adjustment.CREDIT": 100,
                "bp_adjustment.DEBIT": 300,
                # $.80 of those marketplace Failure >> Completes were never
                #   actually paid out, so plug those positive adjustments
                "plug.DEBIT": 80,
                # Expense: -50
                "user_bonus.CREDIT": 0,
                "user_bonus.DEBIT": 50,
                # Prior supplier Payouts = 99
                "bp_payout.CREDIT": 1,
                "bp_payout.DEBIT": 100,
            }
        )

        assert isinstance(instance.model_json_schema(), dict)
        openapi_fields = list(instance.model_json_schema()["properties"].keys())

        # Ensure the SkipJsonSchema is working..
        assert "mp_payment_credit" not in openapi_fields
        assert "mp_payment_debit" not in openapi_fields
        assert "mp_adjustment_credit" not in openapi_fields
        assert "mp_adjustment_debit" not in openapi_fields
        assert "bp_payment_debit" not in openapi_fields
        assert "plug_credit" not in openapi_fields
        assert "plug_debit" not in openapi_fields

        # Confirm the @property computed fields show up in openapi. I don't
        #   know how to do that yet... so this is check to confirm they're
        #   known computed fields for now
        computed_fields = list(instance.model_computed_fields.keys())
        assert "payout" in computed_fields
        assert "adjustment" in computed_fields
        assert "expense" in computed_fields
        assert "payment" in computed_fields
        assert "net" in computed_fields
        assert "balance" in computed_fields
        assert "retainer" in computed_fields
        assert "available_balance" in computed_fields

    def test_repr(self):
        instance = ProductBalances.model_validate(
            {
                # Payouts from surveys: 1000
                "bp_payment.CREDIT": 1000,
                # Adjustments: -200
                "bp_adjustment.CREDIT": 100,
                "bp_adjustment.DEBIT": 300,
                # $.80 of those marketplace Failure >> Completes were never
                #   actually paid out, so plug those positive adjustments
                "plug.DEBIT": 80,
                # Expense: -50
                "user_bonus.CREDIT": 0,
                "user_bonus.DEBIT": 50,
                # Prior supplier Payouts = 99
                "bp_payout.CREDIT": 1,
                "bp_payout.DEBIT": 100,
            }
        )

        assert "Total Adjustment: -$2.80" in str(instance)


class TestBusinessBalanceInitialize:

    def test_validate_product_ids(self):
        instance1 = ProductBalances.model_validate(
            {"bp_payment.CREDIT": 500, "bp_adjustment.DEBIT": 40}
        )

        instance2 = ProductBalances.model_validate(
            {"bp_payment.CREDIT": 500, "bp_adjustment.DEBIT": 40}
        )

        with pytest.raises(expected_exception=ValueError) as cm:
            BusinessBalances.model_validate(
                {"product_balances": [instance1, instance2]}
            )
        assert "'product_id' must be set for BusinessBalance children" in str(cm.value)

        # Confirm that once you add them, it successfully initializes
        instance1.product_id = uuid4().hex
        instance2.product_id = uuid4().hex
        instance = BusinessBalances.model_validate(
            {"product_balances": [instance1, instance2]}
        )
        assert isinstance(instance, BusinessBalances)

    def test_payout(self):
        instance1 = ProductBalances.model_validate(
            {
                "product_id": uuid4().hex,
                "bp_payment.CREDIT": 500,
                "bp_adjustment.DEBIT": 40,
            }
        )

        instance2 = ProductBalances.model_validate(
            {
                "product_id": uuid4().hex,
                "bp_payment.CREDIT": 500,
                "bp_adjustment.DEBIT": 40,
            }
        )

        # Confirm the base payouts are as expected.
        assert instance1.payout == 500
        assert instance2.payout == 500

        # Now confirm that they're correct in the BusinessBalance
        instance = BusinessBalances.model_validate(
            {"product_balances": [instance1, instance2]}
        )
        assert instance.payout == 1_000

    def test_adjustment(self):
        instance1 = ProductBalances.model_validate(
            {
                "product_id": uuid4().hex,
                "bp_payment.CREDIT": 500,
                "bp_adjustment.CREDIT": 20,
                "bp_adjustment.DEBIT": 40,
                "plug.DEBIT": 10,
            }
        )

        instance2 = ProductBalances.model_validate(
            {
                "product_id": uuid4().hex,
                "bp_payment.CREDIT": 500,
                "bp_adjustment.CREDIT": 20,
                "bp_adjustment.DEBIT": 40,
                "plug.DEBIT": 10,
            }
        )

        # Confirm the base adjustment are as expected.
        assert instance1.adjustment == -30
        assert instance2.adjustment == -30

        # Now confirm that they're correct in the BusinessBalance
        instance = BusinessBalances.model_validate(
            {"product_balances": [instance1, instance2]}
        )
        assert instance.adjustment == -60

    def test_expense(self):
        instance1 = ProductBalances.model_validate(
            {
                "product_id": uuid4().hex,
                "bp_payment.CREDIT": 500,
                "bp_adjustment.CREDIT": 20,
                "bp_adjustment.DEBIT": 40,
                "plug.DEBIT": 10,
                "user_bonus.DEBIT": 5,
                "user_bonus.CREDIT": 1,
            }
        )

        instance2 = ProductBalances.model_validate(
            {
                "product_id": uuid4().hex,
                "bp_payment.CREDIT": 500,
                "bp_adjustment.CREDIT": 20,
                "bp_adjustment.DEBIT": 40,
                "plug.DEBIT": 10,
                "user_bonus.DEBIT": 5,
                "user_bonus.CREDIT": 1,
            }
        )

        # Confirm the base adjustment are as expected.
        assert instance1.expense == -4
        assert instance2.expense == -4

        # Now confirm that they're correct in the BusinessBalance
        instance = BusinessBalances.model_validate(
            {"product_balances": [instance1, instance2]}
        )
        assert instance.expense == -8

    def test_net(self):
        instance1 = ProductBalances.model_validate(
            {
                "product_id": uuid4().hex,
                "bp_payment.CREDIT": 500,
                "bp_adjustment.CREDIT": 20,
                "bp_adjustment.DEBIT": 40,
                "plug.DEBIT": 10,
                "user_bonus.DEBIT": 5,
                "user_bonus.CREDIT": 1,
            }
        )

        instance2 = ProductBalances.model_validate(
            {
                "product_id": uuid4().hex,
                "bp_payment.CREDIT": 500,
                "bp_adjustment.CREDIT": 20,
                "bp_adjustment.DEBIT": 40,
                "plug.DEBIT": 10,
                "user_bonus.DEBIT": 5,
                "user_bonus.CREDIT": 1,
            }
        )

        # Confirm the simple net
        assert instance1.net == 466
        assert instance2.net == 466

        # Now confirm that they're correct in the BusinessBalance
        instance = BusinessBalances.model_validate(
            {"product_balances": [instance1, instance2]}
        )
        assert instance.net == 466 * 2

    def test_payment(self):
        instance1 = ProductBalances.model_validate(
            {
                "product_id": uuid4().hex,
                "bp_payment.CREDIT": 500,
                "bp_adjustment.CREDIT": 20,
                "bp_adjustment.DEBIT": 40,
                "plug.DEBIT": 10,
                "user_bonus.DEBIT": 5,
                "user_bonus.CREDIT": 1,
                "bp_payout.CREDIT": 1,
                "bp_payout.DEBIT": 10_001,
            }
        )

        instance2 = ProductBalances.model_validate(
            {
                "product_id": uuid4().hex,
                "bp_payment.CREDIT": 500,
                "bp_adjustment.CREDIT": 20,
                "bp_adjustment.DEBIT": 40,
                "plug.DEBIT": 10,
                "user_bonus.DEBIT": 5,
                "user_bonus.CREDIT": 1,
                "bp_payout.CREDIT": 1,
                "bp_payout.DEBIT": 10_001,
            }
        )
        assert instance1.payment == 10_000
        assert instance2.payment == 10_000

        # Now confirm that they're correct in the BusinessBalance
        instance = BusinessBalances.model_validate(
            {"product_balances": [instance1, instance2]}
        )
        assert instance.payment == 20_000

    def test_balance(self):
        instance1 = ProductBalances.model_validate(
            {
                "product_id": uuid4().hex,
                "bp_payment.CREDIT": 50_000,
                "bp_adjustment.CREDIT": 20,
                "bp_adjustment.DEBIT": 500,
                "plug.DEBIT": 10,
                "user_bonus.DEBIT": 5,
                "user_bonus.CREDIT": 1,
                "bp_payout.CREDIT": 1,
                "bp_payout.DEBIT": 10_001,
            }
        )
        assert instance1.balance == 39_506

        instance2 = ProductBalances.model_validate(
            {
                "product_id": uuid4().hex,
                "bp_payment.CREDIT": 40_000,
                "bp_adjustment.CREDIT": 2_000,
                "bp_adjustment.DEBIT": 400,
                "plug.DEBIT": 983,
                "user_bonus.DEBIT": 392,
                "user_bonus.CREDIT": 0,
                "bp_payout.CREDIT": 0,
                "bp_payout.DEBIT": 8_000,
            }
        )
        assert instance2.balance == 32_225

        # Now confirm that they're correct in the BusinessBalance
        instance = BusinessBalances.model_validate(
            {"product_balances": [instance1, instance2]}
        )
        assert instance.balance == 39_506 + 32_225

    def test_retainer(self):
        instance1 = ProductBalances.model_validate(
            {
                "product_id": uuid4().hex,
                "bp_payment.CREDIT": 50_000,
                "bp_adjustment.CREDIT": 20,
                "bp_adjustment.DEBIT": 500,
                "plug.DEBIT": 10,
                "user_bonus.DEBIT": 5,
                "user_bonus.CREDIT": 1,
                "bp_payout.CREDIT": 1,
                "bp_payout.DEBIT": 10_001,
            }
        )
        assert instance1.balance == 39_506
        assert instance1.retainer == 9_876

        instance2 = ProductBalances.model_validate(
            {
                "product_id": uuid4().hex,
                "bp_payment.CREDIT": 40_000,
                "bp_adjustment.CREDIT": 2_000,
                "bp_adjustment.DEBIT": 400,
                "plug.DEBIT": 983,
                "user_bonus.DEBIT": 392,
                "user_bonus.CREDIT": 0,
                "bp_payout.CREDIT": 0,
                "bp_payout.DEBIT": 8_000,
            }
        )
        assert instance2.balance == 32_225
        assert instance2.retainer == 8_056

        # Now confirm that they're correct in the BusinessBalance
        instance = BusinessBalances.model_validate(
            {"product_balances": [instance1, instance2]}
        )
        assert instance.retainer == 9_876 + 8_056

    def test_available_balance(self):
        instance1 = ProductBalances.model_validate(
            {
                "product_id": uuid4().hex,
                "bp_payment.CREDIT": 50_000,
                "bp_adjustment.CREDIT": 20,
                "bp_adjustment.DEBIT": 500,
                "plug.DEBIT": 10,
                "user_bonus.DEBIT": 5,
                "user_bonus.CREDIT": 1,
                "bp_payout.CREDIT": 1,
                "bp_payout.DEBIT": 10_001,
            }
        )
        assert instance1.balance == 39_506
        assert instance1.retainer == 9_876
        assert instance1.available_balance == 39_506 - 9_876

        instance2 = ProductBalances.model_validate(
            {
                "product_id": uuid4().hex,
                "bp_payment.CREDIT": 40_000,
                "bp_adjustment.CREDIT": 2_000,
                "bp_adjustment.DEBIT": 400,
                "plug.DEBIT": 983,
                "user_bonus.DEBIT": 392,
                "user_bonus.CREDIT": 0,
                "bp_payout.CREDIT": 0,
                "bp_payout.DEBIT": 8_000,
            }
        )
        assert instance2.balance == 32_225
        assert instance2.retainer == 8_056
        assert instance2.available_balance == 32_225 - 8_056

        # Now confirm that they're correct in the BusinessBalance
        instance = BusinessBalances.model_validate(
            {"product_balances": [instance1, instance2]}
        )
        assert instance.retainer == 9_876 + 8_056
        assert instance.available_balance == instance.balance - (9_876 + 8_056)

    def test_negative_net(self):
        instance1 = ProductBalances.model_validate(
            {
                "product_id": uuid4().hex,
                "bp_payment.CREDIT": 50_000,
                "bp_adjustment.DEBIT": 50_001,
                "bp_payout.DEBIT": 4_999,
            }
        )
        assert 50_000 == instance1.payout
        assert -50_001 == instance1.adjustment
        assert 4_999 == instance1.payment

        assert -1 == instance1.net
        assert -5_000 == instance1.balance
        assert 0 == instance1.available_balance

        instance2 = ProductBalances.model_validate(
            {
                "product_id": uuid4().hex,
                "bp_payment.CREDIT": 50_000,
                "bp_adjustment.DEBIT": 10_000,
                "bp_payout.DEBIT": 10_000,
            }
        )
        assert 50_000 == instance2.payout
        assert -10_000 == instance2.adjustment
        assert 10_000 == instance2.payment

        assert 40_000 == instance2.net
        assert 30_000 == instance2.balance
        assert 22_500 == instance2.available_balance

        # Now confirm that they're correct in the BusinessBalance
        instance = BusinessBalances.model_validate(
            {"product_balances": [instance1, instance2]}
        )
        assert 100_000 == instance.payout
        assert -60_001 == instance.adjustment
        assert 14_999 == instance.payment

        assert 39_999 == instance.net
        assert 25_000 == instance.balance

        # Compare the retainers together. We can't just calculate the retainer
        # on the Business.balance because it'll be "masked" by any Products
        # that have a negative balance and actually reduce the Business's
        # retainer as a whole. Therefore, we need to sum together each of the
        # retainers from the child Products
        assert 0 == instance1.retainer
        assert 7_500 == instance2.retainer
        assert 6_250 == instance.balance * 0.25
        assert 6_250 != instance.retainer
        assert 7_500 == instance.retainer
        assert 25_000 - 7_500 == instance.available_balance

    def test_str(self):
        instance = BusinessBalances.model_validate(
            {
                "product_balances": [
                    ProductBalances.model_validate(
                        {
                            "product_id": uuid4().hex,
                            "bp_payment.CREDIT": 50_000,
                            "bp_adjustment.DEBIT": 50_001,
                            "bp_payout.DEBIT": 4_999,
                        }
                    ),
                    ProductBalances.model_validate(
                        {
                            "product_id": uuid4().hex,
                            "bp_payment.CREDIT": 50_000,
                            "bp_adjustment.DEBIT": 10_000,
                            "bp_payout.DEBIT": 10_000,
                        }
                    ),
                ]
            }
        )

        assert "Products: 2" in str(instance)
        assert "Total Adjustment: -$600.01" in str(instance)
        assert "Available Balance: $175.00" in str(instance)

    def test_from_json(self):
        s = '{"product_balances":[{"product_id":"7485124190274248bc14132755c8fc3b","bp_payment_credit":1184,"adjustment_credit":0,"adjustment_debit":0,"supplier_credit":0,"supplier_debit":0,"user_bonus_credit":0,"user_bonus_debit":0,"payout":1184,"adjustment":0,"expense":0,"net":1184,"payment":0,"balance":1184,"retainer":296,"available_balance":888,"adjustment_percent":0.0}],"payout":1184,"adjustment":0,"expense":0,"net":1184,"payment":0,"balance":1184,"retainer":296,"available_balance":888,"adjustment_percent":0.0}'
        instance = BusinessBalances.model_validate_json(s)

        assert instance.payout == 1184
        assert instance.available_balance == 888
        assert instance.retainer == 296
        assert len(instance.product_balances) == 1
        assert instance.adjustment_percent == 0.0
        assert instance.expense == 0

        p = instance.product_balances[0]
        assert p.payout == 1184
        assert p.available_balance == 888
        assert p.retainer == 296


@pytest.mark.parametrize(
    argnames="offset, duration",
    argvalues=list(
        iter_product(
            ["12h", "2D"],
            [timedelta(days=2), timedelta(days=5)],
        )
    ),
)
class TestProductFinanceData:

    def test_base(
        self,
        client_no_amm,
        ledger_collection,
        pop_ledger_merge,
        mnt_filepath,
        session_with_tx_factory,
        product,
        user_factory,
        start,
        duration,
        delete_df_collection,
        thl_lm,
        create_main_accounts,
    ):
        from generalresearch.models.thl.user import User

        # -- Build & Setup
        # assert ledger_collection.start is None
        # assert ledger_collection.offset is None
        u: User = user_factory(product=product, created=ledger_collection.start)

        for item in ledger_collection.items:

            for s_idx in range(3):
                rand_item_time = fake.date_time_between(
                    start_date=item.start,
                    end_date=item.finish,
                    tzinfo=timezone.utc,
                )
                session_with_tx_factory(started=rand_item_time, user=u)

            item.initial_load(overwrite=True)

        # Confirm any of the items are archived
        assert ledger_collection.progress.has_archive.eq(True).all()

        pop_ledger_merge.build(client=client_no_amm, ledger_coll=ledger_collection)
        # assert pop_ledger_merge.progress.has_archive.eq(True).all()

        item_finishes = [i.finish for i in ledger_collection.items]
        item_finishes.sort(reverse=True)
        last_item_finish = item_finishes[0]

        # --
        account = thl_lm.get_account_or_create_bp_wallet(product=u.product)

        ddf = pop_ledger_merge.ddf(
            force_rr_latest=False,
            include_partial=True,
            columns=numerical_col_names + ["time_idx", "account_id"],
            filters=[
                ("account_id", "==", account.uuid),
                ("time_idx", ">=", start),
                ("time_idx", "<", start + duration),
            ],
        )

        df: pd.DataFrame = client_no_amm.compute(collections=ddf, sync=True)

        assert isinstance(df, pd.DataFrame)
        assert not df.empty

        # --

        df = df.groupby([pd.Grouper(key="time_idx", freq="D"), "account_id"]).sum()
        res = POPFinancial.list_from_pandas(df, accounts=[account])

        assert isinstance(res, list)
        assert isinstance(res[0], POPFinancial)

        # On this, we can assert all products are the same, and that there are
        # no overlapping time intervals
        assert 1 == len(set(list([i.product_id for i in res])))
        assert len(res) == len(set(list([i.time for i in res])))


@pytest.mark.parametrize(
    argnames="offset, duration",
    argvalues=list(
        iter_product(
            ["12h", "2D"],
            [timedelta(days=2), timedelta(days=5)],
        )
    ),
)
class TestPOPFinancialData:

    def test_base(
        self,
        client_no_amm,
        ledger_collection,
        pop_ledger_merge,
        mnt_filepath,
        user_factory,
        product,
        start,
        duration,
        create_main_accounts,
        session_with_tx_factory,
        session_manager,
        thl_lm,
        delete_df_collection,
        delete_ledger_db,
    ):
        # -- Build & Setup
        delete_ledger_db()
        create_main_accounts()
        delete_df_collection(coll=ledger_collection)
        # assert ledger_collection.start is None
        # assert ledger_collection.offset is None

        users = []
        for idx in range(5):
            u = user_factory(product=product)

            for item in ledger_collection.items:
                rand_item_time = fake.date_time_between(
                    start_date=item.start,
                    end_date=item.finish,
                    tzinfo=timezone.utc,
                )

                session_with_tx_factory(started=rand_item_time, user=u)
                item.initial_load(overwrite=True)

            users.append(u)

        # Confirm any of the items are archived
        assert ledger_collection.progress.has_archive.eq(True).all()

        pop_ledger_merge.build(client=client_no_amm, ledger_coll=ledger_collection)
        # assert pop_ledger_merge.progress.has_archive.eq(True).all()

        item_finishes = [i.finish for i in ledger_collection.items]
        item_finishes.sort(reverse=True)
        last_item_finish = item_finishes[0]

        accounts = []
        for user in users:
            account = thl_lm.get_account_or_create_bp_wallet(product=u.product)
            accounts.append(account)
        account_ids = [a.uuid for a in accounts]

        # --

        ddf = pop_ledger_merge.ddf(
            force_rr_latest=False,
            include_partial=True,
            columns=numerical_col_names + ["time_idx", "account_id"],
            filters=[
                ("account_id", "in", account_ids),
                ("time_idx", ">=", start),
                ("time_idx", "<", last_item_finish),
            ],
        )
        df: pd.DataFrame = client_no_amm.compute(collections=ddf, sync=True)

        df = df.groupby([pd.Grouper(key="time_idx", freq="D"), "account_id"]).sum()
        res = POPFinancial.list_from_pandas(df, accounts=accounts)

        assert isinstance(res, list)
        for i in res:
            assert isinstance(i, POPFinancial)

            # This does not return the AccountID, it's the Product ID
            assert i.product_id in [u.product_id for u in users]

        # 1 Product, multiple Users
        assert len(users) == len(accounts)

        # We group on days, and duration is a parameter to parametrize
        assert isinstance(duration, timedelta)

        # -- Teardown
        delete_df_collection(ledger_collection)


@pytest.mark.parametrize(
    argnames="offset, duration",
    argvalues=list(
        iter_product(
            ["12h", "1D"],
            [timedelta(days=2), timedelta(days=3)],
        )
    ),
)
class TestBusinessBalanceData:
    def test_from_pandas(
        self,
        client_no_amm,
        ledger_collection,
        pop_ledger_merge,
        user_factory,
        product,
        create_main_accounts,
        session_factory,
        thl_lm,
        session_manager,
        start,
        thl_web_rr,
        duration,
        delete_df_collection,
        delete_ledger_db,
        session_with_tx_factory,
        offset,
        rm_ledger_collection,
    ):
        from generalresearch.models.thl.user import User
        from generalresearch.models.thl.ledger import LedgerAccount

        delete_ledger_db()
        create_main_accounts()
        delete_df_collection(coll=ledger_collection)
        rm_ledger_collection()

        for idx in range(5):
            u: User = user_factory(product=product, created=ledger_collection.start)

            for item in ledger_collection.items:
                item_time = fake.date_time_between(
                    start_date=item.start,
                    end_date=item.finish,
                    tzinfo=timezone.utc,
                )
                session_with_tx_factory(started=item_time, user=u)
                item.initial_load(overwrite=True)

        # Confirm any of the items are archived
        assert ledger_collection.progress.has_archive.eq(True).all()
        pop_ledger_merge.build(client=client_no_amm, ledger_coll=ledger_collection)
        # assert pop_ledger_merge.progress.has_archive.eq(True).all()

        account: LedgerAccount = thl_lm.get_account_or_create_bp_wallet(product=product)

        ddf = pop_ledger_merge.ddf(
            force_rr_latest=False,
            include_partial=True,
            columns=numerical_col_names + ["account_id"],
            filters=[("account_id", "in", [account.uuid])],
        )
        ddf = ddf.groupby("account_id").sum()
        df: pd.DataFrame = client_no_amm.compute(collections=ddf, sync=True)

        assert isinstance(df, pd.DataFrame)

        instance = BusinessBalances.from_pandas(
            input_data=df, accounts=[account], thl_pg_config=thl_web_rr
        )
        balance: int = thl_lm.get_account_balance(account=account)

        assert instance.balance == balance
        assert instance.net == balance
        assert instance.payout == balance

        assert instance.payment == 0
        assert instance.adjustment == 0
        assert instance.adjustment_percent == 0.0

        assert instance.expense == 0

        # Cleanup
        delete_ledger_db()
        delete_df_collection(coll=ledger_collection)
