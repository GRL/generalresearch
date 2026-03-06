from datetime import timedelta, datetime, timezone
from itertools import product as iter_product
from typing import Optional

import pandas as pd
import pytest
from distributed.utils_test import client_no_amm

from generalresearch.incite.schemas.mergers.pop_ledger import (
    numerical_col_names,
)
from test_utils.incite.collections.conftest import ledger_collection
from test_utils.incite.conftest import mnt_filepath, incite_item_factory
from test_utils.incite.mergers.conftest import pop_ledger_merge
from test_utils.managers.ledger.conftest import create_main_accounts


@pytest.mark.parametrize(
    argnames="offset, duration",
    argvalues=list(
        iter_product(
            ["12h", "3D"],
            [timedelta(days=4)],
        )
    ),
)
class TestMergePOPLedger:

    @pytest.fixture
    def start(self) -> "datetime":
        return datetime(year=2020, month=3, day=14, tzinfo=timezone.utc)

    @pytest.fixture
    def duration(self) -> Optional["timedelta"]:
        return timedelta(days=5)

    def test_base(
        self,
        client_no_amm,
        ledger_collection,
        pop_ledger_merge,
        product,
        user_factory,
        create_main_accounts,
        thl_lm,
        delete_df_collection,
        incite_item_factory,
        delete_ledger_db,
    ):
        from generalresearch.models.thl.ledger import LedgerAccount

        u = user_factory(product=product, created=ledger_collection.start)

        # -- Build & Setup
        delete_ledger_db()
        create_main_accounts()
        delete_df_collection(coll=ledger_collection)
        # assert ledger_collection.start is None
        # assert ledger_collection.offset is None

        for item in ledger_collection.items:
            incite_item_factory(item=item, user=u)
            item.initial_load()

        # Confirm any of the items are archived
        assert ledger_collection.progress.has_archive.eq(True).all()

        pop_ledger_merge.build(
            client=client_no_amm,
            ledger_coll=ledger_collection,
        )
        # assert pop_ledger_merge.progress.has_archive.eq(True).all()

        ddf = pop_ledger_merge.ddf()
        df: pd.DataFrame = client_no_amm.compute(collections=ddf, sync=True)

        assert isinstance(df, pd.DataFrame)
        assert not df.empty

        # --

        user_wallet_account: LedgerAccount = thl_lm.get_account_or_create_user_wallet(
            user=u
        )
        cash_account: LedgerAccount = thl_lm.get_account_cash()
        rev_account: LedgerAccount = thl_lm.get_account_task_complete_revenue()

        item_finishes = [i.finish for i in ledger_collection.items]
        item_finishes.sort(reverse=True)
        last_item_finish = item_finishes[0]

        # Pure SQL based lookups
        cash_balance: int = thl_lm.get_account_balance(account=cash_account)
        rev_balance: int = thl_lm.get_account_balance(account=rev_account)
        assert cash_balance > rev_balance

        # (1) Test Cash Account
        ddf = pop_ledger_merge.ddf(
            columns=numerical_col_names,
            filters=[
                ("account_id", "==", cash_account.uuid),
                ("time_idx", ">=", ledger_collection.start),
                ("time_idx", "<", last_item_finish),
            ],
        )
        df: pd.DataFrame = client_no_amm.compute(collections=ddf, sync=True)
        assert df["mp_payment.CREDIT"].sum() == 0
        assert cash_balance > 0
        assert df["mp_payment.DEBIT"].sum() == cash_balance

        # (2) Test Revenue Account
        ddf = pop_ledger_merge.ddf(
            columns=numerical_col_names,
            filters=[
                ("account_id", "==", rev_account.uuid),
                ("time_idx", ">=", ledger_collection.start),
                ("time_idx", "<", last_item_finish),
            ],
        )
        df: pd.DataFrame = client_no_amm.compute(collections=ddf, sync=True)

        assert rev_balance == 0
        assert df["bp_payment.CREDIT"].sum() == 0
        assert df["mp_payment.DEBIT"].sum() == 0
        assert df["mp_payment.CREDIT"].sum() > 0

        # -- Cleanup
        delete_ledger_db()

    def test_pydantic_init(
        self,
        client_no_amm,
        ledger_collection,
        pop_ledger_merge,
        mnt_filepath,
        product,
        user_factory,
        create_main_accounts,
        offset,
        duration,
        start,
        thl_lm,
        incite_item_factory,
        delete_df_collection,
        delete_ledger_db,
        session_collection,
    ):
        from generalresearch.models.thl.ledger import LedgerAccount
        from generalresearch.models.thl.product import Product
        from generalresearch.models.thl.finance import ProductBalances

        u = user_factory(product=product, created=session_collection.start)

        assert ledger_collection.finished is not None
        assert isinstance(u.product, Product)
        delete_ledger_db()
        create_main_accounts(),
        delete_df_collection(coll=ledger_collection)

        bp_account: LedgerAccount = thl_lm.get_account_or_create_bp_wallet(
            product=u.product
        )
        cash_account: LedgerAccount = thl_lm.get_account_cash()
        rev_account: LedgerAccount = thl_lm.get_account_task_complete_revenue()

        for item in ledger_collection.items:
            incite_item_factory(item=item, user=u)
            item.initial_load(overwrite=True)

        pop_ledger_merge.build(client=client_no_amm, ledger_coll=ledger_collection)

        item_finishes = [i.finish for i in ledger_collection.items]
        item_finishes.sort(reverse=True)
        last_item_finish = item_finishes[0]

        # (1) Filter by the Product Account, this means no cash_account, or
        #   rev_account transactions will be present in here...
        ddf = pop_ledger_merge.ddf(
            columns=numerical_col_names + ["time_idx"],
            filters=[
                ("account_id", "==", bp_account.uuid),
                ("time_idx", ">=", ledger_collection.start),
                ("time_idx", "<", last_item_finish),
            ],
        )
        df: pd.DataFrame = client_no_amm.compute(collections=ddf, sync=True)
        df = df.set_index("time_idx")
        assert not df.empty

        instance = ProductBalances.from_pandas(input_data=df.sum())
        assert instance.payout == instance.net == instance.bp_payment_credit
        assert instance.available_balance < instance.net
        assert instance.available_balance + instance.retainer == instance.net
        assert instance.balance == thl_lm.get_account_balance(bp_account)
        assert df["bp_payment.CREDIT"].sum() == thl_lm.get_account_balance(bp_account)

        # (2) Filter by the Cash Account
        ddf = pop_ledger_merge.ddf(
            columns=numerical_col_names + ["time_idx"],
            filters=[
                ("account_id", "==", cash_account.uuid),
                ("time_idx", ">=", ledger_collection.start),
                ("time_idx", "<", last_item_finish),
            ],
        )
        df: pd.DataFrame = client_no_amm.compute(collections=ddf, sync=True)

        cash_balance: int = thl_lm.get_account_balance(account=cash_account)
        assert df["bp_payment.CREDIT"].sum() == 0
        assert cash_balance > 0
        assert df["mp_payment.CREDIT"].sum() == 0
        assert df["mp_payment.DEBIT"].sum() == cash_balance

        # (2) Filter by the Revenue Account
        ddf = pop_ledger_merge.ddf(
            columns=numerical_col_names + ["time_idx"],
            filters=[
                ("account_id", "==", rev_account.uuid),
                ("time_idx", ">=", ledger_collection.start),
                ("time_idx", "<", last_item_finish),
            ],
        )
        df: pd.DataFrame = client_no_amm.compute(collections=ddf, sync=True)

        rev_balance: int = thl_lm.get_account_balance(account=rev_account)
        assert rev_balance == 0
        assert df["bp_payment.CREDIT"].sum() == 0
        assert df["mp_payment.DEBIT"].sum() == 0
        assert df["mp_payment.CREDIT"].sum() > 0

    def test_resample(
        self,
        client_no_amm,
        ledger_collection,
        pop_ledger_merge,
        mnt_filepath,
        user_factory,
        product,
        create_main_accounts,
        offset,
        duration,
        start,
        thl_lm,
        delete_df_collection,
        incite_item_factory,
    ):
        from generalresearch.models.thl.user import User

        assert ledger_collection.finished is not None
        delete_df_collection(coll=ledger_collection)
        u1: User = user_factory(product=product)

        bp_account = thl_lm.get_account_or_create_bp_wallet(product=u1.product)

        for item in ledger_collection.items:
            incite_item_factory(user=u1, item=item)
            item.initial_load(overwrite=True)

        pop_ledger_merge.build(client=client_no_amm, ledger_coll=ledger_collection)

        item_finishes = [i.finish for i in ledger_collection.items]
        item_finishes.sort(reverse=True)
        last_item_finish = item_finishes[0]

        ddf = pop_ledger_merge.ddf(
            columns=numerical_col_names + ["time_idx"],
            filters=[
                ("account_id", "==", bp_account.uuid),
                ("time_idx", ">=", ledger_collection.start),
                ("time_idx", "<", last_item_finish),
            ],
        )
        df = client_no_amm.compute(collections=ddf, sync=True)
        assert isinstance(df, pd.DataFrame)
        assert isinstance(df.index, pd.Index)
        assert not isinstance(df.index, pd.RangeIndex)

        # Now change the index so we can easily resample it
        df = df.set_index("time_idx")
        assert isinstance(df.index, pd.Index)
        assert isinstance(df.index, pd.DatetimeIndex)

        bp_account_balance = thl_lm.get_account_balance(account=bp_account)

        # Initial sum
        initial_sum = df.sum().sum()
        # assert len(df) == 48  # msg="Original df should be 48 rows"

        # Original (1min) to 5min
        df_5min = df.resample("5min").sum()
        # assert len(df_5min) == 12
        assert initial_sum == df_5min.sum().sum()

        # 30min
        df_30min = df.resample("30min").sum()
        # assert len(df_30min) == 2
        assert initial_sum == df_30min.sum().sum()

        # 1hr
        df_1hr = df.resample("1h").sum()
        # assert len(df_1hr) == 1
        assert initial_sum == df_1hr.sum().sum()

        # 1 day
        df_1day = df.resample("1d").sum()
        # assert len(df_1day) == 1
        assert initial_sum == df_1day.sum().sum()
