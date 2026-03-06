from itertools import product as iproduct
from random import randint
from uuid import uuid4

import pytest

from generalresearch.currency import LedgerCurrency
from generalresearch.managers.base import Permission
from generalresearch.managers.thl.ledger_manager.exceptions import (
    LedgerAccountDoesntExistError,
)
from generalresearch.managers.thl.ledger_manager.ledger import LedgerManager
from generalresearch.models.thl.ledger import LedgerAccount, AccountType, Direction
from generalresearch.models.thl.ledger import (
    LedgerEntry,
)
from test_utils.managers.ledger.conftest import ledger_account


@pytest.mark.parametrize(
    argnames="currency, kind, acct_id",
    argvalues=list(
        iproduct(
            ["USD", "test", "EUR"],
            ["expense", "wallet", "revenue", "cash"],
            [uuid4().hex for i in range(3)],
        )
    ),
)
class TestLedgerAccountManagerNoResults:

    def test_get_account_no_results(self, currency, kind, acct_id, lm):
        """Try to query for accounts that we know don't exist and confirm that
        we either get the expected None result or it raises the correct
        exception
        """
        qn = ":".join([currency, kind, acct_id])

        # (1) .get_account is just a wrapper for .get_account_many_ but
        #   call it either way
        assert lm.get_account(qualified_name=qn, raise_on_error=False) is None

        with pytest.raises(expected_exception=LedgerAccountDoesntExistError):
            lm.get_account(qualified_name=qn, raise_on_error=True)

        # (2) .get_account_if_exists is another wrapper
        assert lm.get_account(qualified_name=qn, raise_on_error=False) is None

    def test_get_account_no_results_many(self, currency, kind, acct_id, lm):
        qn = ":".join([currency, kind, acct_id])

        # (1) .get_many_
        assert lm.get_account_many_(qualified_names=[qn], raise_on_error=False) == []

        with pytest.raises(expected_exception=LedgerAccountDoesntExistError):
            lm.get_account_many_(qualified_names=[qn], raise_on_error=True)

        # (2) .get_many
        assert lm.get_account_many(qualified_names=[qn], raise_on_error=False) == []

        with pytest.raises(expected_exception=LedgerAccountDoesntExistError):
            lm.get_account_many(qualified_names=[qn], raise_on_error=True)

        # (3) .get_accounts(..)
        assert lm.get_accounts_if_exists(qualified_names=[qn]) == []

        with pytest.raises(expected_exception=LedgerAccountDoesntExistError):
            lm.get_accounts(qualified_names=[qn])


@pytest.mark.parametrize(
    argnames="currency, account_type, direction",
    argvalues=list(
        iproduct(
            list(LedgerCurrency),
            list(AccountType),
            list(Direction),
        )
    ),
)
class TestLedgerAccountManagerCreate:

    def test_create_account_error_permission(
        self, currency, account_type, direction, lm
    ):
        """Confirm that the Permission values that are set on the Ledger Manger
        allow the Creation action to occur.
        """
        acct_uuid = uuid4().hex

        account = LedgerAccount(
            display_name=f"test-{uuid4().hex}",
            currency=currency,
            qualified_name=f"{currency.value}:{account_type.value}:{acct_uuid}",
            account_type=account_type,
            normal_balance=direction,
        )

        # (1) With no Permissions defined
        test_lm = LedgerManager(
            pg_config=lm.pg_config,
            permissions=[],
            redis_config=lm.redis_config,
            cache_prefix=lm.cache_prefix,
            testing=lm.testing,
        )

        with pytest.raises(expected_exception=AssertionError) as excinfo:
            test_lm.create_account(account=account)
        assert (
            str(excinfo.value) == "LedgerManager does not have sufficient permissions"
        )

        # (2) With Permissions defined, but not CREATE
        test_lm = LedgerManager(
            pg_config=lm.pg_config,
            permissions=[Permission.READ, Permission.UPDATE, Permission.DELETE],
            redis_config=lm.redis_config,
            cache_prefix=lm.cache_prefix,
            testing=lm.testing,
        )

        with pytest.raises(expected_exception=AssertionError) as excinfo:
            test_lm.create_account(account=account)
        assert (
            str(excinfo.value) == "LedgerManager does not have sufficient permissions"
        )

    def test_create(self, currency, account_type, direction, lm):
        """Confirm that the Permission values that are set on the Ledger Manger
        allow the Creation action to occur.
        """

        acct_uuid = uuid4().hex
        qn = f"{currency.value}:{account_type.value}:{acct_uuid}"

        acct_model = LedgerAccount(
            uuid=acct_uuid,
            display_name=f"test-{uuid4().hex}",
            currency=currency,
            qualified_name=qn,
            account_type=account_type,
            normal_balance=direction,
        )
        account = lm.create_account(account=acct_model)
        assert isinstance(account, LedgerAccount)

        # Query for, and make sure the Account was saved in the DB
        res = lm.get_account(qualified_name=qn, raise_on_error=True)
        assert account.uuid == res.uuid

    def test_get_or_create(self, currency, account_type, direction, lm):
        """Confirm that the Permission values that are set on the Ledger Manger
        allow the Creation action to occur.
        """

        acct_uuid = uuid4().hex
        qn = f"{currency.value}:{account_type.value}:{acct_uuid}"

        acct_model = LedgerAccount(
            uuid=acct_uuid,
            display_name=f"test-{uuid4().hex}",
            currency=currency,
            qualified_name=qn,
            account_type=account_type,
            normal_balance=direction,
        )
        account = lm.get_account_or_create(account=acct_model)
        assert isinstance(account, LedgerAccount)

        # Query for, and make sure the Account was saved in the DB
        res = lm.get_account(qualified_name=qn, raise_on_error=True)
        assert account.uuid == res.uuid


class TestLedgerAccountManagerGet:

    def test_get(self, ledger_account, lm):
        res = lm.get_account(qualified_name=ledger_account.qualified_name)
        assert res.uuid == ledger_account.uuid

        res = lm.get_account_many(qualified_names=[ledger_account.qualified_name])
        assert len(res) == 1
        assert res[0].uuid == ledger_account.uuid

        res = lm.get_accounts(qualified_names=[ledger_account.qualified_name])
        assert len(res) == 1
        assert res[0].uuid == ledger_account.uuid

    # TODO: I can't test the get_balance without first having Transaction
    #   creation working

    def test_get_balance_empty(
        self, ledger_account, ledger_account_credit, ledger_account_debit, ledger_tx, lm
    ):
        res = lm.get_account_balance(account=ledger_account)
        assert res == 0

        res = lm.get_account_balance(account=ledger_account_credit)
        assert res == 100

        res = lm.get_account_balance(account=ledger_account_debit)
        assert res == 100

    @pytest.mark.parametrize("n_times", range(5))
    def test_get_account_filtered_balance(
        self,
        ledger_account,
        ledger_account_credit,
        ledger_account_debit,
        ledger_tx,
        n_times,
        lm,
    ):
        """Try searching for random metadata and confirm it's always 0 because
        Tx can be found.
        """
        rand_key = f"key-{uuid4().hex[:10]}"
        rand_value = uuid4().hex

        assert (
            lm.get_account_filtered_balance(
                account=ledger_account, metadata_key=rand_key, metadata_value=rand_value
            )
            == 0
        )

        #  Let's create a transaction with this metadata to confirm it saves
        #   and that we can filter it back
        rand_amount = randint(10, 1_000)

        lm.create_tx(
            entries=[
                LedgerEntry(
                    direction=Direction.CREDIT,
                    account_uuid=ledger_account_credit.uuid,
                    amount=rand_amount,
                ),
                LedgerEntry(
                    direction=Direction.DEBIT,
                    account_uuid=ledger_account_debit.uuid,
                    amount=rand_amount,
                ),
            ],
            metadata={rand_key: rand_value},
        )

        assert (
            lm.get_account_filtered_balance(
                account=ledger_account_credit,
                metadata_key=rand_key,
                metadata_value=rand_value,
            )
            == rand_amount
        )

        assert (
            lm.get_account_filtered_balance(
                account=ledger_account_debit,
                metadata_key=rand_key,
                metadata_value=rand_value,
            )
            == rand_amount
        )

    def test_get_balance_timerange_empty(self, ledger_account, lm):
        res = lm.get_account_balance_timerange(account=ledger_account)
        assert res == 0
