from decimal import Decimal
from random import randint
from uuid import uuid4

import pytest

from generalresearch.currency import LedgerCurrency
from generalresearch.managers.thl.ledger_manager.ledger import LedgerManager
from generalresearch.models.thl.ledger import (
    Direction,
    LedgerEntry,
    LedgerTransaction,
)


class TestLedgerManagerCreateTx:

    def test_create_account_error_permission(self, lm):
        """Confirm that the Permission values that are set on the Ledger Manger
        allow the Creation action to occur.
        """
        acct_uuid = uuid4().hex

        # (1) With no Permissions defined
        test_lm = LedgerManager(
            pg_config=lm.pg_config,
            permissions=[],
            redis_config=lm.redis_config,
            cache_prefix=lm.cache_prefix,
            testing=lm.testing,
        )

        with pytest.raises(expected_exception=AssertionError) as excinfo:
            test_lm.create_tx(entries=[])
        assert (
            str(excinfo.value)
            == "LedgerTransactionManager has insufficient Permissions"
        )

    def test_create_assertions(self, ledger_account_debit, ledger_account_credit, lm):
        with pytest.raises(expected_exception=ValueError) as excinfo:
            lm.create_tx(
                entries=[
                    {
                        "direction": Direction.CREDIT,
                        "account_uuid": uuid4().hex,
                        "amount": randint(a=1, b=100),
                    }
                ]
            )
        assert (
            "Assertion failed, ledger transaction must have 2 or more entries"
            in str(excinfo.value)
        )

    def test_create(self, ledger_account_credit, ledger_account_debit, lm):
        amount = int(Decimal("1.00") * 100)

        entries = [
            LedgerEntry(
                direction=Direction.CREDIT,
                account_uuid=ledger_account_credit.uuid,
                amount=amount,
            ),
            LedgerEntry(
                direction=Direction.DEBIT,
                account_uuid=ledger_account_debit.uuid,
                amount=amount,
            ),
        ]

        # Create a Transaction and validate the operation was successful
        tx = lm.create_tx(entries=entries)
        assert isinstance(tx, LedgerTransaction)

        res = lm.get_tx_by_id(transaction_id=tx.id)
        assert isinstance(res, LedgerTransaction)
        assert len(res.entries) == 2
        assert tx.id == res.id

    def test_create_and_reverse(self, ledger_account_credit, ledger_account_debit, lm):
        amount = int(Decimal("1.00") * 100)

        entries = [
            LedgerEntry(
                direction=Direction.CREDIT,
                account_uuid=ledger_account_credit.uuid,
                amount=amount,
            ),
            LedgerEntry(
                direction=Direction.DEBIT,
                account_uuid=ledger_account_debit.uuid,
                amount=amount,
            ),
        ]

        tx = lm.create_tx(entries=entries)
        res = lm.get_tx_by_id(transaction_id=tx.id)
        assert res.id == tx.id

        assert lm.get_account_balance(account=ledger_account_credit) == 100
        assert lm.get_account_balance(account=ledger_account_debit) == 100
        assert lm.check_ledger_balanced() is True

        # Reverse it
        entries = [
            LedgerEntry(
                direction=Direction.DEBIT,
                account_uuid=ledger_account_credit.uuid,
                amount=amount,
            ),
            LedgerEntry(
                direction=Direction.CREDIT,
                account_uuid=ledger_account_debit.uuid,
                amount=amount,
            ),
        ]

        tx = lm.create_tx(entries=entries)
        res = lm.get_tx_by_id(transaction_id=tx.id)
        assert res.id == tx.id

        assert lm.get_account_balance(ledger_account_credit) == 0
        assert lm.get_account_balance(ledger_account_debit) == 0
        assert lm.check_ledger_balanced()

        # subtract again
        entries = [
            LedgerEntry(
                direction=Direction.DEBIT,
                account_uuid=ledger_account_credit.uuid,
                amount=amount,
            ),
            LedgerEntry(
                direction=Direction.CREDIT,
                account_uuid=ledger_account_debit.uuid,
                amount=amount,
            ),
        ]
        tx = lm.create_tx(entries=entries)
        res = lm.get_tx_by_id(transaction_id=tx.id)
        assert res.id == tx.id

        assert lm.get_account_balance(ledger_account_credit) == -100
        assert lm.get_account_balance(ledger_account_debit) == -100
        assert lm.check_ledger_balanced()


class TestLedgerManagerGetTx:

    # @pytest.mark.parametrize("currency", [LedgerCurrency.TEST], indirect=True)
    def test_get_tx_by_id(self, ledger_tx, lm):
        with pytest.raises(expected_exception=AssertionError):
            lm.get_tx_by_id(transaction_id=ledger_tx)

        res = lm.get_tx_by_id(transaction_id=ledger_tx.id)
        assert res.id == ledger_tx.id

    # @pytest.mark.parametrize("currency", [LedgerCurrency.TEST], indirect=True)
    def test_get_tx_by_ids(self, ledger_tx, lm):
        res = lm.get_tx_by_id(transaction_id=ledger_tx.id)
        assert res.id == ledger_tx.id

    @pytest.mark.parametrize(
        "tag", [f"{LedgerCurrency.TEST}:{uuid4().hex}"], indirect=True
    )
    def test_get_tx_ids_by_tag(self, ledger_tx, tag, lm):
        # (1) search for a random tag
        res = lm.get_tx_ids_by_tag(tag="aaa:bbb")
        assert isinstance(res, set)
        assert len(res) == 0

        # (2) search for the tag that was used during ledger_transaction creation
        res = lm.get_tx_ids_by_tag(tag=tag)
        assert isinstance(res, set)
        assert len(res) == 1

    def test_get_tx_by_tag(self, ledger_tx, tag, lm):
        # (1) search for a random tag
        res = lm.get_tx_by_tag(tag="aaa:bbb")
        assert isinstance(res, list)
        assert len(res) == 0

        # (2) search for the tag that was used during ledger_transaction creation
        res = lm.get_tx_by_tag(tag=tag)
        assert isinstance(res, list)
        assert len(res) == 1

        assert isinstance(res[0], LedgerTransaction)
        assert ledger_tx.id == res[0].id

    def test_get_tx_filtered_by_account(
        self, ledger_tx, ledger_account, ledger_account_debit, ledger_account_credit, lm
    ):
        # (1) Do basic assertion checks first
        with pytest.raises(expected_exception=AssertionError) as excinfo:
            lm.get_tx_filtered_by_account(account_uuid=ledger_account)
        assert str(excinfo.value) == "account_uuid must be a str"

        # (2) This search doesn't return anything because this ledger account
        #   wasn't actually used in the entries for the ledger_transaction
        res = lm.get_tx_filtered_by_account(account_uuid=ledger_account.uuid)
        assert len(res) == 0

        # (3) Either the credit or the debit example ledger_accounts wll work
        #    to find this transaction because they're both used in the entries
        res = lm.get_tx_filtered_by_account(account_uuid=ledger_account_debit.uuid)
        assert len(res) == 1
        assert res[0].id == ledger_tx.id

        res = lm.get_tx_filtered_by_account(account_uuid=ledger_account_credit.uuid)
        assert len(res) == 1
        assert ledger_tx.id == res[0].id

        res2 = lm.get_tx_by_id(transaction_id=ledger_tx.id)
        assert res2.model_dump_json() == res[0].model_dump_json()

    def test_filter_metadata(self, ledger_tx, tx_metadata, lm):
        key, value = next(iter(tx_metadata.items()))

        # (1) Confirm a random key,value pair returns nothing
        res = lm.get_tx_filtered_by_metadata(
            metadata_key=f"key-{uuid4().hex[:10]}", metadata_value=uuid4().hex[:12]
        )
        assert len(res) == 0

        # (2) confirm a key,value pair return the correct results
        res = lm.get_tx_filtered_by_metadata(metadata_key=key, metadata_value=value)
        assert len(res) == 1

    #     assert 0 == THL_lm.get_filtered_account_balance(account2, "thl_wall", "ccc")
    #     assert 300 == THL_lm.get_filtered_account_balance(account1, "thl_wall", "aaa")
    #     assert 300 == THL_lm.get_filtered_account_balance(account2, "thl_wall", "aaa")
    #     assert 0 == THL_lm.get_filtered_account_balance(account3, "thl_wall", "ccc")
    #     assert THL_lm.check_ledger_balanced()
