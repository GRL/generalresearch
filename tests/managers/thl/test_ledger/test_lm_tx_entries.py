from __future__ import annotations

from generalresearch.managers.thl.ledger_manager.ledger import LedgerManager
from generalresearch.models.thl.ledger import (
    LedgerEntry,
    LedgerTransaction,
)


class TestLedgerEntryManager:

    def test_get_tx_entries_by_tx(
        self, ledger_tx: LedgerTransaction, ledger_manager: LedgerManager
    ):
        # First confirm the Ledger TX exists with 2 Entries
        res = ledger_manager.get_tx_by_id(transaction_id=ledger_tx.id)
        assert len(res.entries) == 2

        tx_entries = ledger_manager.get_tx_entries_by_tx(transaction=ledger_tx)
        assert len(tx_entries) == 2

        assert res.entries == tx_entries
        assert isinstance(tx_entries[0], LedgerEntry)

    def test_get_tx_entries_by_txs(
        self, ledger_tx: LedgerTransaction, ledger_manager: LedgerManager
    ):
        # First confirm the Ledger TX exists with 2 Entries
        res = ledger_manager.get_tx_by_id(transaction_id=ledger_tx.id)
        assert len(res.entries) == 2

        tx_entries = ledger_manager.get_tx_entries_by_txs(transactions=[ledger_tx])
        assert len(tx_entries) == 2

        assert res.entries == tx_entries
        assert isinstance(tx_entries[0], LedgerEntry)
