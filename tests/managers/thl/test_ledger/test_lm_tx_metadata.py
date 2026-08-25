from __future__ import annotations

from generalresearch.managers.thl.ledger_manager.ledger import (
    LedgerManager,
    LedgerTransaction,
)


class TestLedgerMetadataManager:

    def test_get_tx_metadata_by_txs(
        self, ledger_tx: LedgerTransaction, ledger_manager: LedgerManager
    ):
        # First confirm the Ledger TX exists with 2 Entries
        res = ledger_manager.get_tx_by_id(transaction_id=ledger_tx.id)
        assert isinstance(res.metadata, dict)

        tx_metadatas = ledger_manager.get_tx_metadata_by_txs(transactions=[ledger_tx])
        assert isinstance(tx_metadatas, dict)
        assert isinstance(tx_metadatas[ledger_tx.id], dict)

        assert res.metadata == tx_metadatas[ledger_tx.id]

    def test_get_tx_metadata_ids_by_tx(
        self, ledger_tx: LedgerTransaction, ledger_manager: LedgerManager
    ):
        # First confirm the Ledger TX exists with 2 Entries
        res = ledger_manager.get_tx_by_id(transaction_id=ledger_tx.id)
        tx_metadata_cnt = len(res.metadata.keys())

        tx_metadata_ids = ledger_manager.get_tx_metadata_ids_by_tx(
            transaction=ledger_tx
        )
        assert isinstance(tx_metadata_ids, set)
        assert isinstance(next(iter(tx_metadata_ids)), int)

        assert tx_metadata_cnt == len(tx_metadata_ids)

    def test_get_tx_metadata_ids_by_txs(
        self, ledger_tx: LedgerTransaction, ledger_manager: LedgerManager
    ):
        # First confirm the Ledger TX exists with 2 Entries
        res = ledger_manager.get_tx_by_id(transaction_id=ledger_tx.id)
        tx_metadata_cnt = len(res.metadata.keys())

        tx_metadata_ids = ledger_manager.get_tx_metadata_ids_by_txs(
            transactions=[ledger_tx]
        )
        assert isinstance(tx_metadata_ids, set)
        assert isinstance(next(iter(tx_metadata_ids)), int)

        assert tx_metadata_cnt == len(tx_metadata_ids)
