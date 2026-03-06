class TestLedgerMetadataManager:

    def test_get_tx_metadata_by_txs(self, ledger_tx, lm):
        # First confirm the Ledger TX exists with 2 Entries
        res = lm.get_tx_by_id(transaction_id=ledger_tx.id)
        assert isinstance(res.metadata, dict)

        tx_metadatas = lm.get_tx_metadata_by_txs(transactions=[ledger_tx])
        assert isinstance(tx_metadatas, dict)
        assert isinstance(tx_metadatas[ledger_tx.id], dict)

        assert res.metadata == tx_metadatas[ledger_tx.id]

    def test_get_tx_metadata_ids_by_tx(self, ledger_tx, lm):
        # First confirm the Ledger TX exists with 2 Entries
        res = lm.get_tx_by_id(transaction_id=ledger_tx.id)
        tx_metadata_cnt = len(res.metadata.keys())

        tx_metadata_ids = lm.get_tx_metadata_ids_by_tx(transaction=ledger_tx)
        assert isinstance(tx_metadata_ids, set)
        assert isinstance(list(tx_metadata_ids)[0], int)

        assert tx_metadata_cnt == len(tx_metadata_ids)

    def test_get_tx_metadata_ids_by_txs(self, ledger_tx, lm):
        # First confirm the Ledger TX exists with 2 Entries
        res = lm.get_tx_by_id(transaction_id=ledger_tx.id)
        tx_metadata_cnt = len(res.metadata.keys())

        tx_metadata_ids = lm.get_tx_metadata_ids_by_txs(transactions=[ledger_tx])
        assert isinstance(tx_metadata_ids, set)
        assert isinstance(list(tx_metadata_ids)[0], int)

        assert tx_metadata_cnt == len(tx_metadata_ids)
