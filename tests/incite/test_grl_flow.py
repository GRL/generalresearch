class TestGRLFlow:

    def test_init(self, mnt_filepath, thl_web_rr):
        from generalresearch.incite.defaults import (
            ledger_df_collection,
            task_df_collection,
            pop_ledger as plm,
        )

        from generalresearch.incite.collections.thl_web import (
            LedgerDFCollection,
            TaskAdjustmentDFCollection,
        )
        from generalresearch.incite.mergers.pop_ledger import PopLedgerMerge

        ledger_df = ledger_df_collection(ds=mnt_filepath, pg_config=thl_web_rr)
        assert isinstance(ledger_df, LedgerDFCollection)

        task_df = task_df_collection(ds=mnt_filepath, pg_config=thl_web_rr)
        assert isinstance(task_df, TaskAdjustmentDFCollection)

        pop_ledger = plm(ds=mnt_filepath)
        assert isinstance(pop_ledger, PopLedgerMerge)
