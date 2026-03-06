class TestGrlIqCategoryResultsReader:

    def test_filter_category_results(self, grliq_dm, grliq_crr):
        from generalresearch.grliq.models.forensic_result import (
            Phase,
            GrlIqForensicCategoryResult,
        )

        # this is just testing that it doesn't fail
        grliq_dm.create_dummy(is_attempt_allowed=True)
        grliq_dm.create_dummy(is_attempt_allowed=True)

        res = grliq_crr.filter_category_results(limit=2, phase=Phase.OFFERWALL_ENTER)[0]
        assert res.get("category_result")
        assert isinstance(res["category_result"], GrlIqForensicCategoryResult)
        assert res["user_agent"].os.family
