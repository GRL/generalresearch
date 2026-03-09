from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from generalresearch.grliq.managers.forensic_data import GrlIqDataManager
    from generalresearch.grliq.managers.forensic_results import (
        GrlIqCategoryResultsReader,
    )


class TestGrlIqCategoryResultsReader:

    def test_filter_category_results(
        self, grliq_dm: "GrlIqDataManager", grliq_crr: "GrlIqCategoryResultsReader"
    ):
        from generalresearch.grliq.models.forensic_result import (
            GrlIqForensicCategoryResult,
            Phase,
        )

        # this is just testing that it doesn't fail
        grliq_dm.create_dummy(is_attempt_allowed=True)
        grliq_dm.create_dummy(is_attempt_allowed=True)

        res = grliq_crr.filter_category_results(limit=2, phase=Phase.OFFERWALL_ENTER)[0]
        assert res.get("category_result")
        assert isinstance(res["category_result"], GrlIqForensicCategoryResult)
        assert res["user_agent"].os.family
