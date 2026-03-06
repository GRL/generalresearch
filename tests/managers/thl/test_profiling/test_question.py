from uuid import uuid4

from generalresearch.managers.thl.profiling.question import QuestionManager
from generalresearch.models import Source


class TestQuestionManager:

    def test_get_multi_upk(self, question_manager: QuestionManager, upk_data):
        qs = question_manager.get_multi_upk(
            question_ids=[
                "8a22de34f985476aac85e15547100db8",
                "0565f87d4bf044298ba169de1339ff7e",
                "b2b32d68403647e3a87e778a6348d34c",
                uuid4().hex,
            ]
        )
        assert len(qs) == 3

    def test_get_questions_ranked(self, question_manager: QuestionManager, upk_data):
        qs = question_manager.get_questions_ranked(country_iso="mx", language_iso="spa")
        assert len(qs) >= 40
        assert qs[0].importance.task_score > qs[40].importance.task_score
        assert all(q.country_iso == "mx" and q.language_iso == "spa" for q in qs)

    def test_lookup_by_property(self, question_manager: QuestionManager, upk_data):
        q = question_manager.lookup_by_property(
            property_code="i:industry", country_iso="us", language_iso="eng"
        )
        assert q.source == Source.INNOVATE

        q.explanation_template = "You work in the {answer} industry."
        q.explanation_fragment_template = "you work in the {answer} industry"
        question_manager.update_question_explanation(q)

        q = question_manager.lookup_by_property(
            property_code="i:industry", country_iso="us", language_iso="eng"
        )
        assert q.explanation_template

    def test_filter_by_property(self, question_manager: QuestionManager, upk_data):
        lookup = [
            ("i:industry", "us", "eng"),
            ("i:industry", "mx", "eng"),
            ("m:age", "us", "eng"),
            (f"m:{uuid4().hex}", "us", "eng"),
        ]
        qs = question_manager.filter_by_property(lookup)
        assert len(qs) == 3
