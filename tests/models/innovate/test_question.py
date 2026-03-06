from generalresearch.models import Source
from generalresearch.models.innovate.question import (
    InnovateQuestion,
    InnovateQuestionType,
    InnovateQuestionOption,
)
from generalresearch.models.thl.profiling.upk_question import (
    UpkQuestionSelectorTE,
    UpkQuestion,
    UpkQuestionSelectorMC,
    UpkQuestionType,
    UpkQuestionChoice,
)


class TestInnovateQuestion:

    def test_text_entry(self):

        q = InnovateQuestion(
            question_id="3",
            country_iso="us",
            language_iso="eng",
            question_key="ZIPCODES",
            question_text="postal code",
            question_type=InnovateQuestionType.TEXT_ENTRY,
            tags=None,
            options=None,
            is_live=True,
            category_id=None,
        )
        assert Source.INNOVATE == q.source
        assert "i:zipcodes" == q.external_id
        assert "zipcodes" == q.internal_id
        assert ("zipcodes", "us", "eng") == q._key

        upk = q.to_upk_question()
        expected_upk = UpkQuestion(
            ext_question_id="i:zipcodes",
            type=UpkQuestionType.TEXT_ENTRY,
            country_iso="us",
            language_iso="eng",
            text="postal code",
            selector=UpkQuestionSelectorTE.SINGLE_LINE,
            choices=None,
        )
        assert expected_upk == upk

    def test_mc(self):

        text = "Have you purchased or received any of the following in past 18 months?"
        q = InnovateQuestion(
            question_key="dynamic_profiling-_1_14715",
            country_iso="us",
            language_iso="eng",
            question_id="14715",
            question_text=text,
            question_type=InnovateQuestionType.MULTI_SELECT,
            tags="Dynamic Profiling- 1",
            options=[
                InnovateQuestionOption(id="1", text="aaa", order=0),
                InnovateQuestionOption(id="2", text="bbb", order=1),
            ],
            is_live=True,
            category_id=None,
        )
        assert "i:dynamic_profiling-_1_14715" == q.external_id
        assert "dynamic_profiling-_1_14715" == q.internal_id
        assert ("dynamic_profiling-_1_14715", "us", "eng") == q._key
        assert 2 == q.num_options

        upk = q.to_upk_question()
        expected_upk = UpkQuestion(
            ext_question_id="i:dynamic_profiling-_1_14715",
            type=UpkQuestionType.MULTIPLE_CHOICE,
            country_iso="us",
            language_iso="eng",
            text=text,
            selector=UpkQuestionSelectorMC.MULTIPLE_ANSWER,
            choices=[
                UpkQuestionChoice(id="1", text="aaa", order=0),
                UpkQuestionChoice(id="2", text="bbb", order=1),
            ],
        )
        assert expected_upk == upk
