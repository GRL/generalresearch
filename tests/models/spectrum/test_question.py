from datetime import datetime, timezone

from generalresearch.models import Source
from generalresearch.models.spectrum.question import (
    SpectrumQuestionOption,
    SpectrumQuestion,
    SpectrumQuestionType,
    SpectrumQuestionClass,
)
from generalresearch.models.thl.profiling.upk_question import (
    UpkQuestion,
    UpkQuestionSelectorMC,
    UpkQuestionType,
    UpkQuestionChoice,
)


class TestSpectrumQuestion:

    def test_parse_from_api_1(self):

        example_1 = {
            "qualification_code": 213,
            "text": "My household earns approximately $%%213%% per year",
            "cat": None,
            "desc": "Income",
            "type": 5,
            "class": 1,
            "condition_codes": [],
            "format": {"min": 0, "max": 999999, "regex": "/^([0-9]{1,6})$/i"},
            "crtd_on": 1502869927688,
            "mod_on": 1706557247467,
        }
        q = SpectrumQuestion.from_api(example_1, "us", "eng")

        expected_q = SpectrumQuestion(
            question_id="213",
            country_iso="us",
            language_iso="eng",
            question_name="Income",
            question_text="My household earns approximately $___ per year",
            question_type=SpectrumQuestionType.TEXT_ENTRY,
            tags=None,
            options=None,
            class_num=SpectrumQuestionClass.CORE,
            created=datetime(2017, 8, 16, 7, 52, 7, 688000, tzinfo=timezone.utc),
            is_live=True,
            source=Source.SPECTRUM,
            category_id=None,
        )
        assert "My household earns approximately $___ per year" == q.question_text
        assert "213" == q.question_id
        assert expected_q == q
        q.to_upk_question()
        assert "s:213" == q.external_id

    def test_parse_from_api_2(self):

        example_2 = {
            "qualification_code": 211,
            "text": "I'm a %%211%%",
            "cat": None,
            "desc": "Gender",
            "type": 1,
            "class": 1,
            "condition_codes": [
                {"id": "111", "text": "Male"},
                {"id": "112", "text": "Female"},
            ],
            "format": {"min": None, "max": None, "regex": ""},
            "crtd_on": 1502869927688,
            "mod_on": 1706557249817,
        }
        q = SpectrumQuestion.from_api(example_2, "us", "eng")
        expected_q = SpectrumQuestion(
            question_id="211",
            country_iso="us",
            language_iso="eng",
            question_name="Gender",
            question_text="I'm a",
            question_type=SpectrumQuestionType.SINGLE_SELECT,
            tags=None,
            options=[
                SpectrumQuestionOption(id="111", text="Male", order=0),
                SpectrumQuestionOption(id="112", text="Female", order=1),
            ],
            class_num=SpectrumQuestionClass.CORE,
            created=datetime(2017, 8, 16, 7, 52, 7, 688000, tzinfo=timezone.utc),
            is_live=True,
            source=Source.SPECTRUM,
            category_id=None,
        )
        assert expected_q == q
        q.to_upk_question()

    def test_parse_from_api_3(self):

        example_3 = {
            "qualification_code": 220,
            "text": "My child is a %%230%% %%221%% old %%220%%",
            "cat": None,
            "desc": "Child Dependent",
            "type": 6,
            "class": 4,
            "condition_codes": [
                {"id": "111", "text": "Boy"},
                {"id": "112", "text": "Girl"},
            ],
            "format": {"min": None, "max": None, "regex": ""},
            "crtd_on": 1502869927688,
            "mod_on": 1706556781278,
        }
        q = SpectrumQuestion.from_api(example_3, "us", "eng")
        # This fails because the text has variables from other questions in it
        assert q is None

    def test_parse_from_api_4(self):

        example_4 = {
            "qualification_code": 1039,
            "text": "Do you suffer from any of the following ailments or medical conditions? (Select all that apply) "
            " %%1039%%",
            "cat": "Ailments, Illness",
            "desc": "Standard Ailments",
            "type": 3,
            "class": 2,
            "condition_codes": [
                {"id": "111", "text": "Allergies (Food, Nut, Skin)"},
                {"id": "999", "text": "None of the above"},
                {"id": "130", "text": "Other"},
                {
                    "id": "129",
                    "text": "Women's Health Conditions (Reproductive Issues)",
                },
            ],
            "format": {"min": None, "max": None, "regex": ""},
            "crtd_on": 1502869927688,
            "mod_on": 1706557241693,
        }
        q = SpectrumQuestion.from_api(example_4, "us", "eng")
        expected_q = SpectrumQuestion(
            question_id="1039",
            country_iso="us",
            language_iso="eng",
            question_name="Standard Ailments",
            question_text="Do you suffer from any of the following ailments or medical conditions? (Select all that "
            "apply)",
            question_type=SpectrumQuestionType.MULTI_SELECT,
            tags="Ailments, Illness",
            options=[
                SpectrumQuestionOption(
                    id="111", text="Allergies (Food, Nut, Skin)", order=0
                ),
                SpectrumQuestionOption(
                    id="129",
                    text="Women's Health Conditions (Reproductive Issues)",
                    order=1,
                ),
                SpectrumQuestionOption(id="130", text="Other", order=2),
                SpectrumQuestionOption(id="999", text="None of the above", order=3),
            ],
            class_num=SpectrumQuestionClass.EXTENDED,
            created=datetime(2017, 8, 16, 7, 52, 7, 688000, tzinfo=timezone.utc),
            is_live=True,
            source=Source.SPECTRUM,
            category_id=None,
        )
        assert expected_q == q

        # todo: we should have something that infers that if the choice text is "None of the above",
        #   then the choice is exclusive
        u = UpkQuestion(
            id=None,
            ext_question_id="s:1039",
            type=UpkQuestionType.MULTIPLE_CHOICE,
            selector=UpkQuestionSelectorMC.MULTIPLE_ANSWER,
            country_iso="us",
            language_iso="eng",
            text="Do you suffer from any of the following ailments or medical conditions? (Select all "
            "that apply)",
            choices=[
                UpkQuestionChoice(
                    id="111",
                    text="Allergies (Food, Nut, Skin)",
                    order=0,
                    group=None,
                    exclusive=False,
                    importance=None,
                ),
                UpkQuestionChoice(
                    id="129",
                    text="Women's Health Conditions (Reproductive Issues)",
                    order=1,
                    group=None,
                    exclusive=False,
                    importance=None,
                ),
                UpkQuestionChoice(
                    id="130",
                    text="Other",
                    order=2,
                    group=None,
                    exclusive=False,
                    importance=None,
                ),
                UpkQuestionChoice(
                    id="999",
                    text="None of the above",
                    order=3,
                    group=None,
                    exclusive=False,
                    importance=None,
                ),
            ],
        )
        assert u == q.to_upk_question()
