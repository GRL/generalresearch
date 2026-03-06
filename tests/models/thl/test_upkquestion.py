import pytest
from pydantic import ValidationError


class TestUpkQuestion:

    def test_importance(self):
        from generalresearch.models.thl.profiling.upk_question import (
            UPKImportance,
        )

        ui = UPKImportance(task_score=1, task_count=None)
        ui = UPKImportance(task_score=0)
        with pytest.raises(ValidationError) as e:
            UPKImportance(task_score=-1)
        assert "Input should be greater than or equal to 0" in str(e.value)

    def test_pattern(self):
        from generalresearch.models.thl.profiling.upk_question import (
            PatternValidation,
        )

        s = PatternValidation(message="hi", pattern="x")
        with pytest.raises(ValidationError) as e:
            s.message = "sfd"
        assert "Instance is frozen" in str(e.value)

    def test_mc(self):
        from generalresearch.models.thl.profiling.upk_question import (
            UpkQuestionChoice,
            UpkQuestionSelectorMC,
            UpkQuestionType,
            UpkQuestion,
            UpkQuestionConfigurationMC,
        )

        q = UpkQuestion(
            id="601377a0d4c74529afc6293a8e5c3b5e",
            country_iso="us",
            language_iso="eng",
            type=UpkQuestionType.MULTIPLE_CHOICE,
            selector=UpkQuestionSelectorMC.MULTIPLE_ANSWER,
            text="whats up",
            choices=[
                UpkQuestionChoice(id="1", text="sky", order=1),
                UpkQuestionChoice(id="2", text="moon", order=2),
            ],
            configuration=UpkQuestionConfigurationMC(max_select=2),
        )
        assert q == UpkQuestion.model_validate(q.model_dump(mode="json"))

        q = UpkQuestion(
            country_iso="us",
            language_iso="eng",
            type=UpkQuestionType.MULTIPLE_CHOICE,
            selector=UpkQuestionSelectorMC.SINGLE_ANSWER,
            text="yes or no",
            choices=[
                UpkQuestionChoice(id="1", text="yes", order=1),
                UpkQuestionChoice(id="2", text="no", order=2),
            ],
            configuration=UpkQuestionConfigurationMC(max_select=1),
        )
        assert q == UpkQuestion.model_validate(q.model_dump(mode="json"))

        q = UpkQuestion(
            country_iso="us",
            language_iso="eng",
            type=UpkQuestionType.MULTIPLE_CHOICE,
            selector=UpkQuestionSelectorMC.MULTIPLE_ANSWER,
            text="yes or no",
            choices=[
                UpkQuestionChoice(id="1", text="yes", order=1),
                UpkQuestionChoice(id="2", text="no", order=2),
            ],
        )
        assert q == UpkQuestion.model_validate(q.model_dump(mode="json"))

        with pytest.raises(ValidationError) as e:
            q = UpkQuestion(
                country_iso="us",
                language_iso="eng",
                type=UpkQuestionType.MULTIPLE_CHOICE,
                selector=UpkQuestionSelectorMC.SINGLE_ANSWER,
                text="yes or no",
                choices=[
                    UpkQuestionChoice(id="1", text="yes", order=1),
                    UpkQuestionChoice(id="2", text="no", order=2),
                ],
                configuration=UpkQuestionConfigurationMC(max_select=2),
            )
        assert "max_select must be 1 if the selector is SA" in str(e.value)

        with pytest.raises(ValidationError) as e:
            q = UpkQuestion(
                country_iso="us",
                language_iso="eng",
                type=UpkQuestionType.MULTIPLE_CHOICE,
                selector=UpkQuestionSelectorMC.MULTIPLE_ANSWER,
                text="yes or no",
                choices=[
                    UpkQuestionChoice(id="1", text="yes", order=1),
                    UpkQuestionChoice(id="2", text="no", order=2),
                ],
                configuration=UpkQuestionConfigurationMC(max_select=4),
            )
        assert "max_select must be >= len(choices)" in str(e.value)

        with pytest.raises(expected_exception=ValidationError) as e:
            q = UpkQuestion(
                country_iso="us",
                language_iso="eng",
                type=UpkQuestionType.MULTIPLE_CHOICE,
                selector=UpkQuestionSelectorMC.MULTIPLE_ANSWER,
                text="yes or no",
                choices=[
                    UpkQuestionChoice(id="1", text="yes", order=1),
                    UpkQuestionChoice(id="2", text="no", order=2),
                ],
                configuration=UpkQuestionConfigurationMC(max_length=2),
            )
        assert "Extra inputs are not permitted" in str(e.value)

    def test_te(self):
        from generalresearch.models.thl.profiling.upk_question import (
            UpkQuestionType,
            UpkQuestion,
            UpkQuestionSelectorTE,
            UpkQuestionValidation,
            PatternValidation,
            UpkQuestionConfigurationTE,
        )

        q = UpkQuestion(
            id="601377a0d4c74529afc6293a8e5c3b5e",
            country_iso="us",
            language_iso="eng",
            type=UpkQuestionType.TEXT_ENTRY,
            selector=UpkQuestionSelectorTE.MULTI_LINE,
            text="whats up",
            choices=[],
            configuration=UpkQuestionConfigurationTE(max_length=2),
            validation=UpkQuestionValidation(
                patterns=[PatternValidation(pattern=".", message="x")]
            ),
        )
        assert q == UpkQuestion.model_validate(q.model_dump(mode="json"))
        assert q.choices is None

    def test_deserialization(self):
        from generalresearch.models.thl.profiling.upk_question import (
            UpkQuestion,
        )

        q = UpkQuestion.model_validate(
            {
                "id": "601377a0d4c74529afc6293a8e5c3b5e",
                "ext_question_id": "m:2342",
                "country_iso": "us",
                "language_iso": "eng",
                "text": "whats up",
                "choices": [
                    {"id": "1", "text": "yes", "order": 1},
                    {"id": "2", "text": "no", "order": 2},
                ],
                "importance": None,
                "type": "MC",
                "selector": "SA",
                "configuration": None,
            }
        )
        assert q == UpkQuestion.model_validate(q.model_dump(mode="json"))

        q = UpkQuestion.model_validate(
            {
                "id": "601377a0d4c74529afc6293a8e5c3b5e",
                "ext_question_id": "m:2342",
                "country_iso": "us",
                "language_iso": "eng",
                "text": "whats up",
                "choices": [
                    {"id": "1", "text": "yes", "order": 1},
                    {"id": "2", "text": "no", "order": 2},
                ],
                "importance": None,
                "question_type": "MC",
                "selector": "MA",
                "configuration": {"max_select": 2},
            }
        )
        assert q == UpkQuestion.model_validate(q.model_dump(mode="json"))

    def test_from_morning(self):
        from generalresearch.models.morning.question import (
            MorningQuestion,
            MorningQuestionType,
        )

        q = MorningQuestion(
            **{
                "id": "gender",
                "country_iso": "us",
                "language_iso": "eng",
                "name": "Gender",
                "text": "What is your gender?",
                "type": "s",
                "options": [
                    {"id": "1", "text": "yes", "order": 1},
                    {"id": "2", "text": "no", "order": 2},
                ],
            }
        )
        q.to_upk_question()
        q = MorningQuestion(
            country_iso="us",
            language_iso="eng",
            type=MorningQuestionType.text_entry,
            text="how old r u",
            id="a",
            name="age",
        )
        q.to_upk_question()

    def test_order(self):
        from generalresearch.models.thl.profiling.upk_question import (
            UpkQuestionChoice,
            UpkQuestionSelectorMC,
            UpkQuestionType,
            UpkQuestion,
            order_exclusive_options,
        )

        q = UpkQuestion(
            country_iso="us",
            language_iso="eng",
            type=UpkQuestionType.MULTIPLE_CHOICE,
            selector=UpkQuestionSelectorMC.MULTIPLE_ANSWER,
            text="yes, no, or NA?",
            choices=[
                UpkQuestionChoice(id="1", text="NA", order=0),
                UpkQuestionChoice(id="2", text="no", order=1),
                UpkQuestionChoice(id="3", text="yes", order=2),
            ],
        )
        order_exclusive_options(q)
        assert (
            UpkQuestion(
                country_iso="us",
                language_iso="eng",
                type=UpkQuestionType.MULTIPLE_CHOICE,
                selector=UpkQuestionSelectorMC.MULTIPLE_ANSWER,
                text="yes, no, or NA?",
                choices=[
                    UpkQuestionChoice(id="2", text="no", order=0),
                    UpkQuestionChoice(id="3", text="yes", order=1),
                    UpkQuestionChoice(id="1", text="NA", order=2, exclusive=True),
                ],
            )
            == q
        )


class TestUpkQuestionValidateAnswer:
    def test_validate_answer_SA(self):
        from generalresearch.models.thl.profiling.upk_question import (
            UpkQuestion,
        )

        question = UpkQuestion.model_validate(
            {
                "choices": [
                    {"order": 0, "choice_id": "0", "choice_text": "Male"},
                    {"order": 1, "choice_id": "1", "choice_text": "Female"},
                    {"order": 2, "choice_id": "2", "choice_text": "Other"},
                ],
                "selector": "SA",
                "country_iso": "us",
                "question_id": "5d6d9f3c03bb40bf9d0a24f306387d7c",
                "language_iso": "eng",
                "question_text": "What is your gender?",
                "question_type": "MC",
            }
        )
        answer = ("0",)
        assert question.validate_question_answer(answer)[0] is True
        answer = ("3",)
        assert question.validate_question_answer(answer) == (
            False,
            "Invalid Options Selected",
        )
        answer = ("0", "0")
        assert question.validate_question_answer(answer) == (
            False,
            "Multiple of the same answer submitted",
        )
        answer = ("0", "1")
        assert question.validate_question_answer(answer) == (
            False,
            "Single Answer MC question with >1 selected " "answers",
        )

    def test_validate_answer_MA(self):
        from generalresearch.models.thl.profiling.upk_question import (
            UpkQuestion,
        )

        question = UpkQuestion.model_validate(
            {
                "choices": [
                    {
                        "order": 0,
                        "choice_id": "none",
                        "exclusive": True,
                        "choice_text": "None of the above",
                    },
                    {
                        "order": 1,
                        "choice_id": "female_under_1",
                        "choice_text": "Female under age 1",
                    },
                    {
                        "order": 2,
                        "choice_id": "male_under_1",
                        "choice_text": "Male under age 1",
                    },
                    {
                        "order": 3,
                        "choice_id": "female_1",
                        "choice_text": "Female age 1",
                    },
                    {"order": 4, "choice_id": "male_1", "choice_text": "Male age 1"},
                    {
                        "order": 5,
                        "choice_id": "female_2",
                        "choice_text": "Female age 2",
                    },
                ],
                # I removed a bunch of choices fyi
                "selector": "MA",
                "country_iso": "us",
                "question_id": "3b65220db85f442ca16bb0f1c0e3a456",
                "language_iso": "eng",
                "question_text": "Please indicate the age and gender of your child or children:",
                "question_type": "MC",
            }
        )
        answer = ("none",)
        assert question.validate_question_answer(answer)[0] is True
        answer = ("male_1",)
        assert question.validate_question_answer(answer)[0] is True
        answer = ("male_1", "female_1")
        assert question.validate_question_answer(answer)[0] is True
        answer = ("xxx",)
        assert question.validate_question_answer(answer) == (
            False,
            "Invalid Options Selected",
        )
        answer = ("male_1", "male_1")
        assert question.validate_question_answer(answer) == (
            False,
            "Multiple of the same answer submitted",
        )
        answer = ("male_1", "xxx")
        assert question.validate_question_answer(answer) == (
            False,
            "Invalid Options Selected",
        )
        answer = ("male_1", "none")
        assert question.validate_question_answer(answer) == (
            False,
            "Invalid exclusive selection",
        )

    def test_validate_answer_TE(self):
        from generalresearch.models.thl.profiling.upk_question import (
            UpkQuestion,
        )

        question = UpkQuestion.model_validate(
            {
                "selector": "SL",
                "validation": {
                    "patterns": [
                        {
                            "message": "Must enter a valid zip code: XXXXX",
                            "pattern": "^[0-9]{5}$",
                        }
                    ]
                },
                "country_iso": "us",
                "question_id": "543de254e9ca4d9faded4377edab82a9",
                "language_iso": "eng",
                "configuration": {"max_length": 5, "min_length": 5},
                "question_text": "What is your zip code?",
                "question_type": "TE",
            }
        )
        answer = ("33143",)
        assert question.validate_question_answer(answer)[0] is True
        answer = ("33143", "33143")
        assert question.validate_question_answer(answer) == (
            False,
            "Multiple of the same answer submitted",
        )
        answer = ("33143", "12345")
        assert question.validate_question_answer(answer) == (
            False,
            "Only one answer allowed",
        )
        answer = ("111",)
        assert question.validate_question_answer(answer) == (
            False,
            "Must enter a valid zip code: XXXXX",
        )
