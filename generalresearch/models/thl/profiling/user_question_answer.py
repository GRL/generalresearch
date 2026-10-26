from __future__ import annotations

import json
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from typing import Any, Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    PositiveInt,
    field_validator,
    model_validator,
)

from generalresearch.managers.thl.profiling.question import QuestionManager
from generalresearch.models.custom_types import AwareDatetimeISO, UUIDStr
from generalresearch.models.definitions import MAX_INT32, Source
from generalresearch.models.thl.locales import CountryISO, LanguageISO
from generalresearch.models.thl.profiling.upk_question import UpkQuestion


class UserQuestionAnswer(BaseModel):
    model_config = ConfigDict(validate_assignment=True)

    user_id: PositiveInt | None = Field(lt=MAX_INT32, default=None)
    question_id: UUIDStr = Field()
    answer: tuple[str, ...] = Field()
    timestamp: AwareDatetimeISO = Field(default_factory=lambda: datetime.now(tz=UTC))

    country_iso: CountryISO | Literal["xx"] = Field()
    language_iso: LanguageISO | Literal["xxx"] = Field()

    # Store a property code associated with this question_id. e.g. "gr:hispanic" or "d:192"
    property_code: str = Field()
    # Stores any question answers that are calculated from this answer
    calc_answers: dict[str, tuple[str, ...]] | None = Field(default=None)

    @field_validator("calc_answers")
    def sorted_calc_answers(
        cls, calc_answers: dict[str, tuple[str, ...]] | None
    ) -> dict[str, tuple[str, ...]] | None:
        if calc_answers is None:
            return None

        return {k: tuple(sorted(v)) for k, v in calc_answers.items()}

    @field_validator("calc_answers")
    def validate_keys(
        cls, calc_answers: dict[str, tuple[str, ...]] | None
    ) -> dict[str, tuple[str, ...]] | None:
        if calc_answers is None:
            return None

        assert all(":" in k for k in calc_answers), (
            "calc_answers expects the keys to be in format source:question_code"
        )
        return calc_answers

    @property
    def is_gold_standard(self) -> bool:
        return self.property_code.startswith("gr:")

    def model_dump_mysql(self, session_id: str | None = None) -> dict[str, Any]:
        d = self.model_dump(mode="json", exclude={"calc_answers", "timestamp"})
        d["answer"] = json.dumps(self.answer)
        # Note naming inconsistency here: calc_answer/s
        d["calc_answer"] = json.dumps(self.calc_answers)
        # Note naming inconsistency here: created vs timestamp
        d["created"] = self.timestamp
        d["session_id"] = session_id
        return d

    def get_mrpqs(
        self, question_manager: QuestionManager
    ) -> Iterator[MarketplaceResearchProfileQuestion]:
        assert self.calc_answers is not None
        property_codes = set(self.calc_answers.keys())
        qs = question_manager.filter_by_properties_and_locale(
            property_codes, country_iso=self.country_iso, language_iso=self.language_iso
        )
        text_lookup = {q.ext_question_id: q.text for q in qs}
        ans_lookup = {
            q.ext_question_id: {c.id: c.text for c in q.choices}
            for q in qs
            if q.choices is not None
        }

        for k, v in self.calc_answers.items():
            source, question_code = k.split(":", 1)
            ans = ans_lookup.get(k, {})
            ans_text = tuple(sorted([ans.get(vv, "") for vv in v]))
            ans_text = ans_text or None
            yield MarketplaceResearchProfileQuestion(
                question_code=question_code,
                question_text=text_lookup.get(k),
                source=Source(source),
                country_iso=self.country_iso,
                language_iso=self.language_iso,
                answer=tuple(sorted(set(v))),
                answer_text=ans_text,
                timestamp=self.timestamp,
                is_derived=self.is_gold_standard
            )

    @field_validator("answer")
    def sorted_answer(cls, answer):
        return tuple(sorted(answer))

    def __hash__(self) -> int:
        return hash((self.question_id, self.answer, self.timestamp))

    def validate_question_answer(self, question: UpkQuestion) -> tuple[bool, str]:
        """
        Returns (is_valid, error_message).
        """
        try:
            assert question.id == self.question_id, "mismatched question id"
            assert question.country_iso == self.country_iso, (
                "country_iso doesn't match question's country"
            )
            assert question.language_iso == self.language_iso, (
                "language_iso doesn't match question's language"
            )
            question._validate_question_answer(self.answer)
        except AssertionError as e:
            return False, str(e)
        else:
            return True, ""

    def is_stale(self) -> bool:
        return self.timestamp < datetime.now(tz=UTC) - timedelta(days=30)


# We can't set a redis list to [] vs None. We'll push this dummy answer into
# the cache to signify the user has no answered questions. It'll get removed
# by the 30 day old check once we pull it back anyways
DUMMY_UQA = UserQuestionAnswer(
    question_id="f118edd01cf1476ba7200a175fb4351d",
    answer=("0",),
    timestamp=datetime(2020, 1, 1, tzinfo=UTC),
    country_iso="xx",
    language_iso="xxx",
    property_code="dummy",
    calc_answers={},
)


class MarketplaceResearchProfileQuestion(BaseModel):
    """Answer submitted to a question by a user that has been transformed
    into a question answer that is specific to a marketplace."""

    question_code: str = Field(
        description="# the question id/code on the marketplace", min_length=1
    )
    question_text: str | None = Field(default=None)
    source: Source = Field()  # the one or two-letter marketplace code
    answer: tuple[str, ...] = Field(min_length=1)
    answer_text: tuple[str, ...] | None = Field(min_length=1, default=None)
    timestamp: AwareDatetimeISO = Field()
    country_iso: CountryISO = Field()
    language_iso: LanguageISO = Field()
    is_derived: bool = Field(
        default=False,
        description='What this answer derived from a "harmonized" / "gold-standard" question?',
    )

    @model_validator(mode="after")
    def validate_keys(self):
        assert ":" not in self.question_code, (
            "question_code expected to not be in curie format"
        )
        return self

    @property
    def answer_str(self) -> str:
        return "|".join(self.answer)
