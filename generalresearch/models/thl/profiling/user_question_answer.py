import json
from datetime import datetime, timezone, timedelta
from typing import Dict, Tuple, Iterator, Optional, Literal, Union, Any

from pydantic import (
    PositiveInt,
    Field,
    field_validator,
    model_validator,
    BaseModel,
    ConfigDict,
)
from typing_extensions import Self

from generalresearch.grpc import timestamp_to_datetime
from generalresearch.models import Source, MAX_INT32
from generalresearch.models.custom_types import AwareDatetimeISO, UUIDStr
from generalresearch.models.thl.locales import CountryISO, LanguageISO
from generalresearch.models.thl.profiling.upk_question import UpkQuestion


class UserQuestionAnswer(BaseModel):

    model_config = ConfigDict(validate_assignment=True)

    user_id: Optional[PositiveInt] = Field(lt=MAX_INT32, default=None)
    question_id: UUIDStr = Field()
    answer: Tuple[str, ...] = Field()
    timestamp: AwareDatetimeISO = Field(
        default_factory=lambda: datetime.now(tz=timezone.utc)
    )

    country_iso: Union[CountryISO, Literal["xx"]] = Field()
    language_iso: Union[LanguageISO, Literal["xxx"]] = Field()

    # Store a property code associated with this question_id. e.g. "gr:hispanic" or "d:192"
    property_code: str = Field()
    # Stores any question answers that are calculated from this answer
    calc_answers: Optional[Dict[str, Tuple[str, ...]]] = Field(default=None)

    @field_validator("calc_answers")
    def sorted_calc_answers(cls, calc_answers) -> Optional[Dict[str, Tuple[str, ...]]]:
        if calc_answers is None:
            return None

        return {k: tuple(sorted(v)) for k, v in calc_answers.items()}

    @field_validator("calc_answers")
    def validate_keys(cls, calc_answers) -> Optional[Dict[str, Tuple[str, ...]]]:
        if calc_answers is None:
            return None

        assert all(
            ":" in k for k in calc_answers.keys()
        ), "calc_answers expects the keys to be in format source:question_code"
        return calc_answers

    def model_dump_mysql(self, session_id: Optional[str] = None) -> Dict[str, Any]:
        d = self.model_dump(mode="json", exclude={"calc_answers", "timestamp"})
        d["answer"] = json.dumps(self.answer)
        # Note naming inconsistency here: calc_answer/s
        d["calc_answer"] = json.dumps(self.calc_answers)
        # Note naming inconsistency here: created vs timestamp
        d["created"] = self.timestamp
        d["session_id"] = session_id
        return d

    def get_mrpqs(self) -> Iterator["MarketplaceResearchProfileQuestion"]:
        for k, v in self.calc_answers.items():
            source, question_code = k.split(":", 1)
            yield MarketplaceResearchProfileQuestion(
                question_code=question_code,
                source=source,
                country_iso=self.country_iso,
                language_iso=self.language_iso,
                answer=tuple(sorted(set(v))),
                timestamp=self.timestamp,
            )

    @field_validator("answer")
    def sorted_answer(cls, answer):
        return tuple(sorted(answer))

    def __hash__(self) -> int:
        return hash((self.question_id, self.answer, self.timestamp))

    def validate_question_answer(self, question: UpkQuestion) -> Tuple[bool, str]:
        """
        Returns (is_valid, error_message).
        """
        try:
            assert question.id == self.question_id, "mismatched question id"
            assert (
                question.country_iso == self.country_iso
            ), "country_iso doesn't match question's country"
            assert (
                question.language_iso == self.language_iso
            ), "language_iso doesn't match question's language"
            question._validate_question_answer(self.answer)
        except AssertionError as e:
            return False, str(e)
        else:
            return True, ""

    def is_stale(self) -> bool:
        return self.timestamp < datetime.now(tz=timezone.utc) - timedelta(days=30)

    @classmethod
    def from_grpc(cls, msg, default_timestamp: datetime) -> Self:
        """
        Handles correctly issues with grpc timestamps
        :param msg: "thl.protos.generalresearch_pb2.ProfilingQuestionAnswer"
        """
        assert default_timestamp.tzinfo is not None, "must use tz-aware timestamps"
        timestamp = timestamp_to_datetime(msg.timestamp)
        timestamp = default_timestamp if timestamp < datetime(2000, 1, 1) else timestamp
        return cls(
            question_id=msg.question_id,
            answer=tuple(msg.answer),
            timestamp=timestamp,
        )


# We can't set a redis list to [] vs None. We'll push this dummy answer into
# the cache to signify the user has no answered questions. It'll get removed
# by the 30 day old check once we pull it back anyways
DUMMY_UQA = UserQuestionAnswer(
    question_id="f118edd01cf1476ba7200a175fb4351d",
    answer=("0",),
    timestamp=datetime(2020, 1, 1, tzinfo=timezone.utc),
    country_iso="xx",
    language_iso="xxx",
    property_code="dummy",
    calc_answers=dict(),
)


class MarketplaceResearchProfileQuestion(BaseModel):
    """Answer submitted to a question by a user, that has been transformed
    into a question answer that is specific to a marketplace."""

    question_code: str = Field(
        description="# the question id/code on the marketplace", min_length=1
    )
    source: Source = Field()  # the one or two-letter marketplace code
    answer: Tuple[str, ...] = Field(min_length=1)
    timestamp: AwareDatetimeISO = Field()
    country_iso: CountryISO = Field()
    language_iso: LanguageISO = Field()

    @model_validator(mode="after")
    def validate_keys(self):
        assert (
            ":" not in self.question_code
        ), "question_code expected to not be in curie format"
        return self

    @property
    def answer_str(self) -> str:
        return "|".join(self.answer)
