import json
from enum import Enum
from typing import List, Optional, Dict, Literal, Any
from uuid import UUID

from pydantic import BaseModel, Field, model_validator, field_validator
from typing_extensions import Self

from generalresearch.locales import Localelator
from generalresearch.models import Source
from generalresearch.models.morning import MorningQuestionID
from generalresearch.models.thl.profiling.marketplace import (
    MarketplaceUserQuestionAnswer,
    MarketplaceQuestion,
)

# todo: we could validate that the country_iso / language_iso exists ...
locale_helper = Localelator()


class MorningQuestionOption(BaseModel, frozen=True):
    # API limit is 50, db limit is 32
    id: str = Field(
        min_length=1,
        max_length=32,
        pattern=r"^[\w\s\.\-]+$",
        description="The unique identifier for a response to a qualification",
        serialization_alias="option_id",
    )
    text: str = Field(
        min_length=1,
        description="The response text shown to respondents",
        serialization_alias="option_text",
    )
    # Order does not come back explicitly in the API, instead they are already ordered. We're
    #   adding this for db sort purposes to explicitly order them. We use the API's order.
    order: int = Field()


class MorningQuestionType(str, Enum):
    # The db stores these as a single letter

    # Geographic questions represent geographic areas within a country.
    #   These behave like multiple_choice questions
    geographic = "g"
    # The 's' is for "single-select". Morning does not support "multi-select"
    #   multiple choice, but if they did, we would use 'm' for "multi-select".
    multiple_choice = "s"
    # Questions whose answers are submitted by respondents. The ID and
    #   response text are both defined as the exact text that was typed by
    #   the respondent. Text entry responses are not case-sensitive
    text_entry = "t"


class MorningUserQuestionAnswer(MarketplaceUserQuestionAnswer):
    question_id: MorningQuestionID = Field()
    question_type: Optional[MorningQuestionType] = Field(default=None)
    # Did this answer come from us asking, or was it passed back from the
    #   marketplace? Note, morning doesn't "pass back" answers, but we can
    #   retrieve a user's profile through API, so it is possible to populate
    #   this from_thl False
    from_thl: bool = Field(default=True)


class MorningQuestion(MarketplaceQuestion):
    # API limit is 100, db limit is 64
    id: str = Field(
        min_length=1,
        max_length=64,
        pattern=r"^[a-z0-9_\s\.]+$",
        description="The unique identifier for the qualification",
        serialization_alias="question_id",
        frozen=True,
    )
    # API has no limit, db limit is 64
    name: str = Field(
        max_length=64,
        min_length=1,
        serialization_alias="question_name",
        description="The human-readable short label for the qualification",
        frozen=True,
    )
    text: str = Field(
        min_length=1,
        description="The text shown to respondents",
        serialization_alias="question_text",
        frozen=True,
    )
    type: MorningQuestionType = Field(
        description="The type of question asked",
        serialization_alias="question_type",
        frozen=True,
    )
    # API calls this "responses", but I think that is a confusing name
    options: Optional[List[MorningQuestionOption]] = Field(
        default=None, min_length=1, frozen=True
    )

    source: Literal[Source.MORNING_CONSULT] = Source.MORNING_CONSULT

    @property
    def internal_id(self) -> str:
        return self.id

    @model_validator(mode="after")
    def check_type_options_agreement(self) -> Self:
        # If type == "text_entry", options is None. Otherwise, must be set.
        if self.type == MorningQuestionType.text_entry:
            assert self.options is None
        else:
            assert self.options is not None
        return self

    @field_validator("options")
    @classmethod
    def order_options(cls, options):
        if options:
            options.sort(key=lambda x: x.order)
        return options

    @classmethod
    def from_api(cls, d: dict, country_iso: str, language_iso: str):
        options = None
        if d.get("responses"):
            options = [
                MorningQuestionOption(id=r["id"], text=r["text"], order=order)
                for order, r in enumerate(d["responses"])
            ]
        return cls(
            id=d["id"],
            name=d["name"],
            text=d["text"],
            type=MorningQuestionType[d["type"]],
            country_iso=country_iso,
            language_iso=language_iso,
            options=options,
        )

    @classmethod
    def from_db(cls, d: dict):
        options = None
        if d["options"]:
            options = [
                MorningQuestionOption(
                    id=r["option_id"], text=r["option_text"], order=r["order"]
                )
                for r in d["options"]
            ]
        return cls(
            id=d["question_id"],
            name=d["question_name"],
            text=d["question_text"],
            type=d["question_type"],
            country_iso=d["country_iso"],
            language_iso=d["language_iso"],
            options=options,
            is_live=d["is_live"],
            category_id=(
                UUID(d.get("category_id")).hex if d.get("category_id") else None
            ),
        )

    def to_mysql(self) -> Dict[str, Any]:
        d = self.model_dump(mode="json", by_alias=True)
        d["options"] = json.dumps(d["options"])
        return d

    def to_upk_question(self):
        from generalresearch.models.thl.profiling.upk_question import (
            UpkQuestionChoice,
            UpkQuestionType,
            UpkQuestionSelectorMC,
            UpkQuestionSelectorTE,
            UpkQuestionSelectorHIDDEN,
            UpkQuestion,
        )

        upk_type_selector_map = {
            # multiple select doesn't exist in morning, only single select
            MorningQuestionType.multiple_choice: (
                UpkQuestionType.MULTIPLE_CHOICE,
                UpkQuestionSelectorMC.SINGLE_ANSWER,
            ),
            MorningQuestionType.text_entry: (
                UpkQuestionType.TEXT_ENTRY,
                UpkQuestionSelectorTE.SINGLE_LINE,
            ),
            MorningQuestionType.geographic: (
                UpkQuestionType.HIDDEN,
                UpkQuestionSelectorHIDDEN.HIDDEN,
            ),
        }
        upk_type, upk_selector = upk_type_selector_map[self.type]
        d = {
            "ext_question_id": self.external_id,
            "country_iso": self.country_iso,
            "language_iso": self.language_iso,
            "type": upk_type,
            "selector": upk_selector,
            "text": self.text,
        }
        if self.type == MorningQuestionType.multiple_choice:
            d["choices"] = [
                UpkQuestionChoice(id=c.id, text=c.text, order=c.order)
                for c in self.options
            ]
        return UpkQuestion(**d)
