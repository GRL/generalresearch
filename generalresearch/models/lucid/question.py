from __future__ import annotations

import logging
from enum import Enum
from typing import List, Optional, Literal, TYPE_CHECKING

from pydantic import BaseModel, Field, model_validator, field_validator
from typing_extensions import Self

from generalresearch.models import Source
from generalresearch.models.lucid import LucidQuestionIdType
from generalresearch.models.thl.profiling.marketplace import (
    MarketplaceQuestion,
)

if TYPE_CHECKING:
    from generalresearch.models.thl.profiling.upk_question import (
        UpkQuestion,
    )

logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.INFO)


class LucidQuestionOption(BaseModel):
    id: str = Field(
        min_length=1,
        max_length=16,
        pattern=r"^[0-9]+|-3105$",
        frozen=True,
        description="precode",
    )
    text: str = Field(
        min_length=1,
        max_length=1024,
        frozen=True,
        description="The response text shown to respondents",
    )
    # Order does not come back explicitly in the API
    order: int = Field()


class LucidQuestionType(str, Enum):
    SINGLE_SELECT = "s"
    MULTI_SELECT = "m"
    TEXT_ENTRY = "t"
    # This is text entry, but only numbers
    NUMERICAL = "n"
    # Dummy means they're calculated
    DUMMY = "d"


class LucidQuestion(MarketplaceQuestion):
    question_id: LucidQuestionIdType = Field(
        description="The unique identifier for the qualification", frozen=True
    )
    question_text: str = Field(
        max_length=1024,
        min_length=1,
        description="The text shown to respondents",
        frozen=False,
    )
    question_type: LucidQuestionType = Field(
        description="The type of question asked", frozen=True
    )
    options: Optional[List[LucidQuestionOption]] = Field(
        default=None, min_length=1, frozen=True
    )

    source: Literal[Source.LUCID] = Source.LUCID

    @property
    def internal_id(self) -> str:
        return self.question_id

    @model_validator(mode="after")
    def check_type_options_agreement(self) -> Self:
        # If type == "text_entry", options is None. Otherwise, must be set.
        if self.question_type in {
            LucidQuestionType.TEXT_ENTRY,
            LucidQuestionType.NUMERICAL,
        }:
            assert self.options is None, "TEXT_ENTRY/NUMERICAL shouldn't have options"
        else:
            assert self.options is not None, "missing options"
        return self

    @field_validator("options")
    @classmethod
    def order_options(cls, options):
        if options:
            options.sort(key=lambda x: x.order)
        return options

    @classmethod
    def from_db(cls, d: dict) -> Self:
        options = None
        if d["options"]:
            options = [
                LucidQuestionOption(id=r["id"], text=r["text"], order=r["order"])
                for r in d["options"]
            ]
        return cls(
            question_id=d["question_id"],
            question_text=d["question_text"],
            question_type=d["question_type"],
            country_iso=d["country_iso"],
            language_iso=d["language_iso"],
            options=options,
        )

    def to_upk_question(self) -> "UpkQuestion":
        from generalresearch.models.thl.profiling.upk_question import (
            UpkQuestionChoice,
            UpkQuestionType,
            UpkQuestionSelectorMC,
            UpkQuestionSelectorTE,
            UpkQuestion,
        )

        upk_type_selector_map = {
            LucidQuestionType.SINGLE_SELECT: (
                UpkQuestionType.MULTIPLE_CHOICE,
                UpkQuestionSelectorMC.SINGLE_ANSWER,
            ),
            LucidQuestionType.DUMMY: (
                UpkQuestionType.MULTIPLE_CHOICE,
                UpkQuestionSelectorMC.SINGLE_ANSWER,
            ),
            LucidQuestionType.MULTI_SELECT: (
                UpkQuestionType.MULTIPLE_CHOICE,
                UpkQuestionSelectorMC.MULTIPLE_ANSWER,
            ),
            LucidQuestionType.TEXT_ENTRY: (
                UpkQuestionType.TEXT_ENTRY,
                UpkQuestionSelectorTE.SINGLE_LINE,
            ),
            LucidQuestionType.NUMERICAL: (
                UpkQuestionType.TEXT_ENTRY,
                UpkQuestionSelectorTE.SINGLE_LINE,
            ),
        }
        upk_type, upk_selector = upk_type_selector_map[self.question_type]
        d = {
            "ext_question_id": self.external_id,
            "country_iso": self.country_iso,
            "language_iso": self.language_iso,
            "type": upk_type,
            "selector": upk_selector,
            "text": self.question_text,
        }
        if self.options:
            d["choices"] = [
                UpkQuestionChoice(id=c.id, text=c.text, order=c.order)
                for c in self.options
            ]
        return UpkQuestion(**d)
