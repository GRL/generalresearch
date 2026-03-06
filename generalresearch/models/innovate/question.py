# https://innovatemr.stoplight.io/docs/supplier-api/d21fa72c538db-lookup-question-library
from __future__ import annotations

import json
import logging
from enum import Enum
from typing import List, Optional, Literal, Any, Dict

from pydantic import BaseModel, Field, model_validator, field_validator

from generalresearch.models import Source
from generalresearch.models.innovate import InnovateQuestionID
from generalresearch.models.thl.profiling.marketplace import (
    MarketplaceQuestion,
    MarketplaceUserQuestionAnswer,
)

logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.INFO)


class InnovateUserQuestionAnswer(MarketplaceUserQuestionAnswer):
    # Note, this is referred to as the KEY in the Question model
    question_id: InnovateQuestionID = Field()
    question_type: Optional[InnovateQuestionType] = Field(default=None)
    # Did this answer come from us asking, or was it passed back from the marketplace
    from_thl: bool = Field(default=True)


class InnovateQuestionOption(BaseModel):
    id: str = Field(
        min_length=1,
        max_length=16,
        pattern=r"^[0-9]+$",
        frozen=True,
        description="The unique identifier for a response to a qualification",
    )
    text: str = Field(
        min_length=1,
        max_length=1024,
        frozen=True,
        description="The response text shown to respondents",
    )
    order: int = Field()


class InnovateQuestionType(str, Enum):
    # API response: {'Multipunch', 'Numeric Open Ended', 'Single Punch'}
    # "Numeric Open Ended" must be wrong... It can't be numeric, as UK's postcode question is marked
    #   as this, but it wants alphanumeric answers. So this is just text_entry.

    SINGLE_SELECT = "s"
    MULTI_SELECT = "m"
    TEXT_ENTRY = "t"

    @staticmethod
    def get_api_map():
        return {
            "Single Punch": InnovateQuestionType.SINGLE_SELECT,
            "Multipunch": InnovateQuestionType.MULTI_SELECT,
            "Numeric Open Ended": InnovateQuestionType.TEXT_ENTRY,
        }

    @classmethod
    def from_api(cls, a: int):
        API_TYPE_MAP = cls.get_api_map()
        return API_TYPE_MAP[a] if a in API_TYPE_MAP else None


class InnovateQuestion(MarketplaceQuestion):
    # Each question has an ID (numerical) and a Name (which they call "Key") which are both unique. The
    #   key is what is used throughout, so this what will be used as the primary key.
    question_key: str = Field(
        min_length=1,
        max_length=64,
        pattern=r"^[^A-Z]+$",
        description="Primary identifier that is used throughout Innovate",
        frozen=True,
    )
    question_id: str = Field(
        min_length=1,
        max_length=16,
        pattern=r"^[0-9]+$",
        description="Numerical identifier for the qualification",
        frozen=True,
    )

    question_text: str = Field(
        max_length=1024,
        min_length=1,
        description="The text shown to respondents",
        frozen=False,
    )
    question_type: InnovateQuestionType = Field(
        description="The type of question asked", frozen=True
    )
    # This comes from the API field "Category". There are some useful categories in here, but a bunch have
    #   categories that are not (e.g. NFX - Adhoc, Testing_Cat). We'll store it as a comma-separated string
    #   here to use it to aid our own real categorization.
    tags: Optional[str] = Field(default=None, frozen=True)
    options: Optional[List[InnovateQuestionOption]] = Field(
        default=None, min_length=1, frozen=True
    )

    source: Literal[Source.INNOVATE] = Source.INNOVATE

    @property
    def internal_id(self) -> str:
        return self.question_key

    @model_validator(mode="after")
    def check_type_options_agreement(self):
        # If type == "text_entry", options is None. Otherwise, must be set.
        if self.question_type == InnovateQuestionType.TEXT_ENTRY:
            assert self.options is None, "TEXT_ENTRY shouldn't have options"
        else:
            assert self.options is not None, "missing options"
        return self

    @field_validator("options")
    @classmethod
    def order_options(cls, options):
        if options:
            options.sort(key=lambda x: x.order)
        return options

    @field_validator("question_key", mode="before")
    @classmethod
    def question_key_lower(cls, v: str) -> str:
        if v.lower() != v:
            logger.warning(f"question key {v} should be lowercase!")
            v = v.lower()
        return v

    @classmethod
    def from_api(
        cls, d: dict, country_iso: str, language_iso: str
    ) -> Optional["InnovateQuestion"]:
        """
        :param d: Raw response from API
        :param country_iso:
        :param language_iso:
        :return:
        """
        try:
            return cls._from_api(d, country_iso, language_iso)
        except Exception as e:
            logger.warning(f"Unable to parse question: {d}. {e}")
            return None

    @classmethod
    def _from_api(
        cls, d: dict, country_iso: str, language_iso: str
    ) -> "InnovateQuestion":
        # Question AGE returns options even though its marked as a text entry (but only in some locales)
        d["QuestionKey"] = d["QuestionKey"].lower()
        if d["QuestionKey"] == "age":
            d["QuestionOptions"] = []

        options = None
        if d.get("QuestionOptions"):
            options = [
                InnovateQuestionOption(
                    id=str(r["id"]), text=r["OptionText"], order=r["Order"]
                )
                for r in d["QuestionOptions"]
            ]
        tags = ",".join(map(str.strip, d["Category"]))
        return cls(
            question_id=str(d["QuestionId"]),
            question_key=d["QuestionKey"],
            question_text=d["QuestionText"],
            question_type=InnovateQuestionType.from_api(d["QuestionType"]),
            tags=tags,
            options=options,
            country_iso=country_iso,
            language_iso=language_iso,
        )

    @classmethod
    def from_db(cls, d: dict) -> "InnovateQuestion":
        options = None
        if d["options"]:
            options = [
                InnovateQuestionOption(id=r["id"], text=r["text"], order=r["order"])
                for r in d["options"]
            ]
        return cls(
            question_id=d["question_id"],
            question_key=d["question_key"],
            question_text=d["question_text"],
            question_type=d["question_type"],
            country_iso=d["country_iso"],
            language_iso=d["language_iso"],
            options=options,
            is_live=d["is_live"],
            category_id=d.get("category_id"),
            tags=d["tags"],
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
            UpkQuestion,
        )

        upk_type_selector_map = {
            InnovateQuestionType.SINGLE_SELECT: (
                UpkQuestionType.MULTIPLE_CHOICE,
                UpkQuestionSelectorMC.SINGLE_ANSWER,
            ),
            InnovateQuestionType.MULTI_SELECT: (
                UpkQuestionType.MULTIPLE_CHOICE,
                UpkQuestionSelectorMC.MULTIPLE_ANSWER,
            ),
            InnovateQuestionType.TEXT_ENTRY: (
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
