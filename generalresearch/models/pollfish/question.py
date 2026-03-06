# https://wss.pollfish.com/mediation/documentation
import json
import logging
from enum import Enum
from typing import List, Optional, Literal, Any, Dict

from pydantic import BaseModel, Field, model_validator

from generalresearch.models import Source
from generalresearch.models.thl.profiling.marketplace import MarketplaceQuestion

logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.INFO)


class PollfishQuestionOption(BaseModel):
    id: str = Field(
        min_length=1,
        max_length=64,
        pattern=r"^[\w\s\.\-]+$",
        frozen=True,
        description="The unique identifier for a response to a qualification",
    )
    text: str = Field(
        min_length=1,
        max_length=1024,
        frozen=True,
        description="The response text shown to respondents",
    )
    # Order does not come back explicitly in the API, but the responses seem to be ordered
    order: int = Field()


class PollfishQuestionType(str, Enum):
    """
    From the API: {'single_punch', 'multi_punch', 'open_ended'}
    """

    SINGLE_SELECT = "s"
    MULTI_SELECT = "m"
    TEXT_ENTRY = "t"


class PollfishQuestion(MarketplaceQuestion):
    question_id: str = Field(
        min_length=1,
        max_length=64,
        pattern=r"^[a-z0-9_\s\.]+$",
        description="The unique identifier for the qualification",
        frozen=True,
    )
    question_text: str = Field(
        max_length=1024, min_length=1, description="The text shown to respondents"
    )
    question_type: PollfishQuestionType = Field(frozen=True)
    options: Optional[List[PollfishQuestionOption]] = Field(default=None, min_length=1)
    # This comes from the API field "category"
    tags: Optional[str] = Field(default=None, frozen=True)
    source: Literal[Source.POLLFISH] = Source.POLLFISH

    @property
    def internal_id(self) -> str:
        return self.question_id

    @model_validator(mode="after")
    def check_type_options_agreement(self):
        # If type == "text_entry", options is None. Otherwise, must be set.
        if self.question_type == PollfishQuestionType.TEXT_ENTRY:
            assert self.options is None, "TEXT_ENTRY shouldn't have options"
        else:
            assert self.options is not None, "missing options"
        return self

    @classmethod
    def from_db(cls, d: dict):
        options = None
        if d["options"]:
            options = [
                PollfishQuestionOption(id=r["id"], text=r["text"], order=r["order"])
                for r in d["options"]
            ]
        return cls(
            question_id=d["question_id"],
            question_text=d["question_text"],
            question_type=d["question_type"],
            country_iso=d["country_iso"],
            language_iso=d["language_iso"],
            options=options,
            tags=d["tags"],
            is_live=d["is_live"],
            category_id=d.get("category_id"),
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
            order_exclusive_options,
        )

        upk_type_selector_map = {
            PollfishQuestionType.SINGLE_SELECT: (
                UpkQuestionType.MULTIPLE_CHOICE,
                UpkQuestionSelectorMC.SINGLE_ANSWER,
            ),
            PollfishQuestionType.MULTI_SELECT: (
                UpkQuestionType.MULTIPLE_CHOICE,
                UpkQuestionSelectorMC.MULTIPLE_ANSWER,
            ),
            PollfishQuestionType.TEXT_ENTRY: (
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
                UpkQuestionChoice(id=c.id, text=c.text, order=n)
                for n, c in enumerate(self.options)
            ]
        q = UpkQuestion(**d)
        order_exclusive_options(q)
        return q
