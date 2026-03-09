# https://developers.dynata.com/docs/rex-respondent-gateway/dc5b33f20a1c9-get-attribute-info
import json
import logging
import re
from datetime import timedelta
from enum import Enum
from functools import cached_property
from typing import Any, Dict, List, Literal, Optional, Set

from pydantic import BaseModel, Field, PositiveInt, field_validator, model_validator

from generalresearch.models import MAX_INT32, Source
from generalresearch.models.custom_types import AwareDatetimeISO
from generalresearch.models.thl.profiling.marketplace import MarketplaceQuestion

logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.INFO)

TAG_RE = re.compile(r"<[^>]+>")


def clean_text(s: str):
    # Some have a bunch of stupid html tags like '<font size="2"><b>What type of phone do you use?</b></font>'
    # thank you stackoverflow
    return TAG_RE.sub("", s).replace("\n", "").replace("&nbsp;", "")


class DynataQuestionOption(BaseModel):
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

    # Order does not come back explicitly in the API, and the options are not ordered at all. We will
    #   order the responses when converting to UpkQuestion

    @field_validator("text", mode="after")
    def clean_text(cls, s: str):
        return clean_text(s)


class DynataQuestionType(str, Enum):
    """
    From the API: {'geo', 'multi_select', 'multi_select_searchable',  'none',
      'single_select',  'single_select_grid',  'single_select_searchable',  'zip'}
    These are of course not defined anywhere...
    """

    # single_select, single_select_grid, single_select_searchable, geo
    SINGLE_SELECT = "s"
    # multi_select, multi_select_searchable
    MULTI_SELECT = "m"
    # zip
    TEXT_ENTRY = "t"
    # Some questions are "restricted"/hidden, and we don't know anything but their ID
    RESTRICTED = "r"

    # none: Some of these are just invalid, some are calculated GEO questions.

    @staticmethod
    def from_api(display_mode: str):
        question_type_map = {
            "multi_select": DynataQuestionType.MULTI_SELECT,
            "multi_select_searchable": DynataQuestionType.MULTI_SELECT,
            "single_select": DynataQuestionType.SINGLE_SELECT,
            "single_select_grid": DynataQuestionType.SINGLE_SELECT,
            "single_select_searchable": DynataQuestionType.SINGLE_SELECT,
            "geo": DynataQuestionType.SINGLE_SELECT,
            "zip": DynataQuestionType.TEXT_ENTRY,
            "text_entry": DynataQuestionType.TEXT_ENTRY,
        }
        # We don't want to fail if it is None b/c some are calculated GEO questions
        return question_type_map.get(display_mode, DynataQuestionType.SINGLE_SELECT)


class DynataUserQuestionAnswer(BaseModel):
    # This is optional b/c this model can be used for eligibility checks for "anonymous" users, which are represented
    #   by a list of question answers not associated with an actual user. No default b/c we must explicitly set
    #   the field to None.
    user_id: Optional[PositiveInt] = Field(lt=MAX_INT32)
    question_id: str = Field(min_length=1, max_length=16, pattern=r"^[0-9]+$")
    # This is optional b/c we do not need it when writing these to the db. When these are fetched from the db
    #   for use in yield-management, we read this field from the question table.
    question_type: Optional[DynataQuestionType] = Field(default=None)
    # This may be a pipe-separated string if the question_type is multi. regex means any chars except capital letters
    option_id: str = Field(pattern=r"^[^A-Z]*$")
    created: AwareDatetimeISO = Field()
    # ISO 3166-1 alpha-2 (two-letter codes, lowercase)
    country_iso: str = Field(
        max_length=2, min_length=2, pattern=r"^[a-z]{2}$", frozen=True
    )
    # 3-char ISO 639-2/B, lowercase
    language_iso: str = Field(
        max_length=3, min_length=3, pattern=r"^[a-z]{3}$", frozen=True
    )

    @cached_property
    def options_ids(self) -> Set[str]:
        return set(self.option_id.split("|"))

    def to_mysql(self) -> Dict[str, Any]:
        d = self.model_dump(mode="json", exclude={"question_type"})
        d["created"] = self.created.replace(tzinfo=None)
        return d


class DynataQuestionDependency(BaseModel, frozen=True):
    # This is not explained or documented. Going to just store it for now
    question_id: str = Field(min_length=1, max_length=16, pattern=r"^[0-9]+$")
    # Some are an empty list. Unclear if this means "any option" or it is broken.
    option_ids: List[str] = Field()


class DynataQuestion(MarketplaceQuestion):
    # This is called "qualification_code" in the API
    question_id: str = Field(
        min_length=1,
        max_length=16,
        pattern=r"^[0-9]+$",
        description="The unique identifier for the qualification",
        frozen=True,
    )
    # In the API: desc
    question_name: str = Field(
        max_length=255, min_length=1, description="A short name for the question"
    )
    description: str = Field(max_length=255, min_length=1)
    question_text: str = Field(
        max_length=1024, min_length=1, description="The text shown to respondents"
    )
    question_type: DynataQuestionType = Field(frozen=True)
    options: Optional[List[DynataQuestionOption]] = Field(default=None, min_length=1)
    # This does not mean that it doesn't expire, it means undefined.
    expiration_duration: Optional[timedelta] = Field(default=None)
    parent_dependencies: List[DynataQuestionDependency] = Field(default_factory=list)

    source: Literal[Source.DYNATA] = Source.DYNATA

    @property
    def internal_id(self) -> str:
        return self.question_id

    @field_validator("question_text", mode="after")
    def clean_text(cls, s: str):
        return clean_text(s)

    @model_validator(mode="after")
    def check_type_options_agreement(self):
        # If type == "text_entry", options is None. Otherwise, must be set.
        if self.question_type in {
            DynataQuestionType.TEXT_ENTRY,
            DynataQuestionType.RESTRICTED,
        }:
            assert self.options is None, "TEXT_ENTRY shouldn't have options"
        else:
            assert self.options is not None, "missing options"
        return self

    @classmethod
    def create_restricted_question(cls, question_id):
        # In a restricted question, we don't know the name/description/etc, but I don't
        #   want these fields nullable
        return cls(
            question_id=question_id,
            question_type=DynataQuestionType.RESTRICTED,
            question_name="unknown",
            question_text="unknown",
            description="unknown",
            is_live=True,
            # We don't know what locale these questions are for
            country_iso="us",
            language_iso="eng",
        )

    @classmethod
    def from_db(cls, d: dict):
        options = None
        if d["options"]:
            options = [
                DynataQuestionOption(id=r["id"], text=r["text"]) for r in d["options"]
            ]
        parent_dependencies = [
            DynataQuestionDependency(
                question_id=pd["question_id"], option_ids=pd["option_ids"]
            )
            for pd in d["parent_dependencies"]
        ]
        expiration_duration = (
            timedelta(seconds=d["expiration_duration_sec"])
            if d["expiration_duration_sec"]
            else None
        )
        return cls(
            question_id=d["question_id"],
            question_name=d["question_name"],
            question_text=d["question_text"],
            question_type=d["question_type"],
            country_iso=d["country_iso"],
            language_iso=d["language_iso"],
            options=options,
            parent_dependencies=parent_dependencies,
            description=d["description"],
            is_live=d["is_live"],
            category_id=d.get("category_id"),
            expiration_duration=expiration_duration,
        )

    def to_mysql(self) -> Dict[str, Any]:
        d = self.model_dump(mode="json", by_alias=True)
        d["options"] = json.dumps(d["options"])
        d["parent_dependencies"] = json.dumps(d["parent_dependencies"])
        d["expiration_duration_sec"] = (
            self.expiration_duration.total_seconds()
            if self.expiration_duration
            else None
        )
        return d

    def to_upk_question(self):
        from generalresearch.models.thl.profiling.upk_question import (
            UpkQuestion,
            UpkQuestionChoice,
            UpkQuestionSelectorMC,
            UpkQuestionSelectorTE,
            UpkQuestionType,
            order_exclusive_options,
        )

        upk_type_selector_map = {
            DynataQuestionType.SINGLE_SELECT: (
                UpkQuestionType.MULTIPLE_CHOICE,
                UpkQuestionSelectorMC.SINGLE_ANSWER,
            ),
            DynataQuestionType.MULTI_SELECT: (
                UpkQuestionType.MULTIPLE_CHOICE,
                UpkQuestionSelectorMC.MULTIPLE_ANSWER,
            ),
            DynataQuestionType.TEXT_ENTRY: (
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
