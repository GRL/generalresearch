# https://developer.prodege.com/surveys-feed/api-reference/lookup-calls/lookup-questions-by-countryid
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from enum import Enum
from functools import cached_property
from typing import List, Optional, Literal, Any, Dict, Set

from pydantic import BaseModel, Field, model_validator, ConfigDict, PositiveInt

from generalresearch.locales import Localelator
from generalresearch.models import Source, MAX_INT32
from generalresearch.models.custom_types import AwareDatetimeISO
from generalresearch.models.prodege import ProdegeQuestionIdType
from generalresearch.models.thl.profiling.marketplace import MarketplaceQuestion

logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.INFO)

locale_helper = Localelator()


class ProdegeUserQuestionAnswer(BaseModel):
    # This is optional b/c this model can be used for eligibility checks for "anonymous" users, which are represented
    #   by a list of question answers not associated with an actual user. No default b/c we must explicitly set
    #   the field to None.
    user_id: Optional[PositiveInt] = Field(lt=MAX_INT32)
    question_id: ProdegeQuestionIdType = Field()
    # This is optional b/c we do not need it when writing these to the db. When these are fetched from the db
    #   for use in yield-management, we read this field from the prodege_question table.
    question_type: Optional[ProdegeQuestionType] = Field(default=None)
    # This may be a pipe-separated string if the question_type is multi. regex means any chars except capital letters
    option_id: str = Field(pattern=r"^[^A-Z]*$")
    created: AwareDatetimeISO = Field(
        default_factory=lambda: datetime.now(tz=timezone.utc)
    )
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


class ProdegeQuestionOption(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    id: str = Field(
        min_length=1,
        max_length=16,
        pattern=r"^([0-9]+)|-1|-3105$",
        frozen=True,
        validation_alias="option_id",
        description="The unique identifier for a response to a qualification",
    )
    text: str = Field(
        min_length=1,
        max_length=1024,
        validation_alias="option_text",
        description="The response text shown to respondents",
    )
    # Order does not come back explicitly in the API, but the responses seem to be ordered
    order: int = Field()
    # Both is_exclusive and is_anchored are returned, but I don't see how they are different.
    #   We are merging them both into is_exclusive.
    is_exclusive: bool = Field(default=False)


class ProdegeQuestionType(str, Enum):
    """
    {'Derived', 'Multi Punch', 'Numeric - Open End', 'Single Punch', 'Zip Code'}
    """

    SINGLE_SELECT = "s"
    MULTI_SELECT = "m"
    TEXT_ENTRY = "t"
    UNKNOWN = "u"

    @classmethod
    def from_api(cls, a: int):
        API_TYPE_MAP = {
            "Single-Select": ProdegeQuestionType.SINGLE_SELECT,
            "Multi-Select": ProdegeQuestionType.MULTI_SELECT,
            "Numeric": ProdegeQuestionType.TEXT_ENTRY,
            "Text": ProdegeQuestionType.TEXT_ENTRY,
        }
        return API_TYPE_MAP[a]


class ProdegeQuestion(MarketplaceQuestion):
    model_config = ConfigDict(extra="ignore", populate_by_name=True)
    question_id: ProdegeQuestionIdType = Field(
        description="The unique identifier for the qualification", frozen=True
    )
    question_name: str = Field(min_length=1, max_length=64, frozen=True)
    question_text: str = Field(max_length=1024, min_length=1)
    question_type: ProdegeQuestionType = Field(frozen=True)
    # This comes from the API category, but is not great (most are "Consumer Lifestyle")
    tags: Optional[str] = Field(default=None, frozen=True)
    options: Optional[List[ProdegeQuestionOption]] = Field(default=None, min_length=1)
    source: Literal[Source.PRODEGE] = Source.PRODEGE

    @property
    def internal_id(self) -> str:
        return self.question_id

    @model_validator(mode="after")
    def check_type_options_agreement(self):
        if self.question_type == ProdegeQuestionType.TEXT_ENTRY:
            assert self.options is None, "TEXT_ENTRY shouldn't have options"
        else:
            assert self.options is not None, "missing options"
        return self

    @classmethod
    def from_api(cls, d: dict, country_iso: str) -> Optional["ProdegeQuestion"]:
        """
        :param d: Raw response from API
        """
        try:
            return cls._from_api(d, country_iso)
        except Exception as e:
            logger.warning(f"Unable to parse question: {d}. {e}")
            return None

    @classmethod
    def _from_api(cls, d: dict, country_iso: str) -> "ProdegeQuestion":
        # The API has no concept of language at all. Questions for a country
        # are returned both in english and other languages. Questions do have
        # a field 'country_specific', and if True, that generally means the
        # question's language is the country's default lang. So we're mostly
        # guessing here ...
        d["question_id"] = str(d["question_id"])
        d["language_iso"] = (
            "eng"
            if d["country_specific"] is False
            else (locale_helper.get_default_lang_from_country(country_iso))
        )
        d["country_iso"] = country_iso
        d["question_type"] = ProdegeQuestionType.from_api(d["question_type"])
        d["tags"] = d["category"].lower()
        if not d["question_text"]:
            d["question_text"] = d["question_name"]
        if d["question_type"] == ProdegeQuestionType.TEXT_ENTRY:
            d["options"] = None
        if d["options"]:
            d["options"] = [
                ProdegeQuestionOption(
                    id=str(r["option_id"]),
                    text=r["option_text"],
                    order=n,
                    is_exclusive=r["is_exclusive"] or r["is_anchored"],
                )
                for n, r in enumerate(d["options"])
                if r and r["option_text"]
            ]
        return cls.model_validate(d)

    @classmethod
    def from_db(cls, d: dict):
        options = None
        if d["options"]:
            options = [
                ProdegeQuestionOption(
                    id=r["id"],
                    text=r["text"],
                    order=r["order"],
                    is_exclusive=r.get("is_exclusive", False),
                )
                for r in d["options"]
            ]
        return cls(
            question_id=d["question_id"],
            question_text=d["question_text"],
            question_name=d["question_name"],
            question_type=d["question_type"],
            country_iso=d["country_iso"],
            language_iso=d["language_iso"],
            options=options,
            is_live=d["is_live"],
            category_id=d.get("category_id"),
            tags=d.get("tags"),
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
            ProdegeQuestionType.SINGLE_SELECT: (
                UpkQuestionType.MULTIPLE_CHOICE,
                UpkQuestionSelectorMC.SINGLE_ANSWER,
            ),
            ProdegeQuestionType.MULTI_SELECT: (
                UpkQuestionType.MULTIPLE_CHOICE,
                UpkQuestionSelectorMC.MULTIPLE_ANSWER,
            ),
            ProdegeQuestionType.TEXT_ENTRY: (
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
