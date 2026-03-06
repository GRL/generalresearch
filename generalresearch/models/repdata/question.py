from __future__ import annotations

import json
import logging
from enum import Enum
from functools import cached_property
from typing import List, Optional, Literal, Any, Dict, Set
from uuid import UUID

from pydantic import (
    BaseModel,
    Field,
    model_validator,
    ConfigDict,
    field_validator,
    PositiveInt,
)

from generalresearch.models import Source, MAX_INT32
from generalresearch.models.custom_types import UUIDStr, AwareDatetimeISO
from generalresearch.models.thl.profiling.marketplace import MarketplaceQuestion

logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.INFO)


class RepDataUserQuestionAnswer(BaseModel):
    # This is optional b/c this model can be used for eligibility checks for
    # "anonymous" users, which are represented by a list of question answers
    # not associated with an actual user. No default b/c we must explicitly
    # set the field to None.
    user_id: Optional[PositiveInt] = Field(lt=MAX_INT32)
    question_id: str = Field(min_length=1, max_length=16, pattern=r"^[0-9]+$")
    # This is optional b/c we do not need it when writing these to the db. When
    # these are fetched from the db for use in yield-management, we read this
    # field from the repdata_question table.
    question_type: Optional[RepDataQuestionType] = Field(default=None)
    # This may be a pipe-separated string if the question_type is multi. regex
    # means any chars except capital letters
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


class RepDataQuestionOption(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    id: str = Field(
        min_length=1,
        max_length=16,
        pattern=r"^(([0-9]+)|-3105)$",
        frozen=True,
        validation_alias="Code",
        description="The unique identifier for a response to a qualification",
    )
    text: str = Field(
        min_length=1,
        max_length=1024,
        frozen=True,
        validation_alias="OptionName",
        description="The response text shown to respondents",
    )
    # Order does not come back explicitly in the API, but the responses seem
    # to be ordered
    order: int = Field()


class RepDataQuestionType(str, Enum):
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
            "Single Punch": RepDataQuestionType.SINGLE_SELECT,
            "Multi Punch": RepDataQuestionType.MULTI_SELECT,
            "Numeric - Open End": RepDataQuestionType.TEXT_ENTRY,
            "Zip Code": RepDataQuestionType.TEXT_ENTRY,
            "Derived": RepDataQuestionType.UNKNOWN,
        }
        return API_TYPE_MAP[a]


class RepDataQuestion(MarketplaceQuestion):
    model_config = ConfigDict(extra="ignore", populate_by_name=True)
    question_id: UUIDStr = Field(
        description="The unique identifier for the qualification",
        validation_alias="QualificationUD",
        frozen=True,
    )
    question_name: str = Field(
        min_length=1, max_length=64, frozen=True, validation_alias="QualificationName"
    )
    lucid_id: Optional[str] = Field(
        min_length=1,
        max_length=16,
        pattern=r"^[0-9]+$",
        validation_alias="StandardGlobalID",
        frozen=True,
    )
    lucid_name: Optional[str] = Field(
        min_length=1, max_length=64, frozen=True, validation_alias="StandardGlobalName"
    )
    question_text: str = Field(
        max_length=1024,
        min_length=1,
        description="The text shown to respondents",
        validation_alias="QualificationText",
    )
    question_type: RepDataQuestionType = Field(
        frozen=True, validation_alias="QualificationType"
    )
    options: Optional[List[RepDataQuestionOption]] = Field(default=None, min_length=1)
    source: Literal[Source.REPDATA] = Source.REPDATA

    @property
    def internal_id(self) -> str:
        return self.lucid_id

    @field_validator("question_id", mode="before")
    @classmethod
    def check_uuid_type(cls, v: str | UUID) -> str:
        return UUID(v).hex if isinstance(v, str) else v.hex

    @model_validator(mode="after")
    def check_type_options_agreement(self):
        # If type == "text_entry", options is None. Otherwise, must be set.
        if self.question_type == RepDataQuestionType.TEXT_ENTRY:
            assert self.options is None, "TEXT_ENTRY shouldn't have options"
        else:
            assert self.options is not None, "missing options"
        return self

    @classmethod
    def from_api(
        cls, d: dict, country_iso: str, language_iso: str
    ) -> Optional["RepDataQuestion"]:
        """
        :param d: Raw response from API
        """
        try:
            return cls._from_api(d, country_iso, language_iso)
        except Exception as e:
            logger.warning(f"Unable to parse question: {d}. {e}")
            return None

    @classmethod
    def _from_api(
        cls, d: dict, country_iso: str, language_iso: str
    ) -> "RepDataQuestion":
        d["QualificationType"] = RepDataQuestionType.from_api(d["QualificationType"])
        # zip code/age has a placeholder invalid option for some reason
        if d["QualificationType"] == RepDataQuestionType.TEXT_ENTRY:
            d["QualificationOptions"] = None
        options = None
        if d["QualificationOptions"]:
            options = [
                RepDataQuestionOption(id=str(r["Code"]), text=r["OptionName"], order=n)
                for n, r in enumerate(d["QualificationOptions"])
                if r
            ]
        return cls(
            **d, options=options, country_iso=country_iso, language_iso=language_iso
        )

    @classmethod
    def from_db(cls, d: dict):
        options = None
        if d["options"]:
            options = [
                RepDataQuestionOption(id=r["id"], text=r["text"], order=r["order"])
                for r in d["options"]
            ]
        return cls(
            question_id=d["question_id"],
            question_text=d["question_text"],
            question_name=d["question_name"],
            lucid_id=d["lucid_id"],
            lucid_name=d["lucid_name"],
            question_type=d["question_type"],
            country_iso=d["country_iso"],
            language_iso=d["language_iso"],
            options=options,
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
            RepDataQuestionType.SINGLE_SELECT: (
                UpkQuestionType.MULTIPLE_CHOICE,
                UpkQuestionSelectorMC.SINGLE_ANSWER,
            ),
            RepDataQuestionType.MULTI_SELECT: (
                UpkQuestionType.MULTIPLE_CHOICE,
                UpkQuestionSelectorMC.MULTIPLE_ANSWER,
            ),
            RepDataQuestionType.TEXT_ENTRY: (
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
