import json
from datetime import datetime, timezone
from enum import Enum
from typing import Optional, List, Literal, Dict, Any
from uuid import UUID

from pydantic import Field, BaseModel, model_validator, field_validator
from typing_extensions import Self

from generalresearch.models import Source, string_utils
from generalresearch.models.cint import CintQuestionIdType
from generalresearch.models.custom_types import AwareDatetimeISO
from generalresearch.models.thl.profiling.marketplace import (
    MarketplaceQuestion,
    MarketplaceUserQuestionAnswer,
)


class CintQuestionType(str, Enum):
    SINGLE_SELECT = "s"
    MULTI_SELECT = "m"
    # Dummy means they're calculated
    DUMMY = "d"
    TEXT_ENTRY = "t"
    NUMERIC_ENTRY = "n"

    @classmethod
    def from_api(cls, a: int):
        API_TYPE_MAP = {
            "Single Punch": CintQuestionType.SINGLE_SELECT,
            "Multi Punch": CintQuestionType.MULTI_SELECT,
            "Dummy": CintQuestionType.DUMMY,
            # What's the difference between dummy and calculated dummy? I thought dummy
            #   was calculated? who knows
            "Calculated Dummy": CintQuestionType.DUMMY,
            "Open Ended": CintQuestionType.TEXT_ENTRY,
            "Numeric - Open-end": CintQuestionType.NUMERIC_ENTRY,
            # This seems to be invalid as there are no options???
            "Grid": None,
        }
        return API_TYPE_MAP[a] if a in API_TYPE_MAP else None


class CintUserQuestionAnswer(MarketplaceUserQuestionAnswer):
    question_id: CintQuestionIdType = Field()
    question_type: Optional[CintQuestionType] = Field(default=None)
    # Did this answer come from us asking, or was it passed back from the marketplace
    from_thl: bool = Field(default=True)


class CintQuestionOption(BaseModel):
    id: str = Field(
        min_length=1,
        max_length=16,
        pattern=r"^[0-9]+|-3105|true|false$",
        frozen=True,
        description="This is called precode in their API",
    )
    text: str = Field(
        min_length=1,
        max_length=1024,
        frozen=True,
        description="The response text shown to respondents",
    )
    order: int = Field()


class CintQuestion(MarketplaceQuestion):
    question_id: CintQuestionIdType = Field(
        description="The unique identifier for the qualification",
        frozen=True,
        examples=["741"],
    )
    question_name: str = Field(examples=["STANDARD_GAMING_TYPE"])
    question_text: str = Field(
        max_length=1024,
        min_length=1,
        description="The text shown to respondents",
        frozen=False,
        examples=["What kind(s) of video/computer games do you play?"],
    )
    question_type: CintQuestionType = Field(
        description="The type of question asked",
        frozen=True,
        examples=[CintQuestionType.MULTI_SELECT],
    )
    options: Optional[List[CintQuestionOption]] = Field(
        default=None, min_length=1, frozen=True
    )
    option_mask: str = Field(examples=["000000000000000000"])
    classification_code: Optional[str] = Field(examples=["ELE"], default=None)
    # This comes from the API! not us
    created_at: AwareDatetimeISO = Field(description="Called create_date in API")

    source: Literal[Source.CINT] = Source.CINT

    @property
    def internal_id(self) -> str:
        return self.question_id

    @field_validator("question_name", "question_text", mode="after")
    def remove_nbsp(cls, s: Optional[str]) -> Optional[str]:
        return string_utils.remove_nbsp(s)

    @model_validator(mode="after")
    def check_type_options_agreement(self) -> Self:
        if self.question_type in {
            CintQuestionType.TEXT_ENTRY,
            CintQuestionType.NUMERIC_ENTRY,
        }:
            assert self.options is None, "TEXT_ENTRY/NUMERICAL shouldn't have options"
        elif self.question_type == CintQuestionType.DUMMY:
            # These are calculated. Sometimes they have options? idk
            pass
        else:
            assert self.options is not None, "missing options"
        return self

    @field_validator("options")
    @classmethod
    def order_options(cls, options):
        if options:
            options.sort(key=lambda x: x.order)
        return options

    @field_validator("options")
    @classmethod
    def validate_options(cls, options):
        if options:
            ids = {x.id for x in options}
            assert len(ids) == len(options), "options.id must be unique"
            orders = {x.order for x in options}
            assert len(orders) == len(options), "options.order must be unique"
        return options

    @classmethod
    def from_api(cls, d: dict, country_iso: str, language_iso: str) -> Self:
        options = None
        created_at = datetime.strptime(
            d["create_date"], "%Y-%m-%dT%H:%M:%S%z"
        ).astimezone(timezone.utc)
        if d.get("question_options"):
            options = [
                CintQuestionOption(
                    id=r["precode"], text=r["text"], order=r.get("order", order)
                )
                for order, r in enumerate(d["question_options"])
            ]
            # Sometimes the order from the api is incorrect
            orders = {opt.order for opt in options}
            if len(orders) != len(options):
                for idx, opt in enumerate(options):
                    opt.order = idx

        return cls(
            question_id=str(d["id"]),
            question_name=d["name"],
            question_text=d["question_text"],
            question_type=CintQuestionType.from_api(d["question_type"]),
            country_iso=country_iso,
            language_iso=language_iso,
            options=options,
            option_mask=d["option_mask"],
            created_at=created_at,
            classification_code=d["classification_code"],
        )

    @classmethod
    def from_db(cls, d: dict) -> Self:
        options = None
        if d["options"]:
            options = [
                CintQuestionOption(id=r["id"], text=r["text"], order=r["order"])
                for r in d["options"]
            ]
        if d.get("created_at"):
            d["created_at"] = d["created_at"].replace(tzinfo=timezone.utc)
        return cls(
            question_id=d["question_id"],
            question_name=d["question_name"],
            question_text=d["question_text"],
            question_type=d["question_type"],
            country_iso=d["country_iso"],
            language_iso=d["language_iso"],
            options=options,
            option_mask=d["option_mask"],
            created_at=d["created_at"],
            classification_code=d["classification_code"],
            category_id=UUID(d["category_id"]).hex if d["category_id"] else None,
        )

    def to_mysql(self) -> Dict[str, Any]:
        d = self.model_dump(mode="json")
        d["options"] = json.dumps(d["options"])
        if self.created_at:
            d["created_at"] = self.created_at.replace(tzinfo=None)
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
            CintQuestionType.SINGLE_SELECT: (
                UpkQuestionType.MULTIPLE_CHOICE,
                UpkQuestionSelectorMC.SINGLE_ANSWER,
            ),
            # CintQuestionType.DUMMY: (
            #     UpkQuestionType.MULTIPLE_CHOICE,
            #     UpkQuestionSelectorMC.SINGLE_ANSWER,
            # ),
            CintQuestionType.MULTI_SELECT: (
                UpkQuestionType.MULTIPLE_CHOICE,
                UpkQuestionSelectorMC.MULTIPLE_ANSWER,
            ),
            CintQuestionType.TEXT_ENTRY: (
                UpkQuestionType.TEXT_ENTRY,
                UpkQuestionSelectorTE.SINGLE_LINE,
            ),
            CintQuestionType.NUMERIC_ENTRY: (
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
