# https://integrations.precisionsample.com/api.html#Get%20Questions
import json
import logging
from enum import Enum
from typing import List, Optional, Literal, Any, Dict

from pydantic import BaseModel, Field, model_validator, field_validator

from generalresearch.models import Source, string_utils
from generalresearch.models.precision import PrecisionQuestionID
from generalresearch.models.thl.profiling.marketplace import (
    MarketplaceQuestion,
    MarketplaceUserQuestionAnswer,
)

logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.INFO)


class PrecisionQuestionOption(BaseModel):
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
    # Order does not come back explicitly in the API, but the responses seem to be ordered
    order: int = Field()


class PrecisionQuestionType(str, Enum):
    """
    From the API: {'Drop Down', 'Multi Select', 'Single Select', 'Single Select Matrix', 'Vertical Question'}
    Of course undocumented. And there doesn't seem to be a text entry option?
    """

    SINGLE_SELECT = "s"
    MULTI_SELECT = "m"
    TEXT_ENTRY = "t"

    @classmethod
    def from_api(cls, a: int):
        API_TYPE_MAP = {
            "Drop Down": PrecisionQuestionType.SINGLE_SELECT,
            "Multi Select": PrecisionQuestionType.MULTI_SELECT,
            "Single Select": PrecisionQuestionType.SINGLE_SELECT,
            "Single Select Matrix": PrecisionQuestionType.SINGLE_SELECT,
            "Vertical Question": PrecisionQuestionType.SINGLE_SELECT,
        }
        return API_TYPE_MAP[a] if a in API_TYPE_MAP else None


class PrecisionUserQuestionAnswer(MarketplaceUserQuestionAnswer):
    question_id: PrecisionQuestionID = Field()
    question_type: Optional[PrecisionQuestionType] = Field(default=None)
    # Was this answer synchronized with precision's user profile API?
    synced: bool = Field(default=False)


class PrecisionQuestion(MarketplaceQuestion):
    question_id: PrecisionQuestionID = Field(
        description="The unique identifier for the qualification"
    )
    question_name: Optional[str] = Field(default=None, max_length=128)
    question_text: str = Field(
        max_length=1024, min_length=1, description="The text shown to respondents"
    )
    question_type: PrecisionQuestionType = Field(frozen=True)
    options: Optional[List[PrecisionQuestionOption]] = Field(default=None, min_length=1)
    # This comes from the API field ProfileName. idk what the possible values are, looks like:
    #   'Personal Profile', 'Work Profile', 'Auto Profile', 'Medical Profile', 'Travel & Entertainment'.
    # I don't know what, if anything, this is used for.
    profile: Optional[str] = Field(default=None, frozen=True)
    source: Literal[Source.PRECISION] = Source.PRECISION

    @property
    def internal_id(self) -> str:
        return self.question_id

    @field_validator("question_text", mode="after")
    def remove_nbsp(cls, s: Optional[str]):
        return string_utils.remove_nbsp(s)

    @model_validator(mode="after")
    def check_type_options_agreement(self):
        # If type == "text_entry", options is None. Otherwise, must be set.
        if self.question_type == PrecisionQuestionType.TEXT_ENTRY:
            assert self.options is None, "TEXT_ENTRY shouldn't have options"
        else:
            assert self.options is not None, "missing options"
        return self

    @classmethod
    def from_api(cls, d: dict) -> Optional["PrecisionQuestion"]:
        """
        :param d: Raw response from API
        """
        try:
            return cls._from_api(d)
        except Exception as e:
            logger.warning(f"Unable to parse question: {d}. {e}")
            return None

    @classmethod
    def _from_api(cls, d: dict) -> "PrecisionQuestion":
        question_type = PrecisionQuestionType.from_api(d["question_type_name"])
        # sometimes an empty option is returned .... ?
        options = [
            PrecisionQuestionOption(
                id=str(r["option_id"]), text=r["option_text"], order=n
            )
            for n, r in enumerate(d["options"])
            if r
        ]
        return cls(
            question_id=str(d["question_id"]),
            profile=d.get("ProfileName"),
            question_name=d.get("question_name"),
            question_text=d["question_text"],
            question_type=question_type,
            options=options,
            country_iso=d["country_iso"],
            language_iso=d["language_iso"],
        )

    @classmethod
    def from_db(cls, d: dict):
        options = None
        if d["options"]:
            options = [
                PrecisionQuestionOption(id=r["id"], text=r["text"], order=r["order"])
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
            PrecisionQuestionType.SINGLE_SELECT: (
                UpkQuestionType.MULTIPLE_CHOICE,
                UpkQuestionSelectorMC.SINGLE_ANSWER,
            ),
            PrecisionQuestionType.MULTI_SELECT: (
                UpkQuestionType.MULTIPLE_CHOICE,
                UpkQuestionSelectorMC.MULTIPLE_ANSWER,
            ),
            PrecisionQuestionType.TEXT_ENTRY: (
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
