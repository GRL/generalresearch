# https://developer-beta.market-cube.com/api-details#api=definition-api&operation=get-api-v1-definition-qualification
# -answers-lanaguge-languageid
import json
import logging
from enum import Enum
from functools import cached_property
from typing import TYPE_CHECKING, Any, Dict, List, Literal, Optional, Set

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    PositiveInt,
    field_validator,
    model_validator,
)

from generalresearch.models import MAX_INT32, Source, string_utils
from generalresearch.models.custom_types import AwareDatetimeISO
from generalresearch.models.thl.profiling.marketplace import MarketplaceQuestion

if TYPE_CHECKING:
    from generalresearch.models.thl.profiling.upk_question import (
        UpkQuestion,
    )

logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.INFO)


class SagoQuestionOption(BaseModel):
    id: str = Field(
        min_length=1,
        max_length=16,
        pattern=r"^[0-9]+$",
        frozen=True,
        description="The unique identifier for a response to a qualification",
    )
    # This is returned by the API but does not seem to be used for anything.
    # Will keep it any ways.
    code: Optional[str] = Field(min_length=1, max_length=16)
    text: str = Field(
        min_length=1,
        max_length=1024,
        frozen=True,
        description="The response text shown to respondents",
    )
    # Order does not come back explicitly in the API, but the responses seem
    # to be ordered
    order: int = Field()

    @field_validator("text", mode="after")
    def remove_nbsp(cls, s: str):
        return string_utils.remove_nbsp(s)


class SagoQuestionType(str, Enum):
    """
    From the API:
        {1: 'Single Punch', 2: 'Multi Punch', 3: 'Open Ended', 4: 'Dummy',
         5: 'Calculated Dummy', 6: 'Range', 7: 'EmailType', 8: 'Info',
         9: 'Compound', 10: 'Calendar', 11: 'Single Punch Image',
         12: 'Multi Punch Image', 14: 'VideoType'}

    Only {1, 2, 3, 6, 7, 8, 12} seem to be used. 8 and 12 seems to be unused.
    """

    # 1
    SINGLE_SELECT = "s"

    # 2
    MULTI_SELECT = "m"

    # 3, 6 (range is just age), 7 (asking for email).
    TEXT_ENTRY = "t"

    @classmethod
    def from_api(cls, a: int):
        API_TYPE_MAP = {
            1: SagoQuestionType.SINGLE_SELECT,
            2: SagoQuestionType.MULTI_SELECT,
            3: SagoQuestionType.TEXT_ENTRY,
            6: SagoQuestionType.TEXT_ENTRY,
            7: SagoQuestionType.TEXT_ENTRY,
        }
        return API_TYPE_MAP[a] if a in API_TYPE_MAP else None


class SagoUserQuestionAnswer(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    # This is optional b/c this model can be used for eligibility checks for
    # "anonymous" users, which are represented by a list of question answers
    # not associated with an actual user. No default b/c we must explicitly set
    # the field to None.
    user_id: Optional[PositiveInt] = Field(lt=MAX_INT32)
    question_id: str = Field(min_length=1, max_length=16, pattern=r"^[0-9]+$")

    # This is optional b/c we do not need it when writing these to the db. When
    # these are fetched from the db for use in yield-management, we read this
    # field from the question table.
    question_type: Optional[SagoQuestionType] = Field(default=None)

    # This may be a pipe-separated string if the question_type is multi.
    # regex means any chars except capital letters
    pre_code: str = Field(pattern=r"^[^A-Z]*$", validation_alias="option_id")
    created: AwareDatetimeISO = Field()

    # ISO 3166-1 alpha-2 (two-letter codes, lowercase)
    country_iso: str = Field(
        max_length=2, min_length=2, pattern=r"^[a-z]{2}$", frozen=True
    )
    # 3-char ISO 639-2/B, lowercase
    language_iso: str = Field(
        max_length=3, min_length=3, pattern=r"^[a-z]{3}$", frozen=True
    )

    @property
    def option_id(self) -> str:
        return self.pre_code

    @cached_property
    def options_ids(self) -> Set[str]:
        return set(self.pre_code.split("|"))

    def to_mysql(self) -> Dict[str, Any]:
        d = self.model_dump(mode="json", exclude={"question_type"})
        d["created"] = self.created.replace(tzinfo=None)
        return d


class SagoQuestion(MarketplaceQuestion):
    question_id: str = Field(
        min_length=1,
        max_length=16,
        pattern=r"^[0-9]+$",
        description="The unique identifier for the qualification",
        frozen=True,
    )
    question_name: str = Field(
        max_length=255, min_length=1, description="A short name for the question"
    )
    question_text: str = Field(
        max_length=1024, min_length=1, description="The text shown to respondents"
    )
    question_type: SagoQuestionType = Field(frozen=True)
    options: Optional[List[SagoQuestionOption]] = Field(default=None, min_length=1)

    # This comes from the API field "qualificationCategoryId"
    tags: Optional[str] = Field(default=None, frozen=True)
    source: Literal[Source.SAGO] = Source.SAGO

    @property
    def internal_id(self) -> str:
        return self.question_id

    @model_validator(mode="after")
    def check_type_options_agreement(self):
        # If type == "text_entry", options is None. Otherwise, must be set.
        if self.question_type == SagoQuestionType.TEXT_ENTRY:
            assert self.options is None, "TEXT_ENTRY shouldn't have options"
        else:
            assert self.options is not None, "missing options"
        return self

    @field_validator("question_name", "question_text", "tags", mode="after")
    def remove_nbsp(cls, s: Optional[str]):
        return string_utils.remove_nbsp(s)

    @classmethod
    def from_api(
        cls, d: Dict[str, Any], country_iso: str, language_iso: str
    ) -> Optional["SagoQuestion"]:
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
        cls, d: Dict[str, Any], country_iso: str, language_iso: str
    ) -> "SagoQuestion":
        sago_category_to_tags = {
            1: "Standard",
            2: "Custom",
            4: "PID",
            5: "Profile",
            12: "SAGO Standard",
        }
        question_type = SagoQuestionType.from_api(d["qualificationTypeId"])
        if question_type == SagoQuestionType.TEXT_ENTRY:
            # The API returns an option for each of these for some reason
            options = None
        else:
            options = [
                SagoQuestionOption(
                    id=str(r["answerId"]),
                    code=r["answerCode"],
                    text=r["text"].strip(),
                    order=n,
                )
                for n, r in enumerate(d["qualificationAnswers"])
            ]
        return cls(
            question_id=str(d["qualificationId"]),
            question_name=d["name"],
            question_text=d["text"],
            question_type=question_type,
            tags=sago_category_to_tags.get(d["qualificationCategoryId"]),
            options=options,
            country_iso=country_iso,
            language_iso=language_iso,
        )

    @classmethod
    def from_db(cls, d: Dict[str, Any]) -> "SagoQuestion":
        options = None
        if d["options"]:
            options = [
                SagoQuestionOption(
                    id=r["id"], code=r["code"], text=r["text"], order=r["order"]
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
            tags=d["tags"],
            is_live=d["is_live"],
            category_id=d.get("category_id"),
        )

    def to_mysql(self) -> Dict[str, Any]:
        d = self.model_dump(mode="json", by_alias=True)
        d["options"] = json.dumps(d["options"])
        return d

    def to_upk_question(self) -> "UpkQuestion":
        from generalresearch.models.thl.profiling.upk_question import (
            UpkQuestion,
            UpkQuestionChoice,
            UpkQuestionSelectorMC,
            UpkQuestionSelectorTE,
            UpkQuestionType,
            order_exclusive_options,
        )

        upk_type_selector_map = {
            SagoQuestionType.SINGLE_SELECT: (
                UpkQuestionType.MULTIPLE_CHOICE,
                UpkQuestionSelectorMC.SINGLE_ANSWER,
            ),
            SagoQuestionType.MULTI_SELECT: (
                UpkQuestionType.MULTIPLE_CHOICE,
                UpkQuestionSelectorMC.MULTIPLE_ANSWER,
            ),
            SagoQuestionType.TEXT_ENTRY: (
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
