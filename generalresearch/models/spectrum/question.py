# https://purespectrum.atlassian.net/wiki/spaces/PA/pages/36851836/Get+Attributes+By+Qualification+Code
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from enum import Enum
from functools import cached_property
from typing import TYPE_CHECKING, Any, Dict, List, Literal, Optional, Set
from uuid import UUID

from pydantic import (
    BaseModel,
    Field,
    PositiveInt,
    field_validator,
    model_validator,
)
from typing_extensions import Self

from generalresearch.models import MAX_INT32, Source, string_utils
from generalresearch.models.custom_types import AwareDatetimeISO
from generalresearch.models.spectrum import SpectrumQuestionIdType
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


class SpectrumUserQuestionAnswer(BaseModel):
    # This is optional b/c this model can be used for eligibility checks
    #   for "anonymous" users, which are represented by a list of question
    #   answers not associated with an actual user. No default b/c we must
    #   explicitly set the field to None.

    user_id: Optional[PositiveInt] = Field(lt=MAX_INT32)
    question_id: SpectrumQuestionIdType = Field()
    # This is optional b/c we do not need it when writing these to the
    #   db. When these are fetched from the db for use in yield-management,
    #   we read this field from the spectrum_question table.
    question_type: Optional[SpectrumQuestionType] = Field(default=None)
    # This may be a pipe-separated string if the question_type is multi. regex
    #   means any chars except capital letters
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


class SpectrumQuestionOption(BaseModel):
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
    # Order does not come back explicitly in the API, and the API does not
    #   order them at all. Generally, they should be ordered by the id, but
    #   this isn't consistent.
    order: int = Field()

    @field_validator("text", mode="after")
    def remove_nbsp(cls, s: str) -> str:
        return string_utils.remove_nbsp(s)


class SpectrumQuestionType(str, Enum):
    # The documentation defines 4 types (1,2,3,4), however 2 is the same as 1
    #   and never comes back in the api, and we also get back 5, 6, and 7,
    #   which are all undocumented.

    # This is for type 1 or 2 in their docs (singlepunch or singlepunch-alt)
    SINGLE_SELECT = "s"
    # Type 3 (multipunch)
    MULTI_SELECT = "m"

    # Type 5 is undocumented, but seems to be integer free response
    # Type 7 is undocumented, but looks to be free-response / open-ended,
    #   generally data quality related (e.g. Please tell us, how would you
    #   like to spend your weekend?)
    TEXT_ENTRY = "t"

    # Type 4 (range). These all seem to be testing questions, and there is
    #   nothing to indicate how this should work at all, so we should not
    #   use this.
    #
    # RANGE = 'r'
    # Type 6 is undocumented, but is a children question that relies on another
    # question using unknown also as a catch-all incase they change their
    # API randomly
    UNKNOWN = "u"

    @staticmethod
    def get_api_map() -> Dict[int, SpectrumQuestionType]:
        return {
            1: SpectrumQuestionType.SINGLE_SELECT,
            2: SpectrumQuestionType.SINGLE_SELECT,
            3: SpectrumQuestionType.MULTI_SELECT,
            5: SpectrumQuestionType.TEXT_ENTRY,
            7: SpectrumQuestionType.TEXT_ENTRY,
        }

    @classmethod
    def from_api(cls, a: int):
        api_type_map = cls.get_api_map()
        return api_type_map[a] if a in api_type_map else None


class SpectrumQuestionClass(int, Enum):
    CORE = 1
    EXTENDED = 2
    CUSTOM = 3


class SpectrumQuestion(MarketplaceQuestion):
    # This is called "qualification_code" in the API
    question_id: SpectrumQuestionIdType = Field(
        description="The unique identifier for the qualification", frozen=True
    )
    # In the API: desc
    question_name: str = Field(
        max_length=255,
        min_length=1,
        frozen=True,
        description="A short name for the question",
    )
    question_text: str = Field(
        max_length=1024,
        min_length=1,
        description="The text shown to respondents",
        frozen=False,
    )
    question_type: SpectrumQuestionType = Field(
        description="The type of question asked", frozen=True
    )
    # This comes from the API field "cat". It is not really documented. It
    #   looks to be a comma-separated str of "tags" or keywords associated
    #   with a question, but they are freeform and don't pertain to any sort
    #   of structured schema. This will be useful ChatGPT
    tags: Optional[str] = Field(default=None, frozen=True)
    options: Optional[List[SpectrumQuestionOption]] = Field(
        default=None, min_length=1, frozen=True
    )
    # This comes from the API. Of course there are more than what is documented.
    #   (1 = Core profiling question, 2 = Extended, 3 = Custom, 4 = ???)
    class_num: SpectrumQuestionClass = Field(frozen=True)
    # This comes from the API. It is when it was created in Spectrum's DB,
    #   not when we created it
    created: Optional[AwareDatetimeISO] = Field(default=None, frozen=True)

    source: Literal[Source.SPECTRUM] = Source.SPECTRUM

    @property
    def internal_id(self) -> str:
        return self.question_id

    @model_validator(mode="before")
    @classmethod
    def clean_text_qid_from_api(cls, data: Any):
        # Almost all questions have "variable names" in the question text.
        # Remove this e.g. 'Are you registered in any of the following US
        # political parties? %%1040%%' or 'My household earns approximately
        # $%%213%% per year'
        s = data["question_text"].strip()
        search_str = f"%%{data['question_id']}%%"
        if search_str in s:
            if s.endswith(search_str):
                s = s.replace(search_str, "").strip()
            else:
                s = s.replace(search_str, "___")
        # After we do this, there shouldn't be any others
        if "%%" in s:
            raise ValueError("question text has unknown variables")
        data["question_text"] = s
        return data

    @field_validator("question_name", "question_text", "tags", mode="after")
    def remove_nbsp(cls, s: Optional[str]):
        return string_utils.remove_nbsp(s)

    @model_validator(mode="before")
    @classmethod
    def crop_name_from_api(cls, data: Any):
        # Some of the names are ridiculously long. They aren't used for
        # anything to its safe to crop it
        data["question_name"] = data["question_name"].strip()[:255]
        return data

    @model_validator(mode="after")
    def check_type_options_agreement(self):
        # If type == "text_entry", options is None. Otherwise, must be set.
        if self.question_type == SpectrumQuestionType.TEXT_ENTRY:
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

    @field_validator("options")
    @classmethod
    def uniquify_options(cls, options: Optional[List[SpectrumQuestionOption]]):
        if options:
            # The API returns questions with identical option IDs multiple
            #   times. They seem to all be typo/corrections to the text, so
            #   it should be safe to just remove the duplicates. We have no
            #   way of knowing which is the intended text though.

            opt_d = {opt.id: opt for opt in options}
            options = list(opt_d.values())
            for n, opt in enumerate(options):
                opt.order = n
        return options

    @classmethod
    def from_api(
        cls, d: Dict[str, Any], country_iso: str, language_iso: str
    ) -> Optional["SpectrumQuestion"]:
        # To not pollute our logs, we know we are skipping any question that
        #   meets the following conditions:
        if not SpectrumQuestionType.from_api(d["type"]):
            return None
        if d["class"] not in {
            x.value for x in SpectrumQuestionClass.__members__.values()
        }:
            return None
        try:
            return cls._from_api(d, country_iso, language_iso)
        except Exception as e:
            logger.warning(f"Unable to parse question: {d}. {e}")
            return None

    @classmethod
    def _from_api(cls, d: Dict[str, Any], country_iso: str, language_iso: str) -> Self:
        options = None
        if d.get("condition_codes"):
            # Sometimes they use the key "name" instead of "text" ... ?
            key = "text" if "text" in d["condition_codes"][0] else "name"
            # Sometimes options are blank
            d["condition_codes"] = [x for x in d["condition_codes"] if x[key]]
            options = [
                SpectrumQuestionOption(id=r["id"], text=r[key], order=order)
                for order, r in enumerate(
                    sorted(d["condition_codes"], key=lambda x: int(x["id"]))
                )
            ]

        created = (
            datetime.utcfromtimestamp(d["crtd_on"] / 1000).replace(tzinfo=timezone.utc)
            if d.get("crtd_on")
            else None
        )
        return cls(
            question_id=str(d["qualification_code"]),
            question_text=d["text"],
            question_type=SpectrumQuestionType.from_api(d["type"]),
            question_name=d["desc"],
            tags=d["cat"],
            class_num=d["class"],
            created=created,
            country_iso=country_iso,
            language_iso=language_iso,
            options=options,
        )

    @classmethod
    def from_db(cls, d: Dict[str, Any]) -> Self:
        options = None
        if d["options"]:
            options = [
                SpectrumQuestionOption(id=r["id"], text=r["text"], order=r["order"])
                for r in d["options"]
            ]
        d["created"] = (
            d["created"].replace(tzinfo=timezone.utc) if d["created"] else None
        )

        return cls(
            question_id=d["question_id"],
            question_name=d["question_name"],
            question_text=d["question_text"],
            question_type=d["question_type"],
            country_iso=d["country_iso"],
            language_iso=d["language_iso"],
            options=options,
            is_live=d["is_live"],
            category_id=(
                UUID(d.get("category_id")).hex if d.get("category_id") else None
            ),
            tags=d["tags"],
            class_num=d["class_num"],
            created=d["created"],
        )

    def to_mysql(self) -> Dict[str, Any]:
        d = self.model_dump(mode="json", by_alias=True)
        d["options"] = json.dumps(d["options"])
        if self.created:
            d["created"] = self.created.replace(tzinfo=None)
        return d

    def to_upk_question(self) -> "UpkQuestion":
        from generalresearch.models.thl.profiling.upk_question import (
            UpkQuestion,
            UpkQuestionChoice,
            UpkQuestionSelectorMC,
            UpkQuestionSelectorTE,
            UpkQuestionType,
        )

        upk_type_selector_map = {
            SpectrumQuestionType.SINGLE_SELECT: (
                UpkQuestionType.MULTIPLE_CHOICE,
                UpkQuestionSelectorMC.SINGLE_ANSWER,
            ),
            SpectrumQuestionType.MULTI_SELECT: (
                UpkQuestionType.MULTIPLE_CHOICE,
                UpkQuestionSelectorMC.MULTIPLE_ANSWER,
            ),
            SpectrumQuestionType.TEXT_ENTRY: (
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
