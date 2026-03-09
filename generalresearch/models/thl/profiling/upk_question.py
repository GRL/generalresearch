from __future__ import annotations

import hashlib
import json
import re
from enum import Enum
from functools import cached_property
from typing import Any, Dict, List, Literal, Optional, Set, Tuple, Union

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    NonNegativeInt,
    PositiveInt,
    field_validator,
    model_validator,
)
from typing_extensions import Annotated

from generalresearch.models import Source
from generalresearch.models.custom_types import UUIDStr
from generalresearch.models.thl.category import Category


class UPKImportance(BaseModel):
    task_count: Optional[int] = Field(
        ge=0,
        default=None,
        examples=[47],
        description="The number of live Tasks that use this UPK Question",
    )

    task_score: Optional[float] = Field(
        ge=0,
        default=None,
        examples=[0.11175522477414712],
        description="GRL's internal ranked score for the UPK Question",
    )

    marketplace_task_count: Optional[Dict[Source, NonNegativeInt]] = Field(
        default=None,
        examples=[{Source.DYNATA: 23, Source.SPECTRUM: 24}],
        description="The number of live Tasks that use this UPK Question per marketplace",
    )


class PatternValidation(BaseModel):
    model_config = ConfigDict(frozen=True)

    message: str = Field(description="Message to display if validation fails")

    pattern: str = Field(
        description="Regex string to validate. min_length and max_length are "
        "checked separately, even if they are part of the regex."
    )


class UpkQuestionChoice(BaseModel):
    model_config = ConfigDict(frozen=False, populate_by_name=True)

    # The choice ID uses the marketplace's code. This needs to be >32 for pollfish
    id: str = Field(
        min_length=1,
        max_length=64,
        pattern=r"^[\w\s\.\-]+$",
        description="The unique identifier for a response to a qualification",
        serialization_alias="choice_id",
        validation_alias="choice_id",
        frozen=True,
    )

    text: str = Field(
        min_length=1,
        description="The response text shown to respondents",
        alias="choice_text",
        frozen=True,
    )

    order: NonNegativeInt = Field()

    # Allows you to group answer choices together (used for display or extra logic)
    group: Optional[int] = Field(default=None)

    exclusive: bool = Field(
        default=False,
        description="If answer is exclusive, it can be the only option selected",
    )

    importance: Optional[UPKImportance] = Field(default=None)

    def __hash__(self):
        # We don't know the question ID!! Unique within a question only!
        return hash(self.id)


class UpkQuestionChoiceOut(UpkQuestionChoice):
    pass
    # importance: Optional[UPKImportance] = Field(default=None, exclude=True)


class UpkQuestionType(str, Enum):
    # The question has options that the user must select from. A MC question
    #   can be e.g. Selector.SINGLE_ANSWER or Selector.MULTIPLE_ANSWER to
    #   indicate only 1 or more than 1 option can be selected respectively.
    MULTIPLE_CHOICE = "MC"
    # The question has no options; the user must enter text.
    TEXT_ENTRY = "TE"
    # The question presents a slider of possible values, typically a numerical range.
    SLIDER = "SLIDER"
    # The question has no UI elements.
    HIDDEN = "HIDDEN"


class UpkQuestionSelector(str, Enum):
    pass


class UpkQuestionSelectorMC(UpkQuestionSelector):
    SINGLE_ANSWER = "SA"
    MULTIPLE_ANSWER = "MA"
    DROPDOWN_LIST = "DL"
    SELECT_BOX = "SB"
    MULTI_SELECT_BOX = "MSB"


class UpkQuestionSelectorTE(UpkQuestionSelector):
    SINGLE_LINE = "SL"
    MULTI_LINE = "ML"
    ESSAY_TEXT_BOX = "ETB"


class UpkQuestionSelectorSLIDER(UpkQuestionSelector):
    HORIZONTAL_SLIDER = "HSLIDER"
    VERTICAL_SLIDER = "VSLIDER"


class UpkQuestionSelectorHIDDEN(UpkQuestionSelector):
    HIDDEN = "HIDDEN"


class UpkQuestionConfigurationMC(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    # --- UpkQuestionType.MULTIPLE_CHOICE Options ---
    # A multiple choice question with MA may allow a limited number of options
    #   to be selected.
    # If the selector is SA, this should be set to 1. If the selector is MA,
    #   then this must be <= len(choices).
    type: Literal[UpkQuestionType.MULTIPLE_CHOICE] = Field(
        exclude=True, default=UpkQuestionType.MULTIPLE_CHOICE
    )

    max_select: Optional[int] = Field(gt=0, default=None)


class UpkQuestionConfigurationTE(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    # --- UpkQuestionType.TEXT_ENTRY Options ---
    type: Literal[UpkQuestionType.TEXT_ENTRY] = Field(
        exclude=True, default=UpkQuestionType.TEXT_ENTRY
    )

    # Sets input form attribute; not the same as regex validation
    max_length: Optional[PositiveInt] = Field(
        default=None,
        description="Maximum str length of any input. Meant as an easy, non"
        "regex based check.",
    )

    # The text input box must contain this number of chars before submission
    # is allowed
    min_length: Optional[PositiveInt] = Field(
        default=None,
        description="Minimum str length of any input. Meant as an easy, non"
        "regex based check.",
    )

    @model_validator(mode="after")
    def check_options_agreement(self):
        if self.max_length is not None and self.min_length is not None:
            assert (
                self.min_length <= self.max_length
            ), "max_length must be >= min_length"
        return self


class UpkQuestionConfigurationSLIDER(BaseModel):
    model_config = ConfigDict(frozen=True)

    # --- UpkQuestionType.SLIDER Options ---
    type: Literal[UpkQuestionType.SLIDER] = Field(
        exclude=True, default=UpkQuestionType.SLIDER
    )

    # TODO: constraints. we don't have any of these so not wasting time on this
    slider_min: Optional[float] = Field(default=None)
    slider_max: Optional[float] = Field(default=None)
    slider_start: Optional[float] = Field(default=None)
    slider_step: Optional[float] = Field(default=None)


class UpkQuestionValidation(BaseModel):
    model_config = ConfigDict(frozen=True)

    # --- UpkQuestionType.TEXT_ENTRY Options ---
    patterns: Optional[List[PatternValidation]] = Field(min_length=1)


SelectorType = Union[
    UpkQuestionSelectorMC,
    UpkQuestionSelectorTE,
    UpkQuestionSelectorSLIDER,
    UpkQuestionSelectorHIDDEN,
]
Configuration = Annotated[
    Union[
        UpkQuestionConfigurationMC,
        UpkQuestionConfigurationTE,
        UpkQuestionConfigurationSLIDER,
    ],
    Field(discriminator="type"),
]

example_upk_question = {
    "choices": [
        {
            "order": 0,
            "choice_id": "1",
            "exclusive": False,
            "choice_text": "Yes",
        },
        {"order": 1, "choice_id": "2", "exclusive": False, "choice_text": "No"},
    ],
    "selector": "SA",
    "task_count": 49,
    "task_score": 3.3401743283265684,
    "marketplace_task_count": {
        "d": 9,
        "w": 20,
        "s": 20,
    },
    "country_iso": "us",
    "question_id": "fb20fd4773304500b39c4f6de0012a5a",
    "language_iso": "eng",
    "question_text": "Are you registered to vote at your present address, or not?",
    "question_type": "MC",
    "importance": UPKImportance(
        task_count=49,
        task_score=3.3401743283265684,
        marketplace_task_count={
            Source.DYNATA: 9,
            Source.WXET: 20,
            Source.SPECTRUM: 20,
        },
    ).model_dump(mode="json"),
    "categories": [
        Category(
            uuid="87b6d819f3ca4815bf1f135b1e829cc6",
            adwords_vertical_id="396",
            label="Politics",
            path="/News/Politics",
            parent_uuid="f66dddba61424ce5be2a38731450a0e1",
        ).model_dump()
    ],
}


class UpkQuestion(BaseModel):
    model_config = ConfigDict(
        populate_by_name=True,
        json_schema_extra={"example": example_upk_question},
        # Don't set this to True. Breaks in model validator (infinite recursion)
        validate_assignment=False,
    )

    # The id is globally unique
    id: Optional[UUIDStr] = Field(default=None, alias="question_id")

    # The format is "{Source}:{question_id}" where Source is 1 or 2 chars, and
    # question_id is the marketplace's ID for this question.
    ext_question_id: Optional[str] = Field(
        default=None,
        description="what marketplace question this question links to",
        pattern=r"^[a-z]{1,2}\:.*",
    )

    type: UpkQuestionType = Field(alias="question_type")

    # ISO 3166-1 alpha-2 (two-letter codes, lowercase)
    country_iso: str = Field(max_length=2, min_length=2, pattern=r"^[a-z]{2}$")
    # 3-char ISO 639-2/B, lowercase
    language_iso: str = Field(max_length=3, min_length=3, pattern=r"^[a-z]{3}$")

    text: str = Field(
        min_length=1,
        description="The text shown to respondents",
        alias="question_text",
    )

    # Don't set a min_length=1 here. We'll allow this to be created, but it
    #   won't be askable with empty choices.
    choices: Optional[List[UpkQuestionChoice]] = Field(default=None)
    selector: SelectorType = Field()
    configuration: Optional[Configuration] = Field(default=None)
    validation: Optional[UpkQuestionValidation] = Field(default=None)
    importance: Optional[UPKImportance] = Field(default=None)

    categories: List[Category] = Field(
        default_factory=list,
        description="Categories associated with this question",
    )

    explanation_template: Optional[str] = Field(
        description="Human-readable template for explaining how a user's answer to this question affects eligibility",
        examples=[
            "The company that administers your employer's retirement plan is {answer}."
        ],
        default=None,
    )
    explanation_fragment_template: Optional[str] = Field(
        description="A very short, natural-language explanation fragment that can be combined with others into a single sentence",
        examples=["whose employer's retirement plan is {answer}"],
        default=None,
    )

    @property
    def _key(self):
        if self.id is None:
            raise ValueError("must set .id first")
        return self.id, self.country_iso, self.language_iso

    @property
    def locale(self) -> str:
        return self.country_iso + "_" + self.language_iso

    @property
    def source(self) -> Optional[Source]:
        if self.ext_question_id:
            return Source(self.ext_question_id.split(":", 1)[0])

    @cached_property
    def choices_text_lookup(self):
        if self.choices is None:
            return None
        return {c.id: c.text for c in self.choices}

    @model_validator(mode="before")
    @classmethod
    def check_configuration_type(cls, data: Dict[str, Any]) -> Dict[str, Any]:
        # The model knows what the type of Configuration to grab depending on
        # the key 'type' which it expects inside the configuration object.
        # Here, we grab the type from the top-level model instead.
        config = data.get("configuration")
        if isinstance(config, dict) and config.get("type") is None:
            data.setdefault("configuration", {})
            data["configuration"]["type"] = data.get("type") or data.get(
                "question_type"
            )
        return data

    @model_validator(mode="after")
    def check_type_options_agreement(self):
        # If type == "text_entry", options is None. Otherwise, must be set.
        if self.type in {UpkQuestionType.TEXT_ENTRY, UpkQuestionType.HIDDEN}:
            if isinstance(self.choices, list) and len(self.choices) == 0:
                self.choices = None
            assert (
                self.choices is None
            ), f"No `choices` are allowed for type `{self.type}`"
        else:
            assert self.choices is not None, f"`choices` must be set"
        return self

    @model_validator(mode="after")
    def set_default_selector(self):
        if self.selector is None:
            if self.type == UpkQuestionType.MULTIPLE_CHOICE:
                self.selector = UpkQuestionSelectorMC.SINGLE_ANSWER
            elif self.type == UpkQuestionType.TEXT_ENTRY:
                self.selector = UpkQuestionSelectorTE.SINGLE_LINE
            elif self.type == UpkQuestionType.SLIDER:
                self.selector = UpkQuestionSelectorSLIDER.HORIZONTAL_SLIDER
            else:
                self.selector = UpkQuestionSelectorHIDDEN.HIDDEN
        return self

    @model_validator(mode="after")
    def check_type_selector_agreement(self):
        if self.type == UpkQuestionType.MULTIPLE_CHOICE:
            assert isinstance(
                self.selector, UpkQuestionSelectorMC
            ), f"type `{self.type}` must have selector UpkQuestionSelectorMC"
        if self.type == UpkQuestionType.TEXT_ENTRY:
            assert isinstance(
                self.selector, UpkQuestionSelectorTE
            ), f"type `{self.type}` must have selector UpkQuestionSelectorTE"
        if self.type == UpkQuestionType.SLIDER:
            assert isinstance(
                self.selector, UpkQuestionSelectorTE
            ), f"type `{self.type}` must have selector UpkQuestionSelectorTE"
        if self.type == UpkQuestionType.HIDDEN:
            assert isinstance(
                self.selector, UpkQuestionSelectorHIDDEN
            ), f"type `{self.type}` must have selector UpkQuestionSelectorTE"
        return self

    @model_validator(mode="after")
    def check_type_validator_agreement(self):
        if self.validation and self.validation.patterns is not None:
            assert (
                self.type == UpkQuestionType.TEXT_ENTRY
            ), "validation.patterns is only allowed on Text Entry Questions"
        return self

    @model_validator(mode="after")
    def check_config_choices(self):
        if self.type == UpkQuestionType.MULTIPLE_CHOICE and self.configuration:
            if self.selector in {
                UpkQuestionSelectorMC.SINGLE_ANSWER,
                UpkQuestionSelectorMC.DROPDOWN_LIST,
                UpkQuestionSelectorMC.SELECT_BOX,
            }:
                assert (
                    self.configuration.max_select == 1
                ), f"configuration.max_select must be 1 if the selector is {self.selector.value}"
            else:
                assert self.configuration.max_select <= len(
                    self.choices
                ), "configuration.max_select must be >= len(choices)"
        return self

    @field_validator("choices")
    @classmethod
    def order_choices(cls, choices: List):
        if choices:
            choices.sort(key=lambda x: x.order)
        return choices

    @field_validator("choices")
    @classmethod
    def validate_choices(
        cls, choices: Optional[List[UpkQuestionChoice]]
    ) -> Optional[List[UpkQuestionChoice]]:
        if choices:
            ids = {x.id for x in choices}
            assert len(ids) == len(choices), "choices.id must be unique"

            orders = {x.order for x in choices}
            assert len(orders) == len(choices), "choices.order must be unique"

        return choices

    @field_validator("explanation_template", "explanation_fragment_template")
    @classmethod
    def validate_explanation_template(cls, v):
        if v is None:
            return v
        if "{answer}" not in v:
            raise ValueError("field must include '{answer}'")
        return v

    @property
    def is_askable(self) -> bool:
        if len(self.text) < 5:
            # It should have some text that is question-like. 5 is chosen
            # because it is the shortest known "real" question (spectrum
            # gender = "I'm a")
            return False

        if len(self.text) > 1024:
            # This usually means it is some sort of ridiculous terms &
            # conditions they want the user to agree to, which we don't want
            # to support
            return False

        # Almost nothing has >1k options, besides location stuff (cities,
        # etc.) which should get harmonized. When presenting them, we'll
        # filter down options to at most 50.
        if self.choices and (len(self.choices) <= 1 or len(self.choices) > 1000):
            return False

        return True

    @property
    def md5sum(self):
        # Used to determine if a question has changed
        d = {
            "question_text": self.text,
            "question_type": self.type.value,
            "selector": self.selector.value,
            "choices": (
                [{"choice_id": x.id, "choice_text": x.text} for x in self.choices]
                if self.choices
                else []
            ),
        }
        return hashlib.md5(json.dumps(d, sort_keys=True).encode("utf-8")).hexdigest()

    def to_api_format(self):
        d = self.model_dump(mode="json", exclude_none=True, by_alias=True)
        # This doesn't currently get included, I think it could but not sure
        # if it would break anything
        d.pop("ext_question_id", None)
        # API expects task_score and task_count on the top-level
        d.update(d.pop("importance", {}))
        return d

    def validate_question_answer(self, answer: Tuple[str, ...]) -> Tuple[bool, str]:
        """
        Returns (is_valid, error_message).
        """
        try:
            self._validate_question_answer(answer)
        except AssertionError as e:
            return False, str(e)
        else:
            return True, ""

    def _validate_question_answer(self, answer: Tuple[str, ...]) -> None:
        """
        If the question is MC, validate:
            - validate selector SA vs MA (1 selected vs >1 selected)
            - the answers match actual codes in the choices
            - validate configuration.max_select
            - validate choices.exclusive
        If the question is TE, validate that:
            - configuration.max_length
            - validation.patterns
        Throws AssertionError if the answer is invalid, otherwise returns None
        """
        answer = tuple(answer)
        # There should never be multiple of the same value
        assert sorted(set(answer)) == sorted(
            answer
        ), "Multiple of the same answer submitted"
        if self.type == UpkQuestionType.MULTIPLE_CHOICE:
            assert len(answer) >= 1, "MC question with no selected answers"
            choice_codes = set(x.id for x in self.choices)
            if self.selector == UpkQuestionSelectorMC.SINGLE_ANSWER:
                assert (
                    len(answer) == 1
                ), "Single Answer MC question with >1 selected answers"
            elif self.selector == UpkQuestionSelectorMC.MULTIPLE_ANSWER:
                assert len(answer) <= len(
                    self.choices
                ), "More options selected than allowed"
            assert all(
                ans in choice_codes for ans in answer
            ), "Invalid Options Selected"
            max_select = (
                self.configuration.max_select
                if self.configuration
                else 0 or len(self.choices)
            )
            assert len(answer) <= max_select, "More options selected than allowed"
            exclusive_choice = next((x for x in self.choices if x.exclusive), None)
            if exclusive_choice:
                exclusive_choice_id = exclusive_choice.id
                assert (
                    answer == (exclusive_choice_id,)
                    or exclusive_choice_id not in answer
                ), "Invalid exclusive selection"
        elif self.type == UpkQuestionType.TEXT_ENTRY:
            assert len(answer) == 1, "Only one answer allowed"
            answer = answer[0]
            assert len(answer) > 0, "Must provide answer"
            max_length = (
                self.configuration.max_length if self.configuration else 0 or 100000
            )
            assert len(answer) <= max_length, "Answer longer than allowed"
            if self.validation and self.validation.patterns:
                for pattern in self.validation.patterns:
                    assert re.search(pattern.pattern, answer), pattern.message
        elif self.type == UpkQuestionType.HIDDEN:
            pass


class UpkQuestionOut(UpkQuestion):
    choices: Optional[List[UpkQuestionChoiceOut]] = Field(default=None)
    # Return both importance top-level model and extracted keys for now.
    # Eventually deprecate one way.
    task_count: Optional[int] = Field(
        ge=0,
        default=None,
        examples=[47],
        description="The number of live Tasks that use this UPK Question",
    )

    task_score: Optional[float] = Field(
        ge=0,
        default=None,
        examples=[0.11175522477414712],
        description="GRL's internal ranked score for the UPK Question",
    )

    marketplace_task_count: Optional[Dict[Source, NonNegativeInt]] = Field(
        default=None,
        examples=[{Source.DYNATA: 23, Source.SPECTRUM: 24}],
        description="The number of live Tasks that use this UPK Question per marketplace",
    )

    @model_validator(mode="after")
    def populate_from_importance(self):
        # When we return through the api, bring the importance keys to the top-level
        if self.importance:
            self.task_count = self.importance.task_count
            self.task_score = self.importance.task_score
            self.marketplace_task_count = self.importance.marketplace_task_count
        return self


def order_exclusive_options(q: UpkQuestion):
    """
    The idea is to call then when doing a MP -> UPK conversion, where the
    marketplace doesn't have the order specified.
    """
    from generalresearch.models.thl.profiling.other_option import (
        option_is_catch_all,
    )

    if q.choices:
        last_choices = [c for c in q.choices if option_is_catch_all(c)]
        for c in last_choices:
            q.choices.remove(c)
            q.choices.append(c)
            c.exclusive = True
        if last_choices:
            for idx, c in enumerate(q.choices):
                c.order = idx


def trim_options(q: UpkQuestion, max_options: int = 50) -> UpkQuestion:
    """Filter weighted MC/SC Options during Offerwall Requests or Refresh

    - Remove any of ZERO importance
    - ~50 option HARD limit, keep only the 50 highest scoring
    - In soft-pair, take up to requested, or 50
    - Implement N-1 to keep options that are a catch-all / exclusive.
    """
    from generalresearch.models.thl.profiling.other_option import (
        option_is_catch_all,
    )

    q = q.model_copy()
    if not q.choices:
        return q
    if q.ext_question_id.startswith("gr:") or q.ext_question_id.startswith("g:"):
        return q

    special_choices: Set[UpkQuestionChoice] = {
        c for c in q.choices if option_is_catch_all(c)
    }

    if q.choices[0].importance is None:
        # We're calculating UpkQuestionChoice important on (1) UpkQuestionChoice
        #   Creation and (2) every 60min, so this should always be set. However,
        #   if isn't for some reason, don't fail... just show a random set of
        #   50 UpkQuestionChoices. Sorry ¯\_(ツ)_/¯
        for c in q.choices:
            c.importance = UPKImportance(task_score=1, task_count=1)

    possible_choices = [
        c for c in q.choices if c.importance.task_count > 0 or c in special_choices
    ]
    if possible_choices:
        q.choices = possible_choices
    else:
        # We can't have a MC question with all choices filtered out.
        pass

    if len(q.choices) > max_options:
        choices = q.choices
        # If there is a Special Choice (eg: "none of the above", "decline to
        #   answer", "prefer not to say", etc) always include it at the bottom.
        idx = max_options - len(special_choices)
        choices = set(
            sorted(choices, key=lambda x: x.importance.task_score, reverse=True)[:idx]
        )
        choices.update(special_choices)
        q.choices = sorted(choices, key=lambda x: x.order)

    return q


UpkQuestionOut.model_rebuild()
