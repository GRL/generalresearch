from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    NonNegativeFloat,
    NonNegativeInt,
    PositiveInt,
    computed_field,
    field_validator,
    model_validator,
)
from typing_extensions import Annotated

from generalresearch.managers.thl.buyer import Buyer
from generalresearch.models import Source
from generalresearch.models.custom_types import (
    AwareDatetimeISO,
    CountryISOLike,
    EnumNameSerializer,
    PropertyCode,
    SurveyKey,
)
from generalresearch.models.thl.category import Category
from generalresearch.models.thl.definitions import Status, StatusCode1
from generalresearch.models.thl.pagination import Page


class SurveyCategoryModel(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    category: Category = Field()
    strength: Optional[float] = Field(default=None)


class SurveyEligibilityDefinition(BaseModel):
    """
    Survey-level declaration of which questions
    may contribute to eligibility.

    This does NOT encode rules or qualifying values.
    """

    # References a marketplace-specific question
    property_codes: Tuple[PropertyCode, ...] = Field(default_factory=tuple)

    @model_validator(mode="after")
    def sort_question_ids(self):
        self.property_codes = tuple(sorted(self.property_codes))
        return self


class Survey(BaseModel):
    model_config = ConfigDict(validate_assignment=True)

    id: Optional[PositiveInt] = Field(default=None, exclude=True)

    source: Source = Field()
    survey_id: str = Field(min_length=1, max_length=32, examples=["127492892"])

    buyer_id: Optional[int] = Field(
        default=None, exclude=True, description="This is the DB's fk id"
    )
    # ---v So the fk id can be looked up from the code
    buyer_code: Optional[str] = Field(
        min_length=1, max_length=128, default=None, examples=["124"]
    )

    created_at: AwareDatetimeISO = Field(
        default_factory=lambda: datetime.now(tz=timezone.utc)
    )
    updated_at: AwareDatetimeISO = Field(
        default_factory=lambda: datetime.now(tz=timezone.utc)
    )

    is_live: bool = Field(default=True)
    is_recontact: bool = Field(default=False)

    categories: List[SurveyCategoryModel] = Field(default_factory=list)

    eligibility_criteria: Optional[SurveyEligibilityDefinition] = Field(default=None)

    @property
    def natural_key(self) -> SurveyKey:
        return f"{self.source.value}:{self.survey_id}"

    @property
    def buyer(self):
        assert self.buyer_code is not None
        return Buyer(source=self.source, code=self.buyer_code)

    @property
    def buyer_natural_key(self) -> str:
        return self.buyer.natural_key

    @model_validator(mode="after")
    def category_strengths(self):
        if any(s.strength is not None for s in self.categories):
            assert all(
                s.strength is not None for s in self.categories
            ), "If any category strength is not None, all should be set"
            assert (
                abs(sum(s.strength for s in self.categories) - 1) <= 0.01
            ), "Strengths should some to 1"
        return self

    def model_dump_sql(self):
        d = self.model_dump(mode="json", exclude={"categories"})
        d["buyer_id"] = self.buyer_id
        d["eligibility_criteria"] = None
        if self.eligibility_criteria is not None:
            d["eligibility_criteria"] = self.eligibility_criteria.model_dump_json()
        return d


class SurveyStat(BaseModel):
    model_config = ConfigDict(validate_assignment=True)

    id: Optional[PositiveInt] = Field(exclude=True, default=None)

    # ---- Identity ----
    survey_id: Optional[PositiveInt] = Field(
        default=None,
        exclude=True,
        description="This is the pk of the Survey object in the db",
    )
    quota_id: str = Field(
        default="__all__",
        max_length=32,
        description="The marketplace's internal quota id",
    )
    country_iso: CountryISOLike = Field()
    version: int = Field(ge=0)

    # --- For lookup / de-normalization, to avoid potentially costly
    # joins on marketplace_survey table ---
    survey_source: Optional[Source] = Field(default=None, exclude=True)
    survey_survey_id: Optional[str] = Field(
        default=None, exclude=True, min_length=1, max_length=32
    )
    survey_is_live: bool = Field(default=True, exclude=True)

    # ---- Pricing / cutoffs ----
    cpi: Decimal = Field(decimal_places=5, lt=1000, ge=0)
    complete_too_fast_cutoff: PositiveInt = Field(description="in seconds")

    # ---- Distributions ----

    prescreen_conv_alpha: float = Field(..., ge=0)
    prescreen_conv_beta: float = Field(..., ge=0)

    conv_alpha: float = Field(..., ge=0)
    conv_beta: float = Field(..., ge=0)

    dropoff_alpha: float = Field(..., ge=0)
    dropoff_beta: float = Field(..., ge=0)

    completion_time_mu: float = Field(...)
    completion_time_sigma: float = Field(..., gt=0)

    # ---- Eligibility (probabilistic) ----

    mobile_eligible_alpha: float = Field(..., ge=0)
    mobile_eligible_beta: float = Field(..., ge=0)

    desktop_eligible_alpha: float = Field(..., ge=0)
    desktop_eligible_beta: float = Field(..., ge=0)

    tablet_eligible_alpha: float = Field(..., ge=0)
    tablet_eligible_beta: float = Field(..., ge=0)

    # ---- Risk / quality ----

    long_fail_rate: float = Field(..., ge=0, le=1)
    user_report_coeff: float = Field(..., ge=0, le=1)
    recon_likelihood: float = Field(..., ge=0, le=1)

    # ---- Scoring ----

    score_x0: float = Field(...)
    score_x1: float = Field(...)
    score: float = Field(...)

    # ---- Metadata ----

    updated_at: AwareDatetimeISO = Field(
        default_factory=lambda: datetime.now(tz=timezone.utc)
    )

    @property
    def natural_key(self) -> str:
        assert self.survey_source is not None and self.survey_survey_id is not None
        return f"{self.survey_source.value}:{self.survey_survey_id}:{self.quota_id}:{self.country_iso}:{self.version}"

    @property
    def survey_natural_key(self) -> str:
        # same as Survey.natural_key
        return f"{self.survey_source.value}:{self.survey_survey_id}"

    @property
    def unique_key(self) -> Tuple[int, Optional[str], str, int]:
        return self.survey_id, self.quota_id, self.country_iso, self.version

    def model_dump_sql(self):
        d = self.model_dump(mode="json")
        d["survey_id"] = self.survey_id
        d["survey_is_live"] = self.survey_is_live
        d["survey_survey_id"] = self.survey_survey_id
        d["survey_source"] = self.survey_source
        return d


class TaskActivity(BaseModel):
    model_config = ConfigDict(validate_assignment=True)

    source: Source = Field()
    survey_id: str = Field(min_length=1, max_length=32, examples=["127492892"])

    status_counts: Dict[Status, NonNegativeInt] = Field(default_factory=dict)
    status_code_1_counts: Dict[StatusCode1, NonNegativeInt] = Field(
        default_factory=dict
    )
    in_progress_count: NonNegativeInt = Field(
        default=0,
        description="Count of entrances that have no Status and were entered within the past 90 minutes",
    )
    last_complete: Optional[AwareDatetimeISO] = Field(default=None)
    last_entrance: Optional[AwareDatetimeISO] = Field(default=None)

    @computed_field
    @property
    def total_finished(self) -> int:
        return sum(self.status_counts.values())

    @computed_field
    @property
    def total_entrances(self) -> int:
        return self.total_finished + self.in_progress_count

    # ---- percentages ----
    @computed_field
    @property
    def status_percentages(self) -> Dict[Status, NonNegativeFloat]:
        total = self.total_finished
        if total == 0:
            return {}
        return {k: round(v / total, 3) for k, v in self.status_counts.items()}

    @computed_field
    @property
    def status_code_1_percentages(self) -> Dict[StatusCode1, NonNegativeFloat]:
        total = sum(self.status_code_1_counts.values())
        if total == 0:
            return {}
        return {k: round(v / total, 3) for k, v in self.status_code_1_counts.items()}


class TaskActivityPublic(BaseModel):
    source: Optional[Source] = Field(exclude=True, default=None)
    survey_id: Optional[str] = Field(
        min_length=1, max_length=32, examples=["127492892"], exclude=True, default=None
    )

    status_percentages: Dict[Status, NonNegativeFloat] = Field(default_factory=dict)
    status_code_1_percentages: Dict[
        Annotated[StatusCode1, EnumNameSerializer], NonNegativeFloat
    ] = Field(default_factory=dict)

    last_complete: Optional[AwareDatetimeISO] = Field(default=None)
    last_entrance: Optional[AwareDatetimeISO] = Field(default=None)

    @field_validator("status_code_1_percentages", mode="before")
    def transform_enum_name_pct(cls, value: Dict[str, Any]) -> Dict[str, Any]:
        # If we are serializing+deserializing this model (i.e. when we cache
        # it), this fails because we've replaced the enum value with the
        # name. Put it back here ...
        return {
            StatusCode1[k] if isinstance(k, str) else k: v for k, v in value.items()
        }

    @property
    def natural_key(self) -> SurveyKey:
        return f"{self.source.value}:{self.survey_id}"


class TaskActivityPrivate(TaskActivityPublic):
    status_counts: Dict[Status, int] = Field(default_factory=dict)
    status_code_1_counts: Dict[Annotated[StatusCode1, EnumNameSerializer], int] = Field(
        default_factory=dict
    )
    in_progress_count: NonNegativeInt = Field(
        description="Count of entrances that have no Status and were entered within the past 90 minutes",
        default=0,
    )

    @field_validator("status_code_1_counts", mode="before")
    def transform_enum_name_cnt(cls, value: Dict[str, Any]) -> Dict[str, Any]:
        # If we are serializing+deserializing this model (i.e. when we cache
        # it), this fails because we've replaced the enum value with the
        # name. Put it back here ...
        return {
            StatusCode1[k] if isinstance(k, str) else k: v for k, v in value.items()
        }


class TaskWithDetail(BaseModel):
    """For API Responses"""

    task: Survey = Field()
    stats: List[SurveyStat] = Field(default_factory=list)
    activity_global: Optional[TaskActivityPublic] = Field(default=None)
    activity_product: Optional[TaskActivityPrivate] = Field(default=None)


class TasksWithDetail(Page):
    """For API Responses"""

    tasks: List[TaskWithDetail] = Field(default_factory=list)
