import statistics
from datetime import timedelta
from decimal import Decimal
from string import Formatter
from typing import Optional, List, Any, Set, Dict, Tuple
from uuid import uuid4

import numpy as np
import pandas as pd
from pydantic import (
    BaseModel,
    Field,
    NonNegativeFloat,
    NonNegativeInt,
    ConfigDict,
    field_validator,
    model_validator,
)
from typing_extensions import Self, Annotated

from generalresearch.models import Source
from generalresearch.models.custom_types import UUIDStr, HttpsUrl
from generalresearch.models.legacy.bucket import (
    Bucket as LegacyBucket,
    Eligibility,
    CategoryAssociation,
    DurationSummary,
    PayoutSummary,
    PayoutSummaryDecimal,
    SurveyEligibilityCriterion,
)
from generalresearch.models.legacy.definitions import OfferwallReason
from generalresearch.models.thl.locales import CountryISO
from generalresearch.models.thl.offerwall import (
    OfferWallType,
    OfferWallTypeClass,
    OFFERWALL_TYPE_CLASS,
)
from generalresearch.models.thl.offerwall.bucket import (
    generate_offerwall_entry_url,
)
from generalresearch.models.thl.profiling.upk_question import UpkQuestion
from generalresearch.models.thl.soft_pair import SoftPairResultType
from generalresearch.models.thl.user import User


class MergeTableFeatures(BaseModel):
    """
    This is just a pydantic representation of the survey stats features from
    a row from the merge table. It isn't meant to be used by itself.
    """

    model_config = ConfigDict(allow_inf_nan=False, populate_by_name=True)

    PRESCREEN_CONVERSION_ALPHA: float = Field(
        description="Alpha parameter from a Beta distribution",
        alias="PRESCREEN_CONVERSION.alpha",
        gt=0,
        default=1,
    )
    PRESCREEN_CONVERSION_BETA: float = Field(
        description="Beta parameter from a Beta distribution",
        alias="PRESCREEN_CONVERSION.beta",
        ge=0,
        default=0,
    )
    PRESCREEN_CONVERSION: float = Field(
        description="Penalized mean value for the task's prescreen conversion. The penalized mean is the 20th "
        "percentile"
        "of the inverse cumulative distribution.",
        ge=0,
        le=1,
        default=1,
    )

    CONVERSION_ALPHA: float = Field(
        description="Alpha parameter from a Beta distribution",
        alias="CONVERSION.alpha",
        gt=0,
    )
    CONVERSION_BETA: float = Field(
        description="Beta parameter from a Beta distribution",
        alias="CONVERSION.beta",
        gt=0,
    )
    CONVERSION: float = Field(
        description="Penalized mean value for the task's conversion. The penalized mean is the 20th percentile"
        "of the inverse cumulative distribution.",
        ge=0,
        le=1,
    )

    # Normal distribution, so mu is real number, but this represents the completion time, so it
    #   has to be positive. We can restrict it more in that me are never going to predict
    #    time longer than ~~ 2 hours (np.log(120*60)) or <= 0 sec (np.log(1) = 0)
    COMPLETION_TIME_MU: float = Field(
        description="Mu parameter from a Normal distribution",
        alias="COMPLETION_TIME.mu",
        gt=1,
        le=10,
    )
    COMPLETION_TIME_SIGMA: float = Field(
        description="Sigma parameter from a Normal distribution",
        alias="COMPLETION_TIME.sigma",
        gt=0,
        lt=10,
    )
    COMPLETION_TIME_LOG: float = Field(
        description="Penalized mean value for the task's log-transformed completion time. The penalized "
        "mean is the 80th percentile of the inverse cumulative distribution.",
        ge=0,
        le=10,
    )
    COMPLETION_TIME: float = Field(
        description="Exponential of the COMPLETION_TIME_LOG. This is in seconds.",
        gt=0,
        le=120 * 60,
    )
    # Note: We generally also will have a predicted_loi or something with is just the inverse-log
    #   of COMPLETION_TIME, so that we can report it in seconds.

    DROPOFF_RATE_ALPHA: float = Field(
        description="Alpha parameter from a Beta distribution",
        alias="DROPOFF_RATE.alpha",
        gt=0,
    )
    DROPOFF_RATE_BETA: float = Field(
        description="Beta parameter from a Beta distribution",
        alias="DROPOFF_RATE.beta",
        gt=0,
    )
    DROPOFF_RATE: float = Field(
        description="Penalized mean value for the task's dropoff/abandonment rate. The penalized mean is the 60th "
        "percentile of the inverse cumulative distribution.",
        ge=0,
        le=1,
    )

    USER_REPORT_COEFF: float = Field(
        description="Lower values indicate the task, or similar tasks, have been reported by users.",
        ge=0,
        le=1,
        default=1,
    )

    LONG_FAIL: float = Field(
        description="Lower values indicate the task is likely to terminate later",
        ge=0,
        le=10,
        default=1,
    )

    RECON_LIKELIHOOD: float = Field(
        description="Likelihood the task will get reconciled.",
        ge=0,
        le=1,
        default=0,
    )

    IS_MOBILE_ELIGIBLE_ALPHA: float = Field(
        description="Alpha parameter from a Beta distribution",
        alias="IS_MOBILE_ELIGIBLE.alpha",
        gt=0,
        default=1,
    )
    IS_MOBILE_ELIGIBLE_BETA: float = Field(
        description="Beta parameter from a Beta distribution",
        alias="IS_MOBILE_ELIGIBLE.beta",
        ge=0,
        default=0,
    )
    IS_MOBILE_ELIGIBLE: float = Field(
        description="Penalized mean likelihood that the task can be completed on a mobile device",
        ge=0,
        le=1,
        default=1,
    )

    IS_DESKTOP_ELIGIBLE_ALPHA: float = Field(
        description="Alpha parameter from a Beta distribution",
        alias="IS_DESKTOP_ELIGIBLE.alpha",
        gt=0,
        default=1,
    )
    IS_DESKTOP_ELIGIBLE_BETA: float = Field(
        description="Beta parameter from a Beta distribution",
        alias="IS_DESKTOP_ELIGIBLE.beta",
        ge=0,
        default=0,
    )
    IS_DESKTOP_ELIGIBLE: float = Field(
        description="Penalized mean likelihood that the task can be completed on a Desktop",
        ge=0,
        le=1,
        default=1,
    )

    IS_TABLET_ELIGIBLE_ALPHA: float = Field(
        description="Alpha parameter from a Beta distribution",
        alias="IS_TABLET_ELIGIBLE.alpha",
        gt=0,
        default=1,
    )
    IS_TABLET_ELIGIBLE_BETA: float = Field(
        description="Beta parameter from a Beta distribution",
        alias="IS_TABLET_ELIGIBLE.beta",
        ge=0,
        default=0,
    )
    IS_TABLET_ELIGIBLE: float = Field(
        description="Penalized mean likelihood that the task can be completed on a Tablet",
        ge=0,
        le=1,
        default=1,
    )

    @model_validator(mode="before")
    @classmethod
    def set_completion_time_log(cls, data: Dict[str, Any]) -> Dict[str, Any]:
        # This isn't actually in the merge table
        data["COMPLETION_TIME_LOG"] = np.log(data["COMPLETION_TIME"])
        return data


class TaskResult(BaseModel):
    """
    This is a task, like as in ScoredTaskResult, but one that does not have
    associated scoring features. This is used only for GRS tasks
    """

    model_config = ConfigDict(allow_inf_nan=False, extra="forbid")

    # used in prioritize_with_stats_ids
    internal_id: str = Field(
        description="This is the survey's id within the marketplace"
    )
    source: Source = Field()
    country_iso: CountryISO = Field()
    buyer_id: Optional[str] = Field(min_length=1, max_length=32, default=None)

    # todo: GRS is allowed to be 0, but all the others can't. make a validator
    cpi: Decimal = Field(ge=0, le=100, decimal_places=5, max_digits=7)

    # Only GRS tasks will have this set. All other marketplaces will have
    #   to make a grpc call to generate this. This is a str b/c it is actually
    #   a format string.
    entry_link: Optional[str] = Field(
        default=None,
        examples=[
            "https://{domain}/session/?39057c8b=c4ed212601494f8c8836e38a55102d10&c184efc0=test&0bb50182={mid}"
        ],
    )

    @model_validator(mode="after")
    def validate_cpi(self) -> Self:
        if self.cpi == 0:
            assert self.source == Source.GRS, "cpi should be >0"
        return self

    @model_validator(mode="after")
    def validate_entry_link(self) -> Self:
        if self.source == Source.GRS:
            if self.entry_link:
                fmt_str = sorted(
                    [
                        fname
                        for _, fname, _, _ in Formatter().parse(self.entry_link)
                        if fname
                    ]
                )
                assert all(
                    x in {"domain", "mid"} for x in fmt_str
                ), "unrecognized format variable"
        else:
            assert self.entry_link is None, f"entry link not allowed for {self.source}"
        return self

    @property
    def external_id(self) -> str:
        return f"{self.source.value}:{self.internal_id}"

    @property
    def id_code(self) -> str:
        return self.external_id


class ScoredTaskResult(TaskResult, MergeTableFeatures):
    """
    This represents a single task, that a user is eligible for, and the task's
    associated scoring features.
    A list of these are used for further filtering and eventually in order to
    generate an offerwall
    """

    model_config = ConfigDict(allow_inf_nan=False, extra="ignore")

    cpi: Decimal = Field(gt=0, le=100, decimal_places=5, max_digits=7)
    payout: Decimal = Field(gt=0, le=100, decimal_places=5, max_digits=7)

    loi: float = Field(
        description="Same as COMPLETION_TIME, but using the 60th percentile. Also in seconds."
        "This is generally used within offerwall creation as a more accurate prediction.",
        gt=0,
        le=120 * 60,
    )

    # range is 0<->Inf (exclusive), but generally between 0 and single digits
    score: NonNegativeFloat = Field(
        description="This is the score as outputted by the scoring function",
        default=0,
    )

    # also called various places as "p"
    scaled_score: float = Field(
        ge=0,
        le=1,
        description="used for offerwall stuff, range 0<->1",
        default=0,
    )

    # --- These 3 are generally used for "SoftPair" offerwalls only. However,
    # in all other types of offerwalls, by default, the pair type is
    # unconditional, we just never check/use this for anything.
    pair_type: SoftPairResultType = Field(default=SoftPairResultType.UNCONDITIONAL)

    # The set of marketplace's question codes (internal id) that are unknown.
    # This should only be set it SoftPairResultType is conditional
    unknown_mp_question_ids: Optional[Set[str]] = Field(default=None)

    # Question ids (from marketplace_question table) for the questions that
    # will be asked (that would fulfill the unknown questions specified in
    # unknown_mp_question_ids)
    unknown_question_ids: Optional[Set[UUIDStr]] = Field(default=None)

    # ---- Soft Pair end ----

    is_recontact: bool = Field(default=False)

    @field_validator("cpi", mode="before")
    def cpi_from_float(cls, v: Decimal) -> Decimal:
        return Decimal(v).quantize(Decimal("0.00000"))

    @property
    def unknown_mp_qids(self) -> Optional[Set[str]]:
        # marketplace's curie-formatted question IDs that are unknown
        return (
            {self.source + ":" + q for q in self.unknown_mp_question_ids}
            if self.unknown_mp_question_ids is not None
            else None
        )

    def to_row(self) -> Dict[str, Any]:
        d = self.model_dump(mode="json")
        d["id_code"] = self.id_code
        return d


class ScoredTaskResults(BaseModel):
    tasks: List[ScoredTaskResult] = Field()

    @property
    def availability_count(self) -> NonNegativeInt:
        return len(self.tasks)

    def to_pandas(self) -> pd.DataFrame:
        columns = (
            list(ScoredTaskResult.model_fields.keys())
            + list(ScoredTaskResult.model_computed_fields.keys())
            + ["id_code"]
        )
        df = pd.DataFrame([x.to_row() for x in self.tasks], columns=columns)
        df["payout"] = df["payout"].astype(float)
        df["cpi"] = df["cpi"].astype(float)
        return df

    def take_top(self, n=100) -> List[ScoredTaskResult]:
        return sorted(self.tasks, key=lambda x: x.score, reverse=True)[:n]


class OfferwallBucket(BaseModel):
    """
    See also py-utils: models.legacy.bucket: Bucket. That is used only in
    handling API responses. This class is used internally to handle offerwall
    creation/management.
    """

    model_config = ConfigDict(
        extra="forbid", validate_assignment=True, ser_json_timedelta="float"
    )

    id: UUIDStr = Field(
        description="Unique identifier this particular bucket",
        examples=["5ba2fe5010cc4d078fc3cc0b0cc264c3"],
        default_factory=lambda: uuid4().hex,
    )
    uri: Optional[HttpsUrl] = Field(
        examples=[
            "https://task.generalresearch.com/api/v1/52d3f63b2709/797df4136c604a6c8599818296aae6d1/?i"
            "=5ba2fe5010cc4d078fc3cc0b0cc264c3&b=test&66482fb=e7baf5e"
        ],
        description="The URL to send a respondent into. Must not edit this URL in any way",
        default=None,
    )

    tasks: List[ScoredTaskResult] = Field()

    category: List[CategoryAssociation] = Field(default_factory=list)

    # Used only in marketplace offerwall
    source: Optional[Source] = Field(default=None)
    source_name: Optional[str] = Field(default=None)

    # Normally these are calculated. However, in some offerwalls we duplicate
    #   buckets, so they're not "true" calculated values.
    custom_min_payout: (
        Annotated[Decimal, Field(max_digits=5, decimal_places=2, ge=0, le=100)] | None
    ) = Field(
        description="Custom: Min payout across all tasks",
        default=None,
    )
    custom_q1_duration: Optional[float] = Field(
        description="Custom: Q1 loi across all tasks",
        default=None,
        gt=0,
        le=120 * 60,
    )

    quality_score: float = Field(default=0)

    eligibility_criteria: Optional[Tuple[SurveyEligibilityCriterion, ...]] = Field(
        description="The reasons the user is eligible for tasks in this bucket",
        default=None,
    )
    eligibility_explanation: Optional[str] = Field(
        default=None,
        description="Human-readable text explaining a user's eligibility for tasks in this bucket",
        examples=[
            "You are a **47-year-old** **white** **male** with a *college degree*, who's employer's retirement plan is **Fidelity Investments**."
        ],
    )

    @property
    def missing_questions(self) -> Set[UUIDStr]:
        # Used only in softpair.
        # The question id is the question's uuid (in the marketplace_question table / UpkQuestion.id)
        # It is just the set union of task.softpair.question_ids for all tasks in this bucket.
        if self.tasks[0].pair_type != SoftPairResultType.CONDITIONAL:
            return set()
        mq = set()
        for task in self.tasks:
            if task.unknown_question_ids:
                mq.update(task.unknown_question_ids)
        return mq

    @property
    def default_quality_score(self) -> float:
        # uses the euclidean norm, which is more influenced by outliers
        score = np.array([x.score for x in self.tasks][:5])
        return float(np.sqrt((score**2).sum()))

    @property
    def payout(self) -> Optional[Decimal]:
        # The payout is the Min payout across all tasks
        return min([x.payout for x in self.tasks], default=None)

    @property
    def loi(self) -> Optional[float]:
        # The loi is the Max LOI across all tasks
        return max([x.loi for x in self.tasks], default=None)

    @property
    def min_payout(self) -> Decimal:
        return self.payout_summary.min

    @property
    def max_payout(self) -> Decimal:
        return self.payout_summary.max

    @property
    def min_duration(self) -> int:
        return self.duration_summary.min

    @property
    def max_duration(self) -> int:
        return self.duration_summary.max

    @property
    def sns(self) -> List[str]:
        return [t.id_code for t in self.tasks]

    @property
    def duration_summary(self) -> DurationSummary:
        # TODO: we could cache these and then have a validator that runs to
        # update them if the tasks change? idk
        # There shouldn't ever be GRS in here anyways, right?
        durations = [res.loi for res in self.tasks if res.source != Source.GRS]
        durations = durations if durations else [0]
        min_duration, q1_duration, q2_duration, q3_duration, max_duration = np.quantile(
            durations, [0, 0.25, 0.5, 0.75, 1]
        )
        mean_duration = round(statistics.mean(durations))
        return DurationSummary(
            min=round(min_duration),
            q1=round(q1_duration),
            q2=round(q2_duration),
            q3=round(q3_duration),
            max=round(max_duration),
            mean=mean_duration,
        )

    @property
    def payout_summary(self) -> PayoutSummaryDecimal:
        return PayoutSummaryDecimal(
            min=Decimal(self.payout_summary_int.min) / 100,
            max=Decimal(self.payout_summary_int.max) / 100,
            q1=Decimal(self.payout_summary_int.q1) / 100,
            q2=Decimal(self.payout_summary_int.q2) / 100,
            q3=Decimal(self.payout_summary_int.q3) / 100,
            mean=Decimal(self.payout_summary_int.mean) / 100,
        )

    @property
    def payout_summary_int(self) -> PayoutSummary:
        payouts = [
            round(res.payout * 100) for res in self.tasks if res.source != Source.GRS
        ]
        payouts = payouts if payouts else [0]  # so min, max, quantile doesnt fail
        min_payout, q1_payout, q2_payout, q3_payout, max_payout = np.quantile(
            payouts, [0, 0.25, 0.5, 0.75, 1]
        )
        mean_payout = round(statistics.mean(payouts))
        return PayoutSummary(
            min=round(min_payout),
            q1=round(q1_payout),
            q2=round(q2_payout),
            q3=round(q3_payout),
            max=round(max_payout),
            mean=mean_payout,
        )

    @property
    def eligibility(self) -> Optional[SoftPairResultType]:
        # We're assuming there is never a conditional or ineligible survey
        #   after a unconditional. There can be unconditional surveys
        #   after conditional surveys, in which case the bucket is still
        #   conditional.
        if self.tasks[0].pair_type is not None:
            pair_type = self.tasks[0].pair_type
            if pair_type in {
                SoftPairResultType.UNCONDITIONAL,
                SoftPairResultType.CONDITIONAL,
                SoftPairResultType.INELIGIBLE,
            }:
                return pair_type
            else:
                raise ValueError(f"Unexpected pair_type {pair_type}")

    @property
    def eligibility_str(self) -> Optional[Eligibility]:
        return (
            {
                SoftPairResultType.UNCONDITIONAL: "unconditional",
                SoftPairResultType.CONDITIONAL: "conditional",
                SoftPairResultType.INELIGIBLE: "ineligible",
            }[self.eligibility]
            if self.eligibility is not None
            else None
        )

    def to_legacy_bucket(self) -> LegacyBucket:
        # The legacy bucket is used in the Session model. I don't want to change it now,
        #   but there's no reason we couldn't
        return LegacyBucket(
            loi_min=timedelta(seconds=self.duration_summary.min),
            loi_max=timedelta(seconds=self.duration_summary.max),
            loi_q1=timedelta(seconds=self.duration_summary.q1),
            loi_q2=timedelta(seconds=self.duration_summary.q2),
            loi_q3=timedelta(seconds=self.duration_summary.q3),
            user_payout_min=self.payout_summary.min,
            user_payout_max=self.payout_summary.max,
            user_payout_q1=self.payout_summary.q1,
            user_payout_q2=self.payout_summary.q2,
            user_payout_q3=self.payout_summary.q3,
        )

    def generate_bucket_entry_url(
        self, user: User, request_id: Optional[str] = None
    ) -> None:
        product_id = user.product_id
        product_user_id = user.product_user_id
        base_enter_url = (
            f"https://task.generalresearch.com/api/v1/52d3f63b2709/{product_id}/?"
        )
        if (
            self.eligibility is None
            or self.eligibility == SoftPairResultType.UNCONDITIONAL
        ):
            self.uri = generate_offerwall_entry_url(
                base_enter_url, self.id, product_user_id, request_id=request_id
            )

        return None

    # def __repr__(self):
    #     exclude = {
    #         "PRESCREEN_CONVERSION_ALPHA",
    #         "PRESCREEN_CONVERSION_BETA",
    #         "CONVERSION_ALPHA",
    #         "CONVERSION_BETA",
    #         "COMPLETION_TIME_MU",
    #         "COMPLETION_TIME_SIGMA",
    #         "COMPLETION_TIME_LOG",
    #         "DROPOFF_RATE_ALPHA",
    #         "DROPOFF_RATE_BETA",
    #         "IS_MOBILE_ELIGIBLE_ALPHA",
    #         "IS_MOBILE_ELIGIBLE_BETA",
    #         "IS_DESKTOP_ELIGIBLE_ALPHA",
    #         "IS_DESKTOP_ELIGIBLE_BETA",
    #         "IS_TABLET_ELIGIBLE_ALPHA",
    #         "IS_TABLET_ELIGIBLE_BETA",
    #         "cpi",
    #         "source",
    #         "internal_id",
    #         "is_recontact",
    #         "IS_MOBILE_ELIGIBLE",
    #         "IS_DESKTOP_ELIGIBLE",
    #         "IS_TABLET_ELIGIBLE",
    #         "USER_REPORT_COEFF",
    #         "PRESCREEN_CONVERSION",
    #     }
    #     return json.dumps(
    #         self.model_dump(mode="json", exclude={"tasks": {"__all__": exclude}}),
    #         indent=4,
    #     )


class OfferwallBase(BaseModel):
    model_config = ConfigDict(
        extra="forbid", validate_assignment=True, ser_json_timedelta="float"
    )

    id: UUIDStr = Field(
        description="Unique identifier for this offerwall",
        default_factory=lambda: uuid4().hex,
    )
    offerwall_type: OfferWallType = Field()
    buckets: List[OfferwallBucket] = Field()

    # Note: this != the sum(len(tasks) in buckets) b/c we filter out a lot
    availability_count: int = Field(default=0, description="Number of available tasks")
    attempted_live_eligible_count: NonNegativeInt = Field(
        description=(
            "Number of currently live opportunities for which the respondent "
            "meets all eligibility criteria but is excluded due to a prior attempt. "
            "Only includes surveys that are still live and otherwise eligible; "
            "does not include previously attempted surveys that are no longer available."
        ),
        examples=[7],
        default=0,
    )
    offerwall_reasons: List[OfferwallReason] = Field(
        default_factory=list,
        description=(
            "Explanations describing why so many or few opportunities are available."
        ),
        examples=[[OfferwallReason.USER_BLOCKED, OfferwallReason.UNDER_MINIMUM_AGE]],
    )

    # Contains the full info about any questions in any bucket's
    #   missing_questions.
    questions: List[UpkQuestion] = Field(default_factory=list)

    @property
    def offerwall_type_class(self) -> OfferWallTypeClass:
        return OFFERWALL_TYPE_CLASS[self.offerwall_type]

    @property
    def task_count(self) -> NonNegativeInt:
        return sum(len(b.tasks) for b in self.buckets)

    def generate_bucket_entry_urls(self, user: User, request_id: str) -> None:
        for bucket in self.buckets:
            bucket.generate_bucket_entry_url(user=user, request_id=request_id)

        return None
