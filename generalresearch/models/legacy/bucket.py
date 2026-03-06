from __future__ import annotations

import logging
import math
from datetime import timedelta
from decimal import Decimal
from typing import Optional, Dict, List, Union, Literal, Tuple
from typing_extensions import Self

from pydantic import (
    BaseModel,
    Field,
    field_validator,
    model_validator,
    ConfigDict,
    NonNegativeInt,
)

from generalresearch.models import Source
from generalresearch.models.custom_types import (
    HttpsUrl,
    UUIDStr,
    PropertyCode,
)
from generalresearch.models.thl.stats import StatisticalSummary

logger = logging.getLogger()

Eligibility = Literal["conditional", "unconditional", "ineligible"]

SourceName = Literal[
    "innovate",
    "dynata",
    "schlesinger",
    "purespectrum",
    "morning",
    "pollfish",
    "precision",
    "repdata",
    "prodege",
]


class CategoryAssociation(BaseModel):
    """Used in an offerwall. Stores the association between a category
    and a bucket, with a score.
    """

    id: UUIDStr = Field(
        description="The category ID",
        examples=["c8642a1b86d9460cbe8f7e8ae6e56ee4"],
    )

    label: str = Field(
        max_length=255,
        description="The category label",
        examples=["People & Society"],
    )

    adwords_id: Optional[str] = Field(default=None, max_length=8, examples=["14"])

    adwords_label: Optional[str] = Field(
        default=None, max_length=255, examples=["People & Society"]
    )

    p: float = Field(
        ge=0,
        le=1,
        examples=[1.0],
        description="The strength of the association of this bucket"
        "with this category. Will sum to 1 within a bucket.",
    )


class BucketTask(BaseModel):
    """
    This represents one of the "tasks" within a bucket's ordered list of tasks.
    """

    id: str = Field(
        min_length=1,
        max_length=32,
        examples=["6ov9jz3"],
        description="The internal task id for this task within the marketplace",
    )
    id_code: str = Field(
        min_length=3,
        max_length=35,
        pattern=r"^[a-z]{1,2}\:.*",
        examples=["o:6ov9jz3"],
        description="The namespaced task id for this task within the marketplace",
    )
    source: Source = Field(examples=[Source.POLLFISH])
    loi: int = Field(
        gt=1, le=90 * 60, description="expected loi in seconds", examples=[612]
    )
    payout: int = Field(gt=1, description="integer cents", examples=[123])

    @model_validator(mode="after")
    def check_id_code(self) -> Self:
        assert self.source.value + ":" + self.id == self.id_code, "ids are wrong!!"
        return self

    def censor(self):
        censor_idx = math.ceil(len(self.id) / 2)
        self.id = self.id[:censor_idx] + ("*" * len(self.id[censor_idx:]))
        self.id_code = self.source.value + ":" + self.id


class BucketBase(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        validate_assignment=True,
        ser_json_timedelta="float",
        arbitrary_types_allowed=True,
    )

    id: UUIDStr = Field(
        description="Unique identifier this particular bucket",
        examples=["5ba2fe5010cc4d078fc3cc0b0cc264c3"],
    )
    uri: HttpsUrl = Field(
        examples=[
            "https://task.generalresearch.com/api/v1/52d3f63b2709/797df4136c604a6c8599818296aae6d1/?i"
            "=5ba2fe5010cc4d078fc3cc0b0cc264c3&b=test&66482fb=e7baf5e"
        ],
        description="The URL to send a respondent into. Must not edit this URL in any way",
    )

    x: int = Field(
        description="For UI. Provides a dimensionality position for the bucket on the x-axis.",
        ge=0,
        default=0,
        examples=[0, 1, 2],
    )
    y: int = Field(
        description="For UI. Provides a dimensionality position for the bucket on the y-axis.",
        ge=0,
        default=0,
        examples=[0, 1, 2],
    )
    name: str = Field(
        description="Currently unused. Will always return empty string",
        default="",
    )
    description: str = Field(
        description="Currently unused. Will always return empty string",
        default="",
    )

    def censor(self):
        if not hasattr(self, "contents"):
            return
        contents: List[BucketTask] = self.contents
        for content in contents:
            content.censor()


class Bucket(BaseModel):
    """
    This isn't returned in any API response. It is used internally to GRL as
    the common form to represent a bucket in all offerwalls. Depending on
    which offerwall is requested, we'll convert from this format to the
    requested format.
    """

    model_config = ConfigDict(
        extra="forbid",
        validate_assignment=True,
        ser_json_timedelta="float",
        arbitrary_types_allowed=True,
    )

    name: Optional[str] = Field(default=None)
    description: Optional[str] = Field(default=None)

    # pydantic serializes this to seconds
    loi_min: Optional[timedelta] = Field(strict=True, default=None)
    loi_max: Optional[timedelta] = Field(strict=True, default=None)
    loi_mean: Optional[timedelta] = Field(strict=True, default=None)
    loi_q1: Optional[timedelta] = Field(strict=True, default=None)
    loi_q2: Optional[timedelta] = Field(strict=True, default=None)
    loi_q3: Optional[timedelta] = Field(strict=True, default=None)
    # decimal USD. This should not have more than 2 decimal places.
    #     There is no way to make this "strict" and optional, so we have a separate pre-validator
    user_payout_min: Optional[Decimal] = Field(default=None, lt=1000, gt=0)
    user_payout_max: Optional[Decimal] = Field(default=None, lt=1000, gt=0)
    user_payout_q1: Optional[Decimal] = Field(default=None, lt=1000, gt=0)
    user_payout_q2: Optional[Decimal] = Field(default=None, lt=1000, gt=0)
    user_payout_q3: Optional[Decimal] = Field(default=None, lt=1000, gt=0)
    user_payout_mean: Optional[Decimal] = Field(default=None, lt=1000, gt=0)

    quality_score: Optional[float] = Field(default=None)

    category: List[CategoryAssociation] = Field(default_factory=list)

    contents: Optional[List[BucketTask]] = Field(default=None)

    # This could store things like "is_recontact=False"
    metadata: Dict[str, Union[str, float, bool, int]] = Field(default_factory=dict)

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

    @field_validator("loi_min", "loi_max", "loi_q1", "loi_q2", "loi_q3")
    @classmethod
    def check_loi_ranges(cls, v):
        if v is not None:
            assert v > timedelta(seconds=0), "lois should be greater than 0"
            assert v <= timedelta(minutes=90), "lois should be less than 90 minutes"
        return v

    @field_validator(
        "user_payout_min",
        "user_payout_max",
        "user_payout_q1",
        "user_payout_q2",
        "user_payout_q3",
        mode="before",
    )
    @classmethod
    def check_decimal_type(cls, v: Decimal) -> Decimal:
        # pydantic is unable to set strict=True, so we'll do that manually here
        if v is not None:
            assert type(v) == Decimal, f"Must pass a Decimal, not a {type(v)}"
        return v

    @field_validator(
        "user_payout_min",
        "user_payout_max",
        "user_payout_q1",
        "user_payout_q2",
        "user_payout_q3",
        mode="after",
    )
    @classmethod
    def check_payout_decimal_places(cls, v: Decimal) -> Decimal:
        if v is not None:
            assert (
                v.as_tuple().exponent >= -2
            ), "Must have 2 or fewer decimal places ('XXX.YY')"
            # explicitly make sure it is 2 decimal places, after checking that it is already 2 or less.
            v = v.quantize(Decimal("0.00"))
        return v

    @model_validator(mode="after")
    def check_lois(self):
        if self.loi_min is not None and self.loi_max is not None:
            assert self.loi_min <= self.loi_max, "loi_min should be <= loi_max"
        if self.loi_q1 or self.loi_q2 or self.loi_q3:
            assert (
                self.loi_q1 and self.loi_q2 and self.loi_q3
            ), "loi_q1, q2, and q3 should all be set or all None"
            assert (
                self.loi_min is not None and self.loi_max is not None
            ), "If loi_q1, q2, or q3 are set, then loi_min and max should be set"
            assert self.loi_q1 >= self.loi_min, "loi_min should be <= loi_q1"
            assert self.loi_q2 >= self.loi_q1, "loi_q1 should be <= loi_q2"
            assert self.loi_q3 >= self.loi_q2, "loi_q2 should be <= loi_q3"
            assert self.loi_max >= self.loi_q3, "loi_q3 should be <= loi_max"
        return self

    @model_validator(mode="after")
    def check_payouts(self):
        if self.user_payout_min is not None and self.user_payout_max is not None:
            assert (
                self.user_payout_min <= self.user_payout_max
            ), "user_payout_min should be <= user_payout_max"
        if self.user_payout_q1 or self.user_payout_q2 or self.user_payout_q3:
            assert (
                self.user_payout_q1 and self.user_payout_q2 and self.user_payout_q3
            ), "user_payout_q1, q2, and q3 should all be set or all None"
            assert (
                self.user_payout_min is not None and self.user_payout_max is not None
            ), "If user_payout_q1, q2, or q3 are set, then user_payout_min and max should be set"
            assert (
                self.user_payout_q1 >= self.user_payout_min
            ), "user_payout_min should be <= user_payout_q1"
            assert (
                self.user_payout_q2 >= self.user_payout_q1
            ), "user_payout_q1 should be <= user_payout_q2"
            assert (
                self.user_payout_q3 >= self.user_payout_q2
            ), "user_payout_q2 should be <= user_payout_q3"
            assert (
                self.user_payout_max >= self.user_payout_q3
            ), "user_payout_q3 should be <= user_payout_max"
        return self

    @field_validator("category")
    @classmethod
    def check_category(cls, v: List[CategoryAssociation]) -> List[CategoryAssociation]:
        assert sum(c.p for c in v) == 1, "sum of category score must be 1"
        return v

    @classmethod
    def parse_from_offerwall(cls, bucket: Dict):
        """
        This isn't really consistent across all offerwalls... Handle three cases:
            Could be {'payout': {'min': 123}}, or {'min_payout': 123} or {'payout': 123}
        Only min_payout is really required. The others can be optional.
        payouts - Should always be integer usd cents.
        duration / loi - Should always be seconds.
        """
        if "min_payout" in bucket:
            return cls.parse_from_offerwall_style1(bucket)
        elif "payout" in bucket and type(bucket["payout"]) is dict:
            return cls.parse_from_offerwall_style2(bucket)
        elif "payout" in bucket and type(bucket["payout"]) is not dict:
            return cls.parse_from_offerwall_style3(bucket)
        else:
            logger.info("unknown bucket format")
            return cls()

    @classmethod
    def parse_from_offerwall_style1(cls, bucket: Dict):
        # {'min_payout': 123}
        return cls(
            user_payout_min=cls.usd_cents_to_decimal(bucket["min_payout"]),
            user_payout_max=cls.usd_cents_to_decimal(bucket.get("max_payout")),
            user_payout_q1=cls.usd_cents_to_decimal(bucket.get("q1_payout")),
            user_payout_q2=cls.usd_cents_to_decimal(bucket.get("q2_payout")),
            user_payout_q3=cls.usd_cents_to_decimal(bucket.get("q3_payout")),
            loi_min=(
                timedelta(seconds=bucket["min_duration"])
                if bucket.get("min_duration") is not None
                else None
            ),
            loi_max=(
                timedelta(seconds=bucket["max_duration"])
                if bucket.get("max_duration") is not None
                else None
            ),
            loi_q1=(
                timedelta(seconds=bucket["q1_duration"])
                if bucket.get("q1_duration") is not None
                else None
            ),
            loi_q2=(
                timedelta(seconds=bucket["q2_duration"])
                if bucket.get("q2_duration") is not None
                else None
            ),
            loi_q3=(
                timedelta(seconds=bucket["q3_duration"])
                if bucket.get("q3_duration") is not None
                else None
            ),
        )

    @classmethod
    def parse_from_offerwall_style2(cls, bucket: Dict):
        # {'payout': {'min': 123}}
        loi_min_sec = bucket.get("duration", {}).get("min")
        loi_max_sec = bucket.get("duration", {}).get("max")
        loi_q1_sec = bucket.get("duration", {}).get("q1")
        loi_q2_sec = bucket.get("duration", {}).get("q2")
        loi_q3_sec = bucket.get("duration", {}).get("q3")
        return cls(
            user_payout_min=cls.usd_cents_to_decimal(bucket["payout"]["min"]),
            user_payout_max=cls.usd_cents_to_decimal(bucket["payout"].get("max")),
            user_payout_q1=cls.usd_cents_to_decimal(bucket["payout"].get("q1")),
            user_payout_q2=cls.usd_cents_to_decimal(bucket["payout"].get("q2")),
            user_payout_q3=cls.usd_cents_to_decimal(bucket["payout"].get("q3")),
            loi_min=(
                timedelta(seconds=loi_min_sec) if loi_min_sec is not None else None
            ),
            loi_max=(
                timedelta(seconds=loi_max_sec) if loi_max_sec is not None else None
            ),
            loi_q1=(timedelta(seconds=loi_q1_sec) if loi_q1_sec is not None else None),
            loi_q2=(timedelta(seconds=loi_q2_sec) if loi_q2_sec is not None else None),
            loi_q3=(timedelta(seconds=loi_q3_sec) if loi_q3_sec is not None else None),
        )

    @classmethod
    def parse_from_offerwall_style3(cls, bucket: Dict):
        # {'payout': 123, 'duration': 123}
        return cls(
            user_payout_min=cls.usd_cents_to_decimal(bucket["payout"]),
            user_payout_max=None,
            loi_min=None,
            loi_max=(
                timedelta(seconds=bucket["duration"])
                if bucket.get("duration") is not None
                else None
            ),
        )

    @staticmethod
    def usd_cents_to_decimal(v: int):
        if v is None:
            return None
        return Decimal(Decimal(int(v)) / Decimal(100))

    @staticmethod
    def decimal_to_usd_cents(d: Decimal):
        if d is None:
            return None
        return round(d * Decimal(100), 2)


class DurationSummary(StatisticalSummary):
    """Durations are in integer seconds.
    Describes the statistical distribution of expected durations of tasks within this bucket.
    """

    min: int = Field(gt=0, le=60 * 90)
    max: int = Field(gt=0, le=60 * 90)
    q1: int = Field(gt=0, le=60 * 90)
    q2: int = Field(gt=0, le=60 * 90)
    q3: int = Field(gt=0, le=60 * 90)
    mean: Optional[int] = Field(gt=0, le=60 * 90, default=None)

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "min": 112,
                    "max": 1180,
                    "q1": 457,
                    "q2": 650,
                    "q3": 1103,
                    "mean": 660,
                }
            ]
        }
    }

    @classmethod
    def from_bucket(cls, bucket: Bucket):
        return cls(
            min=bucket.loi_min.total_seconds(),
            max=bucket.loi_max.total_seconds(),
            q1=bucket.loi_q1.total_seconds(),
            q2=bucket.loi_q2.total_seconds(),
            q3=bucket.loi_q3.total_seconds(),
            mean=(
                bucket.loi_mean.total_seconds() if bucket.loi_mean is not None else None
            ),
        )


class PayoutSummaryDecimal(StatisticalSummary):
    """Payouts are in Decimal USD"""

    min: Decimal = Field(gt=0, le=100)
    max: Decimal = Field(gt=0, le=100)
    q1: Decimal = Field(gt=0, le=100)
    q2: Decimal = Field(gt=0, le=100)
    q3: Decimal = Field(gt=0, le=100)
    mean: Optional[Decimal] = Field(gt=0, le=100, default=None)


class PayoutSummary(StatisticalSummary):
    """Payouts are in Integer USD Cents"""

    min: int = Field(gt=0, le=10000)
    max: int = Field(gt=0, le=10000)
    q1: int = Field(gt=0, le=10000)
    q2: int = Field(gt=0, le=10000)
    q3: int = Field(gt=0, le=10000)
    mean: Optional[int] = Field(gt=0, le=10000, default=None)

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "min": 14,
                    "max": 132,
                    "q1": 45,
                    "q2": 68,
                    "q3": 124,
                }
            ]
        }
    }

    @classmethod
    def from_bucket(cls, bucket: Bucket):
        return cls(
            min=bucket.decimal_to_usd_cents(bucket.user_payout_min),
            max=bucket.decimal_to_usd_cents(bucket.user_payout_max),
            q1=bucket.decimal_to_usd_cents(bucket.user_payout_q1),
            q2=bucket.decimal_to_usd_cents(bucket.user_payout_q2),
            q3=bucket.decimal_to_usd_cents(bucket.user_payout_q3),
            mean=(
                bucket.decimal_to_usd_cents(bucket.user_payout_mean)
                if bucket.user_payout_mean is not None
                else None
            ),
        )


class SurveyEligibilityCriterion(BaseModel):
    """
    Explanatory record of which question answers contributed
        to a user's eligibility for a survey.
    This is INSUFFICIENT for determining eligibility to a task
        as it IGNORES logical operators, dependencies between criteria,
        and other requirements. It is only intended for the UI.
    """

    model_config = ConfigDict(validate_assignment=True)

    question_id: Optional[UUIDStr] = Field(
        examples=["71a367fb71b243dc89f0012e0ec91749"]
    )
    property_code: Optional[PropertyCode] = Field(examples=["c:73629"])
    question_text: str = Field(
        examples=[
            "What company administers the retirement plan for your current employer?"
        ]
    )
    # The answer(s) that were considered qualifying
    qualifying_answer: Tuple[str, ...] = Field(
        description="User answer(s) that satisfied at least one eligibility rule",
        examples=["121"],
    )
    qualifying_answer_label: Optional[Tuple[str, ...]] = Field(
        examples=["Fidelity Investments"]
    )
    explanation: Optional[str] = Field(
        default=None,
        description="Human-readable text explaining how a user's answer to this question affects eligibility",
        examples=[
            "The company that administers your employer's retirement plan is **Fidelity Investments**."
        ],
    )
    explanation_fragment: Optional[str] = Field(
        default=None,
        exclude=True,
        description="For internal use",
        examples=["who's retirement plan is administered by **Fidelity Investments**"],
    )
    # Rank more "interesting"/rare/salient criterion first.
    rank: Optional[NonNegativeInt] = Field(
        default=None,
        description="Lower values are shown more prominently in the UI",
    )


class TopNBucket(BucketBase):
    category: List[CategoryAssociation] = Field(default_factory=list)
    duration: DurationSummary = Field()
    payout: PayoutSummary = Field()
    quality_score: float = Field(
        ge=0,
        le=1,
        examples=[0.29223],
        description="A proprietary score to determine the overall quality of the tasks that "
        "are within the bucket. "
        "Higher is better.",
    )

    @classmethod
    def from_bucket(cls, bucket: Bucket):
        return cls.model_validate(
            {
                "id": bucket.id,
                "uri": bucket.uri,
                "duration": DurationSummary.from_bucket(bucket),
                "payout": PayoutSummary.from_bucket(bucket),
                "quality_score": bucket.quality_score,
                "category": bucket.category,
            }
        )


class SingleEntryBucket(BucketBase):
    x: int = Field(exclude=True, default=0)
    y: int = Field(exclude=True, default=0)
    name: int = Field(exclude=True, default="")
    description: int = Field(exclude=True, default="")


class TopNPlusBucket(BucketBase):
    category: List[CategoryAssociation] = Field(default_factory=list)
    contents: List[BucketTask] = Field()
    duration: DurationSummary = Field()
    payout: PayoutSummary = Field()
    quality_score: float = Field()
    currency: str = Field(
        description="This will always be 'USD'", default="USD", examples=["USD"]
    )

    eligibility_criteria: Tuple[SurveyEligibilityCriterion, ...] = Field(
        description="The reasons the user is eligible for tasks in this bucket",
        default_factory=tuple,
    )
    eligibility_explanation: Optional[str] = Field(
        default=None,
        description="Human-readable text explaining a user's eligibility for tasks in this bucket",
        examples=[
            "You are a **47-year-old** **white** **male** with a *college degree*, who's employer's retirement plan is **Fidelity Investments**."
        ],
    )

    @field_validator("eligibility_criteria", mode="after")
    @classmethod
    def eligibility_ranks(cls, criteria):
        criteria = list(criteria)
        ranks = [c.rank for c in criteria]
        if all(r is None for r in ranks):
            for i, c in enumerate(criteria):
                c.rank = i
            return tuple(criteria)
        if any(r is None for r in ranks):
            raise ValueError("Set all or no ranks in eligibility_criteria")
        if len(ranks) != len(set(ranks)):
            raise ValueError("Duplicate ranks")
        return tuple(sorted(criteria, key=lambda c: c.rank))

    @classmethod
    def from_bucket(cls, bucket: Bucket):
        return cls.model_validate(
            {
                "id": bucket.id,
                "uri": bucket.uri,
                "duration": DurationSummary.from_bucket(bucket),
                "payout": PayoutSummary.from_bucket(bucket),
                "quality_score": bucket.quality_score,
                "category": bucket.category,
                "contents": bucket.contents,
                "eligibility_criteria": bucket.eligibility_criteria,
                "eligibility_explanation": bucket.eligibility_explanation,
            }
        )


class TopNPlusRecontactBucket(BucketBase):
    category: List[CategoryAssociation] = Field(default_factory=list)
    contents: List[BucketTask] = Field()
    duration: DurationSummary = Field()
    payout: PayoutSummary = Field()
    quality_score: float = Field()
    is_recontact: bool = Field()
    currency: str = Field(
        description="This will always be 'USD'", default="USD", examples=["USD"]
    )

    @classmethod
    def from_bucket(cls, bucket: Bucket):
        return cls.model_validate(
            {
                "id": bucket.id,
                "uri": bucket.uri,
                "duration": DurationSummary.from_bucket(bucket),
                "payout": PayoutSummary.from_bucket(bucket),
                "quality_score": bucket.quality_score,
                "category": bucket.category,
                "contents": bucket.contents,
                "is_recontact": bucket.metadata.get("is_recontact", False),
            }
        )


class SoftPairBucket(BucketBase):
    uri: Optional[HttpsUrl] = Field(
        examples=[None],
        description="The URL to send a respondent into. Must not edit this URL in any way. If the eligibility is "
        "conditional or ineligible, the uri will be null.",
    )

    category: List[CategoryAssociation] = Field(default_factory=list)
    contents: List[BucketTask] = Field()

    eligibility: Eligibility = Field(examples=["conditional"])
    missing_questions: List[str] = Field(
        default_factory=list, examples=[["fb20fd4773304500b39c4f6de0012a5a"]]
    )
    loi: int = Field(description="this is the max loi of the contents", examples=[612])
    payout: int = Field(
        description="this is the min payout of the contents", examples=[123]
    )

    x: int = Field(exclude=True, default=0)
    y: int = Field(exclude=True, default=0)
    name: int = Field(exclude=True, default="")
    description: int = Field(exclude=True, default="")


class MarketplaceBucket(BucketBase):
    category: List[CategoryAssociation] = Field(default_factory=list)
    contents: List[BucketTask] = Field()
    duration: DurationSummary = Field()
    payout: PayoutSummary = Field()
    source: SourceName = Field(
        description="this is the source of the contents", examples=["pollfish"]
    )


class TimeBucksBucket(BucketBase):
    duration: int = Field(
        gt=0, le=60 * 90, description="The bucket's q1 duration, in seconds"
    )
    min_payout: int = Field(
        gt=0, le=100_00, description="The bucket's min payout, in usd cents"
    )
    currency: str = Field(
        description="This will always be 'USD'", default="USD", examples=["USD"]
    )


class OneShotOfferwallBucket(BaseModel):
    model_config = ConfigDict(
        extra="forbid", validate_assignment=True, ser_json_timedelta="float"
    )

    id: UUIDStr = Field(
        description="Unique identifier this particular bucket",
        examples=["5ba2fe5010cc4d078fc3cc0b0cc264c3"],
    )
    uri: HttpsUrl = Field(
        examples=[
            "https://task.generalresearch.com/api/v1/52d3f63b2709/797df4136c604a6c8599818296aae6d1/?i"
            "=5ba2fe5010cc4d078fc3cc0b0cc264c3&b=test&66482fb=e7baf5e"
        ],
        description="The URL to send a respondent into. Must not edit this URL in any way",
    )
    duration: int = Field(
        gt=0,
        le=60 * 90,
        description="The bucket's expected duration, in seconds",
    )
    min_payout: int = Field(
        gt=0, le=100_00, description="The bucket's min payout, in usd cents"
    )


class OneShotSoftPairOfferwallBucket(OneShotOfferwallBucket):
    eligibility: Eligibility = Field(examples=["conditional"])
    missing_questions: List[str] = Field(
        default_factory=list, examples=[["fb20fd4773304500b39c4f6de0012a5a"]]
    )


class WXETOfferwallBucket(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        validate_assignment=True,
        ser_json_timedelta="float",
    )

    id: UUIDStr = Field(
        description="Unique identifier this particular bucket",
        examples=["5ba2fe5010cc4d078fc3cc0b0cc264c3"],
    )
    uri: HttpsUrl = Field(
        examples=[
            "https://task.generalresearch.com/api/v1/52d3f63b2709/797df4136c604a6c8599818296aae6d1/?i"
            "=5ba2fe5010cc4d078fc3cc0b0cc264c3&b=test&66482fb=e7baf5e"
        ],
        description="The URL to send a respondent into. Must not edit this URL in any way",
    )
    duration: int = Field(
        gt=0,
        le=60 * 90,
        description="The bucket's expected duration, in seconds",
    )
    min_payout: int = Field(
        gt=0, le=10000, description="The bucket's min payout, in usd cents"
    )
