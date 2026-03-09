from datetime import datetime, timezone
from decimal import Decimal
from math import log
from typing import Annotated, Optional

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    NonNegativeInt,
    PositiveInt,
    computed_field,
    model_validator,
)
from scipy.stats import beta as beta_dist

from generalresearch.models import Source
from generalresearch.models.custom_types import (
    AwareDatetimeISO,
    CountryISOLike,
    UUIDStr,
)


class Buyer(BaseModel):
    """
    The entity that commissions and pays for a task and uses the
    resulting data or insights.
    """

    model_config = ConfigDict(validate_assignment=True)

    id: Optional[PositiveInt] = Field(default=None, exclude=True)
    # todo: need to add to db
    uuid: Optional[UUIDStr] = Field(default=None)

    source: Source = Field(
        description="The marketplace this buyer is on.\n" + Source.as_openapi()
    )
    code: str = Field(
        min_length=1,
        max_length=128,
        description="The internal code on this marketplace for this buyer",
    )
    label: Optional[str] = Field(default=None, max_length=255)
    created: AwareDatetimeISO = Field(
        default_factory=lambda: datetime.now(tz=timezone.utc),
        description="When this entry was made, or when the buyer was first seen",
    )

    @property
    def natural_key(self) -> str:
        return f"{self.source.value}:{self.code}"

    @property
    def source_code(self) -> str:
        return self.natural_key


class BuyerActivity(BaseModel):
    """
    Information about live Tasks from this buyer
    """

    live_task_count: PositiveInt = Field()
    avg_cpi: Decimal = Field()
    avg_score: float = Field()
    max_score: float = Field()


class BuyerWithDetail(BaseModel):
    """For API Responses"""

    buyer: Buyer = Field()
    activity: Optional[BuyerActivity] = Field(default=None)


class BuyerCountryStat(BaseModel):
    """
    Aggregated performance summary for a specific buyer within a single
    country. Metrics are computed across all observed tasks for this (buyer,
    country) pair and include risk-adjusted conversion and dropoff estimates,
    LOI deviation relative to survey expectations, quality signals, and a
    composite ranking score. All rate-based metrics use Bayesian shrinkage to
    reduce small-sample noise. The score is intended for relative ranking
    among buyers within comparable contexts.
    """

    model_config = ConfigDict(validate_assignment=False)

    # ---- Identity ----
    buyer_id: Optional[PositiveInt] = Field(
        default=None,
        exclude=True,
        description="This is the pk of the Buyer object in the db",
    )
    country_iso: Optional[CountryISOLike] = Field(
        default=None,
        description="If null, this is a weighted average across all countries",
        examples=["us"],
    )

    # --- For lookup / de-normalization ---
    source: Source = Field(
        description="The marketplace this buyer is on.\n" + Source.as_openapi(),
        examples=[Source.DYNATA],
    )
    code: str = Field(
        min_length=1,
        max_length=128,
        description="The internal code on this marketplace for this buyer",
        examples=["abc123"],
    )

    # --- Observation Counts ---
    task_count: NonNegativeInt = Field(
        description="The count of observed tasks", examples=[100]
    )

    # ---- Distributions ----
    conversion_alpha: float = Field(
        gt=0, description="Alpha parameter from a Beta distribution", examples=[40.0]
    )
    conversion_beta: float = Field(
        gt=0, description="Beta parameter from a Beta distribution", examples=[190.0]
    )

    @computed_field(
        description="Penalized mean (20th percentile) of conversion",
        examples=[0.15264573817318744],
    )
    @property
    def conversion_p20(self) -> Annotated[float, Field(ge=0, le=1)]:
        return float(beta_dist.ppf(0.2, self.conversion_alpha, self.conversion_beta))

    dropoff_alpha: float = Field(
        gt=0, description="Alpha parameter from a Beta distribution", examples=[20.0]
    )
    dropoff_beta: float = Field(
        gt=0, description="Beta parameter from a Beta distribution", examples=[50.0]
    )

    @computed_field(
        description="Penalized mean (60th percentile) of the dropoff/abandonment rate",
        examples=[0.29748756969632695],
    )
    @property
    def dropoff_p60(self) -> Annotated[float, Field(ge=0, le=1)]:
        return float(beta_dist.ppf(0.6, self.dropoff_alpha, self.dropoff_beta))

    # --- Expectations ---
    loi_excess_ratio: float = Field(
        ge=0,
        description=(
            "Volume-weighted average of (observed LOI / expected LOI) "
            "across all completed tasks. "
            "1.0 means exactly as expected. "
            ">1 longer than expected. <1 shorter."
        ),
        examples=[1],
    )

    # ---- Risk / quality ----
    long_fail_rate: float = Field(
        ge=0,
        le=10,
        description="Lower values indicate tasks are likely to late terminate",
        examples=[1],
    )
    user_report_coeff: float = Field(
        ge=0, le=1, description="Lower values indicate more user reports", examples=[1]
    )
    recon_likelihood: float = Field(
        ge=0, le=1, description="Likelihood tasks will get reconciled", examples=[0.05]
    )

    # ---- Scoring ----
    score: float = Field(
        description="Composite score calculated from all of the individual features",
        examples=[-5.329389837486194],
    )

    @model_validator(mode="after")
    def compute_score(self):
        eps = 1e-12

        # ---- Conversion (logit) ----
        c = min(max(self.conversion_p20, eps), 1 - eps)
        C = log(c / (1 - c))

        # ---- Dropoff ----
        d = min(max(self.dropoff_p60, 0.0), 1.0 - eps)
        D = log(1 - d)

        # ---- LOI symmetric penalty ----
        loi = max(self.loi_excess_ratio, eps)
        L = -abs(log(loi))

        # ---- Long fail ----
        F = -log(1 + max(self.long_fail_rate, 0.0))

        # ---- User report ----
        R = log(max(self.user_report_coeff, eps))

        # ---- Reconciliation ----
        Q = log(max(self.recon_likelihood, eps))

        raw_score = 2.0 * C + 1.5 * D + L + F + R + Q

        # ---- Small-sample shrinkage ----
        n_eff = self.conversion_alpha + self.conversion_beta
        k = 100.0  # tuning parameter

        weight = n_eff / (n_eff + k)

        self.score = weight * raw_score

        return self
