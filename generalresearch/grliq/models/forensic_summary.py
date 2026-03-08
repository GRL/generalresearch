from __future__ import annotations

import random
from typing import (
    Dict,
    List,
    Literal,
    Optional,
    Tuple,
    Union,
    get_args,
    get_origin,
    get_type_hints,
)

import numpy as np
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    NonNegativeInt,
    computed_field,
    create_model,
)
from scipy.stats import lognorm

from generalresearch.grliq.models.custom_types import GrlIqAvgScore, GrlIqRate
from generalresearch.grliq.models.forensic_result import (
    GrlIqCheckerResult,
    GrlIqCheckerResults,
)
from generalresearch.models.custom_types import AwareDatetimeISO, IPvAnyAddressStr
from generalresearch.models.thl.locales import CountryISO
from generalresearch.models.thl.maxmind.definitions import UserType

example_rtt_percentiles = (
    [133.332]
    + list(
        map(float, lognorm.ppf(np.linspace(0.01, 0.99, 99), s=0.1, scale=175).round(3))
    )
    + [890.006]
)


class UserForensicSummary(BaseModel):
    """
    'Top-level' forensic summary for a user
    """

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    period_start: Optional[AwareDatetimeISO] = Field(
        default=None,
        description="Timestamp of the earliest attempt included in this summary (UTC)",
    )
    period_end: Optional[AwareDatetimeISO] = Field(
        default=None,
        description="Timestamp of the latest attempt included in this summary (UTC)",
    )

    # These must be nullable in case a user has 0 attempts!
    category_result_summary: Optional[GrlIqForensicCategorySummary] = Field(
        default=None
    )
    checker_result_summary: Optional[GrlIqCheckerResultsSummary] = Field(default=None)

    country_timing_data_summary: Dict[CountryISO, TimingDataCountrySummary] = Field(
        default_factory=dict
    )
    ip_timing_data_summary: Dict[IPvAnyAddressStr, IPTimingDataSummary] = Field(
        default_factory=dict
    )


class GrlIqForensicCategorySummary(BaseModel):
    """
    GrlIqForensicCategoryResult Summary across multiple attempts by a single user.
    """

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    attempt_count: NonNegativeInt = Field(
        description="Number of attempts included in this summary", examples=[42]
    )
    is_attempt_allowed_count: NonNegativeInt = Field(
        description="The count of attempts that were allowed.", examples=[40]
    )

    is_complete_rate: GrlIqRate = Field(
        description="Proportion of attempts where is_complete=True",
        examples=[random.random()],
    )

    is_bot_avg: GrlIqAvgScore = Field(examples=[random.randint(0, 100)])
    is_velocity_avg: GrlIqAvgScore = Field(examples=[random.randint(0, 100)])
    is_oscillating_avg: GrlIqAvgScore = Field(examples=[random.randint(0, 100)])
    is_teleporting_avg: GrlIqAvgScore = Field(examples=[random.randint(0, 100)])
    is_inconsistent_avg: GrlIqAvgScore = Field(examples=[random.randint(0, 100)])
    is_tampered_avg: GrlIqAvgScore = Field(examples=[random.randint(0, 100)])
    is_anonymous_avg: GrlIqAvgScore = Field(examples=[random.randint(0, 100)])
    suspicious_ip_avg: GrlIqAvgScore = Field(examples=[random.randint(0, 100)])
    platform_ip_inconsistent_avg: GrlIqAvgScore = Field(
        examples=[random.randint(0, 100)]
    )
    fraud_score_avg: Optional[GrlIqAvgScore] = Field(
        default=None, examples=[random.randint(0, 100)]
    )


def is_optional(t):
    return get_origin(t) is Union and type(None) in get_args(t)


def unwrap_optional(t):
    if is_optional(t):
        return next(arg for arg in get_args(t) if arg is not type(None))
    return t


def generate_GrlIqCheckerResultsSummary():
    """
    Dynamically generate GrlIqCheckerResultsSummary model from the
        GrlIqCheckerResults model. Each check_* field is added
        as a float with the name check_*_avg.
    """
    fields = {}
    for field_name, hint in get_type_hints(GrlIqCheckerResults).items():
        is_opt = is_optional(hint)
        base_type = unwrap_optional(hint)
        if base_type == GrlIqCheckerResult:
            if is_opt:
                fields[f"{field_name}_avg"] = (
                    Optional[GrlIqAvgScore],
                    Field(default=None, examples=[random.randint(0, 100)]),
                )
                fields[f"{field_name}_pct_none"] = (
                    GrlIqRate,
                    Field(examples=[random.random()], default=0),
                )
            else:
                fields[f"{field_name}_avg"] = (
                    GrlIqAvgScore,
                    Field(examples=[random.randint(0, 100)]),
                )

    summary_model = create_model(
        "GrlIqCheckerResultsSummary",
        __doc__="GrlIqCheckerResults Summary across multiple attempts by a single user.",
        __config__=ConfigDict(extra="forbid", validate_assignment=True),
        **fields,
    )
    return summary_model


GrlIqCheckerResultsSummary = generate_GrlIqCheckerResultsSummary()


class TimingDataCountrySummary(BaseModel):
    """
    Summary of timing data results for a single user across all of their observed ips,
        within 1 country (to one server_location).
    """

    country_iso: CountryISO = Field(examples=["us"])
    server_location: Literal["fremont_ca"] = Field(default="fremont_ca")

    rtt_min: float = Field(gt=0, examples=[133.332])
    rtt_q25: float = Field(gt=0, examples=[144.928])
    rtt_median: float = Field(gt=0, examples=[167.743])
    rtt_mean: float = Field(gt=0, examples=[179.302])
    rtt_q75: float = Field(gt=0, examples=[220.232])
    rtt_max: float = Field(gt=0, examples=[890.006])

    expected_rtt_range: Tuple[float, float] = Field(
        description="The expected rtt range for this IP (based on country_iso/user_type) to server_location",
        examples=[(45.193, 120.841)],
    )
    mean_z_score: float = Field(
        examples=[1.22238],
        description="Mean of all z-scores. A z-score is calculated"
        "for each of the user's sessions.",
    )


class IPTimingDataSummary(BaseModel):
    """
    Summary of timing data results for a single user on a single ip (across all of
        this user's sessions on this IP)
    """

    client_ip: IPvAnyAddressStr = Field(examples=["123.123.123.123"])
    country_iso: CountryISO = Field(examples=["us"])
    server_location: Literal["fremont_ca"] = Field(default="fremont_ca")
    user_type: Optional[UserType] = Field(default=None, examples=[UserType.RESIDENTIAL])
    expected_rtt_range: Tuple[float, float] = Field(
        description="The expected rtt range for this IP (based on country_iso/user_type) to server_location",
        examples=[(45.193, 120.841)],
    )
    observed_rtt_mean: float = Field(gt=0, examples=[382.983])
    mean_z_score: float = Field(examples=[2.411])


class CountryRTTDistribution(BaseModel):
    """The distribution of observed RTTs (optionally filtered by `is_fraud`)
    from `country_iso` to `server_location`.

    This would be returned from its own endpoint (Get Expected RTT by Country),
        where you could optionally pass url params 'is_fraud', 'user_type'.
    """

    server_location: Literal["fremont_ca"] = Field(default="fremont_ca")
    country_iso: CountryISO = Field(
        description="Country client_ip is located in", examples=["fr"]
    )
    # For users marked as fraud or not
    is_fraud: Optional[bool] = Field(
        default=None,
        description="If timing data from sessions determined to be fraud are included",
    )

    # we could split by this optionally
    user_type: Optional[UserType] = Field(
        default=None,
        description="user_type of the client_ip as determined by MaxMind",
        examples=[UserType.RESIDENTIAL],
    )

    rtt_min: float = Field(gt=0, examples=[133.332])
    rtt_median: float = Field(gt=0, examples=[167.743])
    rtt_mean: float = Field(gt=0, examples=[179.302])
    rtt_max: float = Field(gt=0, examples=[890.006])
    rtt_std: float = Field(gt=0, examples=[46.831])
    rtt_percentiles: List[float] = Field(
        min_length=101, max_length=101, examples=[example_rtt_percentiles]
    )

    rtt_log_median: float = Field(gt=0, examples=[5.122])
    rtt_log_mean: float = Field(gt=0, examples=[5.167])
    rtt_log_std: float = Field(gt=0, examples=[0.191])

    @computed_field(
        examples=[(119.844, 256.873)],
        description="The 95% confidence interval calculated in log-space",
    )
    @property
    def expected_rtt_range(self) -> Tuple[float, float]:
        # This is the log_mean +- 2 log_std, then converted back to non-log space.
        # This is not just the mean + 2x std b/c we calculate the expected
        #   range in log-space (due to high skewness)
        edge = self.rtt_log_std * 2
        return float(np.exp(self.rtt_log_mean - edge)), float(
            np.exp(self.rtt_log_mean + edge)
        )

    def boxplot(self):
        """
        Render a boxplot from the RTT percentiles.
        """
        try:
            # annoying pycharm error
            import matplotlib.pyplot as plt
        except ImportError as e:
            raise e

        p = self.rtt_percentiles
        data = {
            "whislo": p[5],
            "q1": p[25],
            "med": p[50],
            "q3": p[75],
            "whishi": p[95],
            "fliers": [p[0]] + ([p[100]] if p[100] > p[95] else []),
        }

        fig, ax = plt.subplots(figsize=(4, 1.5))
        ax.bxp([data], showfliers=True, vert=False)
        ax.set_title(f"RTT Boxplot for {self.country_iso}")
        ax.set_xlabel("RTT (ms)")
        ax.set_yticks([])

        plt.tight_layout()
        plt.show()
