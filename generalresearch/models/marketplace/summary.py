from __future__ import annotations

from abc import ABC
from typing import Collection, Dict, List, Literal, Optional

import numpy as np
from pydantic import BaseModel, ConfigDict, Field, computed_field
from typing_extensions import Self

from generalresearch.models.thl.stats import StatisticalSummary


class MarketplaceSummary(BaseModel):
    model_config = ConfigDict(validate_assignment=True)

    inventory: MarketplaceInventorySummary = Field(
        description="Inventory of the marketplace"
    )
    user_activity: Optional[str] = Field(
        description="User activity of the marketplace", default=None
    )


class MarketplaceInventorySummary(BaseModel):
    model_config = ConfigDict(validate_assignment=True)

    live_tasks: List[CountStat] = Field(
        default_factory=list,
        description="The count of tasks that are currently live",
    )
    live_gen_pop_tasks: List[CountStat] = Field(
        default_factory=list,
        description="The count of gen-pop tasks that are currently live",
    )
    tasks_created: List[CountStat] = Field(
        default_factory=list,
        description="The count of tasks created",
    )
    required_finishes: List[CountStat] = Field(
        default_factory=list,
        description="Number of finishes needed across all live tasks",
    )

    payout: List[StatisticalSummaryStat] = Field(
        default_factory=list,
        description="The distribution of payouts for all live tasks",
    )
    expected_duration: List[StatisticalSummaryStat] = Field(
        default_factory=list,
        description="The distribution of expected durations for all live tasks",
    )
    required_finishes_per_task: List[StatisticalSummaryStat] = Field(
        default_factory=list,
        description="The distribution of required finishes on all live tasks",
    )


FacetKey = Literal["country_iso", "day", "month"]


class Stat(BaseModel, ABC):
    facet: Dict[FacetKey, str | int | float] = Field(
        examples=[{"country_iso": "us"}], description="The grouping criteria"
    )


class CountStat(Stat):
    count: int = Field(description="The count value for the given metric and facet")


class StatisticalSummaryStat(Stat):
    value: StatisticalSummaryValue = Field(
        description="Statistical Summary for the given metric and facet"
    )


class StatisticalSummaryValue(StatisticalSummary):
    min: float = Field()
    max: float = Field()
    mean: float = Field()
    q1: float = Field()
    q2: float = Field(description="equal to the median")
    q3: float = Field()

    @classmethod
    def from_values(cls, values: Collection[int | float]) -> Self:
        values = sorted(values)
        return cls(
            min=min(values),
            max=max(values),
            q1=np.percentile(values, 25),
            q2=np.percentile(values, 50),
            q3=np.percentile(values, 75),
            mean=np.mean(values),
        )

    @computed_field
    @property
    def lower_whisker(self) -> float:
        return self.q1 - (1.5 * self.iqr)

    @computed_field
    @property
    def upper_whisker(self) -> float:
        return self.q3 + (1.5 * self.iqr)


d = MarketplaceSummary(
    inventory=MarketplaceInventorySummary(
        live_tasks=[
            CountStat(
                facet={"country_iso": "us"},
                count=10,
            ),
            CountStat(
                facet={"country_iso": "ca"},
                count=2,
            ),
            CountStat(facet={}, count=15),
        ],
        tasks_created=[
            CountStat(
                facet={"day": "2024-11-02"},
                count=5,
            ),
            CountStat(
                facet={"day": "2024-11-02", "country_iso": "us"},
                count=4,
            ),
            CountStat(
                facet={"day": "2024-11-01", "country_iso": "us"},
                count=4,
            ),
        ],
        payout=[
            StatisticalSummaryStat(
                facet={},
                value=StatisticalSummaryValue(
                    min=14, q1=40, q2=96, q3=123, max=420, mean=100
                ),
            ),
            StatisticalSummaryStat(
                facet={"country_iso": "us"},
                value=StatisticalSummaryValue(
                    min=16, q1=42, q2=98, q3=123, max=400, mean=100
                ),
            ),
        ],
    )
)
