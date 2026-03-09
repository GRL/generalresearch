import random
from collections import defaultdict
from typing import Collection, List, Optional

from pydantic import BaseModel, ConfigDict, Field

from generalresearch.models.thl.definitions import ReportValue
from generalresearch.models.thl.user import BPUIDStr

# If a report is made with multiple values, we'll take the one with the
# highest priority
REPORT_PRIORITY = defaultdict(
    lambda: 2,
    {
        ReportValue.REASON_UNKNOWN: 0,  # lowest priority
        ReportValue.TECHNICAL_ERROR: 1,  # next highest
        ReportValue.DIDNT_LIKE: 1,
    },
)


def prioritize_report_values(
    report_values: Collection[ReportValue],
) -> Optional[ReportValue]:
    if not report_values:
        return None
    report_values = list(set(report_values))
    random.shuffle(report_values)
    return sorted(report_values, key=lambda x: REPORT_PRIORITY[x])[-1]


class ReportTask(BaseModel):
    model_config = ConfigDict(extra="forbid")

    bpuid: BPUIDStr = Field(
        title="product_user_id",
        description="The unique identifier for the user, which is set by the "
        "Supplier.",
        examples=["app-user-9329ebd"],
    )

    reasons: List[ReportValue] = Field(
        description=ReportValue.as_openapi_with_value_descriptions(),
        examples=[[3, 4]],
        default_factory=list,
    )

    notes: str = Field(
        default="", examples=["The survey wanted to watch me eat Haejang-guk"]
    )
