from datetime import datetime, timezone, timedelta
from enum import Enum
from typing import Literal, List, Tuple

import pandas as pd
from pydantic import BaseModel, Field, model_validator, computed_field

from generalresearch.models.custom_types import AwareDatetimeISO


class ReportType(Enum):
    POP_SESSION = "pop_session"
    POP_EVENT = "pop_event"
    POP_LEDGER = "pop_ledger"


class ReportRequest(BaseModel):
    report_type: ReportType = Field(default=ReportType.POP_SESSION)

    index0: str = Field(
        default="started",
    )
    index1: str = Field(default="product_id")

    start: AwareDatetimeISO = Field(
        default_factory=lambda: datetime.now(tz=timezone.utc) - timedelta(days=14)
    )
    end: AwareDatetimeISO = Field(default_factory=lambda: datetime.now(tz=timezone.utc))

    interval: Literal["5min", "15min", "1h", "6h", "12h", "1d"] = "1h"
    include_open_bucket: bool = Field(default=True)

    @computed_field(
        title="Start floor",
        description="The datetime that this report starts from",
        examples=[datetime(year=2025, month=5, day=1)],
        return_type=datetime,
    )
    @property
    def start_floor(self) -> AwareDatetimeISO:
        """Always floor start time to the interval."""
        return self.ts_start.floor(self.interval).to_pydatetime()

    # --- Validation ---

    @model_validator(mode="after")
    def check_start_end(self):

        assert self.start < self.end - timedelta(
            hours=1
        ), "Reports less than 1 hour are not supported"

        assert (
            self.end - self.start
        ).days < 365 * 10, "Report.starts must not be longer than 10 years"

        return self

    @model_validator(mode="after")
    def check_start_end_tz(self):
        assert self.start.tzinfo == self.end.tzinfo == timezone.utc
        return self

    @model_validator(mode="after")
    def check_index0_for_schema(self):
        if self.report_type == ReportType.POP_SESSION:
            assert self.index0 in [
                "started"
            ], f"session exports can't split by {self.index0}"
        return self

    @model_validator(mode="after")
    def check_index1_for_schema(self):
        if self.report_type == ReportType.POP_SESSION:
            assert self.index1 in [
                "product_id",
                "user_id",
                "country_iso",
                "device_type",
                "status",
                "status_code_1",
                "status_code_2",
            ], f"session exports can't split by {self.index1}"

        if self.report_type == ReportType.POP_EVENT:
            assert self.index1 in [
                "product_id",
                "user_id",
                "country_iso",
                "device_type",
                "source",
                "buyer_id",
                "survey_id",
                "status",
                "status_code_1",
                "status_code_2",
            ], f"wall exports can't split by {self.index1}"

        return self

    # --- Properties ---
    @property
    def pd_interval(self) -> pd.Interval:
        return pd.Interval(
            left=pd.Timestamp(self.start_floor),
            right=pd.Timestamp(self.end),
            closed="both",
        )

    @property
    def interval_timedelta(self) -> pd.Timedelta:
        return pd.Timedelta(self.interval)

    @property
    def start_floor_naive(self) -> datetime:
        return self.start_floor.replace(tzinfo=None)

    @property
    def end_naive(self) -> datetime:
        return datetime.now(tz=None)

    @property
    def ts_start(self) -> pd.Timestamp:
        return pd.Timestamp(self.start)

    @property
    def ts_start_floor(self) -> pd.Timestamp:
        return pd.Timestamp(self.start_floor)

    @property
    def ts_end(self) -> pd.Timestamp:
        return pd.Timestamp(self.end)

    @property
    def finish(self) -> pd.Timestamp:
        return self.end

    @property
    def ts_finish(self) -> pd.Timestamp:
        return pd.Timestamp(self.end)

    # --- Methods ---
    def buckets(self) -> pd.DatetimeIndex:
        """
        Returns all bucket start times.
        """
        return pd.date_range(
            start=self.ts_start_floor,
            end=self.ts_end,
            freq=self.interval,
            tz=timezone.utc,
        )

    def bucket_ranges(self) -> List[Tuple[pd.Timestamp, pd.Timestamp]]:
        """Returns list of (start, end) tuples for each bucket."""
        starts = self.buckets()
        return [(s, s + self.interval_timedelta) for s in starts]
