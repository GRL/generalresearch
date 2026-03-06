from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import List, Literal
from uuid import UUID, uuid3
from zoneinfo import ZoneInfo

import math
import pandas as pd
from pydantic import (
    BaseModel,
    Field,
    NonNegativeInt,
    model_validator,
    computed_field,
    AwareDatetime,
    field_validator,
)

from generalresearch.models.custom_types import UUIDStr, AwareDatetimeISO
from generalresearch.models.legacy.api_status import StatusResponse
from generalresearch.models.thl.locales import CountryISO
from generalresearch.utils.enum import ReprEnumMeta

logger = logging.getLogger()


class LeaderboardCode(str, Enum, metaclass=ReprEnumMeta):
    """
    The type of leaderboard. What the "values" represent.
    """

    # Number of Completes
    COMPLETE_COUNT = "complete_count"
    # Largest Single Payout
    LARGEST_PAYOUT = "largest_user_payout"
    # (Sum of) Total Payouts
    SUM_PAYOUTS = "sum_user_payout"


class LeaderboardFrequency(str, Enum, metaclass=ReprEnumMeta):
    """
    The time period range for the leaderboard.
    """

    # UTC midnight to UTC midnight
    DAILY = "daily"
    # Sunday Midnight to Sunday Midnight UTC
    WEEKLY = "weekly"
    # Jan 1 00:00:00
    MONTHLY = "monthly"


class LeaderboardRow(BaseModel):
    bpuid: str = Field(description="product_user_id", examples=["app-user-9329ebd"])

    rank: int = Field(
        description="The numerical data ranks (1 through n) of the values. Ties "
        "are ranked using the lowest rank in the group.",
        examples=[1],
    )

    value: int = Field(
        description="The value. The meaning of the value is dependent on the LeaderboardCode.",
        examples=[7],
    )

    def censor(self):
        censor_idx = math.ceil(len(self.bpuid) / 2)
        self.bpuid = self.bpuid[:censor_idx] + ("*" * len(self.bpuid[censor_idx:]))


class Leaderboard(BaseModel):
    """A leaderboard exists independently for each product_id in each country.
    Each country is associated with a single timezone. There is a daily,
    weekly, and monthly leaderboard.
    """

    id: UUIDStr = Field(
        description="Unique ID for this leaderboard",
        examples=["845b0074ad533df580ebb9c80cc3bce1"],
        default=None,
    )

    name: str = Field(
        description="Descriptive name for the leaderboard based on the board_code",
        examples=["Number of Completes"],
        default=None,
    )

    board_code: LeaderboardCode = Field(
        description=LeaderboardCode.as_openapi_with_value_descriptions(),
        examples=[LeaderboardCode.COMPLETE_COUNT],
    )

    bpid: UUIDStr = Field(
        description="product_id", examples=["4fe381fb7186416cb443a38fa66c6557"]
    )

    country_iso: CountryISO = Field(
        description="The country this leaderboard is for.", examples=["us"]
    )

    freq: LeaderboardFrequency = Field(
        description=LeaderboardFrequency.as_openapi_with_value_descriptions(),
        examples=[LeaderboardFrequency.DAILY],
    )

    timezone_name: str = Field(
        description="The timezone for the requested country",
        examples=["America/New_York"],
        default=None,
    )

    sort_order: Literal["ascending", "descending"] = Field(default="descending")

    row_count: NonNegativeInt = Field(
        description="The total number of rows in the leaderboard.", examples=[2]
    )

    rows: List[LeaderboardRow] = Field(
        default_factory=list,
        examples=[
            [
                LeaderboardRow(bpuid="app-user-9329ebd", value=4, rank=1),
                LeaderboardRow(bpuid="app-user-7923skw", value=3, rank=2),
            ]
        ],
    )

    period_start_local: AwareDatetime = Field(
        description="The start of the time period covered by this board in local time, tz-aware",
        examples=[
            datetime(2024, 7, 12, 0, 0, 0, 0, tzinfo=ZoneInfo("America/New_York"))
        ],
        # This can't be excluded or else cacheing doesn't work.
        # If we want it not in the API response, we need to make a LeaderboardOut
        # exclude=True,
    )

    period_end_local: AwareDatetime = Field(
        description="The end of the time period covered by this board in local time, tz-aware",
        examples=[
            datetime(
                2024,
                7,
                12,
                23,
                59,
                59,
                999999,
                tzinfo=ZoneInfo("America/New_York"),
            )
        ],
        # exclude=True,
        default=None,
    )

    @property
    def board_key(self):
        product_id = self.bpid
        country_iso = self.country_iso
        freq = self.freq
        board_code = self.board_code
        date_str = self.period_start_local.strftime("%Y-%m-%d")
        return f"leaderboard:{product_id}:{country_iso}:{freq.value}:{date_str}:{board_code.value}"

    @property
    def period_start_utc(self) -> datetime:
        # The start of the time period covered by this board in UTC, tz-aware
        # e.g. datetime(2024, 7, 12, 4, 0, 0, 0, tzinfo=timezone.utc)
        return self.period_start_local.astimezone(timezone.utc)

    @property
    def period_end_utc(self) -> datetime:
        # The end of the time period covered by this board in UTC, tz-aware
        # e.g. datetime(2024, 7, 13, 3, 59, 59, 999999, tzinfo=timezone.utc)
        return self.period_end_local.astimezone(timezone.utc)

    @computed_field(
        description="(unix timestamp) The start time of the time range this leaderboard covers.",
        examples=[1720756800],
    )
    def start_timestamp(self) -> int:
        return int(self.period_start_utc.timestamp())

    @computed_field(
        description="(unix timestamp) The end time of the time range this leaderboard covers.",
        examples=[1720843199],
    )
    def end_timestamp(self) -> int:
        return int(self.period_end_utc.timestamp())

    @property
    def timezone(self) -> ZoneInfo:
        return ZoneInfo(self.timezone_name)

    @computed_field(description="The UTC offset for the timezone", examples=["-0400"])
    @property
    def utc_offset(self) -> str:
        return self.period_start_local.strftime("%z")

    @computed_field(
        description="The start time of the time range this leaderboard covers "
        "(local time, in the leaderboard's timezone).",
        examples=["2024-07-12T00:00:00-04:00"],
    )
    @property
    def local_start_time(self) -> str:
        return self.period_start_local.isoformat()

    @computed_field(
        description="The end time of the time range this leaderboard covers "
        "(local time, in the leaderboard's timezone).",
        examples=["2024-07-12T23:59:59.999999-04:00"],
    )
    @property
    def local_end_time(self) -> str:
        return self.period_end_local.isoformat()

    @computed_field(
        description="A formatted string for time period covered by this "
        "leaderboard. Can be used to display to users.",
        examples=["2024-02-07 to 2024-02-08"],
    )
    @property
    def start_end_str(self) -> str:
        start = self.period_start_local.strftime("%Y-%m-%d")
        end = (self.period_end_local + timedelta(minutes=1)).strftime("%Y-%m-%d")
        return f"{start} to {end}"

    @model_validator(mode="after")
    def set_name(self):
        if self.name is None:
            self.name = {
                LeaderboardCode.COMPLETE_COUNT: "Number of Completes",
                LeaderboardCode.LARGEST_PAYOUT: "Largest Single Payout",
                LeaderboardCode.SUM_PAYOUTS: "Total Payouts",
            }[self.board_code]
        return self

    @model_validator(mode="after")
    def set_id(self):
        if self.id is None:
            self.id = self.generate_leaderboard_id()
        return self

    @model_validator(mode="after")
    def set_timezone_name(self):
        if self.timezone_name is None:
            self.timezone_name = self.period_start_local.tzinfo.key
        return self

    @model_validator(mode="after")
    def validate_period(self):
        t = pd.Timestamp(self.period_start_local).tz_localize(tz=None)
        freq_pd = {
            LeaderboardFrequency.WEEKLY: "W-SUN",
            LeaderboardFrequency.DAILY: "D",
            LeaderboardFrequency.MONTHLY: "M",
        }[self.freq]
        period = t.to_period(freq_pd)
        period_start_local = period.start_time.to_pydatetime().replace(
            tzinfo=self.timezone
        )
        period_end_local = (
            period.end_time.replace(nanosecond=0)
            .to_pydatetime()
            .replace(tzinfo=self.timezone)
        )
        assert (
            period_start_local == self.period_start_local
        ), f"invalid period_start_local {self.period_start_local}. The period starts at {period_start_local}"
        if self.period_end_local is not None:
            assert self.period_end_local == period_end_local, "invalid period"
        else:
            self.period_end_local = period_end_local
        return self

    @field_validator("rows")
    @classmethod
    def sort_rows(cls, rows):
        return sorted(rows, key=lambda row: (row.rank, row.bpuid))

    def generate_leaderboard_id(self) -> str:
        # Consistently generates the same UUID for a given leaderboard instance.
        # https://docs.python.org/3/library/uuid.html#uuid.uuid3
        u = UUID("abee11ed-2943-4fb3-88c5-943921765dc0")  # randomly chosen
        name_str = "-".join(
            [
                self.board_code.value,
                self.bpid,
                self.country_iso,
                self.freq.value,
                str(self.start_timestamp),
            ]
        )
        return uuid3(u, name_str).hex

    def censor(self):
        for row in self.rows:
            row.censor()


class LeaderboardResponse(StatusResponse):
    leaderboard: Leaderboard = Field()


class LeaderboardWinner(BaseModel):
    rank: int = Field(
        description="The user's final rank in the leaderboard", examples=[1]
    )
    freq: LeaderboardFrequency = Field(
        description=LeaderboardFrequency.as_openapi_with_value_descriptions(),
        examples=[LeaderboardFrequency.DAILY],
    )
    board_code: LeaderboardCode = Field(
        description=LeaderboardCode.as_openapi_with_value_descriptions(),
        examples=[LeaderboardCode.COMPLETE_COUNT],
    )
    country_iso: CountryISO = Field(
        description="The country this leaderboard is for.", examples=["us"]
    )
    issued: AwareDatetimeISO = Field(
        description="When the prize was issued.",
        examples=["2022-10-17T05:59:14.570231Z"],
    )
    bpuid: str = Field(description="product_user_id", examples=["app-user-9329ebd"])
    description: str = Field(examples=["Bonus for daily contest"])
    amount: int = Field(description="(USD cents) The reward amount", examples=[1000])
    amount_str: str = Field(
        description="The amount as a formatted string in USD. Can be "
        "displayed to the user.",
        examples=["$10.00"],
    )
    contest_start: AwareDatetimeISO = Field(
        description="When the leaderboard started",
        examples=["2022-10-16T04:00:00Z"],
    )
    contest_end: AwareDatetimeISO = Field(
        description="When the leaderboard ended",
        examples=["2022-10-17T04:00:00Z"],
    )


class LeaderboardWinnerResponse(StatusResponse):
    winners: List[LeaderboardWinner] = Field(default_factory=list)
