from datetime import date, datetime, timedelta
from enum import Enum
from typing import Optional, Tuple

import pandas as pd
from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    NonNegativeInt,
    PositiveInt,
    computed_field,
    model_validator,
)
from pydantic.json_schema import SkipJsonSchema
from zoneinfo import ZoneInfo

from generalresearch.managers.leaderboard import country_timezone
from generalresearch.models import MAX_INT32
from generalresearch.models.thl.locales import CountryISO


class StreakPeriod(str, Enum):
    # Midnight to midnight in the tz associated with the user's country
    DAY = "day"
    # Sunday midnight - sunday midnight
    WEEK = "week"
    # e.g. 2000-01-01 to 2000-01-31 23:59:59.999999
    MONTH = "month"


class StreakFulfillment(str, Enum):
    """
    What has to happen for a user to fulfill a period for a streak
    """

    # User has to finish a Session (excluding Session start failure)
    ACTIVE = "active"
    # User has to complete a Session
    COMPLETE = "complete"


class StreakState(str, Enum):
    # The activity for today was completed!
    ACTIVE = "active"
    # They had activity yesterday, but not today, and can still continue today
    # Should we call this "AT_RISK" instead ?? (I had "open")
    AT_RISK = "at_risk"
    # Missed the window. Streak is broken
    BROKEN = "broken"


PERIOD_TO_PD_FREQ = {
    StreakPeriod.DAY: "D",
    StreakPeriod.WEEK: "W-SUN",  # Sunday-based week
    StreakPeriod.MONTH: "M",
}


class UserStreak(BaseModel):
    model_config = ConfigDict(
        ser_json_timedelta="float", validate_assignment=True, extra="forbid"
    )

    user_id: SkipJsonSchema[Optional[PositiveInt]] = Field(
        exclude=True, default=None, lt=MAX_INT32
    )
    country_iso: CountryISO = Field()

    # What defines the streak
    period: StreakPeriod = Field()
    fulfillment: StreakFulfillment = Field()

    current_streak: NonNegativeInt = Field()
    longest_streak: NonNegativeInt = Field()
    state: StreakState = Field()
    last_fulfilled_period_start: Optional[date] = Field(default=None)

    @computed_field()
    @property
    def timezone_name(self) -> str:
        return str(self.timezone)

    @property
    def timezone(self) -> ZoneInfo:
        return country_timezone()[self.country_iso]

    @property
    def now_local(self) -> AwareDatetime:
        return datetime.now(tz=self.timezone)

    @computed_field()
    @property
    def current_period_bounds(self) -> Tuple[AwareDatetime, AwareDatetime]:
        return self.get_period_bounds(datetime.now(tz=self.timezone).date())

    @computed_field()
    @property
    def last_fulfilled_period_bounds(self) -> Optional[Tuple[datetime, datetime]]:
        return self.get_period_bounds(self.last_fulfilled_period_start)

    @computed_field()
    @property
    def time_remaining_in_period(self) -> Optional[timedelta]:
        # Time left to continue your streak
        if self.state in {StreakState.BROKEN, StreakState.ACTIVE}:
            return None
        period_end = self.current_period_bounds[1]
        return period_end - self.now_local

    @model_validator(mode="after")
    def check_state(self):
        if self.state == StreakState.BROKEN:
            assert (
                self.current_streak == 0
            ), "StreakState.BROKEN but current_streak not 0"

        if self.current_streak != 0:
            assert (
                self.state != StreakState.BROKEN
            ), "current_streak not 0 but StreakState.BROKEN"
        return self

    @model_validator(mode="after")
    def check_longest_streak(self):
        assert (
            self.longest_streak >= self.current_streak
        ), "Current streak can't be longer than longest streak"
        return self

    def get_period_bounds(
        self, start_date: date
    ) -> Optional[Tuple[datetime, datetime]]:
        """
        Returns (period_start_local, period_end_local)
        Both timezone-aware.
        """

        if not start_date:
            return None

        freq = PERIOD_TO_PD_FREQ[self.period]
        tz = self.timezone

        period = pd.Timestamp(start_date).to_period(freq)
        period_start_local = period.start_time.to_pydatetime(warn=False).replace(
            tzinfo=tz
        )
        period_end_local = period.end_time.to_pydatetime(warn=False).replace(tzinfo=tz)

        return period_start_local, period_end_local
