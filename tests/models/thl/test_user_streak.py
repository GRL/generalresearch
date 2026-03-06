from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pytest
from pydantic import ValidationError

from generalresearch.models.thl.user_streak import (
    UserStreak,
    StreakPeriod,
    StreakFulfillment,
    StreakState,
)


def test_user_streak_empty_fail():
    us = UserStreak(
        period=StreakPeriod.DAY,
        fulfillment=StreakFulfillment.COMPLETE,
        country_iso="us",
        user_id=1,
        last_fulfilled_period_start=None,
        current_streak=0,
        longest_streak=0,
        state=StreakState.BROKEN,
    )
    assert us.time_remaining_in_period is None

    with pytest.raises(
        ValidationError, match="StreakState.BROKEN but current_streak not 0"
    ):
        UserStreak(
            period=StreakPeriod.DAY,
            fulfillment=StreakFulfillment.COMPLETE,
            country_iso="us",
            user_id=1,
            last_fulfilled_period_start=None,
            current_streak=1,
            longest_streak=0,
            state=StreakState.BROKEN,
        )

    with pytest.raises(
        ValidationError, match="Current streak can't be longer than longest streak"
    ):
        UserStreak(
            period=StreakPeriod.DAY,
            fulfillment=StreakFulfillment.COMPLETE,
            country_iso="us",
            user_id=1,
            last_fulfilled_period_start=None,
            current_streak=1,
            longest_streak=0,
            state=StreakState.ACTIVE,
        )


def test_user_streak_remaining():
    us = UserStreak(
        period=StreakPeriod.DAY,
        fulfillment=StreakFulfillment.COMPLETE,
        country_iso="us",
        user_id=1,
        last_fulfilled_period_start=None,
        current_streak=1,
        longest_streak=1,
        state=StreakState.AT_RISK,
    )
    now = datetime.now(tz=ZoneInfo("America/New_York"))
    end_of_today = now.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(
        days=1
    )
    print(f"{now.isoformat()=}, {end_of_today.isoformat()=}")
    expected = (end_of_today - now).total_seconds()
    assert us.time_remaining_in_period.total_seconds() == pytest.approx(expected, abs=1)


def test_user_streak_remaining_month():
    us = UserStreak(
        period=StreakPeriod.MONTH,
        fulfillment=StreakFulfillment.COMPLETE,
        country_iso="us",
        user_id=1,
        last_fulfilled_period_start=None,
        current_streak=1,
        longest_streak=1,
        state=StreakState.AT_RISK,
    )
    now = datetime.now(tz=ZoneInfo("America/New_York"))
    end_of_month = (
        now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        + timedelta(days=32)
    ).replace(day=1)
    print(f"{now.isoformat()=}, {end_of_month.isoformat()=}")
    expected = (end_of_month - now).total_seconds()
    assert us.time_remaining_in_period.total_seconds() == pytest.approx(expected, abs=1)
    print(us.time_remaining_in_period)
