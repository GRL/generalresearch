import copy
from datetime import datetime, timezone, timedelta, date
from decimal import Decimal
from zoneinfo import ZoneInfo

import pytest

from generalresearch.managers.thl.user_streak import compute_streaks_from_days
from generalresearch.models.thl.definitions import StatusCode1, Status
from generalresearch.models.thl.user_streak import (
    UserStreak,
    StreakState,
    StreakPeriod,
    StreakFulfillment,
)


def test_compute_streaks_from_days():
    days = [
        date(2026, 1, 1),
        date(2026, 1, 4),
        date(2026, 1, 5),
        date(2026, 1, 6),
        date(2026, 1, 8),
        date(2026, 1, 9),
        date(2026, 2, 11),
        date(2026, 2, 12),
    ]

    # Active
    today = date(2026, 2, 12)
    res = compute_streaks_from_days(days, "us", period=StreakPeriod.DAY, today=today)
    assert res == (2, 3, StreakState.ACTIVE, date(2026, 2, 12))

    # At Risk
    today = date(2026, 2, 13)
    res = compute_streaks_from_days(days, "us", period=StreakPeriod.DAY, today=today)
    assert res == (2, 3, StreakState.AT_RISK, date(2026, 2, 12))

    # Broken
    today = date(2026, 2, 14)
    res = compute_streaks_from_days(days, "us", period=StreakPeriod.DAY, today=today)
    assert res == (0, 3, StreakState.BROKEN, date(2026, 2, 12))

    # Monthly, active
    today = date(2026, 2, 14)
    res = compute_streaks_from_days(days, "us", period=StreakPeriod.MONTH, today=today)
    assert res == (2, 2, StreakState.ACTIVE, date(2026, 2, 1))

    # monthly, at risk
    today = date(2026, 3, 1)
    res = compute_streaks_from_days(days, "us", period=StreakPeriod.MONTH, today=today)
    assert res == (2, 2, StreakState.AT_RISK, date(2026, 2, 1))

    # monthly, broken
    today = date(2026, 4, 1)
    res = compute_streaks_from_days(days, "us", period=StreakPeriod.MONTH, today=today)
    assert res == (0, 2, StreakState.BROKEN, date(2026, 2, 1))


@pytest.fixture
def broken_active_streak(user):
    return [
        UserStreak(
            period=StreakPeriod.DAY,
            fulfillment=StreakFulfillment.ACTIVE,
            country_iso="us",
            user_id=user.user_id,
            last_fulfilled_period_start=date(2025, 2, 11),
            current_streak=0,
            longest_streak=1,
            state=StreakState.BROKEN,
        ),
        UserStreak(
            period=StreakPeriod.WEEK,
            fulfillment=StreakFulfillment.ACTIVE,
            country_iso="us",
            user_id=user.user_id,
            last_fulfilled_period_start=date(2025, 2, 10),
            current_streak=0,
            longest_streak=1,
            state=StreakState.BROKEN,
        ),
        UserStreak(
            period=StreakPeriod.MONTH,
            fulfillment=StreakFulfillment.ACTIVE,
            country_iso="us",
            user_id=user.user_id,
            last_fulfilled_period_start=date(2025, 2, 1),
            current_streak=0,
            longest_streak=1,
            state=StreakState.BROKEN,
        ),
    ]


def create_session_fail(session_manager, start, user):
    session = session_manager.create_dummy(started=start, country_iso="us", user=user)
    session_manager.finish_with_status(
        session,
        finished=start + timedelta(minutes=1),
        status=Status.FAIL,
        status_code_1=StatusCode1.BUYER_FAIL,
    )


def create_session_complete(session_manager, start, user):
    session = session_manager.create_dummy(started=start, country_iso="us", user=user)
    session_manager.finish_with_status(
        session,
        finished=start + timedelta(minutes=1),
        status=Status.COMPLETE,
        status_code_1=StatusCode1.COMPLETE,
        payout=Decimal(1),
    )


def test_user_streak_empty(user_streak_manager, user):
    streaks = user_streak_manager.get_user_streaks(
        user_id=user.user_id, country_iso="us"
    )
    assert streaks == []


def test_user_streaks_active_broken(
    user_streak_manager, user, session_manager, broken_active_streak
):
    # Testing active streak, but broken (not today or yesterday)
    start1 = datetime(2025, 2, 12, tzinfo=timezone.utc)
    end1 = start1 + timedelta(minutes=1)

    # abandon counts as inactive
    session = session_manager.create_dummy(started=start1, country_iso="us", user=user)
    streak = user_streak_manager.get_user_streaks(user_id=user.user_id)
    assert streak == []

    # session start fail counts as inactive
    session_manager.finish_with_status(
        session,
        finished=end1,
        status=Status.FAIL,
        status_code_1=StatusCode1.SESSION_START_QUALITY_FAIL,
    )
    streak = user_streak_manager.get_user_streaks(
        user_id=user.user_id, country_iso="us"
    )
    assert streak == []

    # Create another as a real failure
    create_session_fail(session_manager, start1, user)

    # This is going to be the day before b/c midnight utc is 8pm US
    last_active_day = start1.astimezone(ZoneInfo("America/New_York")).date()
    assert last_active_day == date(2025, 2, 11)
    expected_streaks = broken_active_streak.copy()
    streaks = user_streak_manager.get_user_streaks(user_id=user.user_id)
    assert streaks == expected_streaks

    # Create another the next day
    start2 = start1 + timedelta(days=1)
    create_session_fail(session_manager, start2, user)

    last_active_day = start2.astimezone(ZoneInfo("America/New_York")).date()
    expected_streaks = copy.deepcopy(broken_active_streak)
    expected_streaks[0].longest_streak = 2
    expected_streaks[0].last_fulfilled_period_start = last_active_day

    streaks = user_streak_manager.get_user_streaks(
        user_id=user.user_id, country_iso="us"
    )
    assert streaks == expected_streaks


def test_user_streak_complete_active(user_streak_manager, user, session_manager):
    """Testing active streak that is today"""

    # They completed yesterday NY time. Today isn't over so streak is pending
    start1 = datetime.now(tz=ZoneInfo("America/New_York")) - timedelta(days=1)
    create_session_complete(session_manager, start1.astimezone(tz=timezone.utc), user)

    last_complete_day = start1.date()
    expected_streak = UserStreak(
        longest_streak=1,
        current_streak=1,
        state=StreakState.AT_RISK,
        last_fulfilled_period_start=last_complete_day,
        country_iso="us",
        user_id=user.user_id,
        fulfillment=StreakFulfillment.COMPLETE,
        period=StreakPeriod.DAY,
    )
    streaks = user_streak_manager.get_user_streaks(
        user_id=user.user_id, country_iso="us"
    )
    streak = [
        s
        for s in streaks
        if s.fulfillment == StreakFulfillment.COMPLETE and s.period == StreakPeriod.DAY
    ][0]
    assert streak == expected_streak

    # And now they complete today
    start2 = datetime.now(tz=ZoneInfo("America/New_York"))
    create_session_complete(session_manager, start2.astimezone(tz=timezone.utc), user)
    last_complete_day = start2.date()
    expected_streak = UserStreak(
        longest_streak=2,
        current_streak=2,
        state=StreakState.ACTIVE,
        last_fulfilled_period_start=last_complete_day,
        country_iso="us",
        user_id=user.user_id,
        fulfillment=StreakFulfillment.COMPLETE,
        period=StreakPeriod.DAY,
    )

    streaks = user_streak_manager.get_user_streaks(
        user_id=user.user_id, country_iso="us"
    )
    streak = [
        s
        for s in streaks
        if s.fulfillment == StreakFulfillment.COMPLETE and s.period == StreakPeriod.DAY
    ][0]
    assert streak == expected_streak
