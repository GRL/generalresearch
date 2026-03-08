from datetime import date, datetime
from typing import List, Optional, Tuple

import pandas as pd

from generalresearch.managers.base import PostgresManager
from generalresearch.managers.leaderboard import country_timezone
from generalresearch.models.thl.user_streak import (
    PERIOD_TO_PD_FREQ,
    StreakFulfillment,
    StreakPeriod,
    StreakState,
    UserStreak,
)


class UserStreakManager(PostgresManager):

    def get_user_country(self, user_id: int) -> None:
        # For the purposes of streaks, the country they are in is
        #   the first country they were active in
        res = self.pg_config.execute_sql_query(
            """
        SELECT country_iso 
        FROM thl_session
        WHERE user_id = %(user_id)s
        ORDER BY started
        LIMIT 1;
        """,
            {"user_id": user_id},
        )
        if res:
            return res[0]["country_iso"]  # type: ignore

        return None

    def get_user_active_days_query(self, user_id: int, country_iso: str):
        tz = country_timezone()[country_iso]
        query = """
        SELECT
            (started AT TIME ZONE %(tz)s)::date AS d,
            MAX((status = 'c')::int) AS is_complete
        FROM thl_session
        WHERE user_id = %(user_id)s
          AND status IS NOT NULL
          AND (status_code_1 IS NULL OR status_code_1 NOT IN (16, 18, 19))
        GROUP BY d
        ORDER BY d;"""
        params = {"user_id": user_id, "tz": str(tz)}
        return self.pg_config.execute_sql_query(query, params)

    def get_user_streaks(
        self, user_id: int, country_iso: Optional[str] = None
    ) -> List[UserStreak]:
        country_iso = country_iso or self.get_user_country(user_id=user_id)
        if country_iso is None:
            return []
        res = self.get_user_active_days_query(user_id=user_id, country_iso=country_iso)

        active_days = [x["d"] for x in res]
        complete_days = [x["d"] for x in res if x["is_complete"]]

        streaks: List[UserStreak] = []

        for period in StreakPeriod:
            for fulfillment, days in [
                (StreakFulfillment.ACTIVE, active_days),
                (StreakFulfillment.COMPLETE, complete_days),
            ]:
                current, longest, state, last_period = compute_streaks_from_days(
                    days=days,
                    country_iso=country_iso,
                    period=StreakPeriod(period),
                )

                streaks.append(
                    UserStreak(
                        fulfillment=fulfillment,
                        period=StreakPeriod(period),
                        current_streak=current,
                        longest_streak=longest,
                        state=state,
                        last_fulfilled_period_start=last_period,
                        country_iso=country_iso,
                        user_id=user_id,
                    )
                )
        # Don't return any that are empty (no current or longest streak)
        streaks = [s for s in streaks if s.current_streak or s.longest_streak]

        return streaks


def compute_streaks_from_days(
    days: List[date],
    country_iso: str,
    period: StreakPeriod,
    today: Optional[date] = None,
) -> Tuple[int, int, StreakState, Optional[date]]:
    """
    :returns: (current_streak, longest_streak, streak_state, last_period_start)
    """

    if not days:
        return 0, 0, StreakState.BROKEN, None

    tz = country_timezone()[country_iso]
    today = today or datetime.now(tz=tz).date()

    freq = PERIOD_TO_PD_FREQ[period]

    # Convert raw days -> pandas Periods
    periods = sorted({pd.Timestamp(d).to_period(freq) for d in days})
    today_period = pd.Timestamp(today).to_period(freq)

    # period -> period start dates
    # period_starts = [p.start_time.date() for p in periods]
    # today_start = today_period.start_time.date()

    # ---- longest streak ----
    longest = 1
    running = 1

    for i in range(1, len(periods)):
        if periods[i] == periods[i - 1] + 1:
            running += 1
        else:
            longest = max(longest, running)
            running = 1

    longest = max(longest, running)

    # ---- current streak ----
    last_run = 1
    for i in range(len(periods) - 1, 0, -1):
        if periods[i] == periods[i - 1] + 1:
            last_run += 1
        else:
            break

    last_period = periods[-1]
    last_period_start = last_period.start_time.date()

    if last_period == today_period:
        return last_run, longest, StreakState.ACTIVE, last_period_start

    if last_period + 1 == today_period:
        return last_run, longest, StreakState.AT_RISK, last_period_start

    return 0, longest, StreakState.BROKEN, last_period_start
