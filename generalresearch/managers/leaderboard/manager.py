from datetime import datetime, timedelta, timezone
from decimal import Decimal
from functools import cached_property
from typing import TYPE_CHECKING, List, Optional

import pandas as pd
from pandas import Period
from pydantic import AwareDatetime, NaiveDatetime
from redis import Redis

from generalresearch.managers.leaderboard import country_timezone
from generalresearch.models.thl.leaderboard import (
    Leaderboard,
    LeaderboardCode,
    LeaderboardFrequency,
    LeaderboardRow,
)

if TYPE_CHECKING:
    from generalresearch.models.thl.session import Session


class LeaderboardManager:
    def __init__(
        self,
        redis_client: Redis,
        board_code: LeaderboardCode,
        freq: LeaderboardFrequency,
        product_id: str,
        country_iso: str,
        within_time: Optional[NaiveDatetime | AwareDatetime] = None,
    ):
        """
        :param within_time: Any local datetime falling within the desired leaderboard period.
            e.g. (if freq=daily) within_time = 2024-04-12 01:02:03 will get the '2024-04-12' board
        """
        self.redis_client = redis_client
        self.timezone = country_timezone()[country_iso]

        self.board_code = board_code
        self.freq = freq
        self.product_id = product_id
        self.country_iso = country_iso
        self.within_time_aware = None
        if within_time is None:
            self.within_time_aware = datetime.now(tz=timezone.utc).astimezone(
                self.timezone
            )
        elif within_time.tzinfo is not None:
            self.within_time_aware = within_time.astimezone(self.timezone)
        else:
            self.within_time_aware = within_time.replace(tzinfo=self.timezone)
        self.key = self.board_key()

    @cached_property
    def period(self) -> Period:
        local_ts = self.within_time_aware
        assert local_ts.tzinfo != timezone.utc and local_ts.tzinfo is not None
        t = pd.Timestamp(local_ts).tz_localize(tz=None)
        freq_pd = {
            LeaderboardFrequency.WEEKLY: "W-SUN",
            LeaderboardFrequency.DAILY: "D",
            LeaderboardFrequency.MONTHLY: "M",
        }[self.freq]
        return t.to_period(freq_pd)

    @cached_property
    def expiration(self) -> int:
        # When the redis key for this board should expire
        return {
            LeaderboardFrequency.DAILY: int(timedelta(days=90).total_seconds()),
            LeaderboardFrequency.WEEKLY: int(timedelta(days=365).total_seconds()),
            LeaderboardFrequency.MONTHLY: int(timedelta(days=365 * 2).total_seconds()),
        }[self.freq]

    def board_key(self) -> str:
        product_id = self.product_id
        country_iso = self.country_iso
        freq = self.freq
        board_code = self.board_code
        date_str = self.period.start_time.to_pydatetime().strftime("%Y-%m-%d")
        return f"leaderboard:{product_id}:{country_iso}:{freq.value}:{date_str}:{board_code.value}"

    def get_row_count(self) -> int:
        # How many rows (unique users) does this leaderboard have?
        return self.redis_client.zcard(self.key) or 0

    def get_leaderboard_rows(
        self,
        limit: Optional[int] = None,
    ) -> List[LeaderboardRow]:
        limit = limit if limit else 0
        res = self.redis_client.zrange(
            self.key, start=0, end=limit - 1, withscores=True, desc=True
        )
        # We re-rank using pandas min value for ties. Redis does not consider ties in ranking.
        s = pd.DataFrame(res, columns=["bpuid", "value"]).sort_values(
            by="value", ascending=False
        )
        s["rank"] = s["value"].rank(method="min", ascending=False)
        return [
            LeaderboardRow(bpuid=r.bpuid, value=r.value, rank=r.rank)
            for r in s.itertuples()
        ]

    def get_personal_leaderboard_rows(
        self, bp_user_id: str, limit: Optional[int] = 5
    ) -> List[LeaderboardRow]:
        # We can't just grab this user's rank and nearby rows b/c redis does
        #   not handle ties the same way we do (in redis, each value is a
        #   unique rank, we use lowest rank for all ties). So we have to just
        #   grab everything, then filter
        limit = limit if limit is not None else 5
        rows = self.get_leaderboard_rows()
        rows = sorted(rows, key=lambda x: x.value, reverse=True)
        user_indices = [
            (i, row) for i, row in enumerate(rows) if row.bpuid == bp_user_id
        ]
        if not user_indices:
            return rows[: limit * 2]
        user_idx = user_indices[0][0]
        user_row = user_indices[0][1]
        if user_row.rank == max([row.rank for row in rows]):
            user_idx = [i for i, row in enumerate(rows) if row.rank == user_row.rank][0]
        start: int = max(user_idx - limit, 0)
        end: int = min(user_idx + limit + 1, len(rows))

        return rows[start:end]

    def get_leaderboard(
        self,
        limit: Optional[int] = None,
        bp_user_id: Optional[str] = None,
    ) -> Leaderboard:

        if bp_user_id:
            rows = self.get_personal_leaderboard_rows(
                bp_user_id=bp_user_id, limit=limit
            )
        else:
            rows = self.get_leaderboard_rows(
                limit=limit,
            )
        total = self.get_row_count()

        tz = self.timezone

        return Leaderboard(
            board_code=self.board_code,
            country_iso=self.country_iso,
            bpid=self.product_id,
            freq=self.freq,
            row_count=total,
            rows=rows,
            period_start_local=self.period.start_time.to_pydatetime().replace(
                tzinfo=self.timezone
            ),
            period_end_local=self.period.end_time.replace(nanosecond=0)
            .to_pydatetime()
            .replace(tzinfo=self.timezone),
            timezone_name=str(tz),
        )

    def hit_complete_count(self, product_user_id: str) -> None:
        assert (
            self.board_code == LeaderboardCode.COMPLETE_COUNT
        ), "wrong kind of leaderboard"
        self.redis_client.zincrby(self.key, amount=1, value=product_user_id)
        self.redis_client.expire(self.key, time=self.expiration)

        return None

    def hit_sum_payouts(self, product_user_id: str, user_payout: Decimal) -> None:
        assert (
            self.board_code == LeaderboardCode.SUM_PAYOUTS
        ), "wrong kind of leaderboard"
        self.redis_client.zincrby(
            self.key, amount=round(user_payout * 100), value=product_user_id
        )
        self.redis_client.expire(self.key, time=self.expiration)

        return None

    def hit_largest_payout(self, product_user_id: str, user_payout: Decimal) -> None:
        assert (
            self.board_code == LeaderboardCode.LARGEST_PAYOUT
        ), "wrong kind of leaderboard"
        # Only sets the value if the new value is greater than the existing
        self.redis_client.zadd(
            self.key, {product_user_id: round(user_payout * 100)}, gt=True
        )
        self.redis_client.expire(self.key, time=self.expiration)

        return None

    def hit(self, session: "Session") -> None:
        user = session.user
        match self.board_code:
            case LeaderboardCode.COMPLETE_COUNT:
                return self.hit_complete_count(product_user_id=user.product_user_id)
            case LeaderboardCode.SUM_PAYOUTS:
                return self.hit_sum_payouts(
                    product_user_id=user.product_user_id,
                    user_payout=session.user_payout,
                )
            case LeaderboardCode.LARGEST_PAYOUT:
                return self.hit_largest_payout(
                    product_user_id=user.product_user_id,
                    user_payout=session.user_payout,
                )

        return None
