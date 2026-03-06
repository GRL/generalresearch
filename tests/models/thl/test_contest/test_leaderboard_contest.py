from datetime import timezone
from uuid import uuid4

import pytest

from generalresearch.currency import USDCent
from generalresearch.managers.leaderboard.manager import LeaderboardManager
from generalresearch.models.thl.contest import ContestPrize
from generalresearch.models.thl.contest.definitions import (
    ContestType,
    ContestPrizeKind,
)
from generalresearch.models.thl.contest.leaderboard import (
    LeaderboardContest,
)
from generalresearch.models.thl.contest.utils import (
    distribute_leaderboard_prizes,
)
from generalresearch.models.thl.leaderboard import LeaderboardRow
from tests.models.thl.test_contest.test_contest import TestContest


class TestLeaderboardContest(TestContest):

    @pytest.fixture
    def leaderboard_contest(
        self, product, thl_redis, user_manager
    ) -> "LeaderboardContest":
        board_key = f"leaderboard:{product.uuid}:us:weekly:2025-05-26:complete_count"

        c = LeaderboardContest(
            uuid=uuid4().hex,
            product_id=product.uuid,
            contest_type=ContestType.LEADERBOARD,
            leaderboard_key=board_key,
            name="$15 1st place, $10 2nd, $5 3rd place US weekly",
            prizes=[
                ContestPrize(
                    name="$15 Cash",
                    estimated_cash_value=USDCent(15_00),
                    cash_amount=USDCent(15_00),
                    kind=ContestPrizeKind.CASH,
                    leaderboard_rank=1,
                ),
                ContestPrize(
                    name="$10 Cash",
                    estimated_cash_value=USDCent(10_00),
                    cash_amount=USDCent(10_00),
                    kind=ContestPrizeKind.CASH,
                    leaderboard_rank=2,
                ),
                ContestPrize(
                    name="$5 Cash",
                    estimated_cash_value=USDCent(5_00),
                    cash_amount=USDCent(5_00),
                    kind=ContestPrizeKind.CASH,
                    leaderboard_rank=3,
                ),
            ],
        )
        c._redis_client = thl_redis
        c._user_manager = user_manager
        return c

    def test_init(self, leaderboard_contest, thl_redis, user_1, user_2):
        model = leaderboard_contest.leaderboard_model
        assert leaderboard_contest.end_condition.ends_at is not None

        lbm = LeaderboardManager(
            redis_client=thl_redis,
            board_code=model.board_code,
            country_iso=model.country_iso,
            freq=model.freq,
            product_id=leaderboard_contest.product_id,
            within_time=model.period_start_local,
        )

        lbm.hit_complete_count(product_user_id=user_1.product_user_id)
        lbm.hit_complete_count(product_user_id=user_2.product_user_id)
        lbm.hit_complete_count(product_user_id=user_2.product_user_id)

        lb = leaderboard_contest.get_leaderboard()
        print(lb)

    def test_win(self, leaderboard_contest, thl_redis, user_1, user_2, user_3):
        model = leaderboard_contest.leaderboard_model
        lbm = LeaderboardManager(
            redis_client=thl_redis,
            board_code=model.board_code,
            country_iso=model.country_iso,
            freq=model.freq,
            product_id=leaderboard_contest.product_id,
            within_time=model.period_start_local.astimezone(tz=timezone.utc),
        )

        lbm.hit_complete_count(product_user_id=user_1.product_user_id)
        lbm.hit_complete_count(product_user_id=user_1.product_user_id)

        lbm.hit_complete_count(product_user_id=user_2.product_user_id)

        lbm.hit_complete_count(product_user_id=user_3.product_user_id)

        leaderboard_contest.end_contest()
        assert len(leaderboard_contest.all_winners) == 3

        # Prizes are $15, $10, $5. user 2 and 3 ties for 2nd place, so they split (10 + 5)
        assert leaderboard_contest.all_winners[0].awarded_cash_amount == USDCent(15_00)
        assert (
            leaderboard_contest.all_winners[0].user.product_user_id
            == user_1.product_user_id
        )
        assert leaderboard_contest.all_winners[0].prize == leaderboard_contest.prizes[0]
        assert leaderboard_contest.all_winners[1].awarded_cash_amount == USDCent(
            15_00 / 2
        )
        assert leaderboard_contest.all_winners[2].awarded_cash_amount == USDCent(
            15_00 / 2
        )


class TestLeaderboardContestPrizes:

    def test_distribute_prizes_1(self):
        prizes = [USDCent(15_00)]
        leaderboard_rows = [
            LeaderboardRow(bpuid="a", value=20, rank=1),
            LeaderboardRow(bpuid="b", value=10, rank=2),
        ]
        result = distribute_leaderboard_prizes(prizes, leaderboard_rows)

        # a gets first prize, b gets nothing.
        assert result == {
            "a": USDCent(15_00),
        }

    def test_distribute_prizes_2(self):
        prizes = [USDCent(15_00), USDCent(10_00)]
        leaderboard_rows = [
            LeaderboardRow(bpuid="a", value=20, rank=1),
            LeaderboardRow(bpuid="b", value=10, rank=2),
        ]
        result = distribute_leaderboard_prizes(prizes, leaderboard_rows)

        # a gets first prize, b gets 2nd prize
        assert result == {
            "a": USDCent(15_00),
            "b": USDCent(10_00),
        }

    def test_distribute_prizes_3(self):
        prizes = [USDCent(15_00), USDCent(10_00)]
        leaderboard_rows = [
            LeaderboardRow(bpuid="a", value=20, rank=1),
        ]
        result = distribute_leaderboard_prizes(prizes, leaderboard_rows)

        # A gets first prize, no-one gets $10
        assert result == {
            "a": USDCent(15_00),
        }

    def test_distribute_prizes_4(self):
        prizes = [USDCent(15_00)]
        leaderboard_rows = [
            LeaderboardRow(bpuid="a", value=20, rank=1),
            LeaderboardRow(bpuid="b", value=20, rank=1),
            LeaderboardRow(bpuid="c", value=20, rank=1),
            LeaderboardRow(bpuid="d", value=20, rank=1),
        ]
        result = distribute_leaderboard_prizes(prizes, leaderboard_rows)

        # 4-way tie for the $15 prize; it gets split
        assert result == {
            "a": USDCent(3_75),
            "b": USDCent(3_75),
            "c": USDCent(3_75),
            "d": USDCent(3_75),
        }

    def test_distribute_prizes_5(self):
        prizes = [USDCent(15_00), USDCent(10_00)]
        leaderboard_rows = [
            LeaderboardRow(bpuid="a", value=20, rank=1),
            LeaderboardRow(bpuid="b", value=20, rank=1),
            LeaderboardRow(bpuid="c", value=10, rank=3),
        ]
        result = distribute_leaderboard_prizes(prizes, leaderboard_rows)

        # 2-way tie for the $15 prize; the top two prizes get split. Rank 3
        # and below get nothing
        assert result == {
            "a": USDCent(12_50),
            "b": USDCent(12_50),
        }

    def test_distribute_prizes_6(self):
        prizes = [USDCent(15_00), USDCent(10_00), USDCent(5_00)]
        leaderboard_rows = [
            LeaderboardRow(bpuid="a", value=20, rank=1),
            LeaderboardRow(bpuid="b", value=10, rank=2),
            LeaderboardRow(bpuid="c", value=10, rank=2),
            LeaderboardRow(bpuid="d", value=10, rank=2),
        ]
        result = distribute_leaderboard_prizes(prizes, leaderboard_rows)

        # A gets first prize, 3 way tie for 2nd rank: they split the 2nd and
        #   3rd place prizes (10 + 5)/3
        assert result == {
            "a": USDCent(15_00),
            "b": USDCent(5_00),
            "c": USDCent(5_00),
            "d": USDCent(5_00),
        }
