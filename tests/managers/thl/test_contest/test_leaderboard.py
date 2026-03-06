from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

from generalresearch.currency import USDCent
from generalresearch.models.thl.contest.definitions import (
    ContestStatus,
    ContestEndReason,
)
from generalresearch.models.thl.contest.leaderboard import (
    LeaderboardContest,
    LeaderboardContestCreate,
)
from generalresearch.models.thl.product import Product
from generalresearch.models.thl.user import User
from test_utils.managers.contest.conftest import (
    leaderboard_contest_in_db as contest_in_db,
    leaderboard_contest_create as contest_create,
)


class TestLeaderboardContestCRUD:

    def test_create(
        self,
        contest_create: LeaderboardContestCreate,
        product_user_wallet_yes: Product,
        thl_lm,
        contest_manager,
    ):
        c = contest_manager.create(
            product_id=product_user_wallet_yes.uuid, contest_create=contest_create
        )
        c_out = contest_manager.get(c.uuid)
        assert c == c_out

        assert isinstance(c, LeaderboardContest)
        assert c.prize_count == 2
        assert c.status == ContestStatus.ACTIVE
        # We have it set in the fixture as the daily contest for 2025-01-01
        assert c.end_condition.ends_at == datetime(
            2025, 1, 1, 23, 59, 59, 999999, tzinfo=ZoneInfo("America/New_York")
        ).astimezone(tz=timezone.utc) + timedelta(minutes=90)

    def test_enter(
        self,
        user_with_wallet: User,
        contest_in_db: LeaderboardContest,
        thl_lm,
        contest_manager,
        user_manager,
        thl_redis,
    ):
        contest = contest_in_db
        user = user_with_wallet

        c: LeaderboardContest = contest_manager.get(contest_uuid=contest.uuid)

        c = contest_manager.get_leaderboard_user_view(
            contest_uuid=contest.uuid,
            user=user,
            redis_client=thl_redis,
            user_manager=user_manager,
        )
        assert c.user_rank is None

        lbm = c.get_leaderboard_manager()
        lbm.hit_complete_count(user.product_user_id)

        c = contest_manager.get_leaderboard_user_view(
            contest_uuid=contest.uuid,
            user=user,
            redis_client=thl_redis,
            user_manager=user_manager,
        )
        assert c.user_rank == 1

    def test_contest_ends(
        self,
        user_with_wallet: User,
        contest_in_db: LeaderboardContest,
        thl_lm,
        contest_manager,
        user_manager,
        thl_redis,
    ):
        # The contest should be over. We need to trigger it.
        contest = contest_in_db
        contest._redis_client = thl_redis
        contest._user_manager = user_manager
        user = user_with_wallet

        lbm = contest.get_leaderboard_manager()
        lbm.hit_complete_count(user.product_user_id)

        c = contest_manager.get_leaderboard_user_view(
            contest_uuid=contest.uuid,
            user=user,
            redis_client=thl_redis,
            user_manager=user_manager,
        )
        assert c.user_rank == 1

        bp_wallet = thl_lm.get_account_or_create_bp_wallet_by_uuid(user.product_id)
        bp_wallet_balance = thl_lm.get_account_balance(account=bp_wallet)
        assert bp_wallet_balance == 0
        user_wallet = thl_lm.get_account_or_create_user_wallet(user=user)
        user_balance = thl_lm.get_account_balance(user_wallet)
        assert user_balance == 0

        decision, reason = contest.should_end()
        assert decision
        assert reason == ContestEndReason.ENDS_AT

        contest_manager.end_contest_if_over(contest=contest, ledger_manager=thl_lm)

        c: LeaderboardContest = contest_manager.get(contest_uuid=contest.uuid)
        assert c.status == ContestStatus.COMPLETED
        print(c)

        user_contest = contest_manager.get_leaderboard_user_view(
            contest_uuid=contest.uuid,
            user=user,
            redis_client=thl_redis,
            user_manager=user_manager,
        )
        assert len(user_contest.user_winnings) == 1
        w = user_contest.user_winnings[0]
        assert w.product_user_id == user.product_user_id
        assert w.prize.cash_amount == USDCent(15_00)

        # The prize is $15.00, so the user should get $15, paid by the bp
        assert thl_lm.get_account_balance(account=user_wallet) == 15_00
        # contest wallet is 0, and the BP gets 20c
        contest_wallet = thl_lm.get_account_or_create_contest_wallet_by_uuid(
            contest_uuid=c.uuid
        )
        assert thl_lm.get_account_balance(account=contest_wallet) == 0
        assert thl_lm.get_account_balance(account=bp_wallet) == -15_00
