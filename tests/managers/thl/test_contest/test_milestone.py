from datetime import datetime, timezone

from generalresearch.models.thl.contest.definitions import (
    ContestStatus,
    ContestEndReason,
)
from generalresearch.models.thl.contest.milestone import (
    MilestoneContest,
    MilestoneContestCreate,
    MilestoneUserView,
    ContestEntryTrigger,
)
from generalresearch.models.thl.product import Product
from generalresearch.models.thl.user import User
from test_utils.managers.contest.conftest import (
    milestone_contest as contest,
    milestone_contest_in_db as contest_in_db,
    milestone_contest_create as contest_create,
    milestone_contest_factory as contest_factory,
)


class TestMilestoneContest:

    def test_should_end(self, contest: MilestoneContest, thl_lm, contest_manager):
        # contest is active and has no entries
        should, msg = contest.should_end()
        assert not should, msg

        # Change so that the contest ends now
        contest.end_condition.ends_at = datetime.now(tz=timezone.utc)
        should, msg = contest.should_end()
        assert should
        assert msg == ContestEndReason.ENDS_AT

        # Change the win amount it thinks it past over the target
        contest.end_condition.ends_at = None
        contest.end_condition.max_winners = 10
        contest.win_count = 10
        should, msg = contest.should_end()
        assert should
        assert msg == ContestEndReason.MAX_WINNERS


class TestMilestoneContestCRUD:

    def test_create(
        self,
        contest_create: MilestoneContestCreate,
        product_user_wallet_yes: Product,
        thl_lm,
        contest_manager,
    ):
        c = contest_manager.create(
            product_id=product_user_wallet_yes.uuid, contest_create=contest_create
        )
        c_out = contest_manager.get(c.uuid)
        assert c == c_out

        assert isinstance(c, MilestoneContest)
        assert c.prize_count == 2
        assert c.status == ContestStatus.ACTIVE
        assert c.end_condition.max_winners == 5
        assert c.entry_trigger == ContestEntryTrigger.TASK_COMPLETE
        assert c.target_amount == 3
        assert c.win_count == 0

    def test_enter(
        self,
        user_with_wallet: User,
        contest_in_db: MilestoneContest,
        thl_lm,
        contest_manager,
    ):
        # Users CANNOT directly enter a milestone contest through the api,
        #   but we'll call this manager method when a trigger is hit.
        contest = contest_in_db
        user = user_with_wallet

        contest_manager.enter_milestone_contest(
            contest_uuid=contest.uuid,
            user=user,
            country_iso="us",
            ledger_manager=thl_lm,
            incr=1,
        )

        c: MilestoneContest = contest_manager.get(contest_uuid=contest.uuid)
        assert c.status == ContestStatus.ACTIVE
        assert not hasattr(c, "current_amount")
        assert not hasattr(c, "current_participants")

        c: MilestoneUserView = contest_manager.get_milestone_user_view(
            contest_uuid=contest.uuid, user=user_with_wallet
        )
        assert c.user_amount == 1

        # Contest wallet should have 0 bc there is no ledger
        contest_wallet = thl_lm.get_account_or_create_contest_wallet_by_uuid(
            contest_uuid=contest.uuid
        )
        assert thl_lm.get_account_balance(contest_wallet) == 0

        # Enter again!
        contest_manager.enter_milestone_contest(
            contest_uuid=contest.uuid,
            user=user,
            country_iso="us",
            ledger_manager=thl_lm,
            incr=1,
        )
        c: MilestoneUserView = contest_manager.get_milestone_user_view(
            contest_uuid=contest.uuid, user=user_with_wallet
        )
        assert c.user_amount == 2

        # We should have ONE entry with a value of 2
        e = contest_manager.get_entries_by_contest_id(c.id)
        assert len(e) == 1
        assert e[0].amount == 2

    def test_enter_win(
        self,
        user_with_wallet: User,
        contest_in_db: MilestoneContest,
        thl_lm,
        contest_manager,
    ):
        # User enters contest, which brings the USER'S total amount above the limit,
        #   and the user reaches the milestone
        contest = contest_in_db
        user = user_with_wallet

        user_wallet = thl_lm.get_account_or_create_user_wallet(user=user)
        user_balance = thl_lm.get_account_balance(account=user_wallet)
        bp_wallet = thl_lm.get_account_or_create_bp_wallet_by_uuid(
            product_uuid=user.product_id
        )
        bp_wallet_balance = thl_lm.get_account_balance(account=bp_wallet)

        c: MilestoneUserView = contest_manager.get_milestone_user_view(
            contest_uuid=contest.uuid, user=user_with_wallet
        )
        assert c.user_amount == 0
        res, msg = c.is_user_eligible(country_iso="us")
        assert res, msg

        # User reaches the milestone after 3 completes/whatevers.
        for _ in range(3):
            contest_manager.enter_milestone_contest(
                contest_uuid=contest.uuid,
                user=user,
                country_iso="us",
                ledger_manager=thl_lm,
                incr=1,
            )

        # to be clear, the contest itself doesn't end!
        c: MilestoneContest = contest_manager.get(contest_uuid=contest.uuid)
        assert c.status == ContestStatus.ACTIVE

        c: MilestoneUserView = contest_manager.get_milestone_user_view(
            contest_uuid=contest.uuid, user=user_with_wallet
        )
        assert c.user_amount == 3
        res, msg = c.is_user_eligible(country_iso="us")
        assert not res
        assert msg == "User should have won already"

        assert len(c.user_winnings) == 2
        assert c.win_count == 1

        # The prize was awarded! User should have won $1.00
        assert thl_lm.get_account_balance(user_wallet) - user_balance == 100
        # Which was paid from the BP's balance
        assert thl_lm.get_account_balance(bp_wallet) - bp_wallet_balance == -100

        # winnings = cm.get_winnings_by_user(user=user)
        # assert len(winnings) == 1
        # win = winnings[0]
        # assert win.product_user_id == user.product_user_id

    def test_enter_ends(
        self,
        user_factory,
        product_user_wallet_yes: Product,
        contest_in_db: MilestoneContest,
        thl_lm,
        contest_manager,
    ):
        # Multiple users reach the milestone. Contest ends after 5 wins.
        users = [user_factory(product=product_user_wallet_yes) for _ in range(5)]
        contest = contest_in_db

        for u in users:
            contest_manager.enter_milestone_contest(
                contest_uuid=contest.uuid,
                user=u,
                country_iso="us",
                ledger_manager=thl_lm,
                incr=3,
            )

        c: MilestoneContest = contest_manager.get(contest_uuid=contest.uuid)
        assert c.status == ContestStatus.COMPLETED
        assert c.end_reason == ContestEndReason.MAX_WINNERS

    def test_trigger(
        self,
        user_with_wallet: User,
        contest_in_db: MilestoneContest,
        thl_lm,
        contest_manager,
    ):
        # Pretend user just got a complete
        cnt = contest_manager.hit_milestone_triggers(
            country_iso="us",
            user=user_with_wallet,
            event=ContestEntryTrigger.TASK_COMPLETE,
            ledger_manager=thl_lm,
        )
        assert cnt == 1

        # Assert this contest got entered
        c: MilestoneUserView = contest_manager.get_milestone_user_view(
            contest_uuid=contest_in_db.uuid, user=user_with_wallet
        )
        assert c.user_amount == 1


class TestMilestoneContestUserViews:
    def test_list_user_eligible_country(
        self, user_with_wallet: User, contest_factory, thl_lm, contest_manager
    ):
        # No contests exists
        cs = contest_manager.get_many_by_user_eligible(
            user=user_with_wallet, country_iso="us"
        )
        assert len(cs) == 0

        # Create a contest. It'll be in the US/CA
        contest_factory(country_isos={"us", "ca"})

        # Not eligible in mexico
        cs = contest_manager.get_many_by_user_eligible(
            user=user_with_wallet, country_iso="mx"
        )
        assert len(cs) == 0
        cs = contest_manager.get_many_by_user_eligible(
            user=user_with_wallet, country_iso="us"
        )
        assert len(cs) == 1

        # Create another, any country
        contest_factory(country_isos=None)
        cs = contest_manager.get_many_by_user_eligible(
            user=user_with_wallet, country_iso="mx"
        )
        assert len(cs) == 1
        cs = contest_manager.get_many_by_user_eligible(
            user=user_with_wallet, country_iso="us"
        )
        assert len(cs) == 2

    def test_list_user_eligible(
        self, user_with_money: User, contest_factory, thl_lm, contest_manager
    ):
        # User reaches milestone after 1 complete
        c = contest_factory(target_amount=1)
        user = user_with_money

        cs = contest_manager.get_many_by_user_eligible(
            user=user_with_money, country_iso="us"
        )
        assert len(cs) == 1

        contest_manager.enter_milestone_contest(
            contest_uuid=c.uuid, user=user, country_iso="us", ledger_manager=thl_lm
        )

        # User isn't eligible anymore
        cs = contest_manager.get_many_by_user_eligible(
            user=user_with_money, country_iso="us"
        )
        assert len(cs) == 0

        # But it comes back in the list entered
        cs = contest_manager.get_many_by_user_entered(user=user_with_money)
        assert len(cs) == 1
        c = cs[0]
        assert c.user_amount == 1
        assert isinstance(c, MilestoneUserView)
        assert not hasattr(c, "current_win_probability")

        # They won one contest with 2 prizes
        assert len(contest_manager.get_winnings_by_user(user_with_money)) == 2
