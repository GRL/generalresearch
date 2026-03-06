from datetime import datetime, timezone

import pytest
from pydantic import ValidationError
from pytest import approx

from generalresearch.currency import USDCent
from generalresearch.managers.thl.ledger_manager.exceptions import (
    LedgerTransactionConditionFailedError,
)
from generalresearch.models.thl.contest import (
    ContestPrize,
    ContestEntryRule,
    ContestEndCondition,
)
from generalresearch.models.thl.contest.definitions import (
    ContestStatus,
    ContestPrizeKind,
    ContestEndReason,
)
from generalresearch.models.thl.contest.exceptions import ContestError
from generalresearch.models.thl.contest.raffle import (
    ContestEntry,
    ContestEntryType,
)
from generalresearch.models.thl.contest.raffle import (
    RaffleContest,
    RaffleContestCreate,
    RaffleUserView,
)
from generalresearch.models.thl.product import Product
from generalresearch.models.thl.user import User
from test_utils.managers.contest.conftest import (
    raffle_contest as contest,
    raffle_contest_in_db as contest_in_db,
    raffle_contest_create as contest_create,
    raffle_contest_factory as contest_factory,
)


class TestRaffleContest:

    def test_should_end(self, contest: RaffleContest, thl_lm, contest_manager):
        # contest is active and has no entries
        should, msg = contest.should_end()
        assert not should, msg

        # Change so that the contest ends now
        contest.end_condition.ends_at = datetime.now(tz=timezone.utc)
        should, msg = contest.should_end()
        assert should
        assert msg == ContestEndReason.ENDS_AT

        # Change the entry amount it thinks it has to over the target
        contest.end_condition.ends_at = None
        contest.current_amount = USDCent(100)
        should, msg = contest.should_end()
        assert should
        assert msg == ContestEndReason.TARGET_ENTRY_AMOUNT


class TestRaffleContestCRUD:

    def test_create(
        self,
        contest_create: RaffleContestCreate,
        product_user_wallet_yes: Product,
        thl_lm,
        contest_manager,
    ):
        c = contest_manager.create(
            product_id=product_user_wallet_yes.uuid, contest_create=contest_create
        )
        c_out = contest_manager.get(c.uuid)
        assert c == c_out

        assert isinstance(c, RaffleContest)
        assert c.prize_count == 1
        assert c.status == ContestStatus.ACTIVE
        assert c.end_condition.target_entry_amount == USDCent(100)
        assert c.current_amount == 0
        assert c.current_participants == 0

    @pytest.mark.parametrize("user_with_money", [{"min_balance": 60}], indirect=True)
    def test_enter(
        self,
        user_with_money: User,
        contest_in_db: RaffleContest,
        thl_lm,
        contest_manager,
    ):
        # Raffle ends at $1.00. User enters for $0.60
        print(user_with_money.product_id)
        print(contest_in_db.product_id)
        print(contest_in_db.uuid)
        contest = contest_in_db

        user_wallet = thl_lm.get_account_or_create_user_wallet(user=user_with_money)
        user_balance = thl_lm.get_account_balance(account=user_wallet)

        entry = ContestEntry(
            entry_type=ContestEntryType.CASH, user=user_with_money, amount=USDCent(60)
        )
        entry = contest_manager.enter_contest(
            contest_uuid=contest.uuid,
            entry=entry,
            country_iso="us",
            ledger_manager=thl_lm,
        )
        c: RaffleContest = contest_manager.get(contest_uuid=contest.uuid)
        assert c.current_amount == USDCent(60)
        assert c.current_participants == 1
        assert c.status == ContestStatus.ACTIVE

        c: RaffleUserView = contest_manager.get_raffle_user_view(
            contest_uuid=contest.uuid, user=user_with_money
        )
        assert c.user_amount == USDCent(60)
        assert c.user_amount_today == USDCent(60)
        assert c.projected_win_probability == approx(60 / 100, rel=0.01)

        # Contest wallet should have $0.60
        contest_wallet = thl_lm.get_account_or_create_contest_wallet_by_uuid(
            contest_uuid=contest.uuid
        )
        assert thl_lm.get_account_balance(account=contest_wallet) == 60
        # User spent 60c
        assert user_balance - thl_lm.get_account_balance(account=user_wallet) == 60

    @pytest.mark.parametrize("user_with_money", [{"min_balance": 120}], indirect=True)
    def test_enter_ends(
        self,
        user_with_money: User,
        contest_in_db: RaffleContest,
        thl_lm,
        contest_manager,
    ):
        # User enters contest, which brings the total amount above the limit,
        #   and the contest should end, with a winner selected
        contest = contest_in_db

        bp_wallet = thl_lm.get_account_or_create_bp_wallet_by_uuid(
            user_with_money.product_id
        )
        # I bribed the user, so the balance is not 0
        bp_wallet_balance = thl_lm.get_account_balance(account=bp_wallet)

        for _ in range(2):
            entry = ContestEntry(
                entry_type=ContestEntryType.CASH,
                user=user_with_money,
                amount=USDCent(60),
            )
            contest_manager.enter_contest(
                contest_uuid=contest.uuid,
                entry=entry,
                country_iso="us",
                ledger_manager=thl_lm,
            )
        c: RaffleContest = contest_manager.get(contest_uuid=contest.uuid)
        assert c.status == ContestStatus.COMPLETED
        print(c)

        user_contest = contest_manager.get_raffle_user_view(
            contest_uuid=contest.uuid, user=user_with_money
        )
        assert user_contest.current_win_probability == 1
        assert user_contest.projected_win_probability == 1
        assert len(user_contest.user_winnings) == 1

        # todo: make a all winning method
        winnings = contest_manager.get_winnings_by_user(user=user_with_money)
        assert len(winnings) == 1
        win = winnings[0]
        assert win.product_user_id == user_with_money.product_user_id

        # Contest wallet should have gotten zeroed out
        contest_wallet = thl_lm.get_account_or_create_contest_wallet_by_uuid(
            contest_uuid=contest.uuid
        )
        assert thl_lm.get_account_balance(contest_wallet) == 0
        # Expense wallet gets the $1.00 expense
        expense_wallet = thl_lm.get_account_or_create_bp_expense_by_uuid(
            product_uuid=user_with_money.product_id, expense_name="Prize"
        )
        assert thl_lm.get_account_balance(expense_wallet) == -100
        # And the BP gets 20c
        assert thl_lm.get_account_balance(bp_wallet) - bp_wallet_balance == 20

    @pytest.mark.parametrize("user_with_money", [{"min_balance": 120}], indirect=True)
    def test_enter_ends_cash_prize(
        self, user_with_money: User, contest_factory, thl_lm, contest_manager
    ):
        # Same as test_enter_ends, but the prize is cash. Just
        #   testing the ledger methods
        c = contest_factory(
            prizes=[
                ContestPrize(
                    name="$1.00 bonus",
                    kind=ContestPrizeKind.CASH,
                    estimated_cash_value=USDCent(100),
                    cash_amount=USDCent(100),
                )
            ]
        )
        assert c.prizes[0].kind == ContestPrizeKind.CASH

        user_wallet = thl_lm.get_account_or_create_user_wallet(user=user_with_money)
        user_balance = thl_lm.get_account_balance(user_wallet)
        bp_wallet = thl_lm.get_account_or_create_bp_wallet_by_uuid(
            user_with_money.product_id
        )
        bp_wallet_balance = thl_lm.get_account_balance(bp_wallet)

        ## Enter Contest
        entry = ContestEntry(
            entry_type=ContestEntryType.CASH, user=user_with_money, amount=USDCent(120)
        )
        entry = contest_manager.enter_contest(
            contest_uuid=c.uuid,
            entry=entry,
            country_iso="us",
            ledger_manager=thl_lm,
        )

        # The prize is $1.00, so the user spent $1.20 entering, won, then got $1.00 back
        assert (
            thl_lm.get_account_balance(account=user_wallet) == user_balance + 100 - 120
        )
        # contest wallet is 0, and the BP gets 20c
        contest_wallet = thl_lm.get_account_or_create_contest_wallet_by_uuid(
            contest_uuid=c.uuid
        )
        assert thl_lm.get_account_balance(account=contest_wallet) == 0
        assert thl_lm.get_account_balance(account=bp_wallet) - bp_wallet_balance == 20

    def test_enter_failure(
        self,
        user_with_wallet: User,
        contest_in_db: RaffleContest,
        thl_lm,
        contest_manager,
    ):
        c = contest_in_db
        user = user_with_wallet

        # Tries to enter $0
        with pytest.raises(ValidationError) as e:
            entry = ContestEntry(
                entry_type=ContestEntryType.CASH, user=user, amount=USDCent(0)
            )
        assert "Input should be greater than 0" in str(e.value)

        # User has no money
        entry = ContestEntry(
            entry_type=ContestEntryType.CASH, user=user, amount=USDCent(20)
        )
        with pytest.raises(LedgerTransactionConditionFailedError) as e:
            entry = contest_manager.enter_contest(
                contest_uuid=c.uuid,
                entry=entry,
                country_iso="us",
                ledger_manager=thl_lm,
            )
        assert e.value.args[0] == "insufficient balance"

        # Tries to enter with the wrong entry type (count, on a cash contest)
        entry = ContestEntry(entry_type=ContestEntryType.COUNT, user=user, amount=1)
        with pytest.raises(AssertionError) as e:
            entry = contest_manager.enter_contest(
                contest_uuid=c.uuid,
                entry=entry,
                country_iso="us",
                ledger_manager=thl_lm,
            )
        assert "incompatible entry type" in str(e.value)

    @pytest.mark.parametrize("user_with_money", [{"min_balance": 100}], indirect=True)
    def test_enter_not_eligible(
        self, user_with_money: User, contest_factory, thl_lm, contest_manager
    ):
        # Max entry amount per user $0.10. Contest still ends at $1.00
        c = contest_factory(
            entry_rule=ContestEntryRule(
                max_entry_amount_per_user=USDCent(10),
                max_daily_entries_per_user=USDCent(8),
            )
        )
        c: RaffleContest = contest_manager.get(c.uuid)
        assert c.entry_rule.max_entry_amount_per_user == USDCent(10)
        assert c.entry_rule.max_daily_entries_per_user == USDCent(8)

        # User tries to enter $0.20
        entry = ContestEntry(
            entry_type=ContestEntryType.CASH, user=user_with_money, amount=USDCent(20)
        )
        with pytest.raises(ContestError) as e:
            entry = contest_manager.enter_contest(
                contest_uuid=c.uuid,
                entry=entry,
                country_iso="us",
                ledger_manager=thl_lm,
            )
        assert "Entry would exceed max amount per user." in str(e.value)

        # User tries to enter $0.10
        entry = ContestEntry(
            entry_type=ContestEntryType.CASH, user=user_with_money, amount=USDCent(10)
        )
        with pytest.raises(ContestError) as e:
            entry = contest_manager.enter_contest(
                contest_uuid=c.uuid,
                entry=entry,
                country_iso="us",
                ledger_manager=thl_lm,
            )
        assert "Entry would exceed max amount per user per day." in str(e.value)

        # User enters $0.08 successfully
        entry = ContestEntry(
            entry_type=ContestEntryType.CASH, user=user_with_money, amount=USDCent(8)
        )
        entry = contest_manager.enter_contest(
            contest_uuid=c.uuid,
            entry=entry,
            country_iso="us",
            ledger_manager=thl_lm,
        )

        # Then can't anymore
        entry = ContestEntry(
            entry_type=ContestEntryType.CASH, user=user_with_money, amount=USDCent(1)
        )
        with pytest.raises(ContestError) as e:
            entry = contest_manager.enter_contest(
                contest_uuid=c.uuid,
                entry=entry,
                country_iso="us",
                ledger_manager=thl_lm,
            )
        assert "Entry would exceed max amount per user per day." in str(e.value)


class TestRaffleContestUserViews:
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
        c = contest_factory(
            end_condition=ContestEndCondition(target_entry_amount=USDCent(10)),
            entry_rule=ContestEntryRule(
                max_entry_amount_per_user=USDCent(1),
            ),
        )
        cs = contest_manager.get_many_by_user_eligible(
            user=user_with_money, country_iso="us"
        )
        assert len(cs) == 1

        entry = ContestEntry(
            entry_type=ContestEntryType.CASH,
            user=user_with_money,
            amount=USDCent(1),
        )
        contest_manager.enter_contest(
            contest_uuid=c.uuid,
            entry=entry,
            country_iso="us",
            ledger_manager=thl_lm,
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
        assert c.user_amount == USDCent(1)
        assert c.user_amount_today == USDCent(1)
        assert c.current_win_probability == 1
        assert c.projected_win_probability == approx(1 / 10, rel=0.01)

        # And nothing won yet #todo
        # cs = cm.get_many_by_user_won(user=user_with_money)

        assert len(contest_manager.get_winnings_by_user(user_with_money)) == 0

    def test_list_user_winnings(
        self, user_with_money: User, contest_factory, thl_lm, contest_manager
    ):
        c = contest_factory(
            end_condition=ContestEndCondition(target_entry_amount=USDCent(100)),
        )
        entry = ContestEntry(
            entry_type=ContestEntryType.CASH,
            user=user_with_money,
            amount=USDCent(100),
        )
        contest_manager.enter_contest(
            contest_uuid=c.uuid,
            entry=entry,
            country_iso="us",
            ledger_manager=thl_lm,
        )
        # Contest ends after 100 entry, user enters 100 entry, user wins!
        ws = contest_manager.get_winnings_by_user(user_with_money)
        assert len(ws) == 1
        w = ws[0]
        assert w.user.user_id == user_with_money.user_id
        assert w.prize == c.prizes[0]
        assert w.awarded_cash_amount is None

        cs = contest_manager.get_many_by_user_won(user_with_money)
        assert len(cs) == 1
        c = cs[0]
        w = c.user_winnings[0]
        assert w.prize == c.prizes[0]
        assert w.user.user_id == user_with_money.user_id


class TestRaffleContestCRUDCount:
    # This is a COUNT contest. No cash moves. Not really fleshed out what we'd do with this.
    @pytest.mark.skip
    def test_enter(
        self, user_with_wallet: User, contest_factory, thl_lm, contest_manager
    ):
        c = contest_factory(entry_type=ContestEntryType.COUNT)
        entry = ContestEntry(
            entry_type=ContestEntryType.COUNT,
            user=user_with_wallet,
            amount=1,
        )
        contest_manager.enter_contest(
            contest_uuid=c.uuid,
            entry=entry,
            country_iso="us",
            ledger_manager=thl_lm,
        )
