from collections import Counter
from uuid import uuid4

import pytest
from pytest import approx

from generalresearch.currency import USDCent
from generalresearch.models.thl.contest import (
    ContestPrize,
    ContestEndCondition,
)
from generalresearch.models.thl.contest.contest_entry import ContestEntry
from generalresearch.models.thl.contest.definitions import (
    ContestEntryType,
    ContestPrizeKind,
    ContestType,
    ContestStatus,
    ContestEndReason,
)
from generalresearch.models.thl.contest.raffle import RaffleContest

from tests.models.thl.test_contest.test_contest import TestContest


class TestRaffleContest(TestContest):

    @pytest.fixture(scope="function")
    def raffle_contest(self, product) -> RaffleContest:
        return RaffleContest(
            product_id=product.uuid,
            name=f"Raffle Contest {uuid4().hex}",
            contest_type=ContestType.RAFFLE,
            entry_type=ContestEntryType.CASH,
            prizes=[
                ContestPrize(
                    name="iPod 64GB White",
                    kind=ContestPrizeKind.PHYSICAL,
                    estimated_cash_value=USDCent(100_00),
                )
            ],
            end_condition=ContestEndCondition(target_entry_amount=100),
        )

    @pytest.fixture(scope="function")
    def ended_raffle_contest(self, raffle_contest, utc_now) -> RaffleContest:
        # Fake ending the contest
        raffle_contest = raffle_contest.model_copy()
        raffle_contest.update(
            status=ContestStatus.COMPLETED,
            ended_at=utc_now,
            end_reason=ContestEndReason.ENDS_AT,
        )
        return raffle_contest


class TestRaffleContestUserView(TestRaffleContest):

    def test_user_view(self, raffle_contest, user):
        from generalresearch.models.thl.contest.raffle import RaffleUserView

        data = {
            "current_amount": USDCent(1_00),
            "product_user_id": user.product_user_id,
            "user_amount": USDCent(1),
            "user_amount_today": USDCent(1),
        }
        r = RaffleUserView.model_validate(raffle_contest.model_dump() | data)
        res = r.model_dump(mode="json")

        assert res["product_user_id"] == user.product_user_id
        assert res["user_amount_today"] == 1
        assert res["current_win_probability"] == approx(0.01, rel=0.000001)
        assert res["projected_win_probability"] == approx(0.01, rel=0.000001)

        # Now change the amount
        r.current_amount = USDCent(1_01)
        res = r.model_dump(mode="json")
        assert res["current_win_probability"] == approx(0.0099, rel=0.001)
        assert res["projected_win_probability"] == approx(0.0099, rel=0.001)

    def test_win_pct(self, raffle_contest, user):
        from generalresearch.models.thl.contest.raffle import RaffleUserView

        data = {
            "current_amount": USDCent(10),
            "product_user_id": user.product_user_id,
            "user_amount": USDCent(1),
            "user_amount_today": USDCent(1),
        }
        r = RaffleUserView.model_validate(raffle_contest.model_dump() | data)
        r.prizes = [
            ContestPrize(
                name="iPod 64GB White",
                kind=ContestPrizeKind.PHYSICAL,
                estimated_cash_value=USDCent(100_00),
            ),
            ContestPrize(
                name="iPod 64GB White",
                kind=ContestPrizeKind.PHYSICAL,
                estimated_cash_value=USDCent(100_00),
            ),
        ]
        # Raffle has 10 entries, user has 1 entry.
        # There are 2 prizes.
        assert r.current_win_probability == approx(expected=0.2, rel=0.01)
        # He can only possibly win 1 prize
        assert r.current_prize_count_probability[1] == approx(expected=0.2, rel=0.01)
        # He has a 0 prob of winning 2 prizes
        assert r.current_prize_count_probability[2] == 0
        # Contest end when there are 100 entries, so 1/100 * 2 prizes
        assert r.projected_win_probability == approx(expected=0.02, rel=0.01)

        # Change to user having 2 entries (out of 10)
        # Still with 2 prizes
        r.user_amount = USDCent(2)
        assert r.current_win_probability == approx(expected=0.3777, rel=0.01)
        # 2/10 chance of winning 1st, 8/9 change of not winning 2nd, plus the
        #   same in the other order
        p = (2 / 10) * (8 / 9) * 2  # 0.355555
        assert r.current_prize_count_probability[1] == approx(p, rel=0.01)
        p = (2 / 10) * (1 / 9)  # 0.02222
        assert r.current_prize_count_probability[2] == approx(p, rel=0.01)


class TestRaffleContestWinners(TestRaffleContest):

    def test_winners_1_prize(self, ended_raffle_contest, user_1, user_2, user_3):
        ended_raffle_contest.entries = [
            ContestEntry(
                user=user_1,
                amount=USDCent(1),
                entry_type=ContestEntryType.CASH,
            ),
            ContestEntry(
                user=user_2,
                amount=USDCent(2),
                entry_type=ContestEntryType.CASH,
            ),
            ContestEntry(
                user=user_3,
                amount=USDCent(3),
                entry_type=ContestEntryType.CASH,
            ),
        ]

        # There is 1 prize. If we select a winner 1000 times, we'd expect user 1
        #   to win ~ 1/6th of the time, user 2 ~2/6th and 3 3/6th.
        winners = ended_raffle_contest.select_winners()
        assert len(winners) == 1

        c = Counter(
            [
                ended_raffle_contest.select_winners()[0].user.user_id
                for _ in range(10000)
            ]
        )
        assert c[user_1.user_id] == approx(
            10000 * 1 / 6, rel=0.1
        )  # 10% relative tolerance
        assert c[user_2.user_id] == approx(10000 * 2 / 6, rel=0.1)
        assert c[user_3.user_id] == approx(10000 * 3 / 6, rel=0.1)

    def test_winners_2_prizes(self, ended_raffle_contest, user_1, user_2, user_3):
        ended_raffle_contest.prizes.append(
            ContestPrize(
                name="iPod 64GB Black",
                kind=ContestPrizeKind.PHYSICAL,
                estimated_cash_value=USDCent(100_00),
            )
        )
        ended_raffle_contest.entries = [
            ContestEntry(
                user=user_3,
                amount=USDCent(1),
                entry_type=ContestEntryType.CASH,
            ),
            ContestEntry(
                user=user_1,
                amount=USDCent(9999999),
                entry_type=ContestEntryType.CASH,
            ),
            ContestEntry(
                user=user_2,
                amount=USDCent(1),
                entry_type=ContestEntryType.CASH,
            ),
        ]
        # In this scenario, user 1 should win both prizes
        winners = ended_raffle_contest.select_winners()
        assert len(winners) == 2
        # Two different prizes
        assert len({w.prize.name for w in winners}) == 2
        # Same user
        assert all(w.user.user_id == user_1.user_id for w in winners)

    def test_winners_2_prizes_1_entry(self, ended_raffle_contest, user_3):
        ended_raffle_contest.prizes = [
            ContestPrize(
                name="iPod 64GB White",
                kind=ContestPrizeKind.PHYSICAL,
                estimated_cash_value=USDCent(100_00),
            ),
            ContestPrize(
                name="iPod 64GB Black",
                kind=ContestPrizeKind.PHYSICAL,
                estimated_cash_value=USDCent(100_00),
            ),
        ]
        ended_raffle_contest.entries = [
            ContestEntry(
                user=user_3,
                amount=USDCent(1),
                entry_type=ContestEntryType.CASH,
            ),
        ]

        # One prize goes unclaimed
        winners = ended_raffle_contest.select_winners()
        assert len(winners) == 1

    def test_winners_2_prizes_1_entry_2_pennies(self, ended_raffle_contest, user_3):
        ended_raffle_contest.prizes = [
            ContestPrize(
                name="iPod 64GB White",
                kind=ContestPrizeKind.PHYSICAL,
                estimated_cash_value=USDCent(100_00),
            ),
            ContestPrize(
                name="iPod 64GB Black",
                kind=ContestPrizeKind.PHYSICAL,
                estimated_cash_value=USDCent(100_00),
            ),
        ]
        ended_raffle_contest.entries = [
            ContestEntry(
                user=user_3,
                amount=USDCent(2),
                entry_type=ContestEntryType.CASH,
            ),
        ]
        # User wins both prizes
        winners = ended_raffle_contest.select_winners()
        assert len(winners) == 2

    def test_winners_3_prizes_3_entries(
        self, ended_raffle_contest, product, user_1, user_2, user_3
    ):
        ended_raffle_contest.prizes = [
            ContestPrize(
                name="iPod 64GB White",
                kind=ContestPrizeKind.PHYSICAL,
                estimated_cash_value=USDCent(100_00),
            ),
            ContestPrize(
                name="iPod 64GB Black",
                kind=ContestPrizeKind.PHYSICAL,
                estimated_cash_value=USDCent(100_00),
            ),
            ContestPrize(
                name="iPod 64GB Red",
                kind=ContestPrizeKind.PHYSICAL,
                estimated_cash_value=USDCent(100_00),
            ),
        ]
        ended_raffle_contest.entries = [
            ContestEntry(
                user=user_1,
                amount=USDCent(1),
                entry_type=ContestEntryType.CASH,
            ),
            ContestEntry(
                user=user_2,
                amount=USDCent(2),
                entry_type=ContestEntryType.CASH,
            ),
            ContestEntry(
                user=user_3,
                amount=USDCent(3),
                entry_type=ContestEntryType.CASH,
            ),
        ]

        winners = ended_raffle_contest.select_winners()
        assert len(winners) == 3

        winners = [ended_raffle_contest.select_winners() for _ in range(10000)]

        # There's 3 winners, the 1st should follow the same percentages
        c = Counter([w[0].user.user_id for w in winners])

        assert c[user_1.user_id] == approx(10000 * 1 / 6, rel=0.1)
        assert c[user_2.user_id] == approx(10000 * 2 / 6, rel=0.1)
        assert c[user_3.user_id] == approx(10000 * 3 / 6, rel=0.1)

        # Assume the 1st user won
        ended_raffle_contest.entries.pop(0)
        winners = [ended_raffle_contest.select_winners() for _ in range(10000)]
        c = Counter([w[0].user.user_id for w in winners])
        assert c[user_2.user_id] == approx(10000 * 2 / 5, rel=0.1)
        assert c[user_3.user_id] == approx(10000 * 3 / 5, rel=0.1)
