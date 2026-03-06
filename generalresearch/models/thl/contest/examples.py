from typing import Dict

from pydantic import HttpUrl

from generalresearch.config import EXAMPLE_PRODUCT_ID
from generalresearch.currency import USDCent


def _example_raffle_create(schema: Dict) -> None:
    from generalresearch.models.thl.contest.raffle import (
        RaffleContestCreate,
    )
    from generalresearch.models.thl.contest import (
        ContestEndCondition,
        ContestPrize,
        ContestEntryRule,
    )
    from generalresearch.models.thl.contest.definitions import (
        ContestType,
        ContestPrizeKind,
    )
    from generalresearch.models.thl.contest.contest_entry import (
        ContestEntryType,
    )

    schema["example"] = RaffleContestCreate(
        name="Win an iPhone",
        description="iPhone winner will be drawn in proportion to entry "
        "amount. Contest ends once $800 has been entered.",
        contest_type=ContestType.RAFFLE,
        end_condition=ContestEndCondition(target_entry_amount=USDCent(800_00)),
        prizes=[
            ContestPrize(
                kind=ContestPrizeKind.PHYSICAL,
                name="iPhone 16",
                estimated_cash_value=USDCent(800_00),
            )
        ],
        starts_at="2025-06-12T21:12:58.061170Z",
        terms_and_conditions=None,
        entry_rule=ContestEntryRule(
            max_entry_amount_per_user=10000, max_daily_entries_per_user=1000
        ),
        country_isos={"us", "ca"},
        entry_type=ContestEntryType.CASH,
    ).model_dump(mode="json")


def _example_raffle(schema: Dict) -> None:
    from generalresearch.models.thl.contest.raffle import RaffleContest
    from generalresearch.models.thl.contest import (
        ContestEndCondition,
        ContestPrize,
        ContestEntryRule,
    )
    from generalresearch.models.thl.contest.definitions import (
        ContestStatus,
        ContestPrizeKind,
        ContestType,
    )
    from generalresearch.models.thl.contest.contest_entry import (
        ContestEntryType,
    )

    schema["example"] = RaffleContest(
        name="Win an iPhone",
        description="iPhone winner will be drawn in proportion to entry "
        "amount. Contest ends once $800 has been entered.",
        contest_type=ContestType.RAFFLE,
        end_condition=ContestEndCondition(target_entry_amount=USDCent(800_00)),
        prizes=[
            ContestPrize(
                kind=ContestPrizeKind.PHYSICAL,
                name="iPhone 16",
                estimated_cash_value=USDCent(800_00),
            )
        ],
        starts_at="2025-06-12T21:12:58.061170Z",
        terms_and_conditions=None,
        entry_rule=ContestEntryRule(
            max_entry_amount_per_user=10000, max_daily_entries_per_user=1000
        ),
        country_isos={"us", "ca"},
        entry_type=ContestEntryType.CASH,
        status=ContestStatus.ACTIVE,
        uuid="ce3968b8e18a4b96af62007f262ed7f7",
        created_at="2025-06-12T21:12:58.061205Z",
        updated_at="2025-06-12T21:12:58.061205Z",
        current_amount=4723,
        current_participants=12,
        product_id=EXAMPLE_PRODUCT_ID,
    ).model_dump(mode="json")


def _example_raffle_user_view(schema: Dict) -> None:
    from generalresearch.models.thl.contest.raffle import RaffleUserView
    from generalresearch.models.thl.contest import (
        ContestEndCondition,
        ContestPrize,
        ContestEntryRule,
    )
    from generalresearch.models.thl.contest.definitions import (
        ContestStatus,
        ContestPrizeKind,
        ContestType,
    )
    from generalresearch.models.thl.contest.contest_entry import (
        ContestEntryType,
    )

    schema["example"] = RaffleUserView(
        name="Win an iPhone",
        description="iPhone winner will be drawn in proportion to entry "
        "amount. Contest ends once $800 has been entered.",
        contest_type=ContestType.RAFFLE,
        end_condition=ContestEndCondition(target_entry_amount=USDCent(800_00)),
        prizes=[
            ContestPrize(
                kind=ContestPrizeKind.PHYSICAL,
                name="iPhone 16",
                estimated_cash_value=USDCent(800_00),
            )
        ],
        starts_at="2025-06-12T21:12:58.061170Z",
        terms_and_conditions=None,
        entry_rule=ContestEntryRule(
            max_entry_amount_per_user=10000, max_daily_entries_per_user=1000
        ),
        country_isos={"us", "ca"},
        entry_type=ContestEntryType.CASH,
        status=ContestStatus.ACTIVE,
        uuid="ce3968b8e18a4b96af62007f262ed7f7",
        created_at="2025-06-12T21:12:58.061205Z",
        updated_at="2025-06-12T21:12:58.061205Z",
        current_amount=4723,
        current_participants=12,
        product_id=EXAMPLE_PRODUCT_ID,
        user_amount=420,
        user_amount_today=0,
        product_user_id="test-user",
    ).model_dump(mode="json")


def _example_milestone_create(schema: Dict) -> None:
    from generalresearch.models.thl.contest.milestone import (
        MilestoneContestCreate,
        MilestoneContestEndCondition,
        ContestEntryTrigger,
    )
    from generalresearch.models.thl.contest import (
        ContestPrize,
    )
    from generalresearch.models.thl.contest.definitions import (
        ContestType,
        ContestPrizeKind,
    )

    schema["example"] = MilestoneContestCreate(
        name="Win a 50% bonus for 7 days and a $5 bonus after your first 10 completes!",
        description="Only valid for the first 50 users",
        contest_type=ContestType.MILESTONE,
        end_condition=MilestoneContestEndCondition(max_winners=50),
        prizes=[
            ContestPrize(
                kind=ContestPrizeKind.PROMOTION,
                name="50% bonus on completes for 7 days",
                estimated_cash_value=USDCent(0),
            ),
            ContestPrize(
                kind=ContestPrizeKind.CASH,
                name="$5.00 Bonus",
                cash_amount=USDCent(5_00),
                estimated_cash_value=USDCent(5_00),
            ),
        ],
        entry_trigger=ContestEntryTrigger.TASK_COMPLETE,
        target_amount=10,
        starts_at="2025-06-12T21:12:58.061170Z",
        terms_and_conditions=HttpUrl("https://www.example.com"),
    ).model_dump(mode="json")


def _example_milestone(schema: Dict) -> None:
    from generalresearch.models.thl.contest.milestone import (
        MilestoneContest,
        MilestoneContestEndCondition,
        ContestEntryTrigger,
    )
    from generalresearch.models.thl.contest import (
        ContestPrize,
    )
    from generalresearch.models.thl.contest.definitions import (
        ContestType,
        ContestPrizeKind,
    )

    schema["example"] = MilestoneContest(
        name="Win a 50% bonus for 7 days and a $5 bonus after your first 10 completes!",
        description="Only valid for the first 50 users",
        contest_type=ContestType.MILESTONE,
        end_condition=MilestoneContestEndCondition(max_winners=50),
        prizes=[
            ContestPrize(
                kind=ContestPrizeKind.PROMOTION,
                name="50% bonus on completes for 7 days",
                estimated_cash_value=USDCent(0),
            ),
            ContestPrize(
                kind=ContestPrizeKind.CASH,
                name="$5.00 Bonus",
                cash_amount=USDCent(5_00),
                estimated_cash_value=USDCent(5_00),
            ),
        ],
        entry_trigger=ContestEntryTrigger.TASK_COMPLETE,
        target_amount=10,
        starts_at="2025-06-12T21:12:58.061170Z",
        terms_and_conditions=HttpUrl("https://www.example.com"),
        product_id=EXAMPLE_PRODUCT_ID,
        uuid="747fe3b709ae460e816821dcb81aebb9",
        created_at="2025-06-12T21:12:58.061205Z",
        updated_at="2025-06-12T21:12:58.061205Z",
        win_count=12,
    ).model_dump(mode="json")


def _example_milestone_user_view(schema: Dict) -> None:
    from generalresearch.models.thl.contest.milestone import (
        MilestoneUserView,
        MilestoneContestEndCondition,
        ContestEntryTrigger,
    )
    from generalresearch.models.thl.contest import ContestPrize
    from generalresearch.models.thl.contest.definitions import (
        ContestType,
        ContestPrizeKind,
    )

    schema["example"] = MilestoneUserView(
        name="Win a 50% bonus for 7 days and a $5 bonus after your first 10 completes!",
        description="Only valid for the first 50 users",
        contest_type=ContestType.MILESTONE,
        end_condition=MilestoneContestEndCondition(max_winners=50),
        prizes=[
            ContestPrize(
                kind=ContestPrizeKind.PROMOTION,
                name="50% bonus on completes for 7 days",
                estimated_cash_value=USDCent(0),
            ),
            ContestPrize(
                kind=ContestPrizeKind.CASH,
                name="$5.00 Bonus",
                cash_amount=USDCent(5_00),
                estimated_cash_value=USDCent(5_00),
            ),
        ],
        entry_trigger=ContestEntryTrigger.TASK_COMPLETE,
        target_amount=10,
        starts_at="2025-06-12T21:12:58.061170Z",
        terms_and_conditions=HttpUrl("https://www.example.com"),
        product_id=EXAMPLE_PRODUCT_ID,
        uuid="747fe3b709ae460e816821dcb81aebb9",
        created_at="2025-06-12T21:12:58.061205Z",
        updated_at="2025-06-12T21:12:58.061205Z",
        win_count=12,
        user_amount=8,
        product_user_id="test-user",
    ).model_dump(mode="json")


def _example_leaderboard_contest_create(schema: Dict) -> None:
    from generalresearch.models.thl.contest.leaderboard import (
        LeaderboardContestCreate,
    )
    from generalresearch.models.thl.contest import (
        ContestPrize,
    )
    from generalresearch.models.thl.contest.definitions import (
        ContestType,
        ContestPrizeKind,
    )

    schema["example"] = LeaderboardContestCreate(
        name="Prizes for top survey takers this week",
        description="$15 1st place, $10 2nd, $5 3rd place US weekly",
        contest_type=ContestType.LEADERBOARD,
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
        leaderboard_key=f"leaderboard:{EXAMPLE_PRODUCT_ID}:us:weekly:2025-05-26:complete_count",
    ).model_dump(mode="json")

    return None


def _example_leaderboard_contest(schema: Dict) -> None:
    from generalresearch.models.thl.contest.leaderboard import (
        LeaderboardContest,
    )
    from generalresearch.models.thl.contest import (
        ContestPrize,
    )
    from generalresearch.models.thl.contest.definitions import (
        ContestType,
        ContestPrizeKind,
    )

    schema["example"] = LeaderboardContest(
        name="Prizes for top survey takers this week",
        description="$15 1st place, $10 2nd, $5 3rd place US weekly",
        contest_type=ContestType.LEADERBOARD,
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
        leaderboard_key=f"leaderboard:{EXAMPLE_PRODUCT_ID}:us:weekly:2025-05-26:complete_count",
        product_id=EXAMPLE_PRODUCT_ID,
    ).model_dump(mode="json")

    return None


def _example_leaderboard_contest_user_view(schema: Dict) -> None:
    from generalresearch.models.thl.contest.leaderboard import (
        LeaderboardContestUserView,
    )
    from generalresearch.models.thl.contest import (
        ContestPrize,
    )
    from generalresearch.models.thl.contest.definitions import (
        ContestPrizeKind,
        ContestType,
    )

    schema["example"] = LeaderboardContestUserView(
        name="Prizes for top survey takers this week",
        description="$15 1st place, $10 2nd, $5 3rd place US weekly",
        contest_type=ContestType.LEADERBOARD,
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
        leaderboard_key=f"leaderboard:{EXAMPLE_PRODUCT_ID}:us:weekly:2025-05-26:complete_count",
        product_id=EXAMPLE_PRODUCT_ID,
        product_user_id="test-user",
    ).model_dump(mode="json")
