from datetime import datetime, timezone
from decimal import Decimal
from typing import TYPE_CHECKING, Callable
from uuid import uuid4

import pytest

from generalresearch.currency import USDCent

if TYPE_CHECKING:
    from generalresearch.managers.thl.contest_manager import ContestManager
    from generalresearch.models.thl.contest import (
        ContestEndCondition,
        ContestPrize,
    )
    from generalresearch.models.thl.contest.contest import Contest
    from generalresearch.models.thl.contest.definitions import (
        ContestPrizeKind,
        ContestType,
    )
    from generalresearch.models.thl.contest.io import contest_create_to_contest
    from generalresearch.models.thl.contest.leaderboard import (
        LeaderboardContestCreate,
    )
    from generalresearch.models.thl.contest.milestone import (
        ContestEntryTrigger,
        MilestoneContestCreate,
        MilestoneContestEndCondition,
    )
    from generalresearch.models.thl.contest.raffle import (
        ContestEntryType,
        RaffleContestCreate,
    )
    from generalresearch.models.thl.product import Product
    from generalresearch.models.thl.user import User


@pytest.fixture
def raffle_contest_create() -> "RaffleContestCreate":
    from generalresearch.models.thl.contest import (
        ContestEndCondition,
        ContestPrize,
    )
    from generalresearch.models.thl.contest.definitions import (
        ContestPrizeKind,
        ContestType,
    )
    from generalresearch.models.thl.contest.raffle import (
        ContestEntryType,
        RaffleContestCreate,
    )

    # This is what we'll get from the fastapi endpoint
    return RaffleContestCreate(
        name="test",
        contest_type=ContestType.RAFFLE,
        entry_type=ContestEntryType.CASH,
        prizes=[
            ContestPrize(
                name="iPod 64GB White",
                kind=ContestPrizeKind.PHYSICAL,
                estimated_cash_value=USDCent(100),
            )
        ],
        end_condition=ContestEndCondition(target_entry_amount=USDCent(100)),
    )


@pytest.fixture
def raffle_contest_in_db(
    product_user_wallet_yes: "Product",
    raffle_contest_create: "RaffleContestCreate",
    contest_manager: "ContestManager",
) -> "Contest":
    return contest_manager.create(
        product_id=product_user_wallet_yes.uuid, contest_create=raffle_contest_create
    )


@pytest.fixture
def raffle_contest(
    product_user_wallet_yes: "Product", raffle_contest_create: "RaffleContestCreate"
) -> "Contest":
    from generalresearch.models.thl.contest.io import contest_create_to_contest

    return contest_create_to_contest(
        product_id=product_user_wallet_yes.uuid, contest_create=raffle_contest_create
    )


@pytest.fixture(scope="function")
def raffle_contest_factory(
    product_user_wallet_yes: "Product",
    raffle_contest_create: "RaffleContestCreate",
    contest_manager: "ContestManager",
) -> Callable[..., "Contest"]:

    def _inner(**kwargs):
        raffle_contest_create.update(**kwargs)
        return contest_manager.create(
            product_id=product_user_wallet_yes.uuid,
            contest_create=raffle_contest_create,
        )

    return _inner


@pytest.fixture
def milestone_contest_create() -> "MilestoneContestCreate":
    from generalresearch.models.thl.contest import (
        ContestPrize,
    )
    from generalresearch.models.thl.contest.definitions import (
        ContestPrizeKind,
        ContestType,
    )
    from generalresearch.models.thl.contest.milestone import (
        ContestEntryTrigger,
        MilestoneContestCreate,
        MilestoneContestEndCondition,
    )

    # This is what we'll get from the fastapi endpoint
    return MilestoneContestCreate(
        name="Win a 50% bonus for 7 days and a $1 bonus after your first 3 completes!",
        description="only valid for the first 5 users",
        contest_type=ContestType.MILESTONE,
        prizes=[
            ContestPrize(
                name="50% for 7 days",
                kind=ContestPrizeKind.PROMOTION,
                estimated_cash_value=USDCent(0),
            ),
            ContestPrize(
                name="$1 Bonus",
                kind=ContestPrizeKind.CASH,
                cash_amount=USDCent(1_00),
                estimated_cash_value=USDCent(1_00),
            ),
        ],
        end_condition=MilestoneContestEndCondition(
            ends_at=datetime(year=2030, month=1, day=1, tzinfo=timezone.utc),
            max_winners=5,
        ),
        entry_trigger=ContestEntryTrigger.TASK_COMPLETE,
        target_amount=3,
    )


@pytest.fixture
def milestone_contest_in_db(
    product_user_wallet_yes: "Product",
    milestone_contest_create: "MilestoneContestCreate",
    contest_manager: "ContestManager",
) -> "Contest":
    return contest_manager.create(
        product_id=product_user_wallet_yes.uuid, contest_create=milestone_contest_create
    )


@pytest.fixture
def milestone_contest(
    product_user_wallet_yes: "Product",
    milestone_contest_create: "MilestoneContestCreate",
) -> "Contest":
    from generalresearch.models.thl.contest.io import contest_create_to_contest

    return contest_create_to_contest(
        product_id=product_user_wallet_yes.uuid, contest_create=milestone_contest_create
    )


@pytest.fixture(scope="function")
def milestone_contest_factory(
    product_user_wallet_yes: "Product",
    milestone_contest_create: "MilestoneContestCreate",
    contest_manager: "ContestManager",
) -> Callable[..., "Contest"]:

    def _inner(**kwargs):
        milestone_contest_create.update(**kwargs)
        return contest_manager.create(
            product_id=product_user_wallet_yes.uuid,
            contest_create=milestone_contest_create,
        )

    return _inner


@pytest.fixture
def leaderboard_contest_create(
    product_user_wallet_yes: "Product",
) -> "LeaderboardContestCreate":
    from generalresearch.models.thl.contest import (
        ContestPrize,
    )
    from generalresearch.models.thl.contest.definitions import (
        ContestPrizeKind,
        ContestType,
    )
    from generalresearch.models.thl.contest.leaderboard import (
        LeaderboardContestCreate,
    )

    # This is what we'll get from the fastapi endpoint
    return LeaderboardContestCreate(
        name="test",
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
        ],
        leaderboard_key=f"leaderboard:{product_user_wallet_yes.uuid}:us:daily:2025-01-01:complete_count",
    )


@pytest.fixture
def leaderboard_contest_in_db(
    product_user_wallet_yes: "Product",
    leaderboard_contest_create: "LeaderboardContestCreate",
    contest_manager: "ContestManager",
) -> "Contest":
    return contest_manager.create(
        product_id=product_user_wallet_yes.uuid,
        contest_create=leaderboard_contest_create,
    )


@pytest.fixture
def leaderboard_contest(
    product_user_wallet_yes: "Product",
    leaderboard_contest_create: "LeaderboardContestCreate",
):
    from generalresearch.models.thl.contest.io import contest_create_to_contest

    return contest_create_to_contest(
        product_id=product_user_wallet_yes.uuid,
        contest_create=leaderboard_contest_create,
    )


@pytest.fixture(scope="function")
def leaderboard_contest_factory(
    product_user_wallet_yes: "Product",
    leaderboard_contest_create: "LeaderboardContestCreate",
    contest_manager: "ContestManager",
) -> Callable[..., "Contest"]:

    def _inner(**kwargs):
        leaderboard_contest_create.update(**kwargs)
        return contest_manager.create(
            product_id=product_user_wallet_yes.uuid,
            contest_create=leaderboard_contest_create,
        )

    return _inner


@pytest.fixture
def user_with_money(
    request,
    user_factory: Callable[..., "User"],
    product_user_wallet_yes: "Product",
    thl_lm,
) -> "User":
    from generalresearch.models.thl.user import User

    params = getattr(request, "param", dict()) or {}
    min_balance = int(params.get("min_balance", USDCent(1_00)))

    user: User = user_factory(product=product_user_wallet_yes)
    wallet = thl_lm.get_account_or_create_user_wallet(user)
    balance = thl_lm.get_account_balance(wallet)
    todo = min_balance - balance
    if todo > 0:
        # # Put money in user's wallet
        thl_lm.create_tx_user_bonus(
            user=user,
            ref_uuid=uuid4().hex,
            description="bonus",
            amount=Decimal(todo) / 100,
        )
        print(f"wallet balance: {thl_lm.get_user_wallet_balance(user=user)}")

    return user
