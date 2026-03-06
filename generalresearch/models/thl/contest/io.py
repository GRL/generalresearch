from datetime import datetime, timezone
from typing import Union
from uuid import uuid4

from generalresearch.models.thl.contest.definitions import ContestType
from generalresearch.models.thl.contest.leaderboard import (
    LeaderboardContest,
    LeaderboardContestCreate,
    LeaderboardContestUserView,
)
from generalresearch.models.thl.contest.milestone import (
    MilestoneContest,
    MilestoneContestCreate,
    MilestoneUserView,
)
from generalresearch.models.thl.contest.raffle import (
    RaffleContest,
    RaffleContestCreate,
    RaffleUserView,
)

model_cls = {
    ContestType.RAFFLE: RaffleContest,
    ContestType.MILESTONE: MilestoneContest,
    ContestType.LEADERBOARD: LeaderboardContest,
}
user_model_cls = {
    ContestType.RAFFLE: RaffleUserView,
    ContestType.MILESTONE: MilestoneUserView,
    ContestType.LEADERBOARD: LeaderboardContestUserView,
}
ContestCreate = Union[
    RaffleContestCreate, LeaderboardContestCreate, MilestoneContestCreate
]
from generalresearch.models.thl.contest.contest import Contest


def contest_create_to_contest(
    product_id: str, contest_create: ContestCreate
) -> Contest:
    now = datetime.now(tz=timezone.utc)
    d = contest_create.model_dump(mode="json")
    d["uuid"] = uuid4().hex
    d["product_id"] = product_id
    d["created_at"] = now
    d["updated_at"] = now
    return model_cls[contest_create.contest_type].model_validate(d)
