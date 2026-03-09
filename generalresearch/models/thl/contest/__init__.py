from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Optional
from uuid import uuid4

from pydantic import (
    BaseModel,
    Field,
    PositiveInt,
    computed_field,
    model_validator,
)
from typing_extensions import Self

from generalresearch.currency import USDCent
from generalresearch.models.custom_types import AwareDatetimeISO, UUIDStr
from generalresearch.models.thl.contest.definitions import ContestPrizeKind
from generalresearch.models.thl.user import User


class ContestEntryRule(BaseModel):
    """Defines rules the user must meet to be allowed to enter this contest
    Only applies if the ContestType is ENTRY!
    """

    max_entry_amount_per_user: Optional[USDCent | PositiveInt] = Field(
        description="Maximum total value of entries per user",
        default=None,
    )

    max_daily_entries_per_user: Optional[PositiveInt] = Field(
        description="Maximum entries per user allowed per day for this contest",
        default=None,
    )

    # TODO: Only allow entries if user meets some criteria: gold-membership
    #   status, ID/phone verified, min_completes etc... Maybe these get put
    #   in a separate model b/c the could apply if the ContestType is not ENTRY
    min_completes: Optional[int] = None
    min_membership_level: Optional[int] = None
    id_verified: Optional[bool] = None


class ContestEndCondition(BaseModel):
    """Defines the conditions to evaluate to determine when the contest
    is over. Multiple conditions can be set. The contest is over
    once ANY conditions are met.
    """

    target_entry_amount: USDCent | PositiveInt | None = Field(
        default=None,
        ge=1,
        description="The contest is over once this amount is reached. (sum of all entry amount)",
    )
    # In a LeaderboardContest, ends_at equals the leaderboard's end period plus 90 minutes
    ends_at: Optional[AwareDatetimeISO] = Field(
        default=None, description="The contest is over at this time."
    )


class ContestPrize(BaseModel):
    kind: ContestPrizeKind = Field(
        description=ContestPrizeKind.as_openapi_with_value_descriptions()
    )
    name: Optional[str] = Field(default=None)
    description: Optional[str] = Field(default=None)

    estimated_cash_value: USDCent = Field(
        description="Estimated cash value of prize in USDCents",
    )
    cash_amount: Optional[USDCent] = Field(
        default=None,
        description="If the kind=ContestPrizeKind.CASH, this is the amount of the prize",
    )
    promotion_id: Optional[UUIDStr] = Field(
        default=None,
        description="If the kind=ContestPrizeKind.PROMOTION, this is the promotion ID",
    )
    # only if the contest.contest_type = LEADERBOARD
    leaderboard_rank: Optional[PositiveInt] = Field(
        default=None,
        description="The prize is for achieving this rank in the associated "
        "leaderboard. The highest rank is 1.",
    )

    @model_validator(mode="after")
    def validate_cash_value(self) -> Self:
        if self.kind == ContestPrizeKind.CASH:
            assert (
                self.estimated_cash_value == self.cash_amount
            ), "if kind is CASH, cash_amount must equal estimated_cash_value"
        return self


class ContestWinner(BaseModel):
    """
    In a Raffle, the ContestEntryType can be COUNT or CASH. In the CASH type,
    the unit of entry is 1 USDCent (one penny). Implicitly, each penny entered
    buys 1 entry into the raffle, and one entry is randomly selected for
    each prize.

    A contest should have as many winners as there are prizes
    special case 1: there are fewer entries than prizes
    special case 2: leaderboard contest with ties
    """

    uuid: UUIDStr = Field(default_factory=lambda: uuid4().hex)
    created_at: AwareDatetimeISO = Field(
        default_factory=lambda: datetime.now(tz=timezone.utc),
        description="When this user won this prize",
    )

    user: Optional[User] = Field(exclude=True, default=None)

    prize: ContestPrize = Field()

    awarded_cash_amount: Optional[USDCent] = Field(
        default=None,
        description="The actual amount this user receives. For cash prizes, if there was a tie, "
        "this could be different from the prize amount.",
    )

    @computed_field()
    @property
    def product_user_id(self) -> Optional[str]:
        # TODO: we'll have to pull username or censored emails or something
        if self.user:
            return self.user.product_user_id

    # @computed_field()
    # @property
    # def censored_product_user_id(self) -> str:
    #     return censor_product_user_id(self.user)

    def model_dump_mysql(self, contest_id: int) -> Dict[str, Any]:
        data = self.model_dump(mode="json", exclude={"user"})

        data["contest_id"] = contest_id
        data["created_at"] = self.created_at
        data["user_id"] = self.user.user_id
        data["prize"] = self.prize.model_dump_json()

        return data
