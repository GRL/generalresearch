from __future__ import annotations

import logging
from datetime import timedelta
from typing import Any, Dict, Literal, Optional, Tuple

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    PositiveInt,
)
from typing_extensions import Self

from generalresearch.models.custom_types import AwareDatetimeISO
from generalresearch.models.thl.contest.contest import (
    Contest,
    ContestBase,
    ContestUserView,
)
from generalresearch.models.thl.contest.contest_entry import ContestEntry
from generalresearch.models.thl.contest.definitions import (
    ContestEndReason,
    ContestEntryTrigger,
    ContestEntryType,
    ContestStatus,
    ContestType,
)
from generalresearch.models.thl.contest.examples import (
    _example_milestone,
    _example_milestone_create,
    _example_milestone_user_view,
)

logging.basicConfig()
LOG = logging.getLogger()
LOG.setLevel(logging.INFO)


class MilestoneEntry(ContestEntry):
    # Same as ContestEntry, but always a count.

    entry_type: Literal[ContestEntryType.COUNT] = Field(default=ContestEntryType.COUNT)

    amount: int = Field(
        description="The amount of the entry in integer counts",
        gt=0,
    )


class MilestoneContestEndCondition(BaseModel):
    """Defines the conditions to evaluate to determine when the contest is over.
    Multiple conditions can be set. The contest is over once ANY conditions are met.
    """

    max_winners: Optional[PositiveInt] = Field(
        default=None,
        description="The contest will end once this many users have won (i.e. reached"
        "the milestone).",
    )

    ends_at: Optional[AwareDatetimeISO] = Field(
        default=None, description="The Contest is over at the ends_at time."
    )


class MilestoneContestConfig(BaseModel):
    """
    Contest configuration specific to a milestone contest
    """

    target_amount: PositiveInt = Field(
        description="Each user 'wins' (receives prizes) once this target amount is reached."
    )
    entry_trigger: Optional[ContestEntryTrigger] = Field(
        description="What user action triggers an entry automatically.",
        default=None,
    )

    # These two fields allow something like: "Get a complete in your first 24 hours!"
    valid_for: Optional[timedelta] = Field(
        description="The time after valid_for_event for which the contest is open",
        default=None,
    )
    valid_for_event: Optional[Literal["signup"]] = Field(default=None)


class MilestoneContestCreate(ContestBase, MilestoneContestConfig):
    """Reward is guaranteed for everyone who passes a threshold / meets
    some criteria.

    e.g.    $5 bonus after 10 lifetime completes, OR "after earning $100",
            OR "passing ID verification".

            A milestone has at most 1 entry (contest_entry) table per user
            per contest. In that entry, we track the "amount", whether is
            it completes, money, whatever, as an integer.

    An instance of a milestone contest is "scoped" to an individual user
    (i.e, the entries/balance should only be populated for the user of
    interest only)
    """

    model_config = ConfigDict(
        validate_assignment=True,
        extra="forbid",
        json_schema_extra=_example_milestone_create,
    )

    contest_type: Literal[ContestType.MILESTONE] = Field(default=ContestType.MILESTONE)

    end_condition: MilestoneContestEndCondition = Field()


class MilestoneContest(MilestoneContestCreate, Contest):
    model_config = ConfigDict(
        validate_assignment=True,
        extra="forbid",
        json_schema_extra=_example_milestone,
    )

    entry_type: Literal[ContestEntryType.COUNT] = Field(default=ContestEntryType.COUNT)

    # Note: a milestone can only ever be reached ONCE per user.
    win_count: int = Field(
        description="The number of times the milestone has been reached.",
        default=0,
    )

    def should_end(self) -> Tuple[bool, Optional[ContestEndReason]]:
        res, msg = super().should_end()

        if res:
            return res, msg

        if self.status == ContestStatus.ACTIVE:
            if self.end_condition.max_winners:
                if self.win_count >= self.end_condition.max_winners:
                    return True, ContestEndReason.MAX_WINNERS

        return False, None

    def select_winners(self) -> None:
        # milestone contest winners are selected as each user reaches the milestone, so this
        #   just does nothing
        return None

    def model_dump_mysql(self) -> Dict[str, Any]:
        d = super().model_dump_mysql(
            exclude={
                "entry_trigger",
                "target_amount",
                "valid_for",
                "valid_for_event",
            }
        )
        d["milestone_config"] = MilestoneContestConfig(
            entry_trigger=self.entry_trigger,
            target_amount=self.target_amount,
            valid_for=self.valid_for,
            valid_for_event=self.valid_for_event,
        ).model_dump_json()
        return d

    @classmethod
    def model_validate_mysql(cls, data: Dict[str, Any]) -> Self:
        data.update(
            MilestoneContestConfig.model_validate(data["milestone_config"]).model_dump()
        )
        data["end_condition"] = MilestoneContestEndCondition.model_validate(
            data["end_condition"]
        )
        return super().model_validate_mysql(data)


class MilestoneUserView(MilestoneContest, ContestUserView):
    model_config = ConfigDict(
        validate_assignment=True,
        extra="forbid",
        json_schema_extra=_example_milestone_user_view,
    )

    valid_until: Optional[AwareDatetimeISO] = Field(
        default=None,
        exclude=True,
        description="If valid_for is set, this gets populated wrt this user",
    )
    user_amount: int = Field(
        description="The total amount for this user for this contest"
    )

    def should_award(self):
        if self.status == ContestStatus.ACTIVE:
            if self.should_have_awarded():
                return True
        return False

    def should_have_awarded(self):
        if self.target_amount:
            if self.user_amount >= self.target_amount:
                return True
        return False

    def is_user_eligible(self, country_iso: str) -> Tuple[bool, str]:
        passes, msg = super().is_user_eligible(country_iso=country_iso)
        if not passes:
            return False, msg

        if self.should_have_awarded():
            return False, "User should have won already"

        if self.user_winnings:
            return False, "User already won"

        # todo: check valid_for and valid_for_event
        # i.e. it hasn't been >24 hrs since user signed up, or whatever

        # This would indicate something is wrong, as something else should have done this
        e, _ = self.should_end()
        if e:
            LOG.warning("contest should be over")
            return False, "contest is over"

        # TODO: others in self.entry_rule ... min_completes, id_verified, etc.
        return True, ""
