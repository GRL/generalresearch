from __future__ import annotations

import logging
import random
from collections import defaultdict
from datetime import datetime, timezone
from typing import Literal, List, Dict, Tuple, Optional, Union

from pydantic import (
    Field,
    model_validator,
    computed_field,
    field_validator,
    ConfigDict,
)
from scipy.stats import hypergeom
from typing_extensions import Self

from generalresearch.currency import USDCent
from generalresearch.models.thl.contest import (
    ContestEntryRule,
    ContestWinner,
)
from generalresearch.models.thl.contest.contest import (
    Contest,
    ContestBase,
    ContestUserView,
)
from generalresearch.models.thl.contest.contest_entry import ContestEntry
from generalresearch.models.thl.contest.definitions import (
    ContestEntryType,
    ContestStatus,
    ContestType,
    ContestEndReason,
)
from generalresearch.models.thl.contest.examples import (
    _example_raffle_create,
    _example_raffle,
    _example_raffle_user_view,
)

logging.basicConfig()
LOG = logging.getLogger()
LOG.setLevel(logging.INFO)


class RaffleContestCreate(ContestBase):
    model_config = ConfigDict(
        validate_assignment=True,
        extra="forbid",
        json_schema_extra=_example_raffle_create,
    )

    contest_type: Literal[ContestType.RAFFLE] = Field(default=ContestType.RAFFLE)

    # Only cash supported for now. We don't have ledger methods to deal with ContestEntryType.COUNT
    entry_type: Literal[ContestEntryType.CASH] = Field(default=ContestEntryType.CASH)
    entry_rule: ContestEntryRule = Field(default_factory=ContestEntryRule)

    @model_validator(mode="after")
    def at_least_1_end_condition(self):
        ec = self.end_condition
        if not any([ec.target_entry_amount, ec.ends_at]):
            raise ValueError("At least one end condition must be specified")
        return self


class RaffleContest(RaffleContestCreate, Contest):
    model_config = ConfigDict(
        validate_assignment=True,
        extra="forbid",
        json_schema_extra=_example_raffle,
    )

    entries: List[ContestEntry] = Field(default_factory=list, exclude=True)

    current_amount: Union[int, USDCent] = Field(
        default=0, description="Sum of all entry amounts"
    )
    current_participants: int = Field(
        default=0, description="Count of unique participants"
    )

    @field_validator("entries", mode="after")
    def sort_entries(cls, v: List[ContestEntry]):
        return sorted(v, key=lambda x: x.created_at)

    @model_validator(mode="after")
    def validate_entry_type(self):
        assert all(
            entry.entry_type == self.entry_type for entry in self.entries
        ), f"all entries must be of type {self.entry_type}"
        return self

    @field_validator("current_amount", mode="before")
    def coerce_current_amount(cls, v, info):
        if v is None:
            return None
        if info.data.get("entry_type") == ContestEntryType.CASH:
            return USDCent(v)
        elif info.data.get("entry_type") == ContestEntryType.COUNT:
            return int(v)
        return v

    @model_validator(mode="after")
    def validate_end_condition_cash(self):
        # Make sure target amount is the right type
        if self.end_condition and self.end_condition.target_entry_amount:
            if self.entry_type == ContestEntryType.CASH:
                self.end_condition.target_entry_amount = USDCent(
                    self.end_condition.target_entry_amount
                )
            else:
                self.end_condition.target_entry_amount = int(
                    self.end_condition.target_entry_amount
                )
        return self

    def select_winners(self) -> List["ContestWinner"]:
        from generalresearch.models.thl.contest import ContestWinner

        assert self.is_complete(), "contest must be complete to select a winner"
        if not self.entries:
            return []

        # Each contest entry is one penny. We need to know how many
        #   total entries each user has.
        # If there is more than 1 prize, the winning entry is subtracted
        #   from the user's entry count
        user_amount = defaultdict(int)
        user_id_user = dict()
        for entry in self.entries:
            user_amount[entry.user.user_id] += entry.amount
            user_id_user[entry.user.user_id] = entry.user

        winners = []
        for prize in self.prizes:
            # todo: should the prizes be ordered lowest estimated_cash_value
            #   to highest? or the other way around?
            user_id = self.select_winner(user_amount)
            winners.append(ContestWinner(user=user_id_user[user_id], prize=prize))
            user_amount[user_id] -= 1
            user_amount = {k: v for k, v in user_amount.items() if v > 0}
            if not user_amount:
                break

        return winners

    def should_end(self) -> Tuple[bool, Optional["ContestEndReason"]]:
        res, msg = super().should_end()
        if res:
            return res, msg
        if self.status == ContestStatus.ACTIVE:
            if self.end_condition.target_entry_amount:
                if self.current_amount >= self.end_condition.target_entry_amount:
                    return True, ContestEndReason.TARGET_ENTRY_AMOUNT
        return False, None

    @staticmethod
    def select_winner(user_amount: Dict[int, int]) -> int:
        """
        user_amount: Dict[user_id, amount], is total entry count for each user,
         e.g. {1111: 5, 2222: 1, 3333: 2}
        returns: user_id of winner
        """
        user_idx = []
        total = 0
        for user, amount in user_amount.items():
            total += amount
            user_idx.append((user, total))
        # Generate a list of the cumulative sum of entries, indexed
        #   by each user. e.g. user_idx = [(1111, 5), (2222, 6), (3333, 8)]
        # We then generate a random number between 0 and the max, and
        #   the winner is the first user who's cumcount is greater.
        idx = random.randint(1, total)
        winner = next(x[0] for x in user_idx if idx <= x[1])
        return winner

    # @property
    # def current_entry_count(self) -> int:
    #     assert self.entry_type == ContestEntryType.COUNT
    #     # this is only valid if the amounts are 1
    #     assert not self.entries or all(e.amount == 1 for e in self.entries)
    #     return len(self.entries)

    def get_current_participants(self) -> int:
        return len({entry.user.user_id for entry in self.entries})

    def get_current_amount(self) -> Union[int | USDCent]:
        return sum([x.amount for x in self.entries])

    def get_user_amount(self, product_user_id: str) -> Union[int | USDCent]:
        # Sum of this user's amounts
        return sum(
            e.amount for e in self.entries if e.user.product_user_id == product_user_id
        )

    def is_complete(self) -> bool:
        """Check if contest has reached any completion condition"""
        if self.status == ContestStatus.COMPLETED:
            return True
        c = self.end_condition
        if c.target_entry_amount and self.current_amount >= c.target_entry_amount:
            return True
        if c.ends_at and datetime.now(tz=timezone.utc) >= c.ends_at:
            return True
        return False

    def model_dump_mysql(self):
        d = super().model_dump_mysql()
        d["entry_rule"] = self.entry_rule.model_dump_json()
        return d

    @classmethod
    def model_validate_mysql(cls, data: Dict) -> Self:
        data["entry_rule"] = ContestEntryRule.model_validate(data["entry_rule"])
        return super().model_validate_mysql(data)


class RaffleUserView(RaffleContest, ContestUserView):
    model_config = ConfigDict(
        validate_assignment=True,
        extra="forbid",
        json_schema_extra=_example_raffle_user_view,
    )

    user_amount: Union[int, USDCent] = Field(
        description="The total amount this user has entered"
    )
    user_amount_today: Union[int, USDCent] = Field(
        description="The total amount this user has entered in the past 24 hours"
    )

    @computed_field(
        description="Probability of this user winning 1 or more prizes, if the contest"
        "ended right now"
    )
    @property
    def current_win_probability(self) -> float:
        # equals 1 minus the probability of winning 0 prizes
        # This is equivalent to user_amount / current_amount if there is only 1 prize.
        if self.current_amount == 0:
            # otherwise the result is NaN. If there are no entrances yet, return 0
            return 0.0
        return 1 - hypergeom.pmf(
            0, self.current_amount, self.user_amount, self.prize_count
        )

    @computed_field(
        description="Probability of this user winning 1 or more prizes, once the contest"
        "is projected to end. This value is only calculated if the contest has a target_entry_amount"
        "end condition."
    )
    @property
    def projected_win_probability(self) -> Optional[float]:
        if self.end_condition.target_entry_amount is None:
            return None

        return 1 - hypergeom.pmf(
            0,
            max(self.end_condition.target_entry_amount, self.current_amount),
            self.user_amount,
            self.prize_count,
        )

    # Not sure how to return this in api response, too confusing. Maybe use later.
    # Left for tests only.
    @property
    def current_prize_count_probability(self) -> Dict[int, float]:
        # M: Population size (total entry amount)
        M = self.current_amount
        # n: number of success states (user's entry amount)
        n = self.user_amount
        # N: number of draws
        N = self.prize_count

        # Probability of drawing k of user's tickets (user winning K times)
        probs = {k: hypergeom.pmf(k, M, n, N) for k in range(1, N + 1)}
        return probs

    def is_entry_eligible(self, entry: ContestEntry) -> Tuple[bool, str]:
        if self.entry_rule.max_entry_amount_per_user:
            if (
                self.user_amount + entry.amount
            ) > self.entry_rule.max_entry_amount_per_user:
                return False, "Entry would exceed max amount per user."

        if self.entry_rule.max_daily_entries_per_user:
            if (
                self.user_amount_today + entry.amount
            ) > self.entry_rule.max_daily_entries_per_user:
                return False, "Entry would exceed max amount per user per day."
        return True, ""

    def is_user_eligible(self, country_iso: str) -> Tuple[bool, str]:
        passes, msg = super().is_user_eligible(country_iso=country_iso)
        if not passes:
            return False, msg

        if self.entry_rule.max_entry_amount_per_user:
            # Greater or equal b/c we're asking if the user is eligible to
            # enter MORE, now! If it equals, nothing is wrong, just that they
            # are not eligible anymore.
            if self.user_amount >= self.entry_rule.max_entry_amount_per_user:
                return False, "Reached max amount per user."

        if self.entry_rule.max_daily_entries_per_user:
            if self.user_amount_today >= self.entry_rule.max_daily_entries_per_user:
                return False, "Reached max amount today."

        # This would indicate something is wrong, as something else should have done this
        e, reason = self.should_end()
        if e:
            LOG.warning("contest should be over")
            return False, "contest is over"
        # todo: others in self.entry_rule ... min_completes, id_verified, etc.
        return True, ""
