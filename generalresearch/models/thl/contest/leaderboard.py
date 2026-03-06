from datetime import datetime, timezone, timedelta
from typing import Optional, Literal, List, Tuple, Dict, Any

from pydantic import (
    Field,
    ConfigDict,
    computed_field,
    model_validator,
    PrivateAttr,
)
from redis import Redis
from typing_extensions import Self

from generalresearch.decorators import LOG
from generalresearch.managers.leaderboard import country_timezone
from generalresearch.managers.leaderboard.manager import LeaderboardManager
from generalresearch.managers.thl.user_manager.user_manager import (
    UserManager,
)
from generalresearch.models.thl.contest import (
    ContestWinner,
    ContestEndCondition,
)
from generalresearch.models.thl.contest.contest import (
    Contest,
    ContestBase,
    ContestUserView,
)
from generalresearch.models.thl.contest.definitions import (
    ContestStatus,
    ContestType,
    ContestPrizeKind,
    ContestEndReason,
    LeaderboardTieBreakStrategy,
)
from generalresearch.models.thl.contest.examples import (
    _example_leaderboard_contest_user_view,
    _example_leaderboard_contest,
    _example_leaderboard_contest_create,
)
from generalresearch.models.thl.leaderboard import (
    Leaderboard,
    LeaderboardCode,
    LeaderboardFrequency,
)


class LeaderboardContestCreate(ContestBase):
    model_config = ConfigDict(
        validate_assignment=True,
        extra="forbid",
        json_schema_extra=_example_leaderboard_contest_create,
    )

    contest_type: Literal[ContestType.LEADERBOARD] = Field(
        default=ContestType.LEADERBOARD
    )

    # leaderboard:{product_id}:{country_iso}:{freq.value}:{date_str}:{board_code.value}"
    leaderboard_key: str = Field(
        description="The specific leaderboard instance this contest is connected to",
        examples=[
            "leaderboard:7a9d8d02334449ceb105764f77e1ba97:us:weekly:2025-05-26:complete_count"
        ],
    )

    # This is optional here. It'll get calculated from the leaderboard's end time + 90 min.
    end_condition: ContestEndCondition = Field(default_factory=ContestEndCondition)

    @model_validator(mode="after")
    def check_prize_rank(self) -> Self:
        for prize in self.prizes:
            assert prize.leaderboard_rank, "prize leaderboard_rank must be set"

        self.prizes.sort(key=lambda x: x.leaderboard_rank)
        ranks = {x.leaderboard_rank for x in self.prizes}
        assert None not in ranks, "Must have leaderboard_rank defined"
        assert min(ranks) == 1, "Must start with rank 1"
        assert ranks == set(
            range(min(ranks), max(ranks) + 1)
        ), "cannot skip prize leaderboard_ranks"
        return self

    @model_validator(mode="after")
    def validate_leaderboard_key(self) -> Self:
        # Force validation
        _ = self.leaderboard_model
        return self

    @model_validator(mode="after")
    def check_end_condition(self) -> Self:
        assert (
            not self.end_condition.target_entry_amount
        ), "target_entry_amount not valid in leaderboard contest"
        # the ends_at will get set automatically from the leaderboard_key
        return self

    @property
    def leaderboard_key_parts(self) -> Dict:
        assert self.leaderboard_key.count(":") == 5, "invalid leaderboard_key"
        parts = self.leaderboard_key.split(":")
        _, product_id, country_iso, freq_str, date_str, board_code_value = parts
        freq = LeaderboardFrequency(freq_str)
        board_code = LeaderboardCode(board_code_value)
        timezone = country_timezone()[country_iso]
        period_start_local = datetime.strptime(date_str, "%Y-%m-%d").replace(
            tzinfo=timezone
        )
        return {
            "freq": freq,
            "product_id": product_id,
            "board_code": board_code,
            "period_start_local": period_start_local,
            "country_iso": country_iso,
        }

    @property
    def leaderboard_model(self) -> Leaderboard:
        parts = self.leaderboard_key_parts
        # This isn't hitting the db/redis or anything. Just initializing the model, so we can access
        #   some computed properties.
        return Leaderboard.model_validate(
            parts | {"row_count": 0, "bpid": parts["product_id"]}
        )


class LeaderboardContest(LeaderboardContestCreate, Contest):
    model_config = ConfigDict(
        validate_assignment=True,
        extra="forbid",
        json_schema_extra=_example_leaderboard_contest,
        arbitrary_types_allowed=True,
    )

    # TODO: only this strategy supported for now
    tie_break_strategy: Literal[LeaderboardTieBreakStrategy.SPLIT_PRIZE_POOL] = Field(
        default=LeaderboardTieBreakStrategy.SPLIT_PRIZE_POOL
    )

    _redis_client: Optional[Redis] = PrivateAttr(default=None)
    _user_manager: Optional[UserManager] = PrivateAttr(default=None)

    @model_validator(mode="after")
    def validate_product_lb_key(self) -> Self:
        assert (
            self.product_id == self.leaderboard_key_parts["product_id"]
        ), "leaderboard_key product_id is invalid"
        if self.country_isos:
            assert (
                len(self.country_isos) == 1
            ), "Can only set 1 country_iso in a leaderboard contest"
            assert (
                list(self.country_isos)[0] == self.leaderboard_key_parts["country_iso"]
            ), "leaderboard_key country_iso must match the country_isos"
        else:
            self.country_isos = {self.leaderboard_key_parts["country_iso"]}
        return self

    @model_validator(mode="after")
    def validate_tie_break(self) -> Self:
        if self.tie_break_strategy == LeaderboardTieBreakStrategy.SPLIT_PRIZE_POOL:
            assert all(
                p.kind == ContestPrizeKind.CASH for p in self.prizes
            ), "All prizes must be cash due to the tie-break strategy"
        return self

    @model_validator(mode="after")
    def set_ends_at(self) -> Self:
        ends_at = self.leaderboard_model.period_end_utc + timedelta(minutes=90)
        assert self.end_condition.ends_at in {
            None,
            ends_at,
        }, "Do not set the end_condition. It will be calculated"
        self.end_condition.ends_at = ends_at
        return self

    def get_leaderboard(self) -> Leaderboard:
        lbm = self.get_leaderboard_manager()
        return lbm.get_leaderboard()

    def get_leaderboard_manager(self) -> LeaderboardManager:
        parts = self.leaderboard_key_parts
        lbm = LeaderboardManager(
            redis_client=self._redis_client,
            board_code=parts["board_code"],
            country_iso=parts["country_iso"],
            freq=parts["freq"],
            product_id=parts["product_id"],
            within_time=parts["period_start_local"],
        )
        return lbm

    def should_end(self) -> Tuple[bool, Optional[ContestEndReason]]:
        if self.status == ContestStatus.ACTIVE:
            if self.end_condition.ends_at:
                if datetime.now(tz=timezone.utc) >= self.end_condition.ends_at:
                    return True, ContestEndReason.ENDS_AT

        return False, None

    def select_winners(self) -> List[ContestWinner]:
        from generalresearch.models.thl.contest.utils import (
            distribute_leaderboard_prizes,
        )

        assert self.should_end(), "contest must be complete to select a winner"
        assert (
            self.tie_break_strategy == LeaderboardTieBreakStrategy.SPLIT_PRIZE_POOL
        ), "invalid tie break strategy"
        redis_client = self._redis_client
        user_manager = self._user_manager
        assert redis_client and user_manager, "must set redis_client and user_manager"

        lb = self.get_leaderboard()
        prize_values = [p.cash_amount for p in self.prizes]
        assert all(x for x in prize_values), "invalid prize cash amount"
        result = distribute_leaderboard_prizes(prize_values, lb.rows)
        user_rank = {r.bpuid: r.rank for r in lb.rows}
        winners = []
        prizes = sorted(self.prizes, key=lambda x: x.cash_amount, reverse=True)
        for bpuid, cash_value in result.items():
            prize = prizes[user_rank[bpuid] - 1]  # lb rank starts at 1 :facepalm:
            user = user_manager.get_user(
                product_id=self.product_id, product_user_id=bpuid
            )
            winners.append(
                ContestWinner(user=user, awarded_cash_amount=cash_value, prize=prize)
            )
        return winners

    @computed_field
    @property
    def country_iso(self) -> str:
        return self.leaderboard_key.split(":")[2]

    def model_dump_mysql(self) -> Dict[str, Any]:
        d = super().model_dump_mysql(
            exclude={
                "tie_break_strategy",
                "country_iso",
            }
        )
        return d


class LeaderboardContestUserView(LeaderboardContest, ContestUserView):
    model_config = ConfigDict(
        validate_assignment=True,
        extra="forbid",
        json_schema_extra=_example_leaderboard_contest_user_view,
    )

    @computed_field(description="The current rank of this user in this contest")
    @property
    def user_rank(self) -> Optional[int]:
        if not self._redis_client:
            return None

        lb = self.get_leaderboard()
        for row in lb.rows:
            if row.bpuid == self.product_user_id:
                return row.rank

        return None

    def is_user_eligible(self, country_iso: str) -> Tuple[bool, str]:
        passes, msg = super().is_user_eligible(country_iso=country_iso)
        if not passes:
            return False, msg

        if country_iso != self.country_iso:
            return False, "Invalid country"

        if self.user_winnings:
            return False, "User already won"

        now = datetime.now(tz=timezone.utc)
        if self.leaderboard_model.period_end_utc < now:
            return False, "Contest is over"
        if self.leaderboard_model.period_start_utc > now:
            return False, "Contest hasn't started"

        # This would indicate something is wrong, as something else should have done this
        e, reason = self.should_end()
        if e:
            LOG.warning("contest should be over")
            return False, "contest is over"

        return True, ""
