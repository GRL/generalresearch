from __future__ import annotations

import json
from abc import abstractmethod, ABC
from datetime import timezone, datetime
from typing import List, Tuple, Optional, Dict
from uuid import uuid4

from pydantic import (
    BaseModel,
    Field,
    HttpUrl,
    ConfigDict,
    model_validator,
    NonNegativeInt,
)
from typing_extensions import Self

from generalresearch.models.custom_types import UUIDStr, AwareDatetimeISO
from generalresearch.models.thl.contest import (
    ContestEndCondition,
    ContestPrize,
    ContestWinner,
)
from generalresearch.models.thl.contest.definitions import (
    ContestStatus,
    ContestType,
    ContestEndReason,
)
from generalresearch.models.thl.locales import CountryISOs


class ContestBase(BaseModel, ABC):
    """
    This model will be used also as the "Create" API class, so nothing
    goes on here that is not settable by an api user.
    """

    model_config = ConfigDict(validate_assignment=True)

    name: str = Field(
        max_length=128, description="Name of contest. Can be displayed to user."
    )
    description: Optional[str] = Field(
        default=None,
        max_length=2048,
        description="Description of contest. Can be displayed to user.",
    )

    contest_type: ContestType = Field(
        description=ContestType.as_openapi_with_value_descriptions()
    )

    end_condition: ContestEndCondition = Field()
    """Defines the conditions to win one or more prizes once the contest is ended"""
    prizes: List[ContestPrize] = Field(default_factory=list, min_items=1)

    starts_at: AwareDatetimeISO = Field(
        description="When the contest starts",
        default_factory=lambda: datetime.now(tz=timezone.utc),
    )

    terms_and_conditions: Optional[HttpUrl] = Field(default=None)

    status: ContestStatus = Field(default=ContestStatus.ACTIVE)

    country_isos: Optional[CountryISOs] = Field(
        description="Contest is restricted to these countries. If null, all countries are allowed",
        default=None,
    )

    def update(self, **kwargs) -> None:
        # For dealing with updating multiple fields at once that would
        #   otherwise break validations
        self.model_config["validate_assignment"] = False
        for k, v in kwargs.items():
            setattr(self, k, v)
        self.model_config["validate_assignment"] = True
        self.__class__.model_validate(self)


class Contest(ContestBase):
    id: Optional[int] = Field(
        default=None,
        exclude=True,
        description="pk in db",
    )

    uuid: UUIDStr = Field(default_factory=lambda: uuid4().hex)

    product_id: UUIDStr = Field(description="Contest applies only to a single BP")

    created_at: AwareDatetimeISO = Field(
        default_factory=lambda: datetime.now(tz=timezone.utc),
        description="When this contest was created",
    )
    updated_at: AwareDatetimeISO = Field(
        default_factory=lambda: datetime.now(tz=timezone.utc),
        description="When this contest was last modified. Does not include "
        "entries being created/modified",
    )

    ended_at: Optional[AwareDatetimeISO] = Field(
        default=None,
        description="When the contest ended",
    )

    end_reason: Optional[ContestEndReason] = Field(
        default=None,
        description="The reason the contest ended",
    )

    all_winners: Optional[List[ContestWinner]] = Field(
        default=None,
        exclude=True,
        description="All prize winners of this contest",
    )

    @model_validator(mode="after")
    def validate_end(self):
        if self.status == ContestStatus.ACTIVE:
            assert self.ended_at is None, "ended_at when status is active"
            assert self.end_reason is None, "end_reason when status is active"
            assert self.all_winners is None, "all_winners when status is active"
        else:
            assert self.ended_at, "must set ended_at if contest ended"
            assert self.end_reason, "must set end_reason if contest ended"

        return self

    # def is_user_winner(self, user: User):
    #     assert self.status == ContestStatus.COMPLETED
    #     result = self.result
    #     for winner in result.winners:
    #         if winner.user_id == user.user_id:
    #             return True
    #     return False

    def should_end(self) -> Tuple[bool, Optional[ContestEndReason]]:
        if self.status == ContestStatus.ACTIVE:
            if self.end_condition.ends_at:
                if datetime.now(tz=timezone.utc) >= self.end_condition.ends_at:
                    return True, ContestEndReason.ENDS_AT

        return False, None

    @abstractmethod
    def select_winners(self) -> Optional[List[ContestWinner]]: ...

    def end_contest(self) -> None:
        e, reason = self.should_end()
        if not e:
            return None
        # todo: Acquire a lock here, b/c this next part involves randomness
        #   so we can't have it happen more than once
        winners = self.select_winners()
        if winners is not None:
            self.update(
                status=ContestStatus.COMPLETED,
                ended_at=datetime.now(tz=timezone.utc),
                end_reason=reason,
                all_winners=winners,
            )
        else:
            self.update(
                status=ContestStatus.COMPLETED,
                ended_at=datetime.now(tz=timezone.utc),
                end_reason=reason,
            )
        return None

    def model_dump_mysql(self, **kwargs) -> Dict:
        d = self.model_dump(mode="json", **kwargs)

        d["created_at"] = self.created_at
        d["updated_at"] = self.updated_at
        d["starts_at"] = self.starts_at
        if self.ended_at:
            d["ended_at"] = self.ended_at
        d["end_condition"] = self.end_condition.model_dump_json()
        d["prizes"] = json.dumps([p.model_dump(mode="json") for p in self.prizes])

        return d

    @classmethod
    def model_validate_mysql(cls, data) -> Self:
        data = {k: v for k, v in data.items() if k in cls.model_fields.keys()}
        if isinstance(data["end_condition"], dict):
            data["end_condition"] = ContestEndCondition.model_validate(
                data["end_condition"]
            )
        data["prizes"] = [ContestPrize.model_validate(p) for p in data["prizes"]]
        return cls.model_validate(data)

    @property
    def prize_count(self) -> NonNegativeInt:
        return len(self.prizes)


class ContestUserView(Contest):
    """This is the user's 'view' of a contest."""

    product_user_id: str = Field()

    # TODO: this could show a more detailed ContestWinner model, maybe
    # including like shipping status or whatever
    user_winnings: List[ContestWinner] = Field(
        description="The prizes won in this contest by the requested user",
        default_factory=list,
    )

    def is_user_eligible(self, country_iso: str) -> Tuple[bool, str]:
        now = datetime.now(tz=timezone.utc)

        assert country_iso.lower() == country_iso
        if now < self.starts_at:
            return False, "contest has not yet started"
        if self.status != ContestStatus.ACTIVE:
            return False, "contest not active"
        if self.country_isos is not None and country_iso not in self.country_isos:
            return False, "ineligible country"

        return True, ""
