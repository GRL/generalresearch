from __future__ import annotations

from typing import Optional, Dict, Set, Tuple, List

from pydantic import NonNegativeInt, Field, ConfigDict, BaseModel

from generalresearch.models import Source
from generalresearch.models.custom_types import (
    AwareDatetimeISO,
    UUIDStr,
    CoercedStr,
    BigAutoInteger,
)
from generalresearch.models.thl.locales import CountryISO, LanguageISO
from generalresearch.models.thl.survey.condition import (
    MarketplaceCondition,
    ConditionValueType,
)


class LucidCondition(MarketplaceCondition):
    model_config = ConfigDict(populate_by_name=True, frozen=False, extra="ignore")

    id: BigAutoInteger = Field()
    source: Source = Field(default=Source.LUCID)
    question_id: Optional[CoercedStr] = Field(
        min_length=1,
        max_length=16,
        pattern=r"^[0-9]+$",
    )
    country_iso: CountryISO = Field()
    language_iso: LanguageISO = Field()

    @property
    def criterion_hash(self) -> None:
        # We use the integer ID throughout. Make sure we don't accidentally use this
        raise ValueError()

    def __hash__(self):
        # this is so it can be put into a set / dictionary key
        return hash(self.id)

    @classmethod
    def from_mysql(cls, x):
        x["value_type"] = ConditionValueType.LIST
        x["negate"] = False
        x["values"] = x.pop("pre_codes").split("|")
        x["question_id"] = str(x["question_id"])
        return cls.model_validate(x)


class LucidQualification(BaseModel):
    criterion: int = Field()
    modified: AwareDatetimeISO = Field(description="modified or created")


class LucidQuota(BaseModel):
    id: BigAutoInteger = Field()
    uuid: UUIDStr = Field()
    upper_limit: NonNegativeInt = Field(examples=[20])
    criteria: List[int] = Field(min_length=1, max_length=25)
    modified: AwareDatetimeISO = Field(description="modified or created")
    # We'll look this up with a special mysql query. If None, it means
    #   that we don't know.
    finish_count: Optional[int] = Field(default=None)

    def __hash__(self):
        return hash(self.id)

    @property
    def is_open(self) -> bool:
        return self.upper_limit > self.finish_count

    def passes(self, criteria_evaluation: Dict[int, Optional[bool]]) -> bool:
        # Passes means we 1) meet all conditions (aka "match") AND 2) the quota is open.
        return self.is_open and self.matches(criteria_evaluation)

    def matches(self, criteria_evaluation: Dict[int, Optional[bool]]) -> bool:
        # Matches means we meet all conditions.
        # We can "match" a quota that is closed. In that case, we would not be eligible for the survey.
        return all(criteria_evaluation.get(c) for c in self.criteria)

    # def matches_optional(
    #     self, criteria_evaluation: Dict[int, Optional[bool]]
    # ) -> Optional[bool]:
    #     # We need to know if any conditions are unknown to avoid matching a full quota. If any fail,
    #     #   then we know we fail regardless of any being unknown.
    #     evals = [criteria_evaluation.get(c) for c in self.criteria]
    #     if False in evals:
    #         return False
    #     if None in evals:
    #         return None
    #     return True

    def matches_soft(
        self, criteria_evaluation: Dict[int, Optional[bool]]
    ) -> Tuple[Optional[bool], Set[int]]:
        # Passes back "matches" (T/F/none) and a list of unknown criterion hashes
        hash_evals = {cell: criteria_evaluation.get(cell) for cell in self.criteria}
        evals = set(hash_evals.values())
        if False in evals:
            return False, set()
        if None in evals:
            return None, {cell for cell, ev in hash_evals.items() if ev is None}
        return True, set()
