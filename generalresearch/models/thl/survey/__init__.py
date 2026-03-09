from abc import ABC, abstractmethod
from decimal import Decimal
from itertools import product
from typing import Dict, List, Optional, Set, Type

from more_itertools import flatten
from pydantic import BaseModel, Field

from generalresearch.models import Source
from generalresearch.models.thl.demographics import (
    AgeGroup,
    DemographicTarget,
    Gender,
)
from generalresearch.models.thl.locales import (
    CountryISO,
    CountryISOs,
    LanguageISO,
    LanguageISOs,
)
from generalresearch.models.thl.survey.condition import (
    ConditionValueType,
    MarketplaceCondition,
)


class MarketplaceTask(BaseModel, ABC):
    """This is called a "Task" even though generally it represents a survey
    because some marketplaces have non-standard nesting structures. The
    task is the unit of work we target a user for. So if a marketplace has
    a survey with quotas that have different CPIs and we target a user to
    a specific quota, then the quota is the unit of work.
    """

    # model_config = ConfigDict(extra="allow")

    cpi: Decimal = Field(gt=0, le=100, decimal_places=2, max_digits=5)

    # In some marketplaces, a task can be targeted to one or more country or language.
    country_isos: CountryISOs = Field(min_items=1)
    language_isos: LanguageISOs = Field(min_items=1)

    # For convenience, we'll store a single country/lang field as well, since
    #   99% of tasks across all marketplaces, even those that support multiple,
    #   are only targeted to 1 country/lang. Which specific country/lang is
    #   stored here, for tasks that target more than 1, is undefined.
    country_iso: CountryISO = Field()
    language_iso: LanguageISO = Field()

    # These should be overloaded with more specific type hints
    buyer_id: Optional[str] = Field(min_length=1, max_length=32, default=None)
    # This is in seconds
    bid_loi: Optional[int] = Field(default=None, le=90 * 60)
    bid_ir: Optional[float] = Field(default=None, ge=0, le=1)

    # This should be an "abstract field", but there is no way to do that, so
    #   just listing it here. It should be overridden by the implementation
    source: Source = Field()
    # This should also
    used_question_ids: Set[str] = Field(default_factory=set)

    # This is a "special" key to store all conditions that are used (as
    # "condition_hashes") throughout this survey. In the reduced
    # representation of this task (nearly always, for db i/o, in global_vars)
    # this field will be null.
    conditions: Optional[Dict[str, MarketplaceCondition]] = Field(default=None)

    @property
    @abstractmethod
    def internal_id(self) -> str:
        """This is the value that is used for this survey within the
        marketplace. Typically, this is survey_id/survey_number. Morning
        is quota_id, repdata: stream_id.
        """
        ...

    @property
    def external_id(self) -> str:
        return f"{self.source.value}:{self.internal_id}"

    @property
    @abstractmethod
    def all_hashes(self) -> Set[str]: ...

    @property
    @abstractmethod
    def is_open(self) -> bool: ...

    @property
    @abstractmethod
    def is_live(self) -> bool: ...

    def __hash__(self):
        # We need this so this obj can be added into a set.
        return hash(self.external_id)

    def is_unchanged(self, other) -> bool:
        # Avoiding overloading __eq__ because it looks kind of complicated? I
        # want to be explicit that this is not testing object equivalence,
        # just that the objects don't require any db updates. We also exclude
        # conditions b/c this is just the condition_hash definitions
        return self.model_dump() == other.model_dump()

    def is_changed(self, other) -> bool:
        return not self.is_unchanged(other)

    @property
    @abstractmethod
    def condition_model(self) -> Type[MarketplaceCondition]:
        """
        The Condition Model for this survey class
        """
        pass

    @property
    @abstractmethod
    def age_question(self) -> str:
        """
        The age question ID
        """
        pass

    @property
    @abstractmethod
    def marketplace_genders(
        self,
    ) -> Dict[Gender, Optional[MarketplaceCondition]]:
        """
        Mapping of generic Gender to the marketplace condition for that gender
        """
        pass

    @property
    def marketplace_age_groups(
        self,
    ) -> Dict[AgeGroup, Optional[MarketplaceCondition]]:
        """
        Mapping of generic age groups to the marketplace condition for those ages
        """
        return {
            ag: self.condition_model(
                question_id=self.age_question,
                values=list(map(str, range(ag.low, ag.high + 1))),
                value_type=ConditionValueType.LIST,
            )
            for ag in AgeGroup
        }

    @property
    def targeted_ages(self) -> Set[str]:
        assert self.conditions is not None, "conditions must be populated"
        cs = [self.conditions[k] for k in self.all_hashes if k in self.conditions]
        age_cs = [c for c in cs if c.question_id == self.age_question]
        age_list = [c for c in age_cs if c.value_type == ConditionValueType.LIST]
        age_range = [c for c in age_cs if c.value_type == ConditionValueType.RANGE]
        age_values = set(flatten([c.values for c in age_list]))
        for c in age_range:
            ranges = c.values_ranges
            for r in ranges:
                r = list(r)
                if r[0] == float("-inf"):
                    r[0] = 0
                if r[1] == float("inf"):
                    r[1] = 120
                age_values.update(set(map(str, range(int(r[0]), int(r[1]) + 1))))
        return age_values

    @property
    def targeted_age_groups(self) -> Set[AgeGroup]:
        age_values = self.targeted_ages
        age_conditions = self.marketplace_age_groups
        age_targeting = set()
        for ag, condition in age_conditions.items():
            if condition.evaluate_criterion({condition.question_id: age_values}):
                age_targeting.add(ag)
        if len(age_targeting) == 0:
            # survey with no age targeting is implicitly targeting any age? or only >18? idk
            age_targeting.update(
                {
                    AgeGroup.AGE_18_TO_35,
                    AgeGroup.AGE_36_TO_55,
                    AgeGroup.AGE_56_TO_75,
                    AgeGroup.AGE_OVER_75,
                }
            )
        return age_targeting

    @property
    def targeted_genders(self) -> Set[Gender]:
        mp_genders = self.marketplace_genders
        gender_targeting = set()
        if mp_genders[Gender.MALE].criterion_hash in self.all_hashes:
            gender_targeting.add(Gender.MALE)
        if mp_genders[Gender.FEMALE].criterion_hash in self.all_hashes:
            gender_targeting.add(Gender.FEMALE)
        if len(gender_targeting) == 0:
            gender_targeting.update({Gender.MALE, Gender.FEMALE})
        return gender_targeting

    @property
    def demographic_targets(self) -> List[DemographicTarget]:
        targets = [DemographicTarget(country="*", gender="*", age_group="*")]

        gt = self.targeted_genders
        for gender in gt:
            targets.append(DemographicTarget(country="*", gender=gender, age_group="*"))

        at = self.targeted_age_groups
        for age_grp in at:
            targets.append(
                DemographicTarget(country="*", gender="*", age_group=age_grp)
            )

        for gender, age_grp in product(gt, at):
            targets.append(
                DemographicTarget(country="*", gender=gender, age_group=age_grp)
            )

        for c in self.country_isos:
            orig_targets = targets.copy()
            country_targets = [
                DemographicTarget(country=c, gender=t.gender, age_group=t.age_group)
                for t in orig_targets
            ]
            targets.extend(country_targets)
        return targets
