from __future__ import annotations

import json
import logging
from datetime import timezone
from decimal import Decimal
from functools import cached_property
from typing import (
    Annotated,
    Any,
    Dict,
    List,
    Literal,
    Optional,
    Set,
    Tuple,
    Type,
)

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    NonNegativeInt,
    PositiveInt,
    PrivateAttr,
    computed_field,
    model_validator,
)
from typing_extensions import Self

from generalresearch.locales import Localelator
from generalresearch.models import Source
from generalresearch.models.custom_types import (
    AwareDatetimeISO,
    UUIDStrCoerce,
)
from generalresearch.models.morning import MorningQuestionID, MorningStatus
from generalresearch.models.morning.question import MorningQuestion
from generalresearch.models.thl.demographics import Gender
from generalresearch.models.thl.locales import (
    CountryISO,
    CountryISOs,
    LanguageISOs,
)
from generalresearch.models.thl.survey import MarketplaceTask
from generalresearch.models.thl.survey.condition import (
    ConditionValueType,
    MarketplaceCondition,
)

logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.INFO)

locale_helper = Localelator()


class MorningExclusion(BaseModel):
    group_id: UUIDStrCoerce = Field(
        description="The unique identifier for the exclusion group"
    )
    # The length of time in days to lock out a respondent who has successfully
    # completed another bid in the same exclusion group.
    # When omitted, respondents who have ever participated in that exclusion
    # group will be disallowed from entering the current bid. The value may
    # also be set to 0 to signal group exclusion for future bids without
    # excluding participants from the current bid.
    lockout_period: NonNegativeInt = Field(description="length of time in days")


class MorningStatistics(BaseModel):
    model_config = ConfigDict(populate_by_name=True, frozen=False, extra="ignore")

    # `length_of_interview` changes meaning after 5 completes. We can use
    # `estimated_length_of_interview` and `median_length_of_interview` instead
    # to get the bid and obs values.
    # The bid loi is the same for all quotas in the bid, so we'll put it on the
    # bid bid_loi: int = Field(validation_alias="estimated_length_of_interview",
    # le=120 * 60)
    # If num_completes == 0 , this gets returned as 0. it should be None
    obs_median_loi: Optional[NonNegativeInt] = Field(
        validation_alias="median_length_of_interview", default=None, le=120 * 60
    )

    # API returns 100 until 5 completes! Should be None.
    # This is calculated as the total completes divided by the total number of
    # finished sessions that passed the prescreener.
    qualified_conversion: Optional[float] = Field(
        ge=0, le=1, description="conversion rate of qualified respondents"
    )

    # Panelists should only be sent to a bid or quota if this number is greater
    # than zero. In-progress sessions are taken into account
    num_available: NonNegativeInt = Field(
        description="The number of completes that are currently available to fill"
    )

    num_completes: NonNegativeInt = Field(
        description="The number of people who have successfully completed the survey"
    )
    num_failures: NonNegativeInt = Field(
        description="The number of people who have been rejected for an unknown reason"
    )
    # This includes respondents who are in the prescreener or the survey, but
    # have not yet completed or been rejected.
    num_in_progress: NonNegativeInt = Field(
        description="The number of people with active sessions"
    )
    num_over_quotas: NonNegativeInt = Field(
        description="The number of respondents who have been terminated for meeting a quota "
        "which is already full"
    )
    num_qualified: NonNegativeInt = Field(
        description="The number of respondents who qualified for a quota, including over "
        "quotas"
    )
    num_quality_terminations: NonNegativeInt = Field(
        description="The number of respondents who have been terminated for quality reasons"
    )
    num_timeouts: NonNegativeInt = Field(
        description="The number of respondents who have been timed out"
    )

    # Not using: length_of_interview (meaning changes after 5 completes)

    @model_validator(mode="after")
    def check_api_default(self) -> Self:
        # API returns stupid default values instead of None
        if self.num_completes < 5:
            self.obs_median_loi = None
            self.qualified_conversion = None
        else:
            assert self.obs_median_loi is not None
            assert self.qualified_conversion is not None
        return self


class MorningTaskStatistics(MorningStatistics):
    # This is the "statistics" for the "Bid" aka the survey/task. It contains
    # all the field as for the quota statistics plus extra fields that are not
    # relevant to quotas.

    # API returns 100 until 5 completes! Should be None ...
    system_conversion: Optional[float] = Field(
        description="conversion rate of the system. completes divided by total number of entrants to the system",
        ge=0,
        le=1,
    )
    num_entrants: NonNegativeInt = Field(
        description="The number of people who have entered the respondent router and successfully reached the "
        "prescreener. This includes respondents who have not yet qualified"
    )
    num_screenouts: NonNegativeInt = Field(
        description="Number of screenouts, including those screened out in the prescreener and those screened out in "
        "the survey"
    )
    # this is for the bid only. the quotas dont have bid lois
    bid_loi: PositiveInt = Field(
        validation_alias="estimated_length_of_interview", le=120 * 60
    )

    # Not using: incidence_rate & length_of_interview (meaning changes after 5 completes), earnings_per_click (can
    #   calculate from the other values)


class MorningCondition(MarketplaceCondition):
    model_config = ConfigDict(populate_by_name=True, frozen=False, extra="ignore")
    question_id: Optional[MorningQuestionID] = Field(validation_alias="id")
    values: List[Annotated[str, Field(max_length=128)]] = Field(
        validation_alias="response_ids"
    )
    value_type: ConditionValueType = Field(default=ConditionValueType.LIST)


class MorningQuota(MorningStatistics, MarketplaceTask):
    model_config = ConfigDict(populate_by_name=True, frozen=False)

    id: UUIDStrCoerce = Field()
    cpi: Decimal = Field(
        gt=0,
        le=100,
        decimal_places=2,
        max_digits=5,
        validation_alias="cost_per_interview",
    )
    condition_hashes: List[str] = Field(min_length=1, default_factory=list)

    # since the Quota is the MarketplaceTask, it needs these fields, copied from the Bid
    source: Literal[Source.MORNING_CONSULT] = Field(default=Source.MORNING_CONSULT)
    used_question_ids: Set[MorningQuestionID] = Field(default_factory=set)
    country_iso: CountryISO = Field(frozen=True)
    country_isos: CountryISOs = Field()
    language_isos: LanguageISOs = Field(frozen=True)
    buyer_id: UUIDStrCoerce = Field()

    # Min spots a quota should have open to be OPEN
    _min_open_spots: int = PrivateAttr(default=1)

    def __hash__(self):
        return hash(self.id)

    @model_validator(mode="before")
    @classmethod
    def set_locale(cls, data: Any):
        data["country_isos"] = [data["country_iso"]]
        if isinstance(data["language_isos"], str):
            data["language_isos"] = set(data["language_isos"].split(","))
        data["language_iso"] = sorted(data["language_isos"])[0]
        return data

    @property
    def internal_id(self) -> str:
        return self.id

    @computed_field
    def is_live(self) -> bool:
        return True

    @computed_field
    @cached_property
    def all_hashes(self) -> Set[str]:
        return set(self.condition_hashes)

    @property
    def condition_model(self) -> Type[MarketplaceCondition]:
        return MorningCondition

    @property
    def age_question(self) -> str:
        return "age"

    @property
    def marketplace_genders(
        self,
    ) -> Dict[Gender, Optional[MarketplaceCondition]]:
        return {
            Gender.MALE: MorningCondition(
                question_id="gender",
                values=["1"],
                value_type=ConditionValueType.LIST,
            ),
            Gender.FEMALE: MorningCondition(
                question_id="gender",
                values=["2"],
                value_type=ConditionValueType.LIST,
            ),
            Gender.OTHER: None,
        }

    @property
    def is_open(self) -> bool:
        # num_available includes in-progress (they're already deducted)
        return self.num_available >= self._min_open_spots

    def passes(self, criteria_evaluation: Dict[str, Optional[bool]]) -> bool:
        # Passes means we 1) meet all conditions (aka "match") AND 2) the quota is open.
        return self.is_open and self.matches(criteria_evaluation)

    # TODO: I did some speed tests. This is faster than how this is implemented
    # in sago/spectrum/dynata/etc. We should generalize this logic instead of
    # copying/pasting it 7 times. (matches, matches_optional and _soft)
    def matches(self, criteria_evaluation: Dict[str, Optional[bool]]) -> bool:
        # Matches means we meet all conditions.
        # In Morning, all quotas are mutually exclusive. so if it doesn't
        # matter if we match a closed quota, b/c that means that we won't
        # match any other quota anyway
        return self.matches_optional(criteria_evaluation) is True

    def matches_optional(
        self, criteria_evaluation: Dict[str, Optional[bool]]
    ) -> Optional[bool]:
        for c in self.condition_hashes:
            eval_value = criteria_evaluation.get(c)
            if eval_value is False:
                return False
            if eval_value is None:
                return None
        return True

    def matches_soft(
        self, criteria_evaluation: Dict[str, Optional[bool]]
    ) -> Tuple[Optional[bool], List[str]]:
        # Passes back "matches" (T/F/none) and a list of unknown criterion hashes
        unknowns = list()
        for c in self.condition_hashes:
            eval_value = criteria_evaluation.get(c)
            if eval_value is False:
                return False, list()
            if eval_value is None:
                unknowns.append(c)
        if unknowns:
            return None, unknowns
        return True, unknowns


class MorningBid(MorningTaskStatistics):
    """
    This is the top-level task in Morning Consult; what we would normally call
    a survey. A survey can have 1 or more quotas. Each quota has its own CPI
    and targeting. We use the quota as the generic task throughout THL because
    the quota has a unique ID which we'll use for targeting.
    """

    model_config = ConfigDict(populate_by_name=True)

    id: UUIDStrCoerce = Field()
    status: MorningStatus = Field(
        default=MorningStatus.ACTIVE, validation_alias="state"
    )

    # A survey has 1 country and one or more languages
    country_iso: CountryISO = Field(frozen=True)
    language_isos: LanguageISOs = Field(frozen=True)

    buyer_account_id: UUIDStrCoerce = Field()
    buyer_id: UUIDStrCoerce = Field()
    name: str = Field(min_length=1, max_length=100)
    supplier_exclusive: bool = Field(default=False)
    survey_type: str = Field(min_length=1, max_length=32)
    timeout: PositiveInt = Field(le=24 * 60 * 60)
    topic_id: str = Field(min_length=1, max_length=64)

    exclusions: List[MorningExclusion] = Field(default_factory=list)

    quotas: List[MorningQuota] = Field(default_factory=list)

    source: Literal[Source.MORNING_CONSULT] = Field(default=Source.MORNING_CONSULT)

    used_question_ids: Set[MorningQuestionID] = Field(default_factory=set)

    # This is a "special" key to store all conditions that are used (as
    # "condition_hashes") throughout this survey. In the reduced representation
    # of this task (nearly always, for db i/o, in global_vars) this field will
    # be null.
    conditions: Optional[Dict[str, MorningCondition]] = Field(default=None)

    # This doesn't get stored in the db directly
    experimental_single_use_qualifications: Optional[List[MorningQuestion]] = Field(
        default=None
    )

    # These come from the API
    expected_end: AwareDatetimeISO = Field(validation_alias="end_date")
    created_api: AwareDatetimeISO = Field(validation_alias="published_at")

    # This does not come from the API. We set it when we update this in the db.
    created: Optional[AwareDatetimeISO] = Field(default=None)
    updated: Optional[AwareDatetimeISO] = Field(default=None)

    # ignoring from API: closed_at

    def __hash__(self):
        return hash(self.id)

    @computed_field
    def is_live(self) -> bool:
        return self.status == MorningStatus.ACTIVE

    @property
    def is_open(self) -> bool:
        # The survey is open if the status is ACTIVE and there is at least 1
        # open quota.
        return self.is_live and any(q.is_open for q in self.quotas)

    @property
    def language_iso_any(self):
        return sorted(self.language_isos)[0]

    @property
    def locale(self):
        return self.country_iso, self.language_iso_any

    @computed_field
    @cached_property
    def all_hashes(self) -> Set[str]:
        s = set()
        for q in self.quotas:
            s.update(set(q.condition_hashes))
        return s

    @model_validator(mode="before")
    @classmethod
    def set_locale(cls, data: Any):
        data["country_isos"] = [data["country_iso"]]
        return data

    @model_validator(mode="before")
    @classmethod
    def setup_quota_fields(cls, data: Dict[str, Any]) -> Dict[str, Any]:
        # These fields get "inherited" by each quota from its bid.
        quota_fields = [
            "country_iso",
            "language_isos",
            "buyer_id",
            "bid_loi",
            "used_question_ids",
        ]

        for quota in data["quotas"]:
            for field in quota_fields:
                if field not in quota:
                    quota[field] = data[field]

        return data

    @model_validator(mode="before")
    @classmethod
    def set_used_questions(cls, data: Any):
        if data.get("used_question_ids") is not None:
            return data
        if not data.get("conditions"):
            data["used_question_ids"] = set()
            return data
        data["used_question_ids"] = {
            c.question_id for c in data["conditions"].values() if c.question_id
        }
        return data

    @model_validator(mode="before")
    @classmethod
    def setup_conditions(cls, data: Dict[str, Any]) -> Dict[str, Any]:
        if "conditions" in data:
            return data

        data["conditions"] = dict()
        for quota in data["quotas"]:
            if "qualifications" in quota:
                quota_conditions = [
                    MorningCondition.model_validate(q) for q in quota["qualifications"]
                ]
                quota["condition_hashes"] = [c.criterion_hash for c in quota_conditions]
                data["conditions"].update(
                    {c.criterion_hash: c for c in quota_conditions}
                )
            if "_experimental_single_use_qualifications" in quota:
                quota_conditions = [
                    MorningCondition.model_validate(q)
                    for q in quota["_experimental_single_use_qualifications"]
                ]
                quota["condition_hashes"].extend(
                    [c.criterion_hash for c in quota_conditions]
                )
                data["conditions"].update(
                    {c.criterion_hash: c for c in quota_conditions}
                )
        return data

    @model_validator(mode="before")
    @classmethod
    def clean_alias(cls, data: Dict[str, Any]) -> Dict[str, Any]:
        # Make sure fields are named certain ways, so we don't have to check
        # aliases within other validators
        if "estimated_length_of_interview" in data:
            data["bid_loi"] = data.pop("estimated_length_of_interview")
        return data

    @model_validator(mode="after")
    def sort_quotas(self) -> Self:
        # sort the quotas so that we can do comparisons on bids to see if anything has changed
        self.quotas = sorted(self.quotas, key=lambda x: x.id)
        return self

    def is_unchanged(self, other) -> bool:
        # Avoiding overloading __eq__ because it looks kind of complicated? I
        # want to be explicit that this is not testing object equivalence,
        # just that the objects don't require any db updates. We also exclude
        # conditions b/c this is just the condition_hash definitions
        return self.model_dump(
            exclude={"updated", "conditions", "created"}
        ) == other.model_dump(exclude={"updated", "conditions", "created"})

    def is_changed(self, other) -> bool:
        return not self.is_unchanged(other)

    def to_mysql(self) -> Dict[str, Any]:
        d = self.model_dump(
            mode="json",
            exclude={
                "all_hashes": True,
                "country_isos": True,
                "source": True,
                "conditions": True,
                "quotas": {
                    "__all__": {
                        "all_hashes",
                        "used_question_ids",
                        "is_live",
                        "country_isos",
                        "language_isos",
                    }
                },
            },
        )
        d["used_question_ids"] = json.dumps(sorted(d["used_question_ids"]))
        d["exclusions"] = json.dumps(d["exclusions"])
        for q in d["quotas"]:
            q["condition_hashes"] = json.dumps(q["condition_hashes"])
        d["expected_end"] = self.expected_end
        d["created_api"] = self.created_api
        d["updated"] = self.updated
        d["created"] = self.created
        return d

    @classmethod
    def from_db(cls, d: Dict[str, Any]) -> Self:
        d["created"] = d["created"].replace(tzinfo=timezone.utc)
        d["updated"] = d["updated"].replace(tzinfo=timezone.utc)
        d["expected_end"] = d["expected_end"].replace(tzinfo=timezone.utc)
        d["created_api"] = d["created_api"].replace(tzinfo=timezone.utc)
        d["used_question_ids"] = json.loads(d["used_question_ids"])
        d["exclusions"] = json.loads(d["exclusions"])
        return cls.model_validate(d)

    def passes_quotas(
        self, criteria_evaluation: Dict[str, Optional[bool]]
    ) -> Optional[str]:
        # Quotas are mutually-exclusive. A user can only possibly match 1 quota.
        # Returns the passing quota ID or None (if user doesn't pass any quota)
        for q in self.quotas:
            if q.passes(criteria_evaluation):
                return q.id

    def passes_quotas_soft(
        self, criteria_evaluation: Dict[str, Optional[bool]]
    ) -> Tuple[Optional[bool], Optional[List[str]], Optional[Set[str]]]:
        """
        Quotas are mutually-exclusive. A user can only possibly match 1
            quota. As such, all unknown questions on any quota will be
            the same unknowns on all.

        Returns (the eligibility (True/False/None), passing quota ID or
            None (if eligibility is not True), unknown_hashes (or None))
        """
        unknown_quotas = []
        unknown_hashes = set()
        for q in self.quotas:
            if q.is_open:
                elig, quota_unknown_hashes = q.matches_soft(criteria_evaluation)
                if elig is True:
                    return True, [q.id], None
                if elig is None:
                    unknown_quotas.append(q.id)
                    unknown_hashes.update(quota_unknown_hashes)
        if unknown_quotas:
            return None, unknown_quotas, unknown_hashes
        return False, None, None

    def determine_eligibility(
        self, criteria_evaluation: Dict[str, Optional[bool]]
    ) -> Optional[str]:
        if not self.is_open:
            return None
        return self.passes_quotas(criteria_evaluation)

    def determine_eligibility_soft(
        self, criteria_evaluation: Dict[str, Optional[bool]]
    ) -> Tuple[Optional[bool], Optional[List[str]], Optional[Set[str]]]:
        if not self.is_open:
            return False, None, None
        return self.passes_quotas_soft(criteria_evaluation)
