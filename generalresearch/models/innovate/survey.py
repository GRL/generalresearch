from __future__ import annotations

import json
import logging
from datetime import date, timezone
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

from more_itertools import flatten
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    computed_field,
    model_validator,
)
from typing_extensions import Self

from generalresearch.locales import Localelator
from generalresearch.models import (
    LogicalOperator,
    Source,
    TaskCalculationType,
)
from generalresearch.models.custom_types import (
    AlphaNumStrSet,
    AwareDatetimeISO,
    CoercedStr,
    DeviceTypes,
)
from generalresearch.models.innovate import (
    InnovateDuplicateCheckLevel,
    InnovateQuotaStatus,
    InnovateStatus,
)
from generalresearch.models.innovate.question import InnovateQuestionID
from generalresearch.models.thl.demographics import Gender
from generalresearch.models.thl.survey import MarketplaceTask
from generalresearch.models.thl.survey.condition import (
    ConditionValueType,
    MarketplaceCondition,
)

logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.INFO)

locale_helper = Localelator()


class InnovateCondition(MarketplaceCondition):
    model_config = ConfigDict(populate_by_name=True, frozen=False, extra="ignore")
    # store everything lowercase !
    question_id: Optional[CoercedStr] = Field(
        min_length=1, max_length=64, pattern=r"^[^A-Z]+$"
    )
    # There isn't really a hard limit, but their API is inconsistent and
    #   sometimes returns all the options comma-separated instead of as a list.
    #   Try to catch that.
    values: List[Annotated[str, Field(max_length=128)]] = Field()

    @classmethod
    def from_api(cls, d: Dict[str, Any]) -> "InnovateCondition":
        d["logical_operator"] = LogicalOperator.OR
        d["value_type"] = ConditionValueType.LIST
        d["negate"] = False
        d["values"] = list(set(x.strip().lower() for x in d["values"]))
        return cls.model_validate(d)


class InnovateQuota(BaseModel):
    model_config = ConfigDict(populate_by_name=True, frozen=True)

    desired_count: int = Field()
    remaining_count: int = Field()
    complete_count: int = Field()
    start_count: int = Field()

    status: InnovateQuotaStatus = Field()
    task_calculation_type: TaskCalculationType = Field()
    hard_stop: bool = Field()

    condition_hashes: List[str] = Field(min_length=0, default_factory=list)

    def __hash__(self):
        return hash(tuple((tuple(self.condition_hashes), self.remaining_count)))

    @property
    def is_open(self) -> bool:
        min_open_spots = 3
        return (
            self.remaining_count >= min_open_spots
            and self.status == InnovateQuotaStatus.OPEN
        )

    @classmethod
    def from_api(cls, d: Dict):
        return cls.model_validate(d)

    def passes(self, criteria_evaluation: Dict[str, Optional[bool]]) -> bool:
        # Passes means we 1) meet all conditions (aka "match") AND 2) the quota is open.
        return self.is_open and self.matches(criteria_evaluation)

    def matches(self, criteria_evaluation: Dict[str, Optional[bool]]) -> bool:
        # Matches means we meet all conditions.
        # We can "match" a quota that is closed. In that case, we would not be eligible for the survey.
        return all(criteria_evaluation.get(c) for c in self.condition_hashes)

    def matches_optional(
        self, criteria_evaluation: Dict[str, Optional[bool]]
    ) -> Optional[bool]:
        # We need to know if any conditions are unknown to avoid matching a full quota. If any fail,
        #   then we know we fail regardless of any being unknown.
        evals = [criteria_evaluation.get(c) for c in self.condition_hashes]
        if False in evals:
            return False
        if None in evals:
            return None
        return True

    def matches_soft(
        self, criteria_evaluation: Dict[str, Optional[bool]]
    ) -> Tuple[Optional[bool], Set[str]]:
        # Passes back "matches" (T/F/none) and a list of unknown criterion hashes
        hash_evals = {
            cell: criteria_evaluation.get(cell) for cell in self.condition_hashes
        }
        evals = set(hash_evals.values())
        if False in evals:
            return False, set()
        if None in evals:
            return None, {cell for cell, ev in hash_evals.items() if ev is None}
        return True, set()


class InnovateSurvey(MarketplaceTask):
    model_config = ConfigDict(populate_by_name=True)

    survey_id: CoercedStr = Field(min_length=1, max_length=16, pattern=r"^[0-9]+$")
    # There is no status returned, using one I make up b/c is_live depends on it,
    status: InnovateStatus = Field(default=InnovateStatus.LIVE)
    # is_live: bool = Field(default=True)  # can't overload the is_live property ...
    cpi: Decimal = Field(gt=0, le=100, decimal_places=2, max_digits=5)
    buyer_id: CoercedStr = Field(max_length=32)

    # ISO 3166-1 alpha-2 (two-letter codes, lowercase)
    country_iso: str = Field(
        max_length=2, min_length=2, pattern=r"^[a-z]{2}$", frozen=True
    )
    # 3-char ISO 639-2/B, lowercase
    language_iso: str = Field(
        max_length=3, min_length=3, pattern=r"^[a-z]{3}$", frozen=True
    )

    job_id: str = Field(description="basically a project id")
    survey_name: str = Field()

    desired_count: int = Field()
    remaining_count: int = Field()
    supplier_completes_achieved: int = Field()
    global_completes: int = Field()
    global_starts: int = Field()
    global_median_loi: Optional[int] = Field(le=120 * 60)
    global_conversion: Optional[float] = Field(ge=0, le=1)

    bid_loi: Optional[int] = Field(default=None, le=120 * 60)
    bid_ir: Optional[float] = Field(default=None, ge=0, le=1)

    allowed_devices: DeviceTypes = Field(min_length=1)

    entry_link: str = Field()
    category: str = Field()
    requires_pii: bool = Field(default=False)

    excluded_surveys: Optional[AlphaNumStrSet] = Field(
        description="list of excluded survey ids", default=None
    )
    duplicate_check_level: InnovateDuplicateCheckLevel = Field()

    exclude_pids: Optional[AlphaNumStrSet] = Field(default=None)
    include_pids: Optional[AlphaNumStrSet] = Field(default=None)

    # idk what these mean
    is_revenue_sharing: bool = Field()
    group_type: str = Field()
    # undocumented, not sure how we use this
    off_hour_traffic: Optional[Dict] = Field(default=None)

    qualifications: List[str] = Field(default_factory=list)
    quotas: List[InnovateQuota] = Field(default_factory=list)

    source: Literal[Source.INNOVATE] = Field(default=Source.INNOVATE)

    used_question_ids: Set[InnovateQuestionID] = Field(default_factory=set)

    # This is a "special" key to store all conditions that are used (as "condition_hashes") throughout
    #   this survey. In the reduced representation of this task (nearly always, for db i/o, in global_vars)
    #   this field will be null.
    conditions: Optional[Dict[str, InnovateCondition]] = Field(default=None)

    # These come from the API
    created_api: AwareDatetimeISO = Field(
        description="When the survey was created in innovate's system"
    )
    modified_api: AwareDatetimeISO = Field(
        description="When the survey was last updated in innovate's system"
    )
    expected_end_date: date = Field()

    # This does not come from the API. We set it when we update this in the db.
    created: Optional[AwareDatetimeISO] = Field(default=None)
    updated: Optional[AwareDatetimeISO] = Field(default=None)

    @property
    def internal_id(self) -> str:
        return self.survey_id

    @computed_field
    def is_live(self) -> bool:
        return self.status == InnovateStatus.LIVE

    @property
    def is_open(self) -> bool:
        # The survey is open if the status is OPEN and there is at least 1 open quota (or there are no quotas!)
        return self.is_live and (
            any(q.is_open for q in self.quotas) or len(self.quotas) == 0
        )

    @computed_field
    @cached_property
    def all_hashes(self) -> Set[str]:
        s = set(self.qualifications)
        for q in self.quotas:
            s.update(set(q.condition_hashes))
        return s

    @model_validator(mode="before")
    @classmethod
    def set_locale(cls, data: Any):
        data["country_isos"] = [data["country_iso"]]
        data["language_isos"] = [data["language_iso"]]
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

    @classmethod
    def from_api(cls, d: Dict[str, Any]) -> Optional["InnovateSurvey"]:
        try:
            return cls._from_api(d)
        except Exception as e:
            logger.warning(f"Unable to parse survey: {d}. {e}")
            return None

    @classmethod
    def _from_api(cls, d: Dict[str, Any]) -> "InnovateSurvey":
        d["conditions"] = dict()

        # If we haven't hit the "detail" endpoint, we won't get this
        d.setdefault("qualifications", [])
        for q in d["qualifications"]:
            d["conditions"][q.criterion_hash] = q
        d["qualifications"] = [x.criterion_hash for x in d["qualifications"]]

        quotas = []
        d.setdefault("quotas", [])
        for quota in d["quotas"]:
            conditions = quota["conditions"]
            quota["condition_hashes"] = [x.criterion_hash for x in conditions]
            quotas.append(InnovateQuota.from_api(quota))
            for q in conditions:
                d["conditions"][q.criterion_hash] = q
        d["quotas"] = quotas
        return cls.model_validate(d)

    @property
    def condition_model(self) -> Type[MarketplaceCondition]:
        return InnovateCondition

    @property
    def age_question(self) -> str:
        return "age"

    @property
    def marketplace_genders(self):
        # There is also a "gender_plus", but it doesn't seem widely used.
        return {
            Gender.MALE: InnovateCondition(
                question_id="gender",
                values=["1"],
                value_type=ConditionValueType.LIST,
            ),
            Gender.FEMALE: InnovateCondition(
                question_id="gender",
                values=["2"],
                value_type=ConditionValueType.LIST,
            ),
            Gender.OTHER: None,
        }

    def __repr__(self) -> str:
        # Fancy repr that abbreviates exclude_pids and excluded_surveys
        repr_args = list(self.__repr_args__())
        for n, (k, v) in enumerate(repr_args):
            if k in {"exclude_pids", "include_pids", "excluded_surveys"}:
                if v and len(v) > 6:
                    v = sorted(v)
                    v = v[:3] + ["…"] + v[-3:]
                    repr_args[n] = (k, v)
        join_str = ", "
        repr_str = join_str.join(
            repr(v) if a is None else f"{a}={v!r}" for a, v in repr_args
        )
        return f"{self.__repr_name__()}({repr_str})"

    def is_unchanged(self, other: "InnovateSurvey") -> bool:
        # Avoiding overloading __eq__ because it looks kind of complicated? I
        # want to be explicit that this is not testing object equivalence,
        # just that the objects don't require any db updates. We also exclude
        # conditions b/c this is just the condition_hash definitions
        return self.model_dump(
            exclude={"updated", "conditions", "created"}
        ) == other.model_dump(exclude={"updated", "conditions", "created"})

    def to_mysql(self) -> Dict[str, Any]:
        d = self.model_dump(
            mode="json",
            exclude={
                "all_hashes",
                "country_isos",
                "language_isos",
                "source",
                "conditions",
            },
        )
        d["qualifications"] = json.dumps(d["qualifications"])
        d["quotas"] = json.dumps(d["quotas"])
        d["used_question_ids"] = json.dumps(sorted(d["used_question_ids"]))
        d["off_hour_traffic"] = json.dumps(d["off_hour_traffic"])
        d["modified_api"] = self.modified_api
        d["created_api"] = self.created_api
        d["updated"] = self.updated
        d["created"] = self.created
        return d

    @classmethod
    def from_db(cls, d: Dict[str, Any]) -> Self:
        d["created"] = d["created"].replace(tzinfo=timezone.utc)
        d["updated"] = d["updated"].replace(tzinfo=timezone.utc)
        d["modified_api"] = d["modified_api"].replace(tzinfo=timezone.utc)
        d["created_api"] = d["created_api"].replace(tzinfo=timezone.utc)
        d["qualifications"] = json.loads(d["qualifications"])
        d["used_question_ids"] = json.loads(d["used_question_ids"])
        d["quotas"] = json.loads(d["quotas"])
        d["off_hour_traffic"] = json.loads(d["off_hour_traffic"])
        return cls.model_validate(d)

    def participation_allowed(
        self, att_survey_ids: Set[str], att_job_ids: Set[str]
    ) -> bool:
        """
        Checks if this user can participate in this survey based on the 'duplicate_check_level'-dictated requirements
        :param att_survey_ids: list of the user's previously attempted survey IDs
        :param att_job_ids: list of the user's previously attempted survey ID's Job IDs
        """
        assert isinstance(att_survey_ids, set), "must pass a set"
        assert isinstance(att_job_ids, set), "must pass a set"
        if self.survey_id in att_survey_ids:
            return False
        if self.duplicate_check_level == InnovateDuplicateCheckLevel.JOB:
            if self.job_id in att_job_ids:
                return False
        if self.duplicate_check_level == InnovateDuplicateCheckLevel.EXCLUDED_SURVEYS:
            if self.excluded_surveys.intersection(att_survey_ids):
                return False
        return True

    def passes_qualifications(
        self, criteria_evaluation: Dict[str, Optional[bool]]
    ) -> bool:
        # We have to match all quals
        return all(criteria_evaluation.get(q) for q in self.qualifications)

    def passes_qualifications_soft(
        self, criteria_evaluation: Dict[str, Optional[bool]]
    ) -> Tuple[Optional[bool], Set[str]]:
        # Passes back "passes" (T/F/none) and a list of unknown criterion hashes
        hash_evals = {q: criteria_evaluation.get(q) for q in self.qualifications}
        evals = set(hash_evals.values())
        # We have to match all. So if any are False, we know we don't pass
        if False in evals:
            return False, set()
        # If any are None, we don't know
        if None in evals:
            return None, {cell for cell, ev in hash_evals.items() if ev is None}
        return True, set()

    def passes_quotas(self, criteria_evaluation: Dict[str, Optional[bool]]) -> bool:
        # Many surveys have 0 quotas. Quotas are exclusionary.
        # They can NOT match a quota where currently_open=0
        any_pass = True
        for q in self.quotas:
            matches = q.matches_optional(criteria_evaluation)
            if matches in {True, None} and not q.is_open:
                # We also cannot be unknown for this quota, b/c we might fall into it, which would be a fail.
                return False
        return any_pass

    def passes_quotas_soft(
        self, criteria_evaluation: Dict[str, Optional[bool]]
    ) -> Tuple[Optional[bool], Set[str]]:
        # Many surveys have 0 quotas. Quotas are exclusionary.
        # They can NOT match a quota where currently_open=0
        if len(self.quotas) == 0:
            return True, set()
        quota_eval = {
            quota: quota.matches_soft(criteria_evaluation) for quota in self.quotas
        }
        evals = set(g[0] for g in quota_eval.values())
        if any(m[0] is True and not q.is_open for q, m in quota_eval.items()):
            # matched a full quota
            return False, set()
        if any(m[0] is None and not q.is_open for q, m in quota_eval.items()):
            # Unknown match for full quota
            if True in evals:
                # we match 1 other, so the missing are only this type
                return None, set(
                    flatten(
                        [
                            m[1]
                            for q, m in quota_eval.items()
                            if m[0] is None and not q.is_open
                        ]
                    )
                )
            else:
                # we don't match any quotas, so everything is unknown
                return None, set(
                    flatten([m[1] for q, m in quota_eval.items() if m[0] is None])
                )
        if True in evals:
            return True, set()
        if None in evals:
            return None, set(
                flatten([m[1] for q, m in quota_eval.items() if m[0] is None])
            )

        return False, set()

    def determine_eligibility(
        self, criteria_evaluation: Dict[str, Optional[bool]]
    ) -> bool:
        return (
            self.is_open
            and self.passes_qualifications(criteria_evaluation)
            and self.passes_quotas(criteria_evaluation)
        )

    def determine_eligibility_soft(
        self, criteria_evaluation: Dict[str, Optional[bool]]
    ) -> Tuple[Optional[bool], Set[str]]:
        if self.is_open is False:
            return False, set()
        pass_quals, h_quals = self.passes_qualifications_soft(criteria_evaluation)
        pass_quotas, h_quotas = self.passes_quotas_soft(criteria_evaluation)
        if pass_quals and pass_quotas:
            return True, set()
        elif pass_quals is False or pass_quotas is False:
            return False, set()
        else:
            return None, h_quals | h_quotas
