from __future__ import annotations

import json
import logging
from datetime import timezone
from decimal import Decimal
from typing import Optional, Dict, Any, List, Literal, Set, Tuple, Type
from typing_extensions import Self

from more_itertools import flatten
from pydantic import Field, ConfigDict, BaseModel, model_validator, computed_field

from generalresearch.locales import Localelator
from generalresearch.models import TaskCalculationType, Source
from generalresearch.models.custom_types import (
    CoercedStr,
    AwareDatetimeISO,
    AlphaNumStrSet,
    UUIDStrSet,
    AlphaNumStr,
)
from generalresearch.models.spectrum import SpectrumStatus
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


class SpectrumCondition(MarketplaceCondition):
    model_config = ConfigDict(populate_by_name=True, frozen=False, extra="ignore")

    question_id: Optional[CoercedStr] = Field(
        min_length=1,
        max_length=16,
        pattern=r"^[0-9]+$",
        validation_alias="qualification_code",
    )

    @model_validator(mode="after")
    def change_age_range_to_list(self) -> Self:
        """Spectrum uses ranges usually for ages. Ranges take longer to
        evaluate b/c they have to be converted into ints and then require
        multiple evaluations. Just convert into a list of values which only
        requires one easy match.
            e.g. convert age values from '20-22|20-21|25-26' to '|20|21|22|25|26|'
        """
        if self.question_id == "212" and self.value_type == ConditionValueType.RANGE:
            try:
                values = [tuple(map(int, v.split("-"))) for v in self.values]
                assert all(len(x) == 2 for x in values)
            except (ValueError, AssertionError):
                return self
            self.values = sorted(
                {str(val) for tupl in values for val in range(tupl[0], tupl[1] + 1)}
            )
            self.value_type = ConditionValueType.LIST
        return self

    @classmethod
    def from_api(cls, d: Dict[str, Any]) -> "SpectrumCondition":
        """Ranges can get returns with a key "units" indicating years or
        months. This is ridiculous, and we don't ask for birthdate, so we
        can't really get month accuracy. Normalize to years.
        """
        if "range_sets" in d:
            for rs in d["range_sets"]:
                if rs["units"] == 312:
                    rs["from"] = round(rs["from"] / 12)
                    rs["to"] = round(rs["to"] / 12)
            d["values"] = [
                "{0}-{1}".format(rs["from"] or "inf", rs["to"] or "inf")
                for rs in d["range_sets"]
            ]
            d["value_type"] = ConditionValueType.RANGE
            return cls.model_validate(d)
        else:
            d["values"] = list(map(str.lower, d["condition_codes"]))
            d["value_type"] = ConditionValueType.LIST
            return cls.model_validate(d)


class SpectrumQuota(BaseModel):
    model_config = ConfigDict(populate_by_name=True, frozen=True)

    # We don't ever need this. There's also a crtd_on and mod_on field, which
    # we ignore. quota_id: UUIDStr = Field()

    # API response is quantities.currently_open
    remaining_count: int = Field(
        description="Number of completes currently available in the quota. If "
        "the value is 0, any respondent matching this quota will be rejected."
    )
    condition_hashes: List[str] = Field(min_length=0, default_factory=list)

    # API also returns remaining & achieved, but these are supplier-scoped.
    # There is no explicit status. The quota is closed if the count is 0

    def __hash__(self) -> int:
        return hash(tuple((tuple(self.condition_hashes), self.remaining_count)))

    @property
    def is_open(self) -> bool:
        # currently_open takes into account respondents in progress, so
        # theoretically we should just check that there is >0 spots left
        min_open_spots = 1
        return self.remaining_count >= min_open_spots

    @classmethod
    def from_api(cls, d: Dict) -> Self:
        d["remaining_count"] = d["quantities"]["currently_open"]
        return cls.model_validate(d)

    def passes(self, criteria_evaluation: Dict[str, Optional[bool]]) -> bool:
        # Passes means we 1) meet all conditions (aka "match") AND 2) the
        # quota is open.
        return self.is_open and self.matches(criteria_evaluation)

    def matches(self, criteria_evaluation: Dict[str, Optional[bool]]) -> bool:
        # Matches means we meet all conditions. We can "match" a quota that is
        # closed. In that case, we would not be eligible for the survey.
        return all(criteria_evaluation.get(c) for c in self.condition_hashes)

    def matches_optional(
        self, criteria_evaluation: Dict[str, Optional[bool]]
    ) -> Optional[bool]:
        # We need to know if any conditions are unknown to avoid matching a
        # full quota. If any fail, then we know we fail regardless of any
        # being unknown.
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
        if False in hash_evals.values():
            return False, set()
        if None in hash_evals.values():
            return None, {cell for cell, ev in hash_evals.items() if ev is None}
        return True, set()


class SpectrumSurvey(MarketplaceTask):
    model_config = ConfigDict(populate_by_name=True)
    # Keys in API response that are undocumented: soft_launch, pds, project_last_complete_date
    # Keys in API not used: price_type, buyer_message, last_complete_date (OUR last complete date)
    # supplier_completes key is OUR DATA. It contains a "remaining" count, but this is just the
    #   sum of the quota remaining counts (I think)

    survey_id: CoercedStr = Field(min_length=1, max_length=16, pattern=r"^[0-9]+$")
    survey_name: str = Field(max_length=256)
    status: SpectrumStatus = Field(validation_alias="survey_status")

    field_end_date: AwareDatetimeISO = Field(
        description="When this survey is scheduled to end fielding. May stay open past fielding"
    )
    # Most are 232 - "Exciting New" which I assume is the default
    category_code: CoercedStr = Field(max_length=3, min_length=3, default="232")
    # API calls this "click_balancing"
    calculation_type: TaskCalculationType = Field(
        description="Indicates whether the targets are counted per Complete or Survey Start",
        default=TaskCalculationType.COMPLETES,
    )

    requires_pii: bool = Field(
        default=False, description="unclear what pii is", validation_alias="pii"
    )
    buyer_id: CoercedStr = Field(
        description="Identifier of client requesting the study", max_length=32
    )
    cpi: Decimal = Field(gt=0, le=100, decimal_places=2, max_digits=5)

    # called "survey_grouping" in API. If a respondent has previously taken any
    #   of these surveys, they will be excluded if that survey was taken in
    #   the exclusion_period.
    survey_exclusions: Optional[AlphaNumStrSet] = Field(
        description="list of excluded survey ids", default=None
    )
    exclusion_period: int = Field(default=30, description="in days")

    # API does not explicitly return the Bid values. It returns a LOI and IR
    #   that is the Bid value when the last block is null. As such, sometimes
    #   it may be set, sometimes not. We'll store it in the db if we see it,
    #   but then when we update the survey, it may not be returned, and so
    #   when we update the db, we must not overwrite this with NULL.
    # API key: "survey_performance"

    bid_loi: Optional[int] = Field(default=None, le=120 * 60)
    bid_ir: Optional[float] = Field(default=None, ge=0, le=1)
    overall_loi: Optional[int] = Field(default=None, le=120 * 60)
    overall_ir: Optional[float] = Field(default=None, ge=0, le=1)
    last_block_loi: Optional[int] = Field(default=None, le=120 * 60)
    last_block_ir: Optional[float] = Field(default=None, ge=0, le=1)

    # Undocumented. They sent us an email indicating that this is the last time
    # there was a complete for all suppliers on this survey.
    project_last_complete_date: Optional[AwareDatetimeISO] = Field(default=None)

    # ISO 3166-1 alpha-2 (two-letter codes, lowercase)
    country_iso: str = Field(
        max_length=2, min_length=2, pattern=r"^[a-z]{2}$", frozen=True
    )
    # 3-char ISO 639-2/B, lowercase
    language_iso: str = Field(
        max_length=3, min_length=3, pattern=r"^[a-z]{3}$", frozen=True
    )

    # The API returns 'incl_excl' which is a boolean indicating if the psid
    # list is an exclude or include list. If incl_excl = 1, the survey has an
    # include list, and only those in the list are eligible. This list gets
    # updated everytime someone on the list takes the survey.
    include_psids: Optional[UUIDStrSet] = Field(default=None)
    exclude_psids: Optional[UUIDStrSet] = Field(default=None)

    qualifications: List[str] = Field(default_factory=list)
    quotas: List[SpectrumQuota] = Field(default_factory=list)

    source: Literal[Source.SPECTRUM] = Field(default=Source.SPECTRUM)

    used_question_ids: Set[AlphaNumStr] = Field(default_factory=set)

    # This is a "special" key to store all conditions that are used (as
    #   "condition_hashes") throughout this survey. In the reduced
    #   representation of this task (nearly always, for db i/o, in
    #   global_vars) this field will be null.
    conditions: Optional[Dict[str, SpectrumCondition]] = Field(default=None)

    # These come from the API
    created_api: AwareDatetimeISO = Field(
        description="Creation date of opportunity", validation_alias="crtd_on"
    )
    modified_api: AwareDatetimeISO = Field(
        description="When the survey was last updated in spectrum's system",
        validation_alias="mod_on",
    )

    # This does not come from the API. We set it when we update this in the db.
    updated: Optional[AwareDatetimeISO] = Field(default=None)

    @property
    def internal_id(self) -> str:
        return self.survey_id

    @computed_field
    def is_live(self) -> bool:
        return self.status == SpectrumStatus.LIVE

    @property
    def is_open(self) -> bool:
        # The survey is open if the status is OPEN and there is at least 1
        #   open quota (or there are no quotas!)
        return self.is_live and (
            any(q.is_open for q in self.quotas) or len(self.quotas) == 0
        )

    @computed_field
    @property
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

    @property
    def condition_model(self) -> Type[MarketplaceCondition]:
        return SpectrumCondition

    @property
    def age_question(self) -> str:
        return "212"

    @property
    def marketplace_genders(self) -> Dict[Gender, Optional[MarketplaceCondition]]:
        return {
            Gender.MALE: SpectrumCondition(
                question_id="211",
                values=["111"],
                value_type=ConditionValueType.LIST,
            ),
            Gender.FEMALE: SpectrumCondition(
                question_id="211",
                values=["112"],
                value_type=ConditionValueType.LIST,
            ),
            Gender.OTHER: None,
        }

    @classmethod
    def from_api(cls, d: Dict) -> Optional["SpectrumSurvey"]:
        try:
            return cls._from_api(d)
        except Exception as e:
            logger.warning(f"Unable to parse survey: {d}. {e}")
            return None

    @classmethod
    def _from_api(cls, d: Dict) -> Self:
        assert d["click_balancing"] in {0, 1}, "unknown click_balancing value"
        d["calculation_type"] = (
            TaskCalculationType.STARTS
            if d["click_balancing"]
            else TaskCalculationType.COMPLETES
        )

        d["conditions"] = dict()

        # If we haven't hit the "detail" endpoint, we won't get this
        d.setdefault("qualifications", [])
        qualifications = [SpectrumCondition.from_api(q) for q in d["qualifications"]]
        for q in qualifications:
            d["conditions"][q.criterion_hash] = q
        d["qualifications"] = [x.criterion_hash for x in qualifications]

        quotas = []
        d.setdefault("quotas", [])
        for quota in d["quotas"]:
            criteria = [SpectrumCondition.from_api(q) for q in quota["criteria"]]
            quota["condition_hashes"] = [x.criterion_hash for x in criteria]
            quotas.append(SpectrumQuota.from_api(quota))
            for q in criteria:
                d["conditions"][q.criterion_hash] = q
        d["quotas"] = quotas
        return cls.model_validate(d)

    def is_unchanged(self, other) -> bool:
        # Avoiding overloading __eq__ because it looks kind of complicated? I
        # want to be explicit that this is not testing object equivalence, just
        # that the objects don't require any db updates. We also exclude
        # conditions b/c this is just the condition_hash definitions
        return self.model_dump(exclude={"updated", "conditions"}) == other.model_dump(
            exclude={"updated", "conditions"}
        )

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
        d["created_api"] = self.created_api
        d["updated"] = self.updated
        d["modified_api"] = self.modified_api
        d["field_end_date"] = self.field_end_date
        d["project_last_complete_date"] = self.project_last_complete_date
        return d

    @classmethod
    def from_db(cls, d: Dict[str, Any]) -> Self:
        d["created_api"] = d["created_api"].replace(tzinfo=timezone.utc)
        d["updated"] = d["updated"].replace(tzinfo=timezone.utc)
        d["modified_api"] = d["modified_api"].replace(tzinfo=timezone.utc)
        d["field_end_date"] = (
            d["field_end_date"].replace(tzinfo=timezone.utc)
            if d["field_end_date"]
            else None
        )
        d["project_last_complete_date"] = (
            d["project_last_complete_date"].replace(tzinfo=timezone.utc)
            if d["project_last_complete_date"]
            else None
        )
        if "qualifications" in d:
            d["qualifications"] = json.loads(d["qualifications"])
        if "quotas" in d:
            d["quotas"] = json.loads(d["quotas"])
        d["used_question_ids"] = json.loads(d["used_question_ids"])
        return cls.model_validate(d)

    """
    Yield Management/Eligibility Description:
    # https://purespectrum.atlassian.net/wiki/spaces/PA/pages/33604951/Respondent+Order+of+Operations
    """

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
        # We have to match all. So if any are False, we know we don't pass

        if False in hash_evals.values():
            return False, set()

        # If any are None, we don't know
        if None in hash_evals.values():
            return None, {cell for cell, ev in hash_evals.items() if ev is None}
        return True, set()

    def passes_quotas(self, criteria_evaluation: Dict[str, Optional[bool]]) -> bool:
        # We have to match at least 1 quota, but they can NOT match a quota
        # where currently_open=0
        any_pass = False
        for q in self.quotas:
            matches = q.matches_optional(criteria_evaluation)
            if matches in {True, None} and not q.is_open:
                # We also cannot be unknown for this quota, b/c we might fall
                #   into it, which would be a fail.
                return False
            if matches:
                any_pass = True
        return any_pass

    def passes_quotas_soft(
        self, criteria_evaluation: Dict[str, Optional[bool]]
    ) -> Tuple[Optional[bool], Set[str]]:
        # We have to match at least 1 quota, but they can NOT match a quota
        #   where currently_open=0
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
        # Check for not passing quals before bothering to do the rest
        if pass_quals is False:
            return False, set()
        pass_quotas, h_quotas = self.passes_quotas_soft(criteria_evaluation)
        if pass_quals and pass_quotas:
            return True, set()
        elif pass_quals is False or pass_quotas is False:
            return False, set()
        else:
            return None, h_quals | h_quotas
