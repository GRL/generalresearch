from __future__ import annotations

import json
import logging
from datetime import timezone
from decimal import Decimal
from functools import cached_property
from typing import Optional, Dict, Any, List, Literal, Set, Tuple, Annotated, Type
from typing_extensions import Self

from more_itertools import flatten
from pydantic import Field, ConfigDict, BaseModel, model_validator, computed_field

from generalresearch.locales import Localelator
from generalresearch.models import Source, LogicalOperator
from generalresearch.models.custom_types import (
    CoercedStr,
    AwareDatetimeISO,
    AlphaNumStrSet,
    AlphaNumStr,
    DeviceTypes,
    IPLikeStrSet,
)
from generalresearch.models.sago import SagoStatus
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


class SagoCondition(MarketplaceCondition):
    model_config = ConfigDict(populate_by_name=True, frozen=False, extra="ignore")
    question_id: Optional[CoercedStr] = Field(
        min_length=1, max_length=16, pattern=r"^[0-9]+$"
    )
    # There isn't really a hard limit, but their API is inconsistent and
    # sometimes returns all the options comma-separated instead of as a list.
    # Try to catch that.
    values: List[Annotated[str, Field(max_length=128)]] = Field()

    _CONVERT_LIST_TO_RANGE = ["59"]

    @classmethod
    def from_api(cls, d: Dict[str, Any]) -> "SagoCondition":
        d["logical_operator"] = LogicalOperator.OR
        d["value_type"] = ConditionValueType(d["value_type"])
        d["negate"] = False
        d["values"] = [x.strip().lower() for x in d["values"]]
        return cls.model_validate(d)


class SagoQuota(BaseModel):
    model_config = ConfigDict(populate_by_name=True, frozen=True)

    # We don't ever need this ... ?
    quota_id: str = Field()

    # the docs say nothing about this... are they different in diff quotas???
    cpi: Decimal = Field(gt=0, le=100, decimal_places=2, max_digits=5)

    remaining_count: int = Field()
    condition_hashes: List[str] = Field(min_length=0, default_factory=list)

    # There is no explicit status. The quota is closed if the count is 0

    def __hash__(self) -> int:
        return hash(tuple((tuple(self.condition_hashes), self.remaining_count)))

    @property
    def is_open(self) -> bool:
        min_open_spots = 3
        return self.remaining_count >= min_open_spots

    @classmethod
    def from_api(cls, d: Dict) -> Self:
        return cls.model_validate(d)

    def passes(self, criteria_evaluation: Dict[str, Optional[bool]]) -> bool:
        # Passes means we 1) meet all conditions (aka "match") AND 2) the
        # quota is open.
        return self.is_open and self.matches(criteria_evaluation)

    def matches(self, criteria_evaluation: Dict[str, Optional[bool]]) -> bool:
        # Matches means we meet all conditions.
        # We can "match" a quota that is closed. In that case, we would not be
        # eligible for the survey.
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
        evals = set(hash_evals.values())
        if False in evals:
            return False, set()
        if None in evals:
            return None, {cell for cell, ev in hash_evals.items() if ev is None}
        return True, set()


class SagoSurvey(MarketplaceTask):
    model_config = ConfigDict(populate_by_name=True)

    survey_id: CoercedStr = Field(min_length=1, max_length=16, pattern=r"^[0-9]+$")
    # There is no status returned, using one I make up b/c is_live depends on it,
    status: SagoStatus = Field(default=SagoStatus.LIVE)
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

    # unknown what the values actually correspond to. {1, 71, 73, 105, 116}
    account_id: str = Field(
        description="differentiates Market Cube from Panel Cube accounts"
    )
    study_type_id: str = Field()
    industry_id: str = Field()

    allowed_devices: DeviceTypes = Field(min_length=1)
    collects_pii: bool = Field(default=False)

    survey_exclusions: Optional[AlphaNumStrSet] = Field(
        description="list of excluded survey ids", default=None
    )
    ip_exclusions: Optional[IPLikeStrSet] = Field(
        description="list of excluded IP addresses", default=None
    )

    # Documentation I think is wrong. These are the keys "LOI" and "IR". it
    # doesn't say that they are bid or not, but they never seem to change ...
    bid_loi: Optional[int] = Field(default=None, le=120 * 60)
    bid_ir: Optional[float] = Field(default=None, ge=0, le=1)

    live_link: str = Field()

    # this comes from the Survey Reservation endpoint
    remaining_count: int = Field()

    qualifications: List[str] = Field(default_factory=list)
    quotas: List[SagoQuota] = Field(default_factory=list)

    source: Literal[Source.SAGO] = Field(default=Source.SAGO)

    used_question_ids: Set[AlphaNumStr] = Field(default_factory=set)

    # This is a "special" key to store all conditions that are used (as
    # "condition_hashes") throughout this survey. In the reduced representation
    # of this task (nearly always, for db i/o, in global_vars) this field will
    # be null.
    conditions: Optional[Dict[str, SagoCondition]] = Field(default=None)

    # These come from the API
    modified_api: AwareDatetimeISO = Field(
        description="When the survey was last updated in sago's system"
    )

    # This does not come from the API. We set it when we update this in the db.
    created: Optional[AwareDatetimeISO] = Field(default=None)
    updated: Optional[AwareDatetimeISO] = Field(default=None)

    @property
    def internal_id(self) -> str:
        return self.survey_id

    @computed_field
    def is_live(self) -> bool:
        return self.status == SagoStatus.LIVE

    @property
    def is_open(self) -> bool:
        # The survey is open if the status is OPEN and there is at least 1
        # open quota (or there are no quotas!)
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

    @property
    def condition_model(self) -> Type[MarketplaceCondition]:
        return SagoCondition

    @property
    def age_question(self) -> str:
        return "59"

    @property
    def marketplace_genders(self) -> Dict[Gender, Optional[MarketplaceCondition]]:
        return {
            Gender.MALE: SagoCondition(
                question_id="60",
                values=["58"],
                value_type=ConditionValueType.LIST,
            ),
            Gender.FEMALE: SagoCondition(
                question_id="60",
                values=["59"],
                value_type=ConditionValueType.LIST,
            ),
            Gender.OTHER: None,
        }

    @classmethod
    def from_api(cls, d: Dict) -> Optional["SagoSurvey"]:
        try:
            return cls._from_api(d)
        except Exception as e:
            logger.warning(f"Unable to parse survey: {d}. {e}")
            return None

    @classmethod
    def _from_api(cls, d: Dict):
        return cls.model_validate(d)

    def __repr__(self) -> str:
        # Fancy repr that abbreviates ip_exclusions and survey_exclusions
        repr_args = list(self.__repr_args__())
        for n, (k, v) in enumerate(repr_args):
            if k in {"ip_exclusions", "survey_exclusions"}:
                if v and len(v) > 6:
                    v = sorted(v)
                    v = v[:3] + ["…"] + v[-3:]
                    repr_args[n] = (k, v)
        join_str = ", "
        repr_str = join_str.join(
            repr(v) if a is None else f"{a}={v!r}" for a, v in repr_args
        )
        return f"{self.__repr_name__()}({repr_str})"

    def is_unchanged(self, other):
        # Avoiding overloading __eq__ because it looks kind of complicated? I
        # want to be explicit that this is not testing object equivalence, just
        # that the objects don't require any db updates. We also exclude
        # conditions b/c this is just the condition_hash definitions
        return self.model_dump(
            exclude={"updated", "conditions", "created"}
        ) == other.model_dump(exclude={"updated", "conditions", "created"})

    def to_mysql(self):
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
        d["modified_api"] = self.modified_api
        d["updated"] = self.updated
        return d

    @classmethod
    def from_db(cls, d: Dict[str, Any]):
        d["created"] = d["created"].replace(tzinfo=timezone.utc)
        d["updated"] = d["updated"].replace(tzinfo=timezone.utc)
        d["modified_api"] = d["modified_api"].replace(tzinfo=timezone.utc)
        d["qualifications"] = json.loads(d["qualifications"])
        d["used_question_ids"] = json.loads(d["used_question_ids"])
        d["quotas"] = json.loads(d["quotas"])
        return cls.model_validate(d)

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
