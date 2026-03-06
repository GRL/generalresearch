from __future__ import annotations

import json
from datetime import timezone
from functools import cached_property
from typing import Optional, List, Literal, Set, Dict, Any, Tuple, Type

from more_itertools import flatten
from pydantic import (
    ConfigDict,
    Field,
    PrivateAttr,
    BaseModel,
    computed_field,
    model_validator,
)
from typing_extensions import Annotated

from generalresearch.models import Source
from generalresearch.models.custom_types import (
    CoercedStr,
    UUIDStrCoerce,
    AwareDatetimeISO,
    AlphaNumStrSet,
    DeviceTypes,
)
from generalresearch.models.precision import PrecisionQuestionID, PrecisionStatus
from generalresearch.models.thl.demographics import Gender
from generalresearch.models.thl.survey import MarketplaceTask
from generalresearch.models.thl.survey.condition import (
    MarketplaceCondition,
    ConditionValueType,
)


class PrecisionCondition(MarketplaceCondition):
    question_id: Optional[PrecisionQuestionID] = Field()
    values: List[Annotated[str, Field(max_length=128)]] = Field()
    value_type: ConditionValueType = Field(default=ConditionValueType.LIST)
    _CONVERT_LIST_TO_RANGE = ["age"]


class PrecisionQuota(BaseModel):
    model_config = ConfigDict(populate_by_name=True, frozen=False)

    name: str = Field()
    # Not sure if id or guid are used for anything
    id: str = Field()
    guid: str = Field()
    status: PrecisionStatus = Field(default=PrecisionStatus.OPEN)
    desired_count: int = Field(ge=0)
    # These 3 fields are "global" !
    achieved_count: int = Field(ge=0)
    termination_count: int = Field(ge=0)
    overquota_count: int = Field(ge=0)

    condition_hashes: List[str] = Field(min_length=1, default_factory=list)

    # Min spots a quota should have open to be OPEN
    _min_open_spots: int = PrivateAttr(default=3)

    def __hash__(self):
        return hash(self.guid)

    @property
    def is_live(self) -> bool:
        return self.status == PrecisionStatus.OPEN

    @property
    def is_open(self) -> bool:
        min_open_spots = 3
        return self.is_live and self.remaining_count >= min_open_spots

    @property
    def remaining_count(self) -> int:
        return max(self.desired_count - self.achieved_count, 0)

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


class PrecisionSurvey(MarketplaceTask):
    model_config = ConfigDict(populate_by_name=True, extra="ignore")

    # They call this the project ID (a project is a survey)
    survey_id: CoercedStr = Field(
        min_length=1, max_length=16, pattern=r"^[0-9]+$", validation_alias="prj_id"
    )
    # Almost always equals the survey_id, but we can use this to retrieve user IDs who should be excluded
    group_id: CoercedStr = Field(
        min_length=1, max_length=16, pattern=r"^[0-9]+$", validation_alias="grouping_id"
    )
    # There is no status returned, using one I make up b/c is_live depends on it,
    status: PrecisionStatus = Field(default=PrecisionStatus.OPEN)
    name: str = Field(validation_alias="prj_name")
    survey_guid: UUIDStrCoerce = Field(validation_alias="prj_guid")

    category_id: Optional[str] = Field(validation_alias="sc_id", default=None)
    buyer_id: CoercedStr = Field(max_length=16)

    # This seems to always be 0 ... ?
    # response_rate: float = Field(ge=0, le=1, validation_alias="rr", description="Invites divided by Completes")
    # How is this calculated? makes no sense
    # complete_pct: float = Field(ge=0, le=1, validation_alias="cp")
    # Also skipping: ismultiple (allowing multiple entrances). How is that even possible? They are all False anyways.

    bid_loi: int = Field(default=None, ge=59, le=120 * 60, validation_alias="loi")
    bid_ir: float = Field(ge=0, le=1, validation_alias="ir")
    # Be careful with this, it doesn't make any sense. See survey 452481, has 12 completes with a 100% live_ir,
    #   but the only quotas have 0 completes and 1052 terms. .... ??
    global_conversion: Optional[float] = Field(
        ge=0,
        le=1,
        default=None,
        validation_alias="live_ir",
        description="completes divide by sum of completes & terms",
    )

    desired_count: int = Field(ge=0, validation_alias="total_completes")
    # If achieved_count is 0, the global_conversion should be None
    achieved_count: int = Field(ge=0, validation_alias="cc")

    allowed_devices: DeviceTypes = Field(min_length=1)

    entry_link: str = Field(validation_alias="url")
    excluded_surveys: Optional[AlphaNumStrSet] = Field(
        description="list of excluded survey ids",
        default=None,
        validation_alias="exclusion_project_id",
    )

    quotas: List[PrecisionQuota] = Field(default_factory=list)

    source: Literal[Source.PRECISION] = Field(default=Source.PRECISION)

    used_question_ids: Set[PrecisionQuestionID] = Field(default_factory=set)

    # This is a "special" key to store all conditions that are used (as "condition_hashes") throughout
    #   this survey. In the reduced representation of this task (nearly always, for db i/o, in global_vars)
    #   this field will be null.
    conditions: Optional[Dict[str, PrecisionCondition]] = Field(default=None)

    # This comes from the API
    expected_end_date: Optional[AwareDatetimeISO] = Field(
        default=None, validation_alias="end_date"
    )

    # This does not come from the API. We set it when we update this in the db.
    created: Optional[AwareDatetimeISO] = Field(default=None)
    updated: Optional[AwareDatetimeISO] = Field(default=None)

    @property
    def internal_id(self) -> str:
        return self.survey_id

    @computed_field
    def is_live(self) -> bool:
        return self.status == PrecisionStatus.OPEN

    @property
    def is_open(self) -> bool:
        # The survey is open if the status is OPEN and there is at least 1 open quota (or there are no quotas!)
        return self.is_live and (
            any(q.is_open for q in self.quotas) or len(self.quotas) == 0
        )

    @computed_field
    @cached_property
    def all_hashes(self) -> Set[str]:
        s = set()
        for q in self.quotas:
            s.update(set(q.condition_hashes))
        return s

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
        return PrecisionCondition

    @property
    def age_question(self) -> str:
        return "age"

    @property
    def marketplace_genders(self) -> Dict[Gender, Optional[MarketplaceCondition]]:
        return {
            Gender.MALE: PrecisionCondition(
                question_id="gender",
                values=["male"],
                value_type=ConditionValueType.LIST,
            ),
            Gender.FEMALE: PrecisionCondition(
                question_id="gender",
                values=["female"],
                value_type=ConditionValueType.LIST,
            ),
            Gender.OTHER: None,
        }

    def __repr__(self) -> str:
        # Fancy repr that abbreviates exclude_pids and excluded_surveys
        repr_args = list(self.__repr_args__())
        for n, (k, v) in enumerate(repr_args):
            if k in {"excluded_surveys"}:
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
        return self.model_dump(
            exclude={"updated", "conditions", "created"}
        ) == other.model_dump(exclude={"updated", "conditions", "created"})

    def to_mysql(self):
        d = self.model_dump(
            mode="json",
            exclude={
                "all_hashes",
                "country_iso",
                "language_iso",
                "source",
                "conditions",
                "country_isos",
                "language_isos",
            },
        )
        d["quotas"] = json.dumps(d["quotas"])
        d["used_question_ids"] = json.dumps(sorted(d["used_question_ids"]))
        d["expected_end_date"] = self.expected_end_date
        d["updated"] = self.updated
        d["created"] = self.created
        return d

    @classmethod
    def from_db(cls, d: Dict[str, Any]):
        d["created"] = d["created"].replace(tzinfo=timezone.utc)
        d["updated"] = d["updated"].replace(tzinfo=timezone.utc)
        d["expected_end_date"] = (
            d["expected_end_date"].replace(tzinfo=timezone.utc)
            if d["expected_end_date"]
            else None
        )
        d["quotas"] = json.loads(d["quotas"])
        d["used_question_ids"] = json.loads(d["used_question_ids"])
        return cls.model_validate(d)

    def passes_quotas(self, criteria_evaluation: Dict[str, Optional[bool]]) -> bool:
        # We have to match 1 or more quota.
        # Quotas are exclusionary: they can NOT match a quota where currently_open=0
        any_pass = False
        for q in self.quotas:
            matches = q.matches(criteria_evaluation)
            if matches and not q.is_open:
                return False
            if matches:
                any_pass = True
        return any_pass

    def passes_quotas_soft(
        self, criteria_evaluation: Dict[str, Optional[bool]]
    ) -> Tuple[Optional[bool], Set[str]]:
        # Quotas are exclusionary. They can NOT match a quota where currently_open=0
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
        return self.is_open and self.passes_quotas(criteria_evaluation)

    def determine_eligibility_soft(
        self, criteria_evaluation: Dict[str, Optional[bool]]
    ) -> Tuple[Optional[bool], Optional[Set[str]]]:
        if not self.is_open:
            return False, None
        return self.passes_quotas_soft(criteria_evaluation)

    def participation_allowed(
        self, att_survey_ids: Set[str], att_group_ids: Set[str]
    ) -> bool:
        """
        Checks if this user can participate in this survey
        :param att_survey_ids: list of the user's previously attempted survey IDs
        :param att_job_ids: list of the user's previously attempted survey ID's Job IDs
        """
        assert isinstance(att_survey_ids, set), "must pass a set"
        assert isinstance(att_group_ids, set), "must pass a set"
        if self.survey_id in att_survey_ids:
            return False
        if self.group_id in att_group_ids:
            return False
        if self.excluded_surveys & att_survey_ids:
            return False
        return True
