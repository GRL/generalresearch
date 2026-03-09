# https://developer.prodege.com/surveys-feed/api-reference/survey-matching/surveys
from __future__ import annotations

import json
import logging
from collections import defaultdict
from datetime import datetime, timezone
from decimal import Decimal
from functools import cached_property
from typing import Any, Dict, List, Literal, Optional, Set, Tuple, Type

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    computed_field,
    field_validator,
    model_validator,
)

from generalresearch.locales import Localelator
from generalresearch.models import LogicalOperator, Source, TaskCalculationType
from generalresearch.models.custom_types import (
    AlphaNumStrSet,
    AwareDatetimeISO,
    CoercedStr,
    InclExcl,
    UUIDStr,
)
from generalresearch.models.prodege import (
    ProdegePastParticipationType,
    ProdegeQuestionIdType,
    ProdegeStatus,
    ProdgeRedirectStatus,
)
from generalresearch.models.prodege.definitions import PG_COUNTRY_TO_ISO
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


class ProdegeCondition(MarketplaceCondition):
    model_config = ConfigDict(populate_by_name=True)

    question_id: ProdegeQuestionIdType = Field()
    values: List[str] = Field(validation_alias="precodes")

    @classmethod
    def from_api(cls, d: Dict[str, Any]) -> "ProdegeCondition":
        assert d["operator"] in {
            "OR",
            "NOT",
            "BETWEEN",
        }, f"invalid operator: {d['operator']}"
        d["precodes"] = [s.lower() for s in d["precodes"]]
        if d["operator"] == "BETWEEN":
            # They have a logical operator "between". Make this a range type with 1 range
            assert len(d["precodes"]) == 2, "wtf between"
            d["precodes"] = sorted(d["precodes"])
            d["precodes"] = [d["precodes"][0] + "-" + d["precodes"][1]]
            d["value_type"] = ConditionValueType.RANGE
            d["operator"] = LogicalOperator.OR
        elif d["operator"] == "NOT":
            # unclear if this is not(AND) or not(or). assuming not(or)
            d["value_type"] = ConditionValueType.LIST
            d["operator"] = LogicalOperator.OR
            d["negate"] = True
        else:
            d["value_type"] = ConditionValueType.LIST
            # They said if there are no precodes, it accepts any answer... supposedly.
            # (https://g-r-l.slack.com/archives/C04FMFTV48N/p1712878104684299)
            if len(d["precodes"]) == 0:
                d["value_type"] = ConditionValueType.ANSWERED
        d["question_id"] = str(d["question_id"])
        return cls.model_validate(d)


class ProdegeQuota(BaseModel):
    model_config = ConfigDict(populate_by_name=True, frozen=True)

    # API response is "sample_size"
    desired_count: int = Field(
        description="The desired total number of respondents",
        validation_alias="sample_size",
    )
    # API response is "number_of_respondents"
    remaining_count: int = Field(
        description="The total number of allowed responses that remain from the sample_size",
        validation_alias="number_of_respondents",
    )
    condition_hashes: List[str] = Field(min_length=0, default_factory=list)
    # Each quota can have a different calculation type, instead of on the survey
    calculation_type: TaskCalculationType = Field(
        description="Indicates whether the targets are counted per Complete or Survey Start",
        default=TaskCalculationType.COMPLETES,
    )
    quota_id: CoercedStr = Field()
    # If the parent_quota_id is None, then this is a parent. There can be multiple parent quotas.
    parent_quota_id: Optional[CoercedStr] = Field()

    # ISO 3166-1 alpha-2 (two-letter codes, lowercase)
    country_iso: Optional[str] = Field(
        max_length=2, min_length=2, pattern=r"^[a-z]{2}$", default=None
    )

    # There is no explicit status. The quota is closed if the remaining_count is 0

    def __hash__(self):
        return hash(self.quota_id)

    @property
    def is_parent(self) -> bool:
        return self.parent_quota_id is None

    @property
    def is_open(self) -> bool:
        min_open_spots = 2
        return self.remaining_count >= min_open_spots

    @property
    def condition_model(self) -> Type[MarketplaceCondition]:
        return ProdegeCondition

    @property
    def age_question(self) -> str:
        return "1"

    @property
    def marketplace_genders(self) -> Dict[Gender, Optional[MarketplaceCondition]]:
        return {
            Gender.MALE: ProdegeCondition(
                question_id="3",
                values=["1"],
                value_type=ConditionValueType.LIST,
            ),
            Gender.FEMALE: ProdegeCondition(
                question_id="3",
                values=["2"],
                value_type=ConditionValueType.LIST,
            ),
            Gender.OTHER: None,
        }

    @classmethod
    def from_api(cls, d: Dict[str, Any]) -> "ProdegeQuota":
        # the API doesn't handle None's correctly? idk
        if d["parent_quota_id"] == 0:
            d["parent_quota_id"] = None

        d["calculation_type"] = TaskCalculationType.prodege_from_api(
            d["calculation_type"]
        )

        if d.get("country_id"):
            d["country_iso"] = PG_COUNTRY_TO_ISO[d["country_id"]]

        return cls.model_validate(d)

    def passes(
        self, criteria_evaluation: Dict[str, Optional[bool]], country_iso: str
    ) -> bool:
        # Passes means we 1) meet all conditions (aka "match") AND 2) the quota is open.
        return self.is_open and self.matches(
            criteria_evaluation, country_iso=country_iso
        )

    def matches(
        self, criteria_evaluation: Dict[str, Optional[bool]], country_iso: str
    ) -> bool:
        # Match means we meet all conditions.
        # We can "match" a quota that is closed. In that case, we would
        #   fail the parent quota
        return self.matches_country(country_iso) and all(
            criteria_evaluation.get(c) for c in self.condition_hashes
        )

    def matches_country(self, country_iso: str) -> bool:
        return self.country_iso is None or self.country_iso == country_iso

    def passes_verbose(
        self, criteria_evaluation: Dict[str, Optional[bool]], country_iso: str
    ) -> bool:
        print(f"quota.is_open: {self.is_open}")
        print(
            ", ".join(
                [f"{c}: {criteria_evaluation.get(c)}" for c in self.condition_hashes]
            )
        )
        return (
            self.matches_country(country_iso)
            and self.is_open
            and all(criteria_evaluation.get(c) for c in self.condition_hashes)
        )

    def passes_soft(
        self, criteria_evaluation: Dict[str, Optional[bool]], country_iso: str
    ) -> Tuple[Optional[bool], Set[str]]:
        # Passes back "passes" (T/F/none) and a list of unknown criterion hashes
        if self.is_open is False:
            return False, set()
        if not self.matches_country(country_iso):
            return False, set()
        hash_evals = {
            cell: criteria_evaluation.get(cell) for cell in self.condition_hashes
        }
        evals = set(hash_evals.values())
        # We have to match all. So if any are False, we know we don't pass
        if False in evals:
            return False, set()
        # if any are None, we don't know
        elif None in evals:
            return None, {cell for cell, ev in hash_evals.items() if ev is None}
        else:
            return True, set()


class ProdegeMaxClicksSetting(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    # The total number of clicks allowed before survey traffic is paused.
    cap: int = Field(validation_alias="max_clicks_cap")

    # The current remaining number of clicks before survey traffic is paused.
    allowed_clicks: int = Field(validation_alias="max_clicks_allowed_clicks")

    # The refill rate id for clicks (1: every 30 min, 2: every 1 hour,
    #   3: every 24 hours, 0: one-time setting).
    #  (not going to bother structuring this, we can't really use it...)
    max_click_rate_id: int = Field(validation_alias="max_clicks_max_click_rate_id")


class ProdegeUserPastParticipation(BaseModel):
    # Represents the participation of a user in a Prodege task. This is stored in the
    #   prodege_sessionattempthistory table
    model_config = ConfigDict(frozen=True)

    survey_id: str = Field(min_length=1, max_length=16, pattern=r"^[0-9]+$")
    started: AwareDatetimeISO = Field()
    # This is what is returned in the redirect in the url param "status".
    ext_status_code_1: Optional[ProdgeRedirectStatus] = Field(default=None)

    @property
    def participation_types(self) -> Set[ProdegePastParticipationType]:
        # If the survey is filtering completes, then only a complete
        #   counts. But if the survey is filtering on clicks, then a person
        #   who got a complete ALSO did click. And so, the logic here is that
        #   participation_types should always include "click".
        if self.ext_status_code_1 is None:
            return {ProdegePastParticipationType.CLICK}

        elif self.ext_status_code_1 == "1":
            return {
                ProdegePastParticipationType.CLICK,
                ProdegePastParticipationType.COMPLETE,
            }

        elif self.ext_status_code_1 == "2":
            return {ProdegePastParticipationType.CLICK, ProdegePastParticipationType.OQ}

        elif self.ext_status_code_1 == "3":
            return {ProdegePastParticipationType.CLICK, ProdegePastParticipationType.DQ}

        elif self.ext_status_code_1 == "4":
            # 4 means "Quality Disqualification". unclear which participation type this is.
            return {ProdegePastParticipationType.CLICK, ProdegePastParticipationType.DQ}

        raise ValueError(f"Unknown ext_status_code_1: {self.ext_status_code_1}")

    def days_ago(self) -> float:
        now = datetime.now(timezone.utc)
        return (now - self.started).total_seconds() / (3600 * 24)


class ProdegePastParticipation(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    # They call a survey a project.
    survey_ids: AlphaNumStrSet = Field(validation_alias="participation_project_ids")
    filter_type: InclExcl = Field()
    # API has a mistake. We treat 0 as null
    in_past_days: Optional[int] = Field(default=None)
    participation_types: List[ProdegePastParticipationType] = Field()

    """
    e.g. Anyone who got a complete in either of these projects in the past 7 days, 
        is not allowed to participate in this task.
    {'participation_project_ids': [152677146, 152803285],
    'filter_type': 'exclude',
    'in_past_days': 7,
    'participation_types': ['complete']}
    """

    @classmethod
    def from_api(cls, d: Dict[str, Any]) -> "ProdegePastParticipation":
        # the API doesn't handle None's correctly? idk
        if d["in_past_days"] == 0:
            d["in_past_days"] = None

        d["participation_project_ids"] = list(map(str, d["participation_project_ids"]))
        return cls.model_validate(d)

    def user_participated(self, user_participation: ProdegeUserPastParticipation):
        # Given this user's participation event (1 single event), is it
        #   being filtered by this survey?

        return (
            user_participation.survey_id in self.survey_ids
            and (
                (self.in_past_days is None)
                or (self.in_past_days > user_participation.days_ago())
            )
            and user_participation.participation_types.intersection(
                self.participation_types
            )
        )

    def is_eligible(
        self, user_participations: List[ProdegeUserPastParticipation]
    ) -> bool:
        if self.filter_type == "include":
            # User is only eligible if they HAVE participated. Return True as soon as they match anything.
            for user_participation in user_participations:
                if self.user_participated(user_participation):
                    return True
            return False
        else:
            # User is only eligible if they HAVE NOT participated. We have to check ALL of their past participations,
            #   but we can return False as soon as one fails.
            for user_participation in user_participations:
                if self.user_participated(user_participation):
                    return False
            return True


class ProdegeSurvey(MarketplaceTask):
    model_config = ConfigDict(populate_by_name=True)

    survey_id: CoercedStr = Field(
        min_length=1, max_length=16, pattern=r"^[0-9]+$", validation_alias="projectid"
    )
    survey_name: str = Field(max_length=256, validation_alias="project_name")
    status: ProdegeStatus = Field(default=ProdegeStatus.LIVE)  # not returned from API
    # API returns more than 2 decimal places, but we are storing it in the db with max 2 ...
    cpi: Decimal = Field(gt=0, le=100, decimal_places=2)
    # ISO 3166-1 alpha-2 (two-letter codes, lowercase)
    country_iso: str = Field(
        max_length=2, min_length=2, pattern=r"^[a-z]{2}$", frozen=True
    )
    # 3-char ISO 639-2/B, lowercase
    language_iso: str = Field(
        max_length=3, min_length=3, pattern=r"^[a-z]{3}$", frozen=True
    )

    desired_count: int = Field(
        description="The desired total number of respondents",
        validation_alias="sample_size",
        gt=0,
    )
    # Unclear if this is always completes, or not if the TaskCalculationType is STARTS
    # Unclear if the survey.remaining_completes is tracking the same thing as the quota.number_of_respondents ... ?
    remaining_count: int = Field(
        description="The total number of allowed responses that remain from the sample_size",
        validation_alias="remaining_completes",
        ge=0,
    )
    # Unclear if this is always completes, or not if the TaskCalculationType is STARTS
    achieved_completes: int = Field(
        description="idk, not in the documentation. Seems to show the actual number of"
        "achieved completes globally, not just for us.",
        ge=0,
        default=0,
    )

    # Only the bid or actual value are returned in API res. We're going to
    # have to store it in the db if we see it. In API res, these are called
    # "loi" and "actual_ir", but the actual IR is only actually the actual
    # IR if the "phases" is "actual" :facepalm:
    bid_loi: Optional[int] = Field(default=None, le=120 * 60)
    bid_ir: Optional[float] = Field(default=None, ge=0, le=1)
    actual_loi: Optional[int] = Field(default=None, le=120 * 60)
    actual_ir: Optional[float] = Field(default=None, ge=0, le=1)
    # Unclear what the difference is bw IR and conversion
    conversion_rate: Optional[float] = Field(default=None, ge=0, le=1)

    entrance_url: str = Field(
        description="The link survey respondents should be sent to",
        validation_alias="surveyurl",
    )

    # This described time-based click rate limiting.
    max_clicks_settings: Optional[ProdegeMaxClicksSetting] = Field(default=None)
    # This describes the project/surveygroup exclusions
    past_participation: Optional[ProdegePastParticipation] = Field(default=None)
    # These describe the panelist exclusions/inclusions
    include_psids: Optional[Set[UUIDStr]] = Field(default=None)
    exclude_psids: Optional[Set[UUIDStr]] = Field(default=None)

    # There are no "qualifications" per se. Instead, everyone has the match a
    # parent quota (and its children) qualifications: List[str] =
    # Field(default_factory=list)

    # The eligibility is somewhat complex, with parent and children quotas.
    # Going to keep it flat here.
    quotas: List[ProdegeQuota] = Field(default_factory=list)

    source: Literal[Source.PRODEGE] = Field(default=Source.PRODEGE)

    used_question_ids: Set[ProdegeQuestionIdType] = Field(default_factory=set)

    # This is a "special" key to store all conditions that are used (as
    # "condition_hashes") throughout this survey. In the reduced representation
    # of this task (nearly always, for db i/o, in global_vars) this field will
    # be null.
    conditions: Optional[Dict[str, ProdegeCondition]] = Field(default=None)

    # These do not come from the API. We set them.
    created: Optional[AwareDatetimeISO] = Field(
        description="when we created this survey in our system", default=None
    )
    updated: Optional[AwareDatetimeISO] = Field(default=None)

    @property
    def internal_id(self) -> str:
        return self.survey_id

    @computed_field
    def is_live(self) -> bool:
        return self.status == ProdegeStatus.LIVE

    @computed_field
    def is_recontact(self) -> bool:
        return self.include_psids is not None

    @property
    def is_open(self) -> bool:
        # The survey is open if the status is OPEN and there is at least 1 open
        # quota (or there are no quotas!), and the remaining_count > 0, and
        # the max_clicks (if exists) > 0
        return (
            self.is_live
            and (any(q.is_open for q in self.quotas) or len(self.quotas) == 0)
            and self.remaining_count >= 2
            and (
                self.max_clicks_settings is None
                or self.max_clicks_settings.allowed_clicks > 0
            )
        )

    @model_validator(mode="before")
    @classmethod
    def set_locale(cls, data: Any):
        if not data.get("country_isos"):
            country_isos = [
                q["country_iso"] for q in data["quotas"] if q.get("country_iso")
            ]
            if country_isos:
                data["country_isos"] = country_isos
                data["language_isos"] = [
                    locale_helper.get_default_lang_from_country(c)
                    for c in data["country_isos"]
                ]
            else:
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
        return ProdegeCondition

    @property
    def age_question(self) -> str:
        return "1"

    @property
    def marketplace_genders(self) -> Dict[Gender, Optional[MarketplaceCondition]]:
        return {
            Gender.MALE: ProdegeCondition(
                question_id="3", values=["1"], value_type=ConditionValueType.LIST
            ),
            Gender.FEMALE: ProdegeCondition(
                question_id="3", values=["2"], value_type=ConditionValueType.LIST
            ),
            Gender.OTHER: None,
        }

    @field_validator("cpi", mode="before")
    def round_to_two_decimals(cls, v):
        return round(float(v), 2)

    @classmethod
    def from_api(cls, d: Dict[str, Any]) -> Optional["ProdegeSurvey"]:
        try:
            return cls._from_api(d)
        except Exception as e:
            logger.warning(f"Unable to parse survey: {d}. {e}")
            return None

    @classmethod
    def _from_api(cls, d: Dict[str, Any]) -> "ProdegeSurvey":

        # Handle phases. keys in api response are 'loi' and 'actual_ir'
        if d["phases"]["loi_phase"] == "actual":
            d["actual_loi"] = d.pop("loi") * 60
        else:
            d["bid_loi"] = d.pop("loi") * 60

        if d["phases"]["actual_ir_phase"] == "actual":
            d["actual_ir"] = d.pop("actual_ir") / 100
        else:
            d["bid_ir"] = d.pop("actual_ir") / 100

        d["conversion_rate"] = (
            d["conversion_rate"] / 100 if d["conversion_rate"] else None
        )
        if d.get("country_code"):
            d["country_isos"] = [
                locale_helper.get_country_iso(d.pop("country_code").lower())
            ]
            d["country_iso"] = sorted(d["country_isos"])[0]
            # No languages are returned anywhere for anything
            d["language_isos"] = [
                locale_helper.get_default_lang_from_country(d["country_isos"][0])
            ]
            d["language_iso"] = locale_helper.get_default_lang_from_country(
                d["country_iso"]
            )

        if d.get("past_participation"):
            d["past_participation"] = ProdegePastParticipation.from_api(
                d["past_participation"]
            )
        d["conditions"] = dict()
        for quota in d["quotas"]:
            quota["condition_hashes"] = []
            for c in quota["targeting_criteria"]:
                c["value_type"] = ConditionValueType.LIST
                c = ProdegeCondition.from_api(c)
                d["conditions"][c.criterion_hash] = c
                quota["condition_hashes"].append(c.criterion_hash)
        d["quotas"] = [ProdegeQuota.from_api(q) for q in d["quotas"]]
        countries = {q.country_iso for q in d["quotas"] if q.country_iso}
        if countries:
            d["country_iso"] = sorted(countries)[0]
            d["country_isos"] = countries
            d["language_iso"] = locale_helper.get_default_lang_from_country(
                d["country_iso"]
            )
            d["language_isos"] = [
                locale_helper.get_default_lang_from_country(c)
                for c in d["country_isos"]
            ]
        return cls.model_validate(d)

    @computed_field
    @cached_property
    def all_hashes(self) -> Set[str]:
        s = set()
        for q in self.quotas:
            s.update(set(q.condition_hashes))
        return s

    @property
    def quotas_verbose(self) -> List[List[Dict[str, Any]]]:
        assert self.conditions is not None, "conditions must be set"
        res = []
        for quota_group in self.quotas:
            sub_res = []
            res.append(sub_res)
            for quota in quota_group.root:
                q = quota.model_dump(mode="json")
                q["conditions"] = [
                    self.conditions[c].minified for c in quota.condition_hashes
                ]
                sub_res.append(q)
        return res

    def is_unchanged(self, other) -> bool:
        # Avoiding overloading __eq__ because it looks kind of complicated? I
        # want to be explicit that this is not testing object equivalence,
        # just that the objects don't require any db updates. We also exclude
        # conditions b/c this is just the condition_hash definitions

        # This is also especially bad bc the api returns ONLY bid OR actual
        # values, and so if a survey is stored with bid values in the db, the
        # api doesn't have them, it'll always be changed. Also, the name of
        # the survey changes randomly? idk. ignore that too
        o1 = self.model_dump(
            exclude={"created", "updated", "conditions", "survey_name"}
        )
        o2 = other.model_dump(
            exclude={"created", "updated", "conditions", "survey_name"}
        )
        if o1 == o2:
            # We don't have to check bid/actual, b/c we already know
            # it's not changed
            return True

        # Ignore bid fields if either one is NULL
        for k in ["bid_loi", "bid_ir"]:
            if o1.get(k) is None or o2.get(k) is None:
                o1.pop(k, None)
                o2.pop(k, None)

        return o1 == o2

    def to_mysql(self) -> Dict[str, Any]:
        d = self.model_dump(
            mode="json",
            exclude={
                "all_hashes",
                "country_isos",
                "language_isos",
                "source",
                "conditions",
                "buyer_id",
                "is_recontact",
            },
        )
        d["quotas"] = json.dumps(d["quotas"])

        for k in [
            "max_clicks_settings",
            "past_participation",
            "include_psids",
            "exclude_psids",
        ]:
            d[k] = json.dumps(d[k]) if d[k] else None

        d["used_question_ids"] = json.dumps(d["used_question_ids"])
        d["created"] = self.created
        d["updated"] = self.updated

        return d

    @classmethod
    def from_db(cls, d: Dict[str, Any]) -> "ProdegeSurvey":
        d["created"] = d["created"].replace(tzinfo=timezone.utc)
        d["updated"] = d["updated"].replace(tzinfo=timezone.utc)
        d["quotas"] = json.loads(d["quotas"])
        for k in [
            "max_clicks_settings",
            "past_participation",
            "include_psids",
            "exclude_psids",
            "used_question_ids",
        ]:
            d[k] = json.loads(d[k]) if d[k] else None
        # Need to re set countries from quotas here? Or not?
        # countries should be a property not a field anyways (todo:)
        return cls.model_validate(d)

    # ---- Yield Management ----

    def passes_quotas(
        self,
        criteria_evaluation: Dict[str, Optional[bool]],
        country_iso: str,
        verbose: bool = False,
    ) -> bool:
        # https://developer.prodege.com/surveys-feed/api-reference/survey-matching/quota-structure
        # https://developer.prodege.com/surveys-feed/api-reference/survey-matching/quota-matching-requirements
        parent_quotas = [q for q in self.quotas if q.is_parent]
        child_quotas = [q for q in self.quotas if not q.is_parent]
        quota_map = {q.quota_id: q for q in self.quotas}
        parent_children = defaultdict(set)
        for q in child_quotas:
            parent_children[q.parent_quota_id].add(q.quota_id)

        # To be eligible for a survey, we need to match ANY parent quota. To
        # match a parent quota, we need to match at least 1 child quota and
        # NOT match any closed children.
        passing_parent_quotas = [
            quota
            for quota in parent_quotas
            if quota.passes(criteria_evaluation, country_iso=country_iso)
        ]
        if not passing_parent_quotas:
            if verbose:
                print("No passing parent quotas")
            return False
        for quota in passing_parent_quotas:
            if verbose:
                print("parent")
                print(
                    quota.passes_verbose(criteria_evaluation, country_iso=country_iso)
                )
            child_quotas = [
                quota_map[quota_id] for quota_id in parent_children[quota.quota_id]
            ]
            passes = self.passes_child_quotas(
                criteria_evaluation,
                child_quotas=child_quotas,
                country_iso=country_iso,
                verbose=verbose,
            )
            if passes:
                return True
        return False

    def passes_child_quotas(
        self,
        criteria_evaluation: Dict[str, Optional[bool]],
        child_quotas: List[ProdegeQuota],
        country_iso: str,
        verbose: bool = False,
    ) -> bool:
        if len(child_quotas) == 0:
            # If the parent has no children, we pass
            return True

        # We have to pass at least 1 child
        passes = False
        for quota in child_quotas:
            if quota.matches(criteria_evaluation, country_iso=country_iso):
                if not quota.is_open:
                    # If we match a closed quota, the parent fails.
                    if verbose:
                        print("matched closed quota")
                    return False
                passes = True
                # We pass tentatively now, we still have to check the rest to see if we match any closed quotas.

        if verbose:
            print(
                [
                    quota.passes_verbose(criteria_evaluation, country_iso=country_iso)
                    for quota in child_quotas
                ]
            )

        return passes

    def determine_eligibility(
        self, criteria_evaluation: Dict[str, Optional[bool]], country_iso: str
    ) -> bool:
        return self.is_open and self.passes_quotas(
            criteria_evaluation, country_iso=country_iso
        )

    def print_eligibility(
        self, criteria_evaluation: Dict[str, Optional[bool]], country_iso: str
    ) -> None:
        print(f"is_open: {self.is_open}")
        print("passes_quotas")
        print(
            self.passes_quotas(
                criteria_evaluation, country_iso=country_iso, verbose=True
            )
        )

        return None
