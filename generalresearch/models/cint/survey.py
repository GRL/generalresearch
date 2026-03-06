from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional, Dict, Set, Tuple, List, Literal, Any, Type

from more_itertools import flatten
from pydantic import (
    NonNegativeInt,
    Field,
    ConfigDict,
    BaseModel,
    computed_field,
    model_validator,
)
from typing_extensions import Self, Annotated

from generalresearch.locales import Localelator
from generalresearch.models import Source, TaskCalculationType
from generalresearch.models.cint import CintQuestionIdType
from generalresearch.models.custom_types import (
    AwareDatetimeISO,
    CoercedStr,
    AlphaNumStr,
)
from generalresearch.models.thl.demographics import Gender
from generalresearch.models.thl.survey import MarketplaceTask
from generalresearch.models.thl.survey.condition import (
    MarketplaceCondition,
    ConditionValueType,
)

logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.INFO)

locale_helper = Localelator()


class CintCondition(MarketplaceCondition):
    model_config = ConfigDict(populate_by_name=True, frozen=False, extra="ignore")

    source: Source = Field(default=Source.CINT)
    question_id: CintQuestionIdType = Field()

    @classmethod
    def from_api(cls, d: Dict[str, Any]) -> Self:
        d["question_id"] = str(d["question_id"])
        d["values"] = list(map(str.lower, d["precodes"]))
        d["value_type"] = ConditionValueType.LIST
        if d["logical_operator"] == "NOT":
            # In cint, a not means a negated OR
            d["negate"] = True
            d["logical_operator"] = "OR"
        return cls.model_validate(d)


class CintQuota(BaseModel):
    model_config = ConfigDict(populate_by_name=True, frozen=True)
    quota_id: CoercedStr = Field(validation_alias="survey_quota_id")
    quota_type: Literal["total", "client"] = Field(validation_alias="survey_quota_type")
    conversion: Optional[float] = Field(ge=0, le=1, default=None)
    number_of_respondents: NonNegativeInt = Field(
        description="Number of completes available"
    )
    condition_hashes: Optional[List[str]] = Field(min_length=1, default=None)

    def __hash__(self):
        return hash(tuple((tuple(self.condition_hashes), self.quota_id)))

    @model_validator(mode="after")
    def validate_condition_len(self) -> Self:
        if self.quota_type == "total":
            assert (
                self.condition_hashes is None
            ), "total quota should not have conditions"
        elif self.quota_type == "client":
            assert len(self.condition_hashes) > 0, "quota must have conditions"
        return self

    @property
    def is_open(self) -> bool:
        return self.number_of_respondents >= 2

    @classmethod
    def from_api(cls, d: Dict) -> Self:
        d["survey_quota_type"] = d["survey_quota_type"].lower()
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
        if False in hash_evals.values():
            return False, set()
        if None in hash_evals.values():
            return None, {cell for cell, ev in hash_evals.items() if ev is None}
        return True, set()


class CintSurvey(MarketplaceTask):
    model_config = ConfigDict(populate_by_name=True)

    survey_id: CoercedStr = Field(min_length=1, max_length=16, pattern=r"^[0-9]+$")
    survey_name: str = Field(max_length=128)
    buyer_name: str = Field(
        description="Name of the buyer running the survey",
        validation_alias="account_name",
    )
    buyer_id: CoercedStr = Field(min_length=1, max_length=16, pattern=r"^[0-9]+$")

    is_live_raw: bool = Field(alias="is_live")

    bid_loi: Optional[int] = Field(
        ge=60, le=90 * 60, validation_alias="bid_length_of_interview"
    )
    bid_ir: Optional[float] = Field(ge=0, le=1, validation_alias="bid_incidence")
    collects_pii: Optional[bool] = Field()
    survey_group_ids: Set[CoercedStr] = Field()

    calculation_type: TaskCalculationType = Field(
        description="Indicates whether quotas are calculated based on completes or prescreens",
        default=TaskCalculationType.COMPLETES,
        validation_alias="survey_quota_calc_type",
    )
    is_only_supplier_in_group: bool = Field(
        description="true indicates that an allocation is reserved for a single supplier"
    )

    cpi: Decimal = Field(
        gt=0,
        le=100,
        decimal_places=2,
        max_digits=5,
        validation_alias="revenue_per_interview",
        description="This is AFTER commission",
    )
    gross_cpi: (
        Annotated[
            Decimal,
            Field(
                gt=0,
                le=100,
                decimal_places=2,
                max_digits=5,
                description="This is BEFORE commission",
            ),
        ]
        | None
    ) = None

    industry: str = Field(max_length=64)
    study_type: str = Field(max_length=64)

    total_client_entrants: NonNegativeInt = Field(
        description="Number of total client survey entrants across all suppliers."
    )
    total_remaining: NonNegativeInt = Field(
        description="Number of completes still available to the supplier"
    )
    completion_percentage: float = Field()
    conversion: Optional[float] = Field(
        ge=0,
        le=1,
        description="Percentage of respondents who complete the survey after qualifying",
    )
    mobile_conversion: Optional[float] = Field(
        ge=0,
        le=1,
        description="Percentage of respondents on a mobile device who complete the survey after qualifying.",
    )
    length_of_interview: Optional[NonNegativeInt] = Field(
        description="Median time for a respondent to complete the survey, excluding prescreener, in minutes. This "
        "value will be zero until 6 completes are achieved."
    )
    overall_completes: NonNegativeInt = Field(
        description="Number of completes already achieved across all suppliers on the survey."
    )
    revenue_per_click: Optional[float] = Field(
        description="The Revenue Per Click value of the survey. RPC = (RPI * completes) / system entrants",
        default=None,
    )
    termination_length_of_interview: Optional[NonNegativeInt] = Field(
        description="Median time for a respondent to be termed, in minutes. This value is calculated after six survey "
        "entrants and rounded to the nearest whole number. Until six survey entrants are achieved the "
        "value will be zero."
    )

    respondent_pids: Set[str] = Field(default_factory=set)

    qualifications: List[str] = Field(default_factory=list)
    quotas: List[CintQuota] = Field(default_factory=list)

    source: Literal[Source.CINT] = Field(default=Source.CINT)

    used_question_ids: Set[AlphaNumStr] = Field(default_factory=set)

    # This is a "special" key to store all conditions that are used (as "condition_hashes") throughout
    #   this survey. In the reduced representation of this task (nearly always, for db i/o, in global_vars)
    #   this field will be null.
    conditions: Optional[Dict[str, CintCondition]] = Field(default=None)

    # These do not come from the API. We set it when we update/create in the db.
    created_at: Optional[AwareDatetimeISO] = Field(default=None)
    last_updated: Optional[AwareDatetimeISO] = Field(default=None)

    @property
    def internal_id(self) -> str:
        return self.survey_id

    @property
    def is_open(self) -> bool:
        # The survey is open if the status is OPEN and there is at least 1 open quota (or there are no quotas!)
        return self.is_live and (
            any(q.is_open for q in self.quotas) or len(self.quotas) == 0
        )

    @property
    def is_live(self) -> bool:
        return self.is_live_raw

    def model_dump(self, **kwargs: Any) -> dict:
        data = super().model_dump(**kwargs)
        data["is_live"] = data.pop("is_live_raw", None)
        return data

    @computed_field
    @property
    def all_hashes(self) -> Set[str]:
        s = set(self.qualifications)
        for q in self.quotas:
            s.update(set(q.condition_hashes)) if q.condition_hashes else None
        return s

    @model_validator(mode="before")
    @classmethod
    def set_cpi(cls, data: Any):
        if data.get("gross_cpi") and not data.get("cpi"):
            data["cpi"] = (data["gross_cpi"] * Decimal("0.70")).quantize(
                Decimal("0.01")
            )
        if data.get("gross_cpi"):
            data["gross_cpi"] = data["gross_cpi"].quantize(Decimal("0.01"))
        return data

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
        return CintCondition

    @property
    def age_question(self) -> str:
        return "42"

    @property
    def marketplace_genders(self) -> Dict[Gender, Optional[MarketplaceCondition]]:
        return {
            Gender.MALE: CintCondition(
                question_id="43",
                values=["1"],
                value_type=ConditionValueType.LIST,
            ),
            Gender.FEMALE: CintCondition(
                question_id="43",
                values=["2"],
                value_type=ConditionValueType.LIST,
            ),
            Gender.OTHER: None,
        }

    @classmethod
    def from_api(cls, d: Dict) -> Optional[Self]:
        try:
            return cls._from_api(d)
        except Exception as e:
            logger.warning(f"Unable to parse survey: {d}. {e}")
            return None

    @classmethod
    def _from_api(cls, d: Dict) -> Self:
        if "cpi" in d:
            d["gross_cpi"] = Decimal(d.pop("cpi"))
        if "revenue_per_interview" in d:
            assert d["revenue_per_interview"]["currency_code"] == "USD"
            d["revenue_per_interview"] = Decimal(
                d["revenue_per_interview"]["value"]
            ).quantize(Decimal("0.01"))

        language_iso, country_iso = d["country_language"].split("_")
        d["country_iso"] = locale_helper.get_country_iso(country_iso.lower())
        d["language_iso"] = locale_helper.get_language_iso(language_iso.lower())

        d["bid_length_of_interview"] = round(d["bid_length_of_interview"] * 60)
        d["length_of_interview"] = round(d["length_of_interview"] * 60)
        d["termination_length_of_interview"] = round(
            d["termination_length_of_interview"] * 60
        )
        d["bid_incidence"] /= 100
        d["survey_quota_calc_type"] = TaskCalculationType.from_api(
            d["survey_quota_calc_type"]
        )

        # Cint/Cint Doesn't believe in using nullable values. Nullify them manually

        # termination_length_of_interview: Median time for a respondent to be termed,
        # in minutes. This value is calculated after six survey entrants and rounded
        # to the nearest whole number. Until six survey entrants are achieved the
        # value will be zero.
        if (
            d["termination_length_of_interview"] == 0
            and d["total_client_entrants"] <= 6
        ):
            d["termination_length_of_interview"] = None

        # length_of_interview 	int 	Median time for a respondent to complete the
        # survey, excluding the Cint Exchange (formerly Marketplace) prescreener,
        # in minutes. This value will be zero until a complete is achieved.
        # Documenation is wrong. it is 6 completes, but still some are not right
        if d["length_of_interview"] == 0 and d["overall_completes"] < 6:
            d["length_of_interview"] = None

        # conversion: either 1 or 6 completes? not clear
        if d["overall_completes"] == 0:
            d["conversion"] = None
            d["mobile_conversion"] = None
            d["revenue_per_click"] = None

        d["conditions"] = dict()
        d.setdefault("survey_qualifications", list())
        qualifications = [CintCondition.from_api(q) for q in d["survey_qualifications"]]
        for q in qualifications:
            d["conditions"][q.criterion_hash] = q
        d["qualifications"] = [x.criterion_hash for x in qualifications]

        quotas = []
        for quota in d["survey_quotas"]:
            if quota["survey_quota_type"] == "Total":
                quotas.append(CintQuota.from_api(quota))
            else:
                criteria = [CintCondition.from_api(q) for q in quota["questions"]]
                quota["condition_hashes"] = [x.criterion_hash for x in criteria]
                quotas.append(CintQuota.from_api(quota))
                for q in criteria:
                    d["conditions"][q.criterion_hash] = q
        d["quotas"] = quotas

        now = datetime.now(tz=timezone.utc)
        d["created_at"] = now
        d["last_updated"] = now

        return cls.model_validate(d)

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
        d["survey_group_ids"] = json.dumps(sorted(d["survey_group_ids"]))
        d["respondent_pids"] = json.dumps(sorted(d["respondent_pids"]))
        d["last_updated"] = self.last_updated
        d["created_at"] = self.created_at
        return d

    @classmethod
    def from_mysql(cls, d: Dict[str, Any]) -> Self:
        d["created_at"] = d["created_at"].replace(tzinfo=timezone.utc)
        d["last_updated"] = d["last_updated"].replace(tzinfo=timezone.utc)
        d["qualifications"] = json.loads(d["qualifications"])
        d["used_question_ids"] = json.loads(d["used_question_ids"])
        d["quotas"] = json.loads(d["quotas"])
        d["survey_group_ids"] = json.loads(d["survey_group_ids"])
        d["respondent_pids"] = json.loads(d["respondent_pids"])
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
            if q.quota_type == "total":
                matches = q.is_open
            else:
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
        total_quota = [q for q in self.quotas if q.quota_type == "total"][0]
        if not total_quota.is_open:
            return False, set()
        quotas = [q for q in self.quotas if q.quota_type != "total"]
        if len(quotas) == 0:
            return True, set()
        quota_eval = {
            quota: quota.matches_soft(criteria_evaluation) for quota in quotas
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
        # We check is_open when putting the survey in global_vars. Don't need to check again.
        # if self.is_open is False:
        #     return False, set()
        pass_quals, h_quals = self.passes_qualifications_soft(criteria_evaluation)
        if pass_quals is False:
            # short-circuit fail
            return False, set()
        pass_quotas, h_quotas = self.passes_quotas_soft(criteria_evaluation)
        if pass_quals and pass_quotas:
            return True, set()
        elif pass_quotas is False:
            return False, set()
        else:
            return None, h_quals | h_quotas
