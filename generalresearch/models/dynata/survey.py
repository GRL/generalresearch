from __future__ import annotations

import json
import logging
from datetime import timezone
from decimal import Decimal
from functools import cached_property
from typing import Any, Dict, List, Literal, Optional, Set, Tuple, Type

from more_itertools import flatten
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    RootModel,
    computed_field,
    field_validator,
    model_validator,
)
from typing_extensions import Self

from generalresearch.locales import Localelator
from generalresearch.models import Source, TaskCalculationType
from generalresearch.models.custom_types import (
    AlphaNumStr,
    AlphaNumStrSet,
    AwareDatetimeISO,
    CoercedStr,
    DeviceTypes,
)
from generalresearch.models.dynata import DynataStatus
from generalresearch.models.thl.demographics import (
    Gender,
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


class DynataRequirements(BaseModel):
    # Requires inviting (recontacting) specific respondents to a follow up survey.
    requires_recontact: bool = Field(default=False)

    # Requires respondents to provide personally identifiable
    # information (PII) within client survey.
    requires_pii_collection: bool = Field(default=False)

    # Requires respondents to utilize their webcam to participate.
    requires_webcam: bool = Field(default=False)

    # Requires use of facial recognition technology with
    # respondents, such as eye tracking.
    requires_eye_tracking: bool = Field(default=False)

    # Requires partner to allow Dynata to drop a cookie on respondent.
    requires_cookie_drops: bool = Field(default=False)

    # Requires partner-uploaded respondent PII to expand
    # third-party matched data.
    requires_sample_plus: bool = Field(default=False)

    # Requires respondents to download a software application.
    requires_app_vpn: bool = Field(default=False)

    # Requires additional incentives to be manually awarded to
    # respondent by partner outside of the typical online survey flow.
    requires_manual_rewards: bool = Field(default=False)

    def __repr__(self) -> str:
        # Fancy repr that only shows values if they are True
        repr_args = list(self.__repr_args__())
        repr_args = [(k, v) for k, v in repr_args if v]
        join_str = ", "
        repr_str = join_str.join(
            repr(v) if a is None else f"{a}={v!r}" for a, v in repr_args
        )
        return f"{self.__repr_name__()}({repr_str})"


class DynataCondition(MarketplaceCondition):
    question_id: Optional[CoercedStr] = Field(
        min_length=1,
        max_length=16,
        pattern=r"^[0-9]+$",
        validation_alias="attribute_id",
    )

    # This comes in the API and is used to match "cells" to quotas they're associated with. Once
    #   we parse the API response, we don't need this tag id anymore.
    tag: Optional[str] = Field(default=None, max_length=36)

    @classmethod
    def from_api(cls, cell: Dict[str, Any]) -> "DynataCondition":
        """
        We perform some preprocessing before calling this to pull in the data from COLLECTION cells.
        """
        if cell["kind"] == "COLLECTION":
            raise ValueError(
                "this should be converted into a LIST type by the API helper first"
            )

        assert cell["kind"] in {
            "VALUE",
            "LIST",
            "RANGE",
            "INEFFABLE",
            "ANSWERED",
            "RECONTACT",
            "INVITE_COLLECTIONS",
            "STATIC_INVITE_COLLECTIONS",
        }, f"unknown cell kind {cell['kind']}"
        d = {k: cell[k] for k in ["tag", "attribute_id", "negate"]}

        if cell["kind"] == "VALUE":
            d["values"] = [cell["value"]]
            d["value_type"] = ConditionValueType.LIST
            d["logical_operator"] = "OR"
            return cls.model_validate(d)

        if cell["kind"] == "LIST":
            d["values"] = list(map(str.lower, cell["list"]))
            d["value_type"] = ConditionValueType.LIST
            d["logical_operator"] = cell.get("operator", "OR")
            return cls.model_validate(d)

        if cell["kind"] == "RANGE":
            d["values"] = [
                "{0}-{1}".format(
                    cell["range"]["from"] or "inf", cell["range"]["to"] or "inf"
                )
            ]
            d["value_type"] = ConditionValueType.RANGE
            return cls.model_validate(d)

        if cell["kind"] == "INEFFABLE":
            d["value_type"] = ConditionValueType.INEFFABLE
            d["values"] = []
            return cls.model_validate(d)

        if cell["kind"] == "ANSWERED":
            d["value_type"] = ConditionValueType.ANSWERED
            d["values"] = []
            return cls.model_validate(d)

        if cell["kind"] in {"INVITE_COLLECTIONS", "STATIC_INVITE_COLLECTIONS"}:
            d["values"] = list(map(str.lower, cell["invite_collections"]))
            d["value_type"] = ConditionValueType.RECONTACT
            d["logical_operator"] = cell["operator"]
            d["attribute_id"] = None
            return cls.model_validate(d)


class DynataQuota(BaseModel):
    # This is called a Quota Object in Dynata
    model_config = ConfigDict(populate_by_name=True, frozen=True)

    # We don't ever need this
    # quota_id: CoercedStr = Field(min_length=1, max_length=64, validation_alias="id")
    count: int = Field(description="Limit of completes available")
    # Each condition_hash is called in Dynata a "Quota Cell"
    # Some quotas have no conditions. I'm not sure how eligibility is supposed to work for this.
    condition_hashes: List[str] = Field(min_length=0, default_factory=list)
    status: DynataStatus = Field()

    def __hash__(self):
        return hash(tuple((tuple(self.condition_hashes), self.count, self.status)))

    @property
    def is_open(self) -> bool:
        # todo: should we make this configurable somehow? Until we have like a bag-holding score back,
        #   this has be hardcoded...
        min_open_spots = 3
        return self.status == DynataStatus.OPEN and (self.count >= min_open_spots)

    def passes(self, criteria_evaluation: Dict[str, Optional[bool]]) -> bool:
        # We have to match all conditions (aka cells) within the quota (aka quota object).
        return self.is_open and all(
            criteria_evaluation.get(c) for c in self.condition_hashes
        )

    def passes_verbose(self, criteria_evaluation: Dict[str, Optional[bool]]) -> bool:
        print(f"quota.is_open: {self.is_open}")
        print(
            ", ".join(
                [f"{c}: {criteria_evaluation.get(c)}" for c in self.condition_hashes]
            )
        )
        return self.is_open and all(
            criteria_evaluation.get(c) for c in self.condition_hashes
        )

    def passes_soft(
        self, criteria_evaluation: Dict[str, Optional[bool]]
    ) -> Tuple[Optional[bool], Set[str]]:
        # Passes back "passes" (T/F/none) and a list of unknown criterion hashes
        if self.is_open is False:
            return False, set()
        cell_evals = {
            cell: criteria_evaluation.get(cell) for cell in self.condition_hashes
        }
        evals = set(cell_evals.values())
        # We have to match all. So if any are False, we know we don't pass
        if False in evals:
            return False, set()
        # if any are None, we don't know
        elif None in evals:
            return None, {cell for cell, ev in cell_evals.items() if ev is None}
        else:
            return True, set()


class DynataQuotaGroup(RootModel):
    root: List[DynataQuota] = Field()

    def __iter__(self):
        return iter(self.root)

    def __hash__(self):
        return hash(tuple(self.root))

    def passes(self, criteria_evaluation: Dict[str, Optional[bool]]) -> bool:
        # Qualify for ANY quota object within a quota group
        return any(quota.passes(criteria_evaluation) for quota in self.root)

    def passes_verbose(self, criteria_evaluation: Dict[str, Optional[bool]]) -> bool:
        # Qualify for ANY quota object within a quota group
        for quota in self.root:
            print("---")
            print(quota.passes_verbose(criteria_evaluation))
            print("---")
        return any(quota.passes(criteria_evaluation) for quota in self.root)

    @property
    def is_open(self) -> bool:
        return any(cell.is_open for cell in self.root)

    def passes_soft(
        self, criteria_evaluation: Dict[str, Optional[bool]]
    ) -> Tuple[Optional[bool], Set[str]]:
        # Qualify for ANY quota object within a quota group
        obj_evals = {obj: obj.passes_soft(criteria_evaluation) for obj in self.root}
        evals = set(v[0] for v in obj_evals.values())
        # If we match 1 obj, then the others don't matter
        if any(evals):
            return True, set()
        # If we have none passing, and at least 1 unknown, then it is conditional
        elif None in evals:
            conditional_hashes = set(
                flatten([v[1] for v in obj_evals.values() if v[0] is None])
            )
            return None, conditional_hashes
        else:
            return False, set()


class DynataFilterObject(RootModel):
    root: List[str] = Field()  # list of criterion hashes

    def __iter__(self):
        return iter(self.root)

    def __hash__(self):
        return hash(tuple(self.root))

    def passes(self, criteria_evaluation: Dict[str, Optional[bool]]) -> bool:
        # We have to match all cells within an object.
        return all(criteria_evaluation.get(cell) for cell in self.root)

    def passes_verbose(self, criteria_evaluation: Dict[str, Optional[bool]]) -> bool:
        for cell in self.root:
            print(f"{cell}: {criteria_evaluation.get(cell)}")
        # We have to match all cells within an object.
        return all(criteria_evaluation.get(cell) for cell in self.root)

    def passes_soft(
        self, criteria_evaluation: Dict[str, Optional[bool]]
    ) -> Tuple[Optional[bool], Set[str]]:
        # Passes back "passes" (T/F/none) and a list of unknown criterion hashes
        cell_evals = {cell: criteria_evaluation.get(cell) for cell in self.root}
        evals = set(cell_evals.values())
        # We have to match all. So if any are False, we know we don't pass
        if False in evals:
            return False, set()
        # if any are None, we don't know
        elif None in evals:
            return None, {cell for cell, ev in cell_evals.items() if ev is None}
        else:
            return True, set()


class DynataFilterGroup(RootModel):
    root: List[DynataFilterObject] = Field()

    def __iter__(self):
        return iter(self.root)

    def __hash__(self):
        return hash(tuple(self.root))

    def passes(self, criteria_evaluation: Dict[str, Optional[bool]]) -> bool:
        # A filter group is matched if we match at least 1 filter objs in the group.
        return any(obj.passes(criteria_evaluation) for obj in self.root)

    def passes_verbose(self, criteria_evaluation: Dict[str, Optional[bool]]) -> bool:
        # A filter group is matched if we match at least 1 filter objs in the group.
        for obj in self.root:
            print("---")
            print(obj.passes_verbose(criteria_evaluation))
            print("---")
        return any(obj.passes(criteria_evaluation) for obj in self.root)

    def passes_soft(
        self, criteria_evaluation: Dict[str, Optional[bool]]
    ) -> Tuple[Optional[bool], Set[str]]:
        # Passes back "passes" (T/F/none) and a list of unknown criterion hashes
        obj_evals = {obj: obj.passes_soft(criteria_evaluation) for obj in self.root}
        evals = set(v[0] for v in obj_evals.values())
        # If we match 1 obj, then the others don't matter
        if any(evals):
            return True, set()
        # If we have none passing, and at least 1 unknown, then it is conditional
        elif None in evals:
            conditional_hashes = set(
                flatten([v[1] for v in obj_evals.values() if v[0] is None])
            )
            return None, conditional_hashes
        else:
            return False, set()


class DynataSurvey(MarketplaceTask):
    model_config = ConfigDict(populate_by_name=True)

    survey_id: CoercedStr = Field(
        min_length=1, max_length=16, pattern=r"^[0-9]+$", validation_alias="id"
    )
    status: DynataStatus = Field()

    client_id: CoercedStr = Field(
        description="Identifier of client requesting the study", max_length=32
    )
    order_number: str = Field(description="Unique project identifier", max_length=32)
    project_id: CoercedStr = Field(
        max_length=32,
        min_length=1,
        description="opportunities in the same project have mutual participation exclusions",
    )
    group_id: CoercedStr = Field(
        description="Identifier of opportunity group",
        max_length=32,
        min_length=1,
    )

    # There are 91 min surveys. We'll filter them out later
    bid_loi: Optional[int] = Field(
        default=None,
        le=120 * 60,
        description="Docs says 'Estimated length of interview', but this is "
        "really the bid LOI'",
        validation_alias="length_of_interview",
    )
    bid_ir: Optional[float] = Field(validation_alias="incidence_rate", ge=0, le=1)
    cpi: Decimal = Field(gt=0, le=100, validation_alias="cost_per_interview")
    days_in_field: int = Field(description="Expected duration of opportunity in days")
    # This isn't checked for eligibility determination
    expected_count: int = Field(
        validation_alias="completes",
        description="Total fielding completes requested",
    )

    calculation_type: TaskCalculationType = Field(
        description="Indicates whether the targets are counted per Complete or Survey Start",
        validation_alias="evaluation",
    )
    category_ids: AlphaNumStrSet = Field(default_factory=set)

    # ISO 3166-1 alpha-2 (two-letter codes, lowercase)
    country_iso: str = Field(
        max_length=2, min_length=2, pattern=r"^[a-z]{2}$", frozen=True
    )
    # 3-char ISO 639-2/B, lowercase
    language_iso: str = Field(
        max_length=3, min_length=3, pattern=r"^[a-z]{3}$", frozen=True
    )

    allowed_devices: DeviceTypes = Field(min_length=1, validation_alias="devices")
    live_link: str = Field(description="entry link")
    created: AwareDatetimeISO = Field(description="Creation date of opportunity")

    project_exclusions: AlphaNumStrSet = Field(default_factory=set)
    category_exclusions: AlphaNumStrSet = Field(default_factory=set)
    requirements: DynataRequirements = Field()

    filters: List[DynataFilterGroup] = Field(default_factory=list)
    quotas: List[DynataQuotaGroup] = Field(default_factory=list)

    source: Literal[Source.DYNATA] = Field(default=Source.DYNATA)

    used_question_ids: Set[AlphaNumStr] = Field(default_factory=set)

    # This is a "special" key to store all conditions that are used (as "condition_hashes") throughout
    #   this survey. In the reduced representation of this task (nearly always, for db i/o, in global_vars)
    #   this field will be null.
    conditions: Optional[Dict[str, DynataCondition]] = Field(default=None)

    # These do not come from the API. We set them ourselves
    last_updated: Optional[AwareDatetimeISO] = Field(default=None)

    @property
    def internal_id(self) -> str:
        return self.survey_id

    @computed_field
    def is_live(self) -> bool:
        return self.status == DynataStatus.OPEN

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
        for fg in self.filters:
            for f in fg.root:
                s.update(f.root)
        for qg in self.quotas:
            for q in qg.root:
                s.update(set(q.condition_hashes))
        return s

    @field_validator("category_ids", mode="before")
    def split_category_ids(cls, v: object) -> object:
        if isinstance(v, str):
            return v.split("|")
        return v

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

    @model_validator(mode="after")
    def set_buyer_id(self):
        # In dynata, this is called "client_id", in the generic MarketplaceTask, we're using "buyer_id"
        self.buyer_id = self.client_id
        return self

    @property
    def filters_verbose(self) -> List[List[str]]:
        assert self.conditions is not None, "conditions must be set"
        res = []
        for filter_group in self.filters:
            sub_res = []
            res.append(sub_res)
            for filter in filter_group.root:
                sub_res.extend([self.conditions[c].minified for c in filter.root])
        return res

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

    @property
    def condition_model(self) -> Type[MarketplaceCondition]:
        return DynataCondition

    @property
    def age_question(self) -> str:
        return "80"

    @property
    def marketplace_genders(self):
        return {
            Gender.MALE: DynataCondition(
                question_id="1",
                values=["1"],
                value_type=ConditionValueType.LIST,
            ),
            Gender.FEMALE: DynataCondition(
                question_id="1",
                values=["2"],
                value_type=ConditionValueType.LIST,
            ),
            Gender.OTHER: None,
        }

    def is_unchanged(self, other) -> bool:
        # Avoiding overloading __eq__ because it looks kind of complicated? I want to be explicit that
        #   this is not testing object equivalence, just that the objects don't require any db updates.
        # We also exclude conditions b/c this is just the condition_hash definitions
        return self.model_dump(
            exclude={"created", "last_updated", "conditions"}
        ) == other.model_dump(exclude={"created", "last_updated", "conditions"})

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
        d["filters"] = json.dumps(d["filters"])
        d["quotas"] = json.dumps(d["quotas"])
        d["used_question_ids"] = json.dumps(sorted(d["used_question_ids"]))
        d["requirements"] = json.dumps(d["requirements"])
        d["created"] = self.created
        d["last_updated"] = self.last_updated
        return d

    @classmethod
    def from_db(cls, d: Dict[str, Any]) -> Self:
        d["created"] = d["created"].replace(tzinfo=timezone.utc)
        d["last_updated"] = d["last_updated"].replace(tzinfo=timezone.utc)
        d["filters"] = json.loads(d["filters"])
        d["quotas"] = json.loads(d["quotas"])
        d["used_question_ids"] = json.loads(d["used_question_ids"])
        d["requirements"] = json.loads(d["requirements"])
        return cls.model_validate(d)

    def passes_filters(self, criteria_evaluation: Dict[str, Optional[bool]]) -> bool:
        # We have to match all filter groups
        return all(group.passes(criteria_evaluation) for group in self.filters)

    def passes_filters_verbose(
        self, criteria_evaluation: Dict[str, Optional[bool]]
    ) -> bool:
        # We have to match all filter groups
        for group in self.filters:
            print("+++")
            group.passes_verbose(criteria_evaluation)
            print("+++")
        return all(group.passes(criteria_evaluation) for group in self.filters)

    def passes_filters_soft(
        self, criteria_evaluation: Dict[str, Optional[bool]]
    ) -> Tuple[Optional[bool], Set[str]]:
        # We have to match all filter groups
        group_eval = {
            group: group.passes_soft(criteria_evaluation) for group in self.filters
        }
        evals = set(g[0] for g in group_eval.values())
        if False in evals:
            return False, set()
        elif None in evals:
            conditional_hashes = set(
                flatten([v[1] for v in group_eval.values() if v[0] is None])
            )
            return None, conditional_hashes
        else:
            return True, set()

    def passes_quotas(self, criteria_evaluation: Dict[str, Optional[bool]]) -> bool:
        # We have to match all quota groups
        return all(
            quota_group.passes(criteria_evaluation) for quota_group in self.quotas
        )

    def passes_quotas_verbose(
        self, criteria_evaluation: Dict[str, Optional[bool]]
    ) -> bool:
        # We have to match all quota groups
        for quota_group in self.quotas:
            print("+++")
            quota_group.passes_verbose(criteria_evaluation)
            print("+++")
        return all(
            quota_group.passes(criteria_evaluation) for quota_group in self.quotas
        )

    def passes_quotas_soft(
        self, criteria_evaluation: Dict[str, Optional[bool]]
    ) -> Tuple[Optional[bool], Set[str]]:
        # We have to match all quota groups
        group_eval = {
            quota: quota.passes_soft(criteria_evaluation) for quota in self.quotas
        }
        evals = set(g[0] for g in group_eval.values())
        if False in evals:
            return False, set()
        elif None in evals:
            conditional_hashes = set(
                flatten([v[1] for v in group_eval.values() if v[0] is None])
            )
            return None, conditional_hashes
        else:
            return True, set()

    def determine_eligibility(
        self, criteria_evaluation: Dict[str, Optional[bool]]
    ) -> bool:
        return (
            self.is_open
            and self.passes_filters(criteria_evaluation)
            and self.passes_quotas(criteria_evaluation)
        )

    def determine_eligibility_verbose(
        self, criteria_evaluation: Dict[str, Optional[bool]]
    ) -> bool:
        print(f"is_open: {self.is_open}")
        print("passes_filters")
        print(self.passes_filters_verbose(criteria_evaluation))
        print("passes_quotas")
        print(self.passes_quotas_verbose(criteria_evaluation))
        return (
            self.is_open
            and self.passes_filters(criteria_evaluation)
            and self.passes_quotas(criteria_evaluation)
        )

    def determine_eligibility_soft(
        self, criteria_evaluation: Dict[str, Optional[bool]]
    ) -> Tuple[Optional[bool], Set[str]]:
        if self.is_open is False:
            return False, set()
        pass_filters, h_filters = self.passes_filters_soft(criteria_evaluation)
        pass_quotas, h_quotas = self.passes_quotas_soft(criteria_evaluation)
        if pass_filters and pass_quotas:
            return True, set()
        elif pass_filters is False or pass_quotas is False:
            return False, set()
        else:
            return None, h_filters | h_quotas
