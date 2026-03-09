# docs are a pdf
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from decimal import Decimal
from functools import cached_property
from typing import Any, Dict, List, Literal, Optional, Set, Type
from uuid import UUID

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    computed_field,
    field_validator,
    model_validator,
)
from typing_extensions import Self

from generalresearch.grpc import timestamp_from_datetime
from generalresearch.locales import Localelator
from generalresearch.models import (
    DeviceType,
    LogicalOperator,
    Source,
    TaskCalculationType,
)
from generalresearch.models.custom_types import (
    AwareDatetimeISO,
    CoercedStr,
    UUIDStr,
)
from generalresearch.models.repdata import RepDataStatus
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


class RepDataCondition(MarketplaceCondition):
    question_id: CoercedStr = Field(
        min_length=1,
        max_length=16,
        pattern=r"^[0-9]+$",
        validation_alias="StandardGlobalQuestionID",
    )
    values: List[str] = Field(min_length=1, validation_alias="PreCodes")
    value_type: Literal[ConditionValueType.LIST] = Field(
        default=ConditionValueType.LIST
    )

    @classmethod
    def from_api(cls, d: Dict[str, Any]) -> "RepDataCondition":
        if d["Condition"] == "Is":
            d["logical_operator"] = LogicalOperator.OR
            d["negate"] = False
        elif d["Condition"] == "IsNot":
            # todo: idk if this is really and, but its safer to say it is (not all values)
            d["logical_operator"] = LogicalOperator.AND
            d["negate"] = True
        else:
            raise ValueError(f"unknown condition: {d['Condition']}")
        return cls.model_validate(d)


class RepDataQuota(BaseModel):
    """
    A quota that can be on a stream. The parent stream has a CalculationType,
    which dictates the meaning of the fields “Quota”, “QuotaAchieved” and
    “QuotaRemaining
    """

    model_config = ConfigDict(populate_by_name=True, frozen=True)
    quota_id: CoercedStr = Field(
        min_length=1, max_length=16, pattern=r"^[0-9]+$", validation_alias="QuotaId"
    )
    quota_uuid: UUIDStr = Field(validation_alias="QuotaUd")
    name: str = Field(validation_alias="QuotaName")
    desired_count: Optional[int] = Field(
        default=None,
        validation_alias="Quota",
        description="Desired completes or starts (depending on calculation_type)",
    )
    achieved_count: int = Field(
        validation_alias="QuotaAchieved",
        description="Achieved completes or starts (depending on calculation_type)",
    )
    remaining_count: Optional[int] = Field(
        validation_alias="QuotaRemaining",
        description="Completes or starts remaining (depending on calculation_type). Should "
        "be used as the indicator for whether more respondents are needed to a "
        "specific quota. If QuotaRemaining value = 0, then pause. If None, then the quota"
        "is completely open (i.e. infinity). Unclear if this is true though (see .is_open)",
    )
    conditions: List[RepDataCondition] = Field(min_length=1)
    condition_hashes: List[str] = Field(min_length=1, default_factory=list)

    @field_validator("quota_uuid", mode="before")
    @classmethod
    def check_uuid_type(cls, v: str | UUID) -> str:
        return UUID(v).hex if isinstance(v, str) else v.hex

    @model_validator(mode="before")
    @classmethod
    def set_condition_hashes(cls, data: Any):
        if data.get("conditions"):
            data["condition_hashes"] = [q.criterion_hash for q in data["conditions"]]
        return data

    @property
    def is_open(self) -> bool:
        # According to the docs, if remaining count is None then the quota is
        # open, but this does not seem to be the case. See e.g. stream_id='125928'
        return self.remaining_count and self.remaining_count > 0

    @classmethod
    def from_api(cls, quota_res) -> Self:
        d = quota_res.copy()
        d["conditions"] = [RepDataCondition.from_api(q) for q in d["Questions"]]
        # Sometimes this is an empty string. (todo: does that mean 0? who knows?)
        d["QuotaAchieved"] = d["QuotaAchieved"] if d["QuotaAchieved"] != "" else 0
        d["Quota"] = d["Quota"] if d["Quota"] != "" else 0
        d["QuotaRemaining"] = d["QuotaRemaining"] if d["QuotaRemaining"] != "" else None
        return cls.model_validate(d)

    def to_hashed_quota(self):
        d = self.model_dump(mode="json", exclude={"conditions"})
        return RepDataHashedQuota.model_validate(d)

    def passes(self, criteria_evaluation: Dict[str, Optional[bool]]) -> Optional[bool]:
        # We have to match all conditions within the quota.
        return self.is_open and all(
            criteria_evaluation.get(c) for c in self.condition_hashes
        )


class RepDataHashedQuota(RepDataQuota):
    conditions: None = Field(default=None, exclude=True)


class RepDataStream(MarketplaceTask):
    model_config = ConfigDict(populate_by_name=True)
    stream_id: CoercedStr = Field(
        min_length=1, max_length=16, pattern=r"^[0-9]+$", validation_alias="StreamId"
    )
    stream_uuid: UUIDStr = Field(validation_alias="StreamUd")

    stream_name: str = Field(max_length=256, validation_alias="StreamName")
    stream_status: RepDataStatus = Field(validation_alias="StreamStatus")
    calculation_type: TaskCalculationType = Field(
        description="Indicates whether the targets are counted per Complete or Survey Start",
        validation_alias="CalculationType",
    )

    qualifications: List[RepDataCondition] = Field(min_length=1)
    qualification_hashes: List[str] = Field(min_length=1, default_factory=list)
    quotas: List[RepDataQuota] = Field(min_length=1)
    hashed_quotas: List[RepDataHashedQuota] = Field(min_length=1, default_factory=list)

    used_question_ids: Set[str] = Field(default_factory=set)

    # Note: The API returns both Expected and ExpectedStreamCompletes which are the same
    expected_count: int = Field(
        validation_alias="ExpectedStreamCompletes",
        description="If CalculationType = COMPLETES, represents the required completes from"
        "the suppler, if STARTS, then the required survey starts",
    )
    # Note: this is new as of 2024-May
    remaining_count: int = Field(
        description="Remaining number of Completes or Survey Starts. If “Remaining”= 0, then pause sample",
        validation_alias="Remaining",
    )

    cpi: Decimal = Field(gt=0, le=100, validation_alias="CPI")
    days_in_field: Optional[int] = Field(validation_alias="DaysInField", default=None)

    # # -------------- # #
    #  Below here: these fields are useless because it is our own data.
    # # -------------- # #
    actual_ir: int = Field(
        ge=0,
        le=100,
        validation_alias="ActualIR",
        description="In-field survey incidence rate",
    )
    actual_loi: int = Field(
        ge=0,
        le=120 * 60,
        validation_alias="ActualLOI",
        description="In-field median LOI (in seconds)",
    )
    actual_conversion: int = Field(
        ge=0,
        le=100,
        validation_alias="Conversion",
        description="Represents the live conversion rate for the supplier",
    )

    actual_complete_count: int = Field(
        validation_alias="ActualStreamCompletes",
        description="the total number of completes for the supplier",
    )
    actual_count: int = Field(
        validation_alias="Actual",
        description="If CalculationType = COMPLETES, represents the total completes from "
        "the supplier, if STARTS, then the total survey starts",
    )
    source: Literal[Source.REPDATA] = Field(default=Source.REPDATA)

    # These are copied from the survey so that this can implement the
    # MarketplaceTask class ISO 3166-1 alpha-2 (two-letter codes, lowercase)
    country_iso: str = Field(
        max_length=2, min_length=2, pattern=r"^[a-z]{2}$", frozen=True
    )
    # 3-char ISO 639-2/B, lowercase
    language_iso: str = Field(
        max_length=3, min_length=3, pattern=r"^[a-z]{3}$", frozen=True
    )

    @field_validator("stream_uuid", mode="before")
    @classmethod
    def check_uuid_type(cls, v: str | UUID) -> str:
        return UUID(v).hex if isinstance(v, str) else v.hex

    @model_validator(mode="before")
    @classmethod
    def set_qualification_hashes(cls, data: Any):
        if data.get("qualifications"):
            data["qualification_hashes"] = [
                q.criterion_hash for q in data["qualifications"]
            ]
        return data

    @model_validator(mode="before")
    @classmethod
    def set_hashed_quotas(cls, data: Any):
        if data.get("quotas"):
            data["hashed_quotas"] = [q.to_hashed_quota() for q in data["quotas"]]
        return data

    @model_validator(mode="before")
    @classmethod
    def set_used_questions(cls, data: Any):
        if data.get("used_question_ids"):
            return data
        s = set()
        if data.get("qualifications"):
            s.update({q.question_id for q in data["qualifications"]})
        if data.get("quotas"):
            for quota in data["quotas"]:
                s.update({q.question_id for q in quota.conditions})
        data["used_question_ids"] = s
        return data

    @model_validator(mode="before")
    @classmethod
    def set_locale(cls, data: Any):
        data["country_isos"] = [data["country_iso"]]
        data["language_isos"] = [data["language_iso"]]
        return data

    @property
    def internal_id(self) -> str:
        return self.stream_id

    @computed_field
    @cached_property
    def all_hashes(self) -> Set[str]:
        s = set(self.qualification_hashes.copy())
        for q in self.hashed_quotas:
            s.update(set(q.condition_hashes))
        return s

    @property
    def all_conditions(self) -> List[RepDataCondition]:
        cs = self.qualifications.copy()
        for quota in self.quotas:
            cs.extend(quota.conditions.copy())
        cs = list({c.criterion_hash: c for c in cs}.values())
        return cs

    @property
    def is_open(self) -> bool:
        # The stream is open if the status is open and there is at least 1 open
        # quota, and the expected_count > actual_count
        return (
            self.stream_status == RepDataStatus.LIVE
            and any(q.is_open for q in self.hashed_quotas)
            and self.remaining_count > 0
        )

    @property
    def is_live(self) -> bool:
        return self.stream_status == RepDataStatus.LIVE

    @property
    def condition_model(self) -> Type[MarketplaceCondition]:
        return RepDataCondition

    @property
    def age_question(self) -> str:
        return "42"

    @property
    def marketplace_genders(self) -> Dict[Gender, Optional[MarketplaceCondition]]:
        return {
            Gender.MALE: RepDataCondition(
                question_id="43",
                values=["1"],
                value_type=ConditionValueType.LIST,
            ),
            Gender.FEMALE: RepDataCondition(
                question_id="43",
                values=["2"],
                value_type=ConditionValueType.LIST,
            ),
            Gender.OTHER: None,
        }

    @classmethod
    def from_api(cls, stream_res, country_iso: str, language_iso: str):
        # qualifications and quotas need to be added to the stream_res manually
        d = stream_res.copy()
        d["CalculationType"] = TaskCalculationType.from_api(d["CalculationType"])
        d["ActualLOI"] = d["ActualLOI"] * 60
        d["StreamStatus"] = d["StreamStatus"].upper()
        # todo: cpi decimal places?
        d["CPI"] = d["CPI"]
        d["qualifications"] = [
            RepDataCondition.from_api(q) for q in d["qualifications"]
        ]
        d["quotas"] = [RepDataQuota.from_api(q) for q in d["quotas"]]
        return cls.model_validate(
            d | {"country_iso": country_iso, "language_iso": language_iso}
        )

    def to_hashed_stream(self):
        d = self.model_dump(mode="json", exclude={"qualifications", "quotas"})
        return RepDataStreamHashed.model_validate(d)


class RepDataStreamHashed(RepDataStream):
    qualifications: None = Field(default=None, exclude=True)
    quotas: None = Field(default=None, exclude=True)

    def to_mysql(self):
        d = self.model_dump(mode="json")
        d["qualification_hashes"] = json.dumps(d["qualification_hashes"])
        d["hashed_quotas"] = json.dumps(d["hashed_quotas"])
        d["used_question_ids"] = json.dumps(d["used_question_ids"])
        return d

    @classmethod
    def from_db(
        cls, res: Dict[str, Any], survey: RepDataSurveyHashed
    ) -> "RepDataStreamHashed":
        # We need certain fields copied over here so that a stream can exist
        # independent of the survey
        res["country_iso"] = survey.country_iso
        res["language_iso"] = survey.language_iso
        return cls.model_validate(res)


class RepDataSurvey(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    survey_id: CoercedStr = Field(
        min_length=1,
        max_length=16,
        pattern=r"^[0-9]+$",
        validation_alias="SurveyNumber",
    )
    survey_uuid: UUIDStr = Field(validation_alias="SurveyUd")
    survey_name: str = Field(max_length=256, validation_alias="SurveyName")
    project_uuid: UUIDStr = Field(
        validation_alias="ProjectUd", description="ID for the parent project"
    )

    survey_status: RepDataStatus = Field(validation_alias="SurveyStatus")

    # ISO 3166-1 alpha-2 (two-letter codes, lowercase)
    country_iso: str = Field(
        max_length=2, min_length=2, pattern=r"^[a-z]{2}$", frozen=True
    )
    # 3-char ISO 639-2/B, lowercase
    language_iso: str = Field(
        max_length=3, min_length=3, pattern=r"^[a-z]{3}$", frozen=True
    )

    estimated_loi: int = Field(
        gt=0,
        le=90 * 60,
        validation_alias="EstimatedLOI",
        description="Expected median time that respondents will need to take the "
        "Survey from start to finish.",
    )
    estimated_ir: int = Field(ge=0, le=100, validation_alias="EstimatedIR")
    collects_pii: bool = Field(
        validation_alias="PII", description="Indicates whether PII is collected"
    )

    allowed_devices: List[DeviceType] = Field(
        min_length=1, validation_alias="Device Compatibility"
    )

    streams: List[RepDataStream] = Field(min_length=1)
    hashed_streams: List[RepDataStreamHashed] = Field(
        min_length=1, default_factory=list
    )

    # These do not come from the API. We set them ourselves
    created: Optional[AwareDatetimeISO] = Field(default=None)
    last_updated: Optional[AwareDatetimeISO] = Field(default=None)

    @field_validator("survey_uuid", "project_uuid", mode="before")
    @classmethod
    def check_uuid_type(cls, v: str | UUID) -> str:
        return UUID(v).hex if isinstance(v, str) else v.hex

    @model_validator(mode="before")
    @classmethod
    def set_hashed_streams(cls, data: Any):
        if data.get("streams"):
            data["hashed_streams"] = [q.to_hashed_stream() for q in data["streams"]]
        return data

    @field_validator("allowed_devices", mode="after")
    def sort_allowed_devices(cls, values: List[str]):
        return sorted(values)

    @property
    def is_open(self) -> bool:
        # The survey is open if the status is open and there is at least 1
        # open stream
        return self.survey_status == RepDataStatus.LIVE and any(
            q.is_open for q in self.hashed_streams
        )

    @property
    def is_live(self) -> bool:
        # A survey may be live, but it only has 1 stram which is not live.
        # And so it is not really live. We have to check this separately.
        return self.survey_status == RepDataStatus.LIVE

    @property
    def all_hashes(self) -> Set[str]:
        s = set()
        for stream in self.hashed_streams:
            s.update(stream.all_hashes)
        return s

    @property
    def all_conditions(self) -> List[RepDataCondition]:
        cs = list()
        for stream in self.streams:
            cs.extend(stream.all_conditions)
        # dedupe by criterion_hash
        cs = list({c.criterion_hash: c for c in cs}.values())
        return cs

    @property
    def allowed_devices_str(self) -> str:
        return ",".join(map(str, sorted([d.value for d in self.allowed_devices])))

    @classmethod
    def from_api(cls, survey_response) -> Optional["RepDataSurvey"]:
        """
        :param survey_response: Raw response from API
        """
        try:
            return cls._from_api(survey_response)
        except Exception as e:
            survey_id = survey_response.get("survey_id") or survey_response.get(
                "SurveyNumber"
            )
            logger.warning(f"Unable to parse survey {survey_id}. {e}")
            return None

    @classmethod
    def _from_api(cls, survey_response) -> "RepDataSurvey":
        d = survey_response.copy()
        d["country_iso"] = locale_helper.get_country_iso(d["SurveyCountry"].lower())
        d["language_iso"] = locale_helper.get_language_iso(d["SurveyLanguage"].lower())
        d["EstimatedLOI"] = d["EstimatedLOI"] * 60
        d["SurveyStatus"] = d["SurveyStatus"].upper()
        d["allowed_devices"] = [
            DeviceType[x["device_name"].upper()] for x in d["DeviceCompatibility"]
        ]
        d["streams"] = [
            RepDataStream.from_api(
                stream, country_iso=d["country_iso"], language_iso=d["language_iso"]
            )
            for stream in d["Streams"]
        ]
        return cls.model_validate(d)

    def __hash__(self):
        # We need this so this obj can be added into a set.
        return hash(self.survey_id)

    def is_unchanged(self, other) -> bool:
        # Avoiding overloading __eq__ because it looks kind of complicated? I
        # want to be explicit that this is not testing object equivalence, just
        # that the objects don't require any db updates
        return self.model_dump(exclude={"created", "last_updated"}) == other.model_dump(
            exclude={"created", "last_updated"}
        )

    def is_changed(self, other) -> bool:
        return not self.is_unchanged(other)

    def to_mysql(self) -> Dict[str, Any]:
        return self.to_hashed_survey().to_mysql()

    def to_hashed_survey(self) -> "RepDataSurveyHashed":
        d = self.model_dump(mode="json", exclude={"streams"})
        return RepDataSurveyHashed(**d)

    def to_marketplace_task(self):
        pass


class RepDataSurveyHashed(RepDataSurvey):
    streams: None = Field(default=None, exclude=True)

    @classmethod
    def from_db(cls, res: Dict[str, Any]) -> "RepDataSurveyHashed":
        res["allowed_devices"] = [
            DeviceType(int(x)) for x in res["allowed_devices"].split(",")
        ]
        if res["created"] is not None:
            res["created"] = res["created"].replace(tzinfo=timezone.utc)
        res["last_updated"] = res["last_updated"].replace(tzinfo=timezone.utc)
        return cls.model_validate(res)

    def to_mysql(self) -> Dict[str, Any]:
        d = self.model_dump(mode="json", by_alias=True, exclude={"hashed_streams"})
        d["allowed_devices"] = ",".join(
            map(str, sorted([d.value for d in self.allowed_devices]))
        )
        d["streams"] = [stream.to_mysql() for stream in self.hashed_streams]
        if self.created:
            d["created"] = self.created.replace(tzinfo=None)
        return d

    def to_grpc(self, repdata_pb2):
        now = datetime.now(tz=timezone.utc)
        timestamp = timestamp_from_datetime(now)

        return repdata_pb2.RepDataOpportunity(
            json_str=self.model_dump_json(),
            timestamp=timestamp,
            is_live=self.is_live,
            survey_id=self.survey_id,
        )

    @classmethod
    def from_grpc(cls, msg):
        return cls.model_validate_json(msg.json_str)
