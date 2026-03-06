from __future__ import annotations

from enum import Enum
from typing import Optional, List, Set
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, computed_field

from generalresearch.grliq.models.custom_types import GrlIqScore
from generalresearch.grliq.models.decider import (
    Decider,
    AttemptDecision,
    GrlIqAttemptResult,
)
from generalresearch.models.custom_types import UUIDStr, AwareDatetimeISO


class Phase(str, Enum):
    # The 'phase' of a THL-Session experience. grliq may be collected in
    # multiple places multiple times within one session

    # Within a custom offerwall. Very optional, as most BPs won't be running our code
    OFFERWALL = "offerwall"
    # When a user clicks on a bucket. Each session should go through this
    OFFERWALL_ENTER = "offerwall-enter"
    # Running in GRS. Not every session will have this.
    PROFILING = "profiling"
    # We could run grl-iq again when a user continues a session
    SESSION_CONTINUE = "session-continue"


class GrlIqForensicCategoryResult(BaseModel):
    """
    This is for reporting external to GRL.

    There is a balance between exposing enough to answer "why did this user get blocked?" without
    giving away technical knowledge that could be used to bypass.
    """

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    uuid: Optional[UUIDStr] = Field(
        description="The uuid for the GrlIqData model these results are based on",
        default=None,
        examples=[uuid4().hex],
    )

    updated_at: Optional[AwareDatetimeISO] = Field(default=None)
    is_complete: bool = Field(
        description="This is based on whether or not the GrlIqCheckerResults"
        "object that this data was based on was complete at that time.",
        default=False,
    )

    # ----- Behavioral -----
    is_bot: GrlIqScore = Field(
        description="User is behaving in a bot-like manner, for e.g. clicking "
        "buttons without moving the mouse",
        default=0,
    )
    is_velocity: GrlIqScore = Field(
        description="User is making HTTP Requests faster than a typical user",
        default=0,
    )
    is_oscillating: GrlIqScore = Field(
        description="User is changing IPs suspiciously (eg: may indicate "
        "non-sticky SOCKS connections.",
        default=0,
    )
    is_teleporting: GrlIqScore = Field(
        description="User is moving Countries, Geographic Regions, and/or "
        "locations faster than is humanly possible.",
        default=0,
    )
    # .....

    # ----- Technical -----
    is_inconsistent: GrlIqScore = Field(
        description="The User's platform (browser/device/OS) is inconsistent.",
        default=0,
    )
    is_tampered: GrlIqScore = Field(
        description="The User attempted to interfere with or modify the "
        "GRL-IQ security platform.",
        default=0,
    )

    # ----- GeoIP -----

    # Should this be a bool??? would it ever not be 0 or 100 ? answer: I guess if we check
    #   the IP via multiple sources, it could not be.
    is_anonymous: GrlIqScore = Field(
        description="The User's IP is flagged as anonymous",
        default=0,
    )
    suspicious_ip: GrlIqScore = Field(
        description="The User's IP properties are suspicious",
        default=0,
    )
    platform_ip_inconsistent: GrlIqScore = Field(
        description="The User's platform (browser/device/OS) is inconsistent "
        "with the User's IP",
        default=0,
    )

    @staticmethod
    def model_score_fields() -> List[str]:
        return [
            "is_bot",
            "is_velocity",
            "is_oscillating",
            "is_teleporting",
            "is_inconsistent",
            "is_tampered",
            "is_anonymous",
            "suspicious_ip",
            "platform_ip_inconsistent",
        ]

    @property
    def fraud_score(self) -> GrlIqScore:
        return max([getattr(self, k) for k in self.model_score_fields()])

    def is_attempt_allowed(self) -> bool:
        # this could take the buyer's security tolerances
        threshold = 50
        return all(getattr(self, k) < threshold for k in self.model_score_fields())

    def make_decision(self) -> GrlIqAttemptResult:
        decision = (
            AttemptDecision.PASS if self.is_attempt_allowed() else AttemptDecision.FAIL
        )
        return GrlIqAttemptResult(
            decider=Decider.GRL_IQ, decision=decision, fraud_score=self.fraud_score
        )


class GrlIqCheckerResult(BaseModel):
    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    score: GrlIqScore = Field(default=0)
    msg: Optional[str] = Field(default=None)

    @property
    def passes(self) -> bool:
        return self.score < 50


class GrlIqObservations(BaseModel):

    fingerprint_count: int = Field(
        default=0, description="Count of unique fingerprints (past 30 days)"
    )
    shared_fingerprint_count: int = Field(
        default=0,
        description="Count of users sharing the same fingerprints (past 30 days, same product_id)",
    )
    cellular_ip_count: int = Field(
        default=0, description="Count of unique cellular IPs used (past 30 days)"
    )
    non_cellular_ip_count: int = Field(
        default=0, description="Count of unique cellular IPs used (past 30 days)"
    )
    isp_count: int = Field(default=0, description="Count of unique ISPs (past 30 days)")
    timezone_count: int = Field(
        default=0, description="Count of unique timezones (by IP) (past 30 days)"
    )

    paste_event_count: Optional[int] = Field(
        default=None, description="Count of paste events (user pasted in text)"
    )
    visibilitychange_event_count: Optional[int] = Field(
        default=None,
        description="Count of visibilitychange events (entire page isn't visible)",
    )
    blur_event_count: Optional[int] = Field(
        default=None, description="Count of blur events (page lost focus)"
    )
    devicemotion_event_count: Optional[int] = Field(
        default=None,
        description="Count of devicemotion events (device gyroscope motion)",
    )
    click_event_count: Optional[int] = Field(
        default=None,
        description="Count of click events (any pointer type)",
    )
    # all clicks are marked as pointerType = 'mouse', but other pointermove events have a pointerType
    #   of 'touch' or 'mouse'
    pointermove_pointer_types: Optional[Set[str]] = Field(
        default=None, description="pointer types"
    )


class GrlIqCheckerResults(BaseModel):
    """
    Holds results for each individual checker.
    Used to calculate the category-based results and a final attempt-allowed decision
    """

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    uuid: Optional[UUIDStr] = Field(
        description="The uuid for the GrlIqData model these results are based on",
        default=None,
        examples=[uuid4().hex],
    )

    updated_at: Optional[AwareDatetimeISO] = Field(default=None)

    observations: Optional[GrlIqObservations] = Field(default=None)

    # browser_props
    check_environment: GrlIqCheckerResult = Field()
    check_environment_critical: GrlIqCheckerResult = Field()
    check_codecs: GrlIqCheckerResult = Field(default_factory=GrlIqCheckerResult)

    # fingerprints
    check_fingerprint_cycling: GrlIqCheckerResult = Field(
        default_factory=GrlIqCheckerResult
    )
    check_fingerprint_reuse: GrlIqCheckerResult = Field(
        default_factory=GrlIqCheckerResult
    )

    # System Fonts
    check_required_fonts: GrlIqCheckerResult = Field(default_factory=GrlIqCheckerResult)
    check_prohibited_fonts: GrlIqCheckerResult = Field(
        default_factory=GrlIqCheckerResult
    )

    # IP Info
    check_ip_country: GrlIqCheckerResult = Field()
    check_user_type: GrlIqCheckerResult = Field()
    check_ip_timezone: GrlIqCheckerResult = Field()
    check_user_anonymous: GrlIqCheckerResult = Field()
    check_ip_changes: GrlIqCheckerResult = Field(default_factory=GrlIqCheckerResult)
    check_isp_changes: GrlIqCheckerResult = Field(default_factory=GrlIqCheckerResult)
    check_timezone_changes: GrlIqCheckerResult = Field(
        default_factory=GrlIqCheckerResult
    )

    # tampered
    check_timestamp: GrlIqCheckerResult = Field(default_factory=GrlIqCheckerResult)
    check_seen_timestamps: GrlIqCheckerResult = Field(
        default_factory=GrlIqCheckerResult
    )
    check_execution_time_ms: GrlIqCheckerResult = Field(
        default_factory=GrlIqCheckerResult
    )

    # timezone
    check_timezone: GrlIqCheckerResult = Field()
    check_country_timezone: GrlIqCheckerResult = Field()

    # useragents
    check_useragent_other_enums: GrlIqCheckerResult = Field()
    check_useragent_ip_properties: GrlIqCheckerResult = Field()
    check_useragent_js: GrlIqCheckerResult = Field()
    check_useragent_data_properties: GrlIqCheckerResult = Field()
    check_useragent_device_family_brand: GrlIqCheckerResult = Field()

    # webrtc
    check_webrtc_success: GrlIqCheckerResult = Field()
    check_ip_webrtc_ip_detail: GrlIqCheckerResult = Field()

    # websocket (events)
    check_page_load_events: Optional[GrlIqCheckerResult] = Field(default=None)
    check_grliq_events: Optional[GrlIqCheckerResult] = Field(default=None)
    check_pasting: Optional[GrlIqCheckerResult] = Field(default=None)
    check_pointer_movements: Optional[GrlIqCheckerResult] = Field(default=None)
    check_device_motion: Optional[GrlIqCheckerResult] = Field(default=None)
    check_pointer_type: Optional[GrlIqCheckerResult] = Field(default=None)
    check_for_bad_events: Optional[GrlIqCheckerResult] = Field(default=None)

    # websocket (ping)
    check_average_rtt: Optional[GrlIqCheckerResult] = Field(default=None)

    # todo: we might also have a "fingerprint" in here ???

    @property
    def checker_fields(self):
        fields = list(self.model_fields.keys())
        return [f for f in fields if f.startswith("check_")]

    @computed_field
    @property
    def is_complete(self) -> bool:
        return all(getattr(self, f) is not None for f in self.checker_fields)
