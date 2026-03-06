from datetime import datetime, timezone
from enum import Enum
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field

from generalresearch.models.custom_types import AwareDatetimeISO


class Decider(str, Enum):
    # This decision was made in the thl-core: pre-offerwall-entry view
    PRE_ENTRY = "pre_entry"
    # This decision made by grl-iq (synchronously)
    GRL_IQ = "grl_iq"
    # This decision made by ym-user-predict (asynchronously)
    YM_USER = "ym_user"


class AttemptDecision(str, Enum):
    # This attempt should be allowed to continue
    PASS = "pass"
    # This attempt is deemed fraudulent
    FAIL = "fail"


class GrlIqAttemptResult(BaseModel):
    """
    This model is used via Redis to communicate between GRL-IQ/YM and thl-core
    to set or update a real-time (or close to real-time) decision about if
    a session should be allowed to proceed.
    """

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    timestamp: AwareDatetimeISO = Field(
        description="When this decision was made",
        default_factory=lambda: datetime.now(tz=timezone.utc),
    )
    decider: Decider = Field(description="Where this decision was made")
    decision: AttemptDecision = Field(
        description="Whether an attempt should be allowed to continue, based on the evidence"
        "available to the decider at this point in time"
    )
    fraud_score: Optional[int] = Field(
        ge=0,
        le=100,
        description="Higher equals more likely to be fraudulent",
        default=None,
    )
    fingerprint: Optional[str] = Field(
        default=None,
        description="Fingerprint that should be unique to this particular device",
    )
