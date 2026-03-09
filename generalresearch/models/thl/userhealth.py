from datetime import datetime, timezone
from enum import Enum
from typing import Dict, Optional

from pydantic import BaseModel, Field, NonNegativeFloat, PositiveInt
from typing_extensions import Self

from generalresearch.models.custom_types import AwareDatetimeISO


class AuditLogLevel(int, Enum):
    CRITICAL = 50
    FATAL = CRITICAL
    ERROR = 40
    WARNING = 30
    WARN = WARNING
    INFO = 20
    DEBUG = 10
    NOTSET = 0


class AuditLog(BaseModel):
    """Table / Model for logging "actions" taken by a user or "events" that
    are related to a User
    """

    id: Optional[PositiveInt] = Field(default=None)
    user_id: PositiveInt = Field()

    created: AwareDatetimeISO = Field(
        default_factory=lambda: datetime.now(tz=timezone.utc),
        examples=[datetime.now(tz=timezone.utc)],
        description="When did this event occur",
    )

    level: AuditLogLevel = Field(
        description="The level of importance for this event. Works the same as "
        "python logging levels. It is an integer 0 - 50, and "
        "implementers of this field could map it to the predefined "
        "levels: (`CRITICAL`, `ERROR`, `WARNING`, `INFO`, `DEBUG`)."
        "This is NOT the same concept as the 'strength' of whatever "
        "event happened; it is just for sorting, filtering and "
        "display purposes. For e.g. multiple level 20 events != the "
        "'importance' of one level 40 event.",
        examples=[AuditLogLevel.WARNING],
    )

    # The "class" or "type" or event that happened.
    # e.g. "upk-audit", "ip-audit", "entrance-limit"
    event_type: str = Field(max_length=64, examples=["entrance-limit"])

    event_msg: Optional[str] = Field(
        default=None,
        min_length=3,
        max_length=256,
        description="The event message. Could be displayed on user's page",
    )

    event_value: Optional[NonNegativeFloat] = Field(
        default=None,
        description="Optionally store a numeric value associated with this "
        "event. For e.g. if we recalculate the user's normalized "
        "recon rate, and it is 'high', we could store an event like "
        "(event_type='recon-rate', event_msg='higher than allowed "
        "recon rate' event_value=0.42)",
        examples=[0.42],
    )

    def model_dump_mysql(self, **kwargs) -> Dict:
        d = self.model_dump(mode="json", **kwargs)
        d["created"] = self.created.replace(tzinfo=None)
        return d

    @classmethod
    def from_mysql(cls, d: Dict) -> Self:
        d["created"] = d["created"].replace(tzinfo=timezone.utc)
        return AuditLog.model_validate(d)
