from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, PositiveInt, model_validator

from generalresearch.models import MAX_INT32, Source
from generalresearch.models.custom_types import AwareDatetimeISO, UUIDStr
from generalresearch.models.thl.definitions import (
    WallAdjustedStatus,
)


class TaskAdjustmentEvent(BaseModel):
    """
    This represents a notification that we've received from a marketplace
    about the adjustment of a Wall's status. We might have multiple events
    for the same wall event. The Wall.adjusted_status stores the latest
    status, while the thl_taskadjustment table stores each time a
    change occurred.
    """

    model_config = ConfigDict(validate_assignment=True, extra="forbid")

    uuid: UUIDStr = Field(default_factory=lambda: uuid4().hex)
    created: AwareDatetimeISO = Field(
        default_factory=lambda: datetime.now(tz=timezone.utc),
        description="When this event was created in the db",
    )
    alerted: AwareDatetimeISO = Field(
        default_factory=lambda: datetime.now(tz=timezone.utc),
        description="When we were notified about this change",
    )

    """
    Please read carefully. These 3 are to be interpreted differently than how
        they are used on a Wall/Session.adjusted_status or .adjusted_cpi.

    Scenario:
        - Wall was originally a fail, and then it is adjusted to complete,
        and then adjusted back to fail.
        - We'll have two  TaskAdjustmentEvents, one with adjusted_status 
        ADJUSTED_TO_COMPLETE and one with ADJUSTED_TO_FAIL.
        - The Wall's adjusted_status will be NULL! (b/c it was adjusted back 
        to what it originally was)

    In other words, the TaskAdjustmentEvent's adjustment records the direction 
    of the adjustment, regardless of the current state of the Wall, whereas 
    the Wall.adjusted_status is the latest value.
    """

    adjusted_status: WallAdjustedStatus = Field()
    # If WallAdjustedStatus == ac, amount is positive, af amount is negative
    # Same thing as with adjusted_status, the amount is the "amount the cpi
    # is changing by"!!

    amount: Optional[Decimal] = Field(lt=1000, ge=-1000, default=None)
    ext_status_code: Optional[str] = Field(default=None, max_length=32)

    wall_uuid: UUIDStr = Field(description="The wall event being adjusted")

    # These 4 are just for convenience (repeated from the Wall/Session)
    user_id: PositiveInt = Field(
        lt=MAX_INT32, description="The user who did this wall event"
    )
    started: AwareDatetimeISO = Field(
        description="The wall event's started",
    )
    source: Source = Field()
    survey_id: str = Field(max_length=32)

    @model_validator(mode="after")
    def validate_amount(self):
        if self.adjusted_status == WallAdjustedStatus.ADJUSTED_TO_FAIL:
            assert self.amount < 0, (
                "The amount is the amount the cpi is changing by, so for a adj to fail,"
                "the amount should be negative"
            )
        elif self.adjusted_status == WallAdjustedStatus.ADJUSTED_TO_COMPLETE:
            assert self.amount > 0, (
                "The amount is the amount the cpi is changing by, so for a adj to complete,"
                "the amount should be positive"
            )
        elif self.adjusted_status == WallAdjustedStatus.CONFIRMED_COMPLETE:
            assert self.amount is None, "cannot change the cpi for a confirmed complete"
        elif self.adjusted_status == WallAdjustedStatus.CPI_ADJUSTMENT:
            assert self.amount is not None
        return self
