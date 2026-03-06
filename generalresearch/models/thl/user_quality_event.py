from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
from typing import List, Literal, Optional

from pydantic import BaseModel, Field, PositiveInt

from generalresearch.models import Source, MAX_INT32
from generalresearch.models.custom_types import UUIDStr, AwareDatetimeISO
from generalresearch.models.thl.definitions import WallAdjustedStatus
from generalresearch.models.thl.user import BPUIDStr
from generalresearch.utils.enum import ReprEnumMeta

"""
Typically used internally. These affect a user's quality standing.
"""


class QualityEventType(str, Enum, metaclass=ReprEnumMeta):
    """
    Currently, the grpc call SendUserQualityEvents handles both the
        recons/task adj, access control, and "security/hash failure" events.
        Splitting those up for the web api, even though in the backend, all
        3 might hit the same grpc call.
    """

    # Used to adjust a Wall's adjustment_status
    task_adjustment = "task_adjustment"

    # Manually adding a user to the whitelist
    add_to_whitelist = "add_to_whitelist"

    # Manually adding a user to the blacklist
    add_to_blacklist = "add_to_blacklist"

    # Clear any manual access control for a user
    clear_access_control_list = "clear_access_control_list"


class AccessControlEvent(BaseModel):
    quality_event_type: Literal[
        QualityEventType.add_to_whitelist,
        QualityEventType.add_to_blacklist,
        QualityEventType.clear_access_control_list,
    ] = Field()
    # One of user_id / (product_id, bpuid) is required.
    product_id: Optional[UUIDStr] = Field(
        default=None, examples=["4fe381fb7186416cb443a38fa66c6557"]
    )
    bpuid: Optional[BPUIDStr] = Field(default=None, examples=["app-user-9329ebd"])
    user_id: Optional[PositiveInt] = Field(default=None, lt=MAX_INT32)


class AccessControlEventBody(BaseModel):
    events: List[AccessControlEvent] = Field(max_length=100, min_length=1)


class TaskAdjustmentEvent(BaseModel):
    mid: UUIDStr = Field()
    source: Source = Field()
    status: WallAdjustedStatus = Field()
    alert_time: AwareDatetimeISO = Field(
        default_factory=lambda: datetime.now(tz=timezone.utc)
    )
    quality_event_type: Literal[QualityEventType.task_adjustment] = Field(
        default=QualityEventType.task_adjustment
    )

    # Only MID is needed to populate all the following, however we can pass them in order
    #   to perform validation. If any disagree, an error should be raised.
    survey_id: Optional[str] = Field(max_length=32, default=None)
    amount: Optional[Decimal] = Field(
        description="If negative, the status should adjusted to incomplete",
        default=None,
    )
    event_time: Optional[AwareDatetimeISO] = Field(
        description="This is when the original wall event was started",
        default=None,
    )
    product_id: Optional[UUIDStr] = Field(
        default=None, examples=["4fe381fb7186416cb443a38fa66c6557"]
    )
    bpuid: Optional[BPUIDStr] = Field(default=None, examples=["app-user-9329ebd"])
    user_id: Optional[PositiveInt] = Field(default=None, lt=MAX_INT32)


class TaskAdjustmentEventBody(BaseModel):
    events: List[TaskAdjustmentEvent] = Field(max_length=100, min_length=1)
