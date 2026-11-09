from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Literal, Self

from pydantic import BaseModel, Field, PositiveInt, model_validator

from generalresearch.models.custom_types import AwareDatetimeISO, UUIDStr
from generalresearch.models.definitions import MAX_INT32, Source
from generalresearch.models.thl.definitions import WallAdjustedStatus
from generalresearch.models.thl.user_identifiers import BPUIDStr
from generalresearch.utils.enum import ReprEnumMeta



class QualityEventType(StrEnum, metaclass=ReprEnumMeta):
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
    product_id: UUIDStr | None = Field(
        default=None, examples=["4fe381fb7186416cb443a38fa66c6557"]
    )
    bpuid: BPUIDStr | None = Field(default=None, examples=["app-user-9329ebd"])
    user_id: PositiveInt | None = Field(default=None, lt=MAX_INT32)


class AccessControlEventBody(BaseModel):
    events: list[AccessControlEvent] = Field(max_length=100, min_length=1)


class TaskAdjustmentRequest(BaseModel):
    """
    A marketplace-initiated task adjustment. Used in API request.
    This model has not yet passed any kind of validation or been applied.
    It may be a duplicate, it may not exist, it may be invalid.
    """
    quality_event_type: Literal[QualityEventType.task_adjustment] = Field(
        default=QualityEventType.task_adjustment
    )

    mid: UUIDStr = Field()
    source: Source = Field()
    status: WallAdjustedStatus = Field()
    alert_time: AwareDatetimeISO = Field(default_factory=lambda: datetime.now(tz=UTC))
    ext_status_code: str | None = Field(
        default=None,
        max_length=32,
        description="Optional, marketplace-specific status code. This goes in the thl_taskadjustment table.",
    )
    # Amount is required for WallAdjustedStatus.CPI_ADJUSTMENT and forbidden otherwise.
    amount: Decimal | None = Field(default=None, gt=0, le=Decimal(40))

    @model_validator(mode="after")
    def validate_amount(self) -> Self:
        if self.status == WallAdjustedStatus.CPI_ADJUSTMENT:
            if self.amount is None:
                raise ValueError("amount is required for a CPI adjustment")
        elif self.amount is not None:
            raise ValueError("amount is only allowed for a CPI adjustment")
        return self

