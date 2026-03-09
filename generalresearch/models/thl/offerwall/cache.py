from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from generalresearch.models import Source
from generalresearch.models.custom_types import AwareDatetimeISO, UUIDStr
from generalresearch.models.thl.offerwall import OfferWallRequest
from generalresearch.models.thl.offerwall.base import (
    OfferwallBase,
    ScoredTaskResult,
    TaskResult,
)


class GetOfferWallCache(BaseModel):
    """
    This object gets cached by thl-grpc when an offerwall request is made. If/when
    the user enters a bucket, this object is read in thl-core. It is also
    used if the offerwall needs to be "refreshed".
    """

    request: OfferWallRequest = Field()
    request_id: str = Field()
    offerwall: OfferwallBase = Field()
    all_sids: List[str] = Field()
    timestamp: AwareDatetimeISO = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    latest_ip_info: Dict[str, Any] = Field(
        description="So we can easily check if user's IP info has changed"
    )
    profiling_task: Optional[TaskResult] = Field(
        description="Profiling task", default=None
    )
    is_avg_offerwall: bool = Field()

    # These only get set once a bucket is clicked.
    clicked_timestamp: Optional[AwareDatetimeISO] = Field(default=None)
    clicked_bucket: Optional[UUIDStr] = Field(default=None)


class SessionInfoCache(BaseModel):
    """
    This is used within thl-core to manage a session
    """

    # This starts out as just the tasks within the clicked bucket, but
    #   will get pruned as tasks are attempted
    tasks: List[ScoredTaskResult] = Field()

    started: AwareDatetimeISO = Field(
        default_factory=lambda: datetime.now(tz=timezone.utc)
    )

    # The count of attempts per marketplace
    mp_retry_count: Dict[Source, int] = Field(default_factory=dict)

    hard_retry_count: int = Field(default=0)
