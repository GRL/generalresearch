from datetime import datetime, timedelta, timezone
from enum import StrEnum
from typing import Dict, Literal, Optional, Union
from uuid import uuid4

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    NonNegativeInt,
    PositiveFloat,
    TypeAdapter,
    model_validator,
)
from typing_extensions import Annotated

from generalresearch.models import Source
from generalresearch.models.custom_types import (
    AwareDatetimeISO,
    CountryISOLike,
    UUIDStr,
)
from generalresearch.models.thl.definitions import (
    SessionStatusCode2,
    Status,
    StatusCode1,
    WallStatusCode2,
)


class MessageKind(StrEnum):
    # An event, with type EventType (enum below)
    EVENT = "event"
    # A message containing bulk/aggregated stats
    STATS = "stats"
    # Heartbeats
    PING = "ping"
    PONG = "pong"
    # Must be the first message sent from client
    SUBSCRIBE = "subscribe"


class EventType(StrEnum):
    # Task Lifecycle
    #   (enter/finish could also be called start/end)
    TASK_ENTER = "task.enter"
    TASK_FINISH = "task.finish"
    # Session Lifecycle
    SESSION_ENTER = "session.enter"
    SESSION_FINISH = "session.finish"
    # Wallet / payments
    WALLET_CREDIT = "wallet.credit"
    WALLET_DEBIT = "wallet.debit"

    # User
    USER_CREATED = "user.created"  # A user we've never seen before
    USER_ACTIVE = "user.active"
    # USER_AUDIT = "user.audit"  # Something happened with this user


class TaskEnterPayload(BaseModel):
    event_type: Literal[EventType.TASK_ENTER] = EventType.TASK_ENTER

    source: Source = Field()
    survey_id: str = Field(min_length=1, max_length=32, examples=["127492892"])
    quota_id: Optional[str] = Field(
        default=None,
        max_length=32,
        description="The marketplace's internal quota id",
    )
    country_iso: CountryISOLike = Field()


class TaskFinishPayload(TaskEnterPayload):
    event_type: Literal[EventType.TASK_FINISH] = EventType.TASK_FINISH

    duration_sec: PositiveFloat = Field()
    status: Status
    status_code_1: Optional[StatusCode1] = None
    status_code_2: Optional[WallStatusCode2] = None
    cpi: Optional[NonNegativeInt] = Field(le=4000, default=None)


class SessionEnterPayload(BaseModel):
    event_type: Literal[EventType.SESSION_ENTER] = EventType.SESSION_ENTER
    country_iso: CountryISOLike = Field()


class SessionFinishPayload(SessionEnterPayload):
    event_type: Literal[EventType.SESSION_FINISH] = EventType.SESSION_FINISH

    duration_sec: PositiveFloat = Field()
    status: Status
    status_code_1: Optional[StatusCode1] = None
    status_code_2: Optional[SessionStatusCode2] = None
    user_payout: Optional[NonNegativeInt] = Field(default=None, le=4000, ge=0)


EventPayload = Annotated[
    Union[
        TaskEnterPayload,
        TaskFinishPayload,
        SessionEnterPayload,
        SessionFinishPayload,
    ],
    Field(discriminator="event_type"),
]


class EventEnvelope(BaseModel):
    event_uuid: UUIDStr = Field(default_factory=lambda: uuid4().hex)
    event_type: EventType = Field()
    timestamp: AwareDatetimeISO = Field(
        default_factory=lambda: datetime.now(tz=timezone.utc)
    )
    version: int = 1

    product_user_id: Optional[str] = Field(
        min_length=3,
        max_length=128,
        examples=["app-user-9329ebd"],
        description="A unique identifier for each user. This is hidden unless"
        "the event is for the requesting user.",
        default=None,
    )
    product_id: UUIDStr = Field(examples=["4fe381fb7186416cb443a38fa66c6557"])

    payload: EventPayload

    @model_validator(mode="after")
    def event_type_matches_payload(self):
        if self.event_type != self.payload.event_type:
            raise ValueError("event_type must match payload.event_type")
        return self


class AggregateBySource(BaseModel):
    total: NonNegativeInt = Field(default=0)
    by_source: Dict[Source, NonNegativeInt] = Field(default_factory=dict)

    @model_validator(mode="after")
    def remove_zero(self):
        self.by_source = {k: v for k, v in self.by_source.items() if v}
        return self


class MaxGaugeBySource(BaseModel):
    value: Optional[NonNegativeInt] = Field(default=None)
    by_source: Dict[Source, NonNegativeInt] = Field(default_factory=dict)

    @model_validator(mode="after")
    def remove_zero(self):
        self.by_source = {k: v for k, v in self.by_source.items() if v}
        return self


class TaskStatsSnapshot(BaseModel):
    # Counts: Task related
    live_task_count: AggregateBySource = Field(default_factory=AggregateBySource)

    task_created_count_last_1h: AggregateBySource = Field(
        default_factory=AggregateBySource
    )
    task_created_count_last_24h: AggregateBySource = Field(
        default_factory=AggregateBySource
    )

    live_tasks_max_payout: MaxGaugeBySource = Field(
        description="In integer USDCents", default_factory=MaxGaugeBySource
    )


class StatsSnapshot(TaskStatsSnapshot):
    model_config = ConfigDict(ser_json_timedelta="float")

    # If this is set, then everything is scoped to this country.
    country_iso: Optional[CountryISOLike] = Field(default=None)

    timestamp: AwareDatetimeISO = Field(
        default_factory=lambda: datetime.now(tz=timezone.utc)
    )

    # Counts: User related
    active_users_last_1h: NonNegativeInt = Field(
        description="""Count of users (in this product_id) that were active in the past 60 minutes.
        Behaviors that trigger active:
        - Request an offerwall
        - Enter an offerwall bucket
        - Request profiling questions
        - Submit profiling answers
        - Update user profile
        """,
        default=0,
    )
    active_users_last_24h: NonNegativeInt = Field(
        description="Count of users (in this product_id) that were active in the past 24 hours.",
        default=0,
    )
    # decrements upon either 90 min since enter, or upon finish.
    in_progress_users: NonNegativeInt = Field(
        description="Count of users that are currently doing work at this moment"
    )
    signups_last_24h: NonNegativeInt = Field(description="Count of users created")
    # Requires db for lookback. Skip for now
    # total_users: NonNegativeInt = Field(description="Total count of users over all time")

    # Counts: Activity related
    session_enters_last_1h: NonNegativeInt = Field()
    session_enters_last_24h: NonNegativeInt = Field()
    session_fails_last_1h: NonNegativeInt = Field()
    session_fails_last_24h: NonNegativeInt = Field()
    session_completes_last_1h: NonNegativeInt = Field()
    session_completes_last_24h: NonNegativeInt = Field()
    sum_payouts_last_1h: NonNegativeInt = Field(ge=0, description="In integer USDCents")
    sum_payouts_last_24h: NonNegativeInt = Field(
        ge=0, description="In integer USDCents"
    )

    # Rolling averages
    session_avg_payout_last_24h: Optional[NonNegativeInt] = Field(
        description="Average (actual) payout of all tasks completed in the past 24 hrs"
    )
    session_avg_user_payout_last_24h: Optional[NonNegativeInt] = Field(
        description="Average (actual) user payout of all tasks completed in the past 24 hrs"
    )

    session_fail_avg_loi_last_24h: Optional[timedelta] = Field(
        description="Average LOI of all tasks terminated in the past 24 hrs (excludes abandons)"
    )
    session_complete_avg_loi_last_24h: Optional[timedelta] = Field(
        description="Average LOI of all tasks completed in the past 24 hrs"
    )

    # # todo:
    # avg_user_earned_last_24h: Optional[NonNegativeFloat] = Field(
    #     ge=0,
    #     default=None,
    #     description="The average amount active users earned in total in the past 24 hrs",
    # )


# ----------------
# Top-level messages
# ----------------


class EventMessage(BaseModel):
    kind: Literal[MessageKind.EVENT] = Field(default=MessageKind.EVENT)
    timestamp: AwareDatetimeISO = Field(
        default_factory=lambda: datetime.now(tz=timezone.utc)
    )
    data: EventEnvelope


class StatsMessage(BaseModel):
    kind: Literal[MessageKind.STATS] = Field(default=MessageKind.STATS)
    timestamp: AwareDatetimeISO = Field(
        default_factory=lambda: datetime.now(tz=timezone.utc)
    )
    # The data/StatsSnapshot can optionally be scoped to a country
    country_iso: Optional[CountryISOLike] = Field(default=None)
    data: StatsSnapshot


class PingMessage(BaseModel):
    kind: Literal[MessageKind.PING] = Field(default=MessageKind.PING)
    timestamp: AwareDatetimeISO = Field(
        default_factory=lambda: datetime.now(tz=timezone.utc)
    )


class PongMessage(BaseModel):
    kind: Literal[MessageKind.PONG] = Field(default=MessageKind.PONG)
    timestamp: AwareDatetimeISO = Field(
        default_factory=lambda: datetime.now(tz=timezone.utc)
    )


class SubscribeMessage(BaseModel):
    kind: Literal[MessageKind.SUBSCRIBE] = Field(default=MessageKind.SUBSCRIBE)
    product_id: UUIDStr = Field(examples=["4fe381fb7186416cb443a38fa66c6557"])


ServerToClientMessage = Union[EventMessage, StatsMessage, PingMessage]
ServerToClientMessageField = Annotated[
    ServerToClientMessage,
    Field(discriminator="kind"),
]
ServerToClientMessageAdapter = TypeAdapter(ServerToClientMessageField)

ClientToServerMessage = Union[
    SubscribeMessage,
    PongMessage,
]
ClientToServerMessageField = Annotated[
    ClientToServerMessage,
    Field(discriminator="kind"),
]
ClientToServerMessageAdapter = TypeAdapter(ClientToServerMessageField)
