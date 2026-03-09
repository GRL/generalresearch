from datetime import datetime
from typing import Annotated, Any, Dict, List, Literal, Optional

from pydantic import (
    BaseModel,
    Field,
    NonNegativeInt,
    computed_field,
    field_serializer,
    field_validator,
    model_validator,
)
from typing_extensions import Self

from generalresearch.models.custom_types import (
    AwareDatetimeISO,
    EnumNameSerializer,
    UUIDStr,
)
from generalresearch.models.thl import decimal_to_int_cents
from generalresearch.models.thl.definitions import (
    SessionAdjustedStatus,
    SessionStatusCode2,
    Status,
    StatusCode1,
)
from generalresearch.models.thl.pagination import Page
from generalresearch.models.thl.payout_format import (
    PayoutFormatOptionalField,
    PayoutFormatType,
)
from generalresearch.models.thl.product import (
    PayoutTransformation,
    Product,
)
from generalresearch.models.thl.session import Session, WallOut

# API uses the ints, b/c this is what the grpc returned originally ...
STATUS_MAP = {
    None: 1,  # generalresearch_pb2.STATUS_ENTER
    Status.ABANDON: 2,  # generalresearch_pb2.STATUS_INCOMPLETE
    Status.TIMEOUT: 2,  # generalresearch_pb2.STATUS_INCOMPLETE
    Status.FAIL: 2,  # generalresearch_pb2.STATUS_INCOMPLETE
    Status.COMPLETE: 3,  # generalresearch_pb2.STATUS_COMPLETE
}
REVERSE_STATUS_MAP = {v: k for k, v in STATUS_MAP.items()}


class TaskStatusResponse(BaseModel):
    """The status of a session"""

    tsid: UUIDStr = Field(
        description="A unique identifier for the session",
        examples=["a3848e0a53d64f68a74ced5f61b6eb68"],
    )

    product_id: UUIDStr = Field(
        description="The BP ID of the associated respondent",
        examples=["1188cb21cb6741d79f614f6d02e9bc2a"],
    )

    product_user_id: str = Field(
        min_length=3,
        max_length=128,
        description="A unique identifier for each user, which is set by the "
        "Supplier",
        examples=["app-user-9329ebd"],
    )

    started: AwareDatetimeISO = Field(description="When the session was started")

    finished: Optional[AwareDatetimeISO] = Field(
        default=None, description="When the session was finished"
    )

    # This uses the grpc's Status enum. It gets serialized to an int.
    status: Optional[Status] = Field(
        default=None,
        examples=[3],
        description="The outcome of a session."
        "\n  - 0 - UNKNOWN\n"
        "  - 1 - ENTER (the user has not yet returned)\n"
        "  - 2 - INCOMPLETE (the user failed)\n"
        "  - 3 - COMPLETE (the user completed the task)",
    )

    payout: Optional[NonNegativeInt] = Field(
        default=None,
        lt=100_000,
        examples=[500],
        description="The amount paid to the supplier, in integer USD cents",
    )

    user_payout: Optional[NonNegativeInt] = Field(
        default=None,
        lt=100_000,
        description="If a payout transformation is configured on this account, "
        "this is the amount the user should earn, in integer USD cents",
        examples=[337],
    )

    payout_format: Optional[PayoutFormatType] = PayoutFormatOptionalField

    user_payout_string: Optional[str] = Field(
        default=None,
        description="If a payout transformation is configured on this account, "
        "this is the amount to display to the user",
        examples=["3370 Points"],
    )

    kwargs: Dict[str, str] = Field(
        default_factory=dict,
        description="Any extra url params used in the offerwall request will be "
        "passed back here",
    )

    status_code_1: Optional[Annotated[StatusCode1, EnumNameSerializer]] = Field(
        default=None,
        examples=[StatusCode1.COMPLETE.name],
        description=StatusCode1.as_openapi_with_value_descriptions_name(),
    )

    status_code_2: Optional[Annotated[SessionStatusCode2, EnumNameSerializer]] = Field(
        default=None,
        examples=[None],
        description=SessionStatusCode2.as_openapi_with_value_descriptions_name(),
    )

    adjusted_status: Optional[SessionAdjustedStatus] = Field(
        default=None,
        description=SessionAdjustedStatus.as_openapi_with_value_descriptions(),
        examples=[None],
    )

    adjusted_timestamp: Optional[AwareDatetimeISO] = Field(
        default=None,
        description="When the adjusted status was last set.",
        examples=[None],
    )

    adjusted_payout: Optional[NonNegativeInt] = Field(
        default=None,
        lt=100_000,
        description="The new payout after adjustment.",
        examples=[None],
    )

    adjusted_user_payout: Optional[NonNegativeInt] = Field(
        default=None,
        lt=100_000,
        description="The new user_payout after adjustment.",
        examples=[None],
    )

    adjusted_user_payout_string: Optional[str] = Field(
        default=None,
        description="The new user_payout_string after adjustment.",
        examples=[None],
    )

    # This is used for validation purposes only. It won't get serialized
    payout_transformation: Optional[PayoutTransformation] = Field(
        default=None, exclude=True
    )

    wall_events: Optional[List[WallOut]] = Field(default=None)

    currency: Literal["USD"] = Field(default="USD")
    final_status: int = Field(default=0, description="This is deprecated")

    # Serialize enum → int
    @field_serializer("status", return_type=int)
    def serialize_status(self, v: Optional[Status], _info):
        return STATUS_MAP[v]

    # Accept int OR string for input, but internally store a Status enum
    @field_validator("status", mode="before")
    def deserialize_status(cls, v):
        # int → enum
        if isinstance(v, int):
            return REVERSE_STATUS_MAP[v]

        if isinstance(v, str):
            return Status(v)

        return v

    @model_validator(mode="before")
    @classmethod
    def user_payout_none(cls, data: Any):
        # We changed the behaviour of user_payout at some point so that if the
        #   user_transformation is None, the user_payout is None, but this is
        #   not reflected in mysql. Change that here.
        if "payout_transformation" in data and data["payout_transformation"] is None:
            data["user_payout"] = None
            data["adjusted_user_payout"] = None
            data["user_payout_string"] = None
            data["adjusted_user_payout_string"] = None
        return data

    @field_validator("status_code_1", mode="before")
    def transform_enum_name(cls, v: str | int) -> int:
        # If we are serializing+deserializing this model (i.e. when we cache
        # it), this fails because we've replaced the enum value with the
        # name. Put it back here ...
        if isinstance(v, str):
            return StatusCode1[v]
        return v

    @field_validator("status_code_2", mode="before")
    def transform_enum_name2(cls, v: str | int) -> int:
        # If we are serializing+deserializing this model (i.e. when we cache
        # it), this fails because we've replaced the enum value with the
        # name. But it back here ...

        if isinstance(v, str):
            return SessionStatusCode2[v]

        return v

    @field_validator("payout", mode="before")
    def transform_payout(cls, v: Optional[NonNegativeInt]) -> NonNegativeInt:
        return v or 0

    @field_validator("kwargs", mode="after")
    def sanitize_kwargs(cls, v: Optional[Dict]) -> Optional[Dict]:
        if v and "clicked_timestamp" in v:
            try:
                clicked_timestamp = datetime.strptime(
                    v["clicked_timestamp"], "%Y-%m-%d %H:%M:%S.%f"
                )
                v["clicked_timestamp"] = (
                    clicked_timestamp.isoformat(timespec="microseconds") + "Z"
                )
            except ValueError:
                pass
        return v

    @model_validator(mode="before")
    def transform_user_payout(cls, d):
        # If the user_payout is None and there is a payout_format, make the user_payout 0
        if d.get("user_payout") is None and d.get("payout_format"):
            d["user_payout"] = 0
        return d

    # --- Properties ---
    @computed_field(return_type=str)
    @property
    def bpuid(self) -> str:
        return self.product_user_id

    @classmethod
    def from_session(cls, session: Session, product: Product) -> Self:

        user_payout_string = None
        if session.user_payout is not None:
            user_payout_string = product.format_payout_format(session.user_payout)

        adjusted_user_payout_string = None
        if session.adjusted_user_payout is not None:
            adjusted_user_payout_string = product.format_payout_format(
                session.adjusted_user_payout
            )

        return TaskStatusResponse(
            tsid=session.uuid,
            status=session.status,
            started=session.started,
            finished=session.finished,
            payout=decimal_to_int_cents(session.payout),
            user_payout=decimal_to_int_cents(session.user_payout),
            payout_format=product.payout_config.payout_format,
            user_payout_string=user_payout_string,
            product_id=session.user.product_id,
            product_user_id=session.user.product_user_id,
            kwargs=session.url_metadata or dict(),
            status_code_1=session.status_code_1,
            status_code_2=session.status_code_2,
            adjusted_status=session.adjusted_status,
            adjusted_payout=decimal_to_int_cents(session.adjusted_payout),
            adjusted_user_payout=decimal_to_int_cents(session.adjusted_user_payout),
            adjusted_timestamp=session.adjusted_timestamp,
            adjusted_user_payout_string=adjusted_user_payout_string,
            payout_transformation=product.payout_config.payout_transformation,
            wall_events=[
                WallOut.from_wall(w, product=product) for w in session.wall_events
            ],
        )


class TasksStatusResponse(Page):
    tasks_status: List[TaskStatusResponse] = Field(default_factory=list)
