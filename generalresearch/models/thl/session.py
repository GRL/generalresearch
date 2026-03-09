import json
import logging
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import TYPE_CHECKING, Annotated, Any, Dict, List, Optional, Tuple, Union
from uuid import uuid4

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    computed_field,
    field_serializer,
    field_validator,
    model_validator,
)
from typing_extensions import Self

from generalresearch.models import DeviceType, Source
from generalresearch.models.custom_types import (
    AwareDatetimeISO,
    EnumNameSerializer,
    IPvAnyAddressStr,
    UUIDStr,
)
from generalresearch.models.legacy.bucket import Bucket
from generalresearch.models.thl import (
    Product,
    decimal_to_int_cents,
    int_cents_to_decimal,
)
from generalresearch.models.thl.definitions import (
    WALL_ALLOWED_STATUS_CODE_1_2,
    WALL_ALLOWED_STATUS_STATUS_CODE,
    ReportValue,
    SessionAdjustedStatus,
    SessionStatusCode2,
    Status,
    StatusCode1,
    WallAdjustedStatus,
    WallStatusCode2,
)
from generalresearch.models.thl.user import User

if TYPE_CHECKING:
    from generalresearch.managers.thl.ledger_manager.thl_ledger import (
        ThlLedgerManager,
    )

logger = logging.getLogger("Wall")


class WallBase(BaseModel):
    """
    TODO: We want to extend a new test that does more rigorous testing
        and usage of any Wall.user_id vs Session.user_id on manually
        setting of the attribute vs retrieving any results from the database
    """

    model_config = ConfigDict(
        extra="forbid", validate_assignment=True, ser_json_timedelta="float"
    )

    uuid: UUIDStr = Field(default_factory=lambda: uuid4().hex)
    source: Source
    buyer_id: Optional[str] = Field(default=None, max_length=32)
    req_survey_id: str = Field(max_length=32)
    req_cpi: Decimal = Field(decimal_places=5, lt=1000, ge=0)
    started: AwareDatetimeISO = Field(
        default_factory=lambda: datetime.now(tz=timezone.utc)
    )

    # These get set on creation, or updated when the wall event is finished. So
    #   they shouldn't really ever be NULL, but you don't have to pass them in
    #   on instantiation
    survey_id: Optional[str] = Field(max_length=32, default=None)
    cpi: Optional[Decimal] = Field(lt=1000, ge=0, default=None)

    # Gets set when a wall is "finished"
    finished: Optional[AwareDatetimeISO] = Field(default=None)
    status: Optional[Status] = None
    status_code_1: Optional[StatusCode1] = None
    status_code_2: Optional[WallStatusCode2] = None

    ext_status_code_1: Optional[str] = Field(default=None, max_length=32)
    ext_status_code_2: Optional[str] = Field(default=None, max_length=32)
    ext_status_code_3: Optional[str] = Field(default=None, max_length=32)

    report_value: Optional[ReportValue] = None
    report_notes: Optional[str] = Field(default=None, max_length=255)

    # This is the most recent reconciliation status of the wall event.
    # Possible values: 'ac' (adjusted to complete), 'af' (adj to fail)
    # If a wall gets adjusted and adjusted back to its original status, the
    # adjusted_status = None
    adjusted_status: Optional[WallAdjustedStatus] = None

    # This is not really used, it is only important if the requested CPI
    #   doesn't match the adjusted amount, which shouldn't happen as no
    #   marketplaces support partial reconciles, and because the req_cpi and
    #   cpi are set whether or not the survey completed.
    # - If adjusted_status = 'ac': adjusted_cpi is the amount paid (should
    #       equal the `cpi`)
    # - If adjusted_status = 'af': adjusted_cpi is 0.00
    adjusted_cpi: Optional[Decimal] = Field(default=None, lt=1000, ge=0)

    # This timestamp gets updated every time there is an adjustment. Even if
    # we flip-flop, this will be set (and adjusted_status will be None).
    adjusted_timestamp: Optional[AwareDatetimeISO] = Field(default=None)

    # --- Validation ---

    # noinspection PyNestedDecorators
    @field_validator("req_cpi", "cpi", "adjusted_cpi", mode="before")
    @classmethod
    def check_decimal_type(cls, v: Decimal) -> Decimal:
        # pydantic is unable to set strict=True, so we'll do that manually here
        if v is not None:
            assert type(v) == Decimal, f"Must pass a Decimal, not a {type(v)}"
        return v

    # noinspection PyNestedDecorators
    @field_validator("req_cpi", "cpi", "adjusted_cpi", mode="after")
    @classmethod
    def check_cpi_decimal_places(cls, v: Decimal) -> Decimal:
        if v is not None:
            assert (
                v.as_tuple().exponent >= -5
            ), "Must have 5 or fewer decimal places ('XXX.YYYYY')"
        return v

    @model_validator(mode="before")
    @classmethod
    def set_survey_id(cls, data: Any):
        # This gets called upon assignment also, so we can't do this. IDK how
        # to make it only run upon initialization ...
        #
        # if data.get('survey_id'):
        #     assert data.get('survey_id') == data['req_survey_id'], \
        #         "upon init, survey_id must equal req_survey_id"
        data["survey_id"] = (
            data["survey_id"] if data.get("survey_id") else data["req_survey_id"]
        )
        return data

    # noinspection PyNestedDecorators
    @field_validator("buyer_id", mode="before")
    @classmethod
    def set_buyer_id(cls, v: str) -> str:
        # Max limit of 32 char, but I don't think we should fail if not,
        # we'll just crop it
        if v is not None:
            v = v[:32]
        return v

    @model_validator(mode="after")
    def check_timestamps(self):
        assert self.started <= datetime.now(
            tz=timezone.utc
        ), "Started must not be in the future"
        if self.finished:
            assert self.finished > self.started, "Finished must be after started"
            assert self.finished - self.started <= timedelta(
                minutes=90
            ), "Maximum wall event time is 90 min"
        return self

    @model_validator(mode="after")
    def check_ext_statuses(self):
        if self.ext_status_code_3 is not None:
            assert (
                self.ext_status_code_1 is not None
            ), "Set ext_status_code_1 before ext_status_code_3"
            assert (
                self.ext_status_code_2 is not None
            ), "Set ext_status_code_2 before ext_status_code_3"
        if self.ext_status_code_2 is not None:
            assert (
                self.ext_status_code_1 is not None
            ), "Set ext_status_code_1 before ext_status_code_2"
        return self

    @model_validator(mode="after")
    def check_status(self):
        if self.status in {Status.COMPLETE, Status.FAIL}:
            assert self.finished is not None, "finished should be set"
        if self.status == Status.COMPLETE:
            assert (
                self.status_code_1 == StatusCode1.COMPLETE
            ), "status_code_1 should be COMPLETE"
        return self

    @model_validator(mode="after")
    def check_status_status_code_agreement(self) -> Self:
        if self.status_code_1:
            options = WALL_ALLOWED_STATUS_STATUS_CODE.get(self.status, {})
            assert (
                self.status_code_1 in options
            ), f"If status is {self.status.value}, status_code_1 should be in {options}"
        return self

    @model_validator(mode="after")
    def check_status_code1_2_agreement(self) -> Self:
        if self.status_code_2:
            options = WALL_ALLOWED_STATUS_CODE_1_2.get(self.status_code_1, {})
            assert (
                self.status_code_2 in options
            ), f"If status_code_1 is {self.status_code_1.value}, status_code_2 should be in {options}"
        return self

    # --- Methods ---

    @classmethod
    def from_json(cls, s: str) -> Self:
        d = json.loads(s)
        d["req_cpi"] = Decimal(d["req_cpi"])
        d["cpi"] = Decimal(d["cpi"]) if d.get("cpi") is not None else None
        d["adjusted_cpi"] = (
            Decimal(d["adjusted_cpi"]) if d.get("adjusted_cpi") is not None else None
        )
        return cls.model_validate(d)

    def is_visible(self) -> bool:
        # I don't know what to call this. It's just checking if source != 'g',
        #   but it could be changed.  We need this to determine if a complete
        #   on this wall event could make the session complete
        return self.source != "g"

    def is_visible_complete(self) -> bool:
        # I think we could also instead of checking source != 'g', we could
        # check if `payout` is not NULL. This would basically have the same
        # effect.
        #
        # return self.status == Status.COMPLETE and self.payout is not None
        # and self.payout > 0
        return self.is_visible() and self.status == Status.COMPLETE

    def allow_session(self) -> bool:
        if self.status == Status.COMPLETE:
            return False

        return True

    def update(self, **kwargs) -> None:
        """
        We might have to update multiple fields at once, or else we'll get
        validation errors. There doesn't seem to be a clean way of doing this..

        We need to be careful to not ignore a validation here b/c the
        assignment will take either way.

        We shouldn't use this if the same object is being handled by multiple
        threads. But I don't envision that happening.

        https://stackoverflow.com/questions/73718577/updating-multiple-pydantic-fields-that-are-validated-together
        """
        self.model_config["validate_assignment"] = False
        for k, v in kwargs.items():
            setattr(self, k, v)
        self.model_config["validate_assignment"] = True
        self.__class__.model_validate(self)

        return None

    def finish(
        self,
        status: Status,
        status_code_1: StatusCode1,
        status_code_2: Optional[WallStatusCode2] = None,
        finished: Optional[datetime] = None,
        ext_status_code_1: Optional[str] = None,
        ext_status_code_2: Optional[str] = None,
        ext_status_code_3: Optional[str] = None,
        survey_id: Optional[str] = None,
        cpi: Optional[Decimal] = None,
    ) -> None:

        # This is just used in tests at the moment. This needs to be adjusted.
        if finished is None:
            finished = datetime.now(tz=timezone.utc)

        self.update(
            status=status,
            status_code_1=status_code_1,
            status_code_2=status_code_2,
            finished=finished,
            ext_status_code_1=ext_status_code_1,
            ext_status_code_2=ext_status_code_2,
            ext_status_code_3=ext_status_code_3,
        )

        if survey_id is not None:
            self.survey_id = survey_id

        if cpi is not None:
            self.cpi = cpi

        return None

    def annotate_status_codes(
        self,
        ext_status_code_1: str,
        ext_status_code_2: Optional[str] = None,
        ext_status_code_3: Optional[str] = None,
        finished: Optional[datetime] = None,
    ) -> None:
        # This should be called by the wall manager in order to actually update db
        from generalresearch import wall_status_codes

        status, status_code_1, status_code_2 = wall_status_codes.annotate_status_code(
            self.source,
            ext_status_code_1,
            ext_status_code_2,
            ext_status_code_3,
        )
        if finished is None:
            finished = datetime.now(tz=timezone.utc)
        self.update(
            status=status,
            status_code_1=status_code_1,
            status_code_2=status_code_2,
            ext_status_code_1=ext_status_code_1,
            ext_status_code_2=ext_status_code_2,
            ext_status_code_3=ext_status_code_3,
            finished=finished,
        )

        return None

    def is_soft_fail(self) -> bool:
        from generalresearch import wall_status_codes

        assert self.status is not None, "status should not be None"
        assert self.status_code_1 is not None, "status_code_1 should not be None"
        return wall_status_codes.is_soft_fail(self)

    def stop_marketplace_session(self) -> bool:
        from generalresearch import wall_status_codes

        assert self.status is not None, "status should not be None"
        assert self.status_code_1 is not None, "status_code_1 should not be None"
        return wall_status_codes.stop_marketplace_session(self)

    def get_status_after_adjustment(self) -> Status:
        if self.adjusted_status in {
            WallAdjustedStatus.ADJUSTED_TO_COMPLETE,
            WallAdjustedStatus.CPI_ADJUSTMENT,
        }:
            return Status.COMPLETE
        elif self.adjusted_status == WallAdjustedStatus.ADJUSTED_TO_FAIL:
            return Status.FAIL
        elif self.status == Status.COMPLETE:
            return Status.COMPLETE
        else:
            return Status.FAIL

    def get_cpi_after_adjustment(self) -> Decimal:
        if self.adjusted_status in {
            WallAdjustedStatus.ADJUSTED_TO_COMPLETE,
            WallAdjustedStatus.CPI_ADJUSTMENT,
        }:
            return self.adjusted_cpi
        elif self.adjusted_status == WallAdjustedStatus.ADJUSTED_TO_FAIL:
            return Decimal(0)
        elif self.status == Status.COMPLETE:
            return self.cpi
        else:
            return Decimal(0)

    def report(
        self,
        report_value: ReportValue,
        report_notes: Optional[str] = None,
        report_timestamp: Optional[AwareDatetime] = None,
    ) -> None:
        """When a wall event is reported:

        - IF wall event already has a status:
            we only set the report_value and don't touch any timestamps
        - ELSE (the wall event currently has no status):
            we also set the finished timestamp and set the status to ABANDON

        Only 1 report is allowed. If this is called multiple times, the
            report_value gets updated each time.
        The report_timestamp shouldn't be used in practice. It is only used
            to backfill from vendor_wall (where the report_timestamp is the
            vw.finished for reported events)

        TODO: Transition this over to use the ReportTask pydantic model.
        """
        report_timestamp = (
            report_timestamp if report_timestamp else datetime.now(tz=timezone.utc)
        )
        if self.status is None and self.finished is None:
            self.status = Status.ABANDON
            self.finished = report_timestamp
        self.report_value = report_value
        self.report_notes = report_notes


class Wall(WallBase):
    # Avoiding using the Session model here because of cyclic dependency issues.
    #   We just store the session id (integer) which we can use to look up a
    #   session. The Session model is the one who has the actual reference to a
    #   list of Wall models.
    session_id: int

    # This is in the Session, but for convenience, add it here too, but just
    #   the user ID. Any user related operations should be done through the
    #   Session
    user_id: int

    @model_validator(mode="before")
    @classmethod
    def set_cpi(cls, data: Any):
        # if data.get('cpi'):
        #     assert data.get('cpi') == data['req_cpi'], \
        #         "upon init, cpi must equal req_cpi"
        data["cpi"] = data["cpi"] if data.get("cpi") else data["req_cpi"]
        return data

    @model_validator(mode="after")
    def check_adjusted_null(self) -> Self:
        if self.adjusted_status is not None or self.adjusted_cpi is not None:
            assert (
                self.adjusted_cpi is not None
            ), "Set adjusted_cpi if the wall has been adjusted"
            assert (
                self.adjusted_status is not None
            ), "Set adjusted_status if the wall has been adjusted"
            assert (
                self.adjusted_timestamp is not None
            ), "Set adjusted_timestamp if the wall has been adjusted"
        return self

    @model_validator(mode="after")
    def check_adjusted_status_consistent(self) -> Self:
        check_adjusted_status_consistent(
            self.status, self.cpi, self.adjusted_status, self.adjusted_cpi
        )
        return self

    # --- Properties ---

    @computed_field
    @property
    def elapsed(self) -> timedelta:
        return self.finished - self.started if self.finished else None

    def to_json(self) -> str:
        # We have to handle the computed_fields manually. I'm not sure if there is a better way
        #   to do this natively in pydantic...
        d = self.model_dump(mode="json", exclude={"elapsed"})
        return json.dumps(d)

    def model_dump_mysql(self, *args, **kwargs) -> Dict:
        # Generate a dictionary representation of the model, with special handling for datetimes
        d = self.model_dump(mode="json", exclude={"elapsed"}, *args, **kwargs)
        d["started"] = self.started.replace(tzinfo=None)
        if self.finished:
            d["finished"] = self.finished.replace(tzinfo=None)
        if self.adjusted_timestamp:
            d["adjusted_timestamp"] = self.adjusted_timestamp.replace(tzinfo=None)
        return d


class WallOut(WallBase):

    # These get serialized to the enum name instead of the int value (for ease in UI)
    status_code_1: Optional[Annotated[StatusCode1, EnumNameSerializer]] = Field(
        default=None,
        examples=[StatusCode1.COMPLETE.name],
        description=StatusCode1.as_openapi_with_value_descriptions_name(),
    )

    status_code_2: Optional[Annotated[WallStatusCode2, EnumNameSerializer]] = Field(
        default=None,
        examples=[None],
        description=WallStatusCode2.as_openapi_with_value_descriptions_name(),
    )

    # Exclude these 3 fields
    cpi: Optional[Decimal] = Field(lt=1000, ge=0, default=None, exclude=True)
    req_cpi: Optional[Decimal] = Field(
        decimal_places=5, lt=1000, ge=0, default=None, exclude=True
    )
    adjusted_cpi: Optional[Decimal] = Field(lt=1000, ge=0, default=None, exclude=True)

    # user_cpi is serialized to integer cents!!!
    user_cpi: Optional[Decimal] = Field(
        lt=1000,
        ge=0,
        default=None,
        description="""
    The amount the user would earn from completing this task, if the status was a complete.
    If the BP has no payout xform, the user_cpi is None. This is analogous to the session's
    user_payout.
    """,
        examples=[123],
    )

    user_cpi_string: Optional[str] = Field(
        default=None,
        description="If a payout transformation is configured on this account, "
        "this is the amount to display to the user",
        examples=["123 Points"],
    )

    # Serialize user_cpi to an int
    @field_serializer("user_cpi", return_type=int)
    def serialize_user_cpi(self, v: Decimal, _info):
        return decimal_to_int_cents(v)

    # If user_cpi is an int, put it back to a decimal
    @field_validator("user_cpi", mode="before")
    def deserialize_user_cpi(cls, v):
        if isinstance(v, int):
            return int_cents_to_decimal(v)
        return v

    # noinspection PyNestedDecorators
    @field_validator("user_cpi", mode="after")
    @classmethod
    def check_cpi_decimal_places(cls, v: Decimal) -> Decimal:
        if v is not None:
            assert (
                v.as_tuple().exponent >= -5
            ), "Must have 5 or fewer decimal places ('XXX.YYYYY')"
        return v

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
        # If we are serializing+deserializing this model (i.e. when we cache it), this fails because
        #   we've replaced the enum value with the name. But it back here ...
        if isinstance(v, str):
            return WallStatusCode2[v]
        return v

    @classmethod
    def from_wall(cls, wall: Wall, product: Product) -> Self:
        d = wall.model_dump(exclude={"session_id", "user_id"}, round_trip=True)
        d["user_cpi"] = None
        if product.payout_config.payout_transformation is not None:
            d["user_cpi"] = product.calculate_user_payment(
                product.determine_bp_payment(wall.cpi)
            )
            d["user_cpi_string"] = product.format_payout_format(d["user_cpi"])
        return cls.model_validate(d)


class WallAttempt(BaseModel):
    """
    - We use this to de-duplicate entrances into surveys (prevent
    sending the user in multiple times to the same survey).
    - This could be just a Wall model instead, but avoiding doing that
    because in this use case we only care about the "entrance", and
    are not tracking/updating/caring about status/status_codes/finished.
    - This is just a "minimal" Wall
    """

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    uuid: UUIDStr = Field()
    source: Source = Field()
    req_survey_id: str = Field(max_length=32)
    started: AwareDatetimeISO = Field()
    user_id: int = Field()

    @property
    def task_sid(self) -> str:
        return self.source.value + ":" + self.req_survey_id


class Session(BaseModel):
    model_config = ConfigDict(
        extra="forbid", validate_assignment=True, ser_json_timedelta="float"
    )

    # id will be None until db_create is called (or if this is instantiated
    #   from an existing session)
    id: Optional[int] = None
    uuid: UUIDStr = Field(default_factory=lambda: uuid4().hex)
    user: User
    started: AwareDatetimeISO = Field(
        default_factory=lambda: datetime.now(tz=timezone.utc)
    )

    # This is the "bucket" the user clicked on to start this session. We only
    # store the 4 fields: loi_min, loi_max, user_payout_min, user_payout_max
    # in the db, but there may be other metadata associated with the bucket
    # that is cached, such as the category.
    clicked_bucket: Optional[Bucket] = Field(default=None)

    country_iso: Optional[str] = Field(
        default=None, max_length=2, pattern=r"^[a-z]{2}$"
    )
    device_type: Optional[DeviceType] = Field(default=None)
    ip: Optional[IPvAnyAddressStr] = Field(default=None)

    url_metadata: Optional[Dict[str, str]] = Field(default=None)

    # Below here shouldn't be set upon initialization, or directly.
    wall_events: List[Wall] = Field(default_factory=list)

    # Gets set when a session is "finished"
    finished: Optional[AwareDatetimeISO] = Field(default=None)

    status: Optional[Status] = None
    status_code_1: Optional[StatusCode1] = None
    status_code_2: Optional[SessionStatusCode2] = None

    # There are two scenarios. Let's say the user payout transformation is
    #   40% and this session pays out $1.
    #
    #   a) user wallet enabled: The BP gets $0.60 & the user gets $0.40.
    #   b) user wallet disabled: The BP gets $1.00. We store what the
    #       user_payout should be ($0.40) only on this model, but it is not
    #       actually paid.
    #
    # This is potentially confusing b/c in case (b), the fields would be
    #   ($0.60) and ($0.40), but we paid $1 to the BP.
    #
    # To try to address this: The `payout` is the total amount that "we" are
    #   paying for this session. The `user_payout` "comes out" of the `payout`.
    #   So, in both cases (a) and (b), the payout is $1.00 and user_payout is
    #   $0.40. If the user wallet is enabled, we interpret this is as ($1-$0.40)
    #   going to the BP and ($0.40) to the user, and if the wallet is disabled,
    #   then the whole $1 goes to the BP and $0 to the user, but the $0.40 value
    #   is saved, so it can be displayed in the task status endpoint.
    payout: Optional[Decimal] = Field(default=None, lt=1000, ge=0)
    user_payout: Optional[Decimal] = Field(default=None, lt=1000, ge=0)

    # This is the most recent reconciliation status of the session. Generally,
    #   we would adjust this if the last survey in the session was adjusted
    #   from complete to incomplete. If any survey in the session was adjusted
    #   from fail -> complete (and the user didn't already get a complete)
    #   we'll adjust this to a complete.
    adjusted_status: Optional[SessionAdjustedStatus] = None

    # If adjusted_status = 'ac': payout = 0 and adjusted_payout is the amount paid
    # If adjusted_status = 'af': payout = the amount paid, adjusted_payout is 0.00
    #   (the `payout` never changed, only the adjusted_payout can change).
    adjusted_payout: Optional[Decimal] = Field(default=None, lt=1000, ge=0)
    adjusted_user_payout: Optional[Decimal] = Field(default=None, lt=1000, ge=0)

    # This timestamp gets updated every time there is an adjustment (even if
    #   there are flip-flops).
    adjusted_timestamp: Optional[AwareDatetimeISO] = Field(default=None)

    # --- Validation ---

    @field_validator(
        "payout",
        "user_payout",
        "adjusted_payout",
        "adjusted_user_payout",
        mode="before",
    )
    @classmethod
    def check_decimal_type(cls, v: Decimal) -> Decimal:
        # pydantic is unable to set strict=True, so we'll do that manually here
        if v is not None:
            assert type(v) == Decimal, f"Must pass a Decimal, not a {type(v)}"
        return v

    @field_validator(
        "payout",
        "user_payout",
        "adjusted_payout",
        "adjusted_user_payout",
        mode="after",
    )
    @classmethod
    def check_payout_decimal_places(cls, v: Decimal) -> Decimal:
        if v is not None:
            assert (
                v.as_tuple().exponent >= -2
            ), "Must have 2 or fewer decimal places ('XXX.YY')"
            # explicitly make sure it is 2 decimal places, after checking that it is already 2 or less.
            v = v.quantize(Decimal("0.00"))
        return v

    @model_validator(mode="after")
    def check_statuses(self):
        if self.status_code_1 is None:
            return self
        if self.status == Status.FAIL:
            assert self.status_code_1 in {
                StatusCode1.SESSION_START_FAIL,
                StatusCode1.SESSION_CONTINUE_FAIL,
                StatusCode1.SESSION_START_QUALITY_FAIL,
                StatusCode1.SESSION_CONTINUE_QUALITY_FAIL,
                StatusCode1.BUYER_FAIL,
                StatusCode1.BUYER_QUALITY_FAIL,
                StatusCode1.PS_OVERQUOTA,
                StatusCode1.PS_DUPLICATE,
                StatusCode1.PS_FAIL,
                StatusCode1.PS_QUALITY,
                StatusCode1.PS_BLOCKED,
            }, f"status_code_1 {self.status_code_1.name} invalid for status {self.status.value}"
        elif self.status in {Status.TIMEOUT, Status.ABANDON}:
            assert self.status_code_1 in {
                StatusCode1.PS_ABANDON,
                StatusCode1.GRS_ABANDON,
                StatusCode1.BUYER_ABANDON,
            }, f"status_code_1 {self.status_code_1.name} invalid for status {self.status.value}"
        elif self.status == Status.COMPLETE:
            assert (
                self.status_code_1 == StatusCode1.COMPLETE
            ), f"status_code_1 {self.status_code_1.name} invalid for status {self.status.value}"
        else:
            assert self.status_code_1 is None, (
                f"status_code_1 {self.status_code_1.name} invalid for status "
                f"{self.status.value}"
            )
        return self

    @model_validator(mode="after")
    def check_timestamps(self):
        if self.finished:
            assert self.finished > self.started, "finished is before started!"
        return self

    @model_validator(mode="after")
    def check_status_when_finished(self):
        if self.finished:
            assert self.status is not None, "once finished, we should have a status!"
        return self

    @model_validator(mode="after")
    def check_payout_when_complete(self):
        if self.status == Status.COMPLETE:
            assert (
                self.payout is not None
            ), "there should be a payout if the session is marked complete"
        return self

    # @model_validator(mode='after')
    # def check_payouts(self):
    #     if self.status == 'c':
    #         assert self.payout > 0
    #         if self.user_payout is not None:
    #             assert self.payout > self.user_payout
    #     else:
    #         assert self.payout is None
    #     return self

    @field_validator("wall_events")
    @classmethod
    def check_wall_events(cls, wall_events: List[Wall]):
        # Note: this can't work on modifications as pydantic/python doesn't
        #   know if a list is mutated. We have to run it manually, or hide
        #   the self.wall_events attr and wrap all access
        assert sorted(wall_events, key=lambda x: x.started) == wall_events, "sorted"
        assert len({w.uuid for w in wall_events}) == len(wall_events)
        return wall_events

    @model_validator(mode="after")
    def check_adjusted(self):
        if self.adjusted_status is not None or self.adjusted_payout is not None:
            assert (
                self.adjusted_payout is not None
            ), "Set adjusted_payout if the session has been adjusted"
            assert (
                self.adjusted_status is not None
            ), "Set adjusted_status if the session has been adjusted"
            assert (
                self.adjusted_timestamp is not None
            ), "Set adjusted_timestamp if the session has been adjusted"
        if self.adjusted_user_payout is not None:
            assert (
                self.adjusted_payout is not None
            ), "Set adjusted_payout if adjusted_user_payout is set"
            # NOTE: the other way around is NOT required!
            # (the adjusted_user_payout / user_payout can be null)
        return self

    @model_validator(mode="after")
    def check_adjusted_status(self):
        if self.adjusted_status == SessionAdjustedStatus.ADJUSTED_TO_COMPLETE:
            assert self.status != Status.COMPLETE, (
                "If a Session was originally completed, reversed, and then re-reversed to complete,"
                "the adjusted_status should be null"
            )
        if self.adjusted_status == SessionAdjustedStatus.ADJUSTED_TO_FAIL:
            assert (
                self.status == Status.COMPLETE
            ), "Session.status must be COMPLETE for the adjusted_status to be ADJUSTED_TO_FAIL"
        return self

    # --- Properties ---

    @property
    def user_id(self):
        return self.user.user_id

    @property
    def elapsed(self) -> timedelta:
        return self.finished - self.started if self.finished else None

    # --- Methods ---

    def update(self, **kwargs) -> None:
        """We might have to update multiple fields at once, or else we'll
        get validation errors. There doesn't seem to be a clean way of
        doing this ...

        We need to be careful to not ignore a validation here b/c the
        assignment will take either way.

        We shouldn't use this if the same object is being handled by
        multiple threads. But I don't envision that happening.

        https://stackoverflow.com/questions/73718577/updating-multiple-pydantic-fields-that-are-validated-together
        """
        self.model_config["validate_assignment"] = False
        for k, v in kwargs.items():
            setattr(self, k, v)
        self.model_config["validate_assignment"] = True
        self.__class__.model_validate(self)

    def model_dump_mysql(
        self, *args, **kwargs
    ) -> Dict[str, Union[str, int, datetime, float, None]]:

        # Generate a dictionary representation of the model, with special
        #   handling for datetimes, and nested models such as User & Bucket

        d = self.model_dump(mode="json", *args, **kwargs)
        d["started"] = self.started.replace(tzinfo=None)

        if self.finished:
            d["finished"] = self.finished.replace(tzinfo=None)

        if self.adjusted_timestamp:
            d["adjusted_timestamp"] = self.adjusted_timestamp.replace(tzinfo=None)

        d["url_metadata_json"] = json.dumps(d.pop("url_metadata", {}))
        clicked_bucket = d.pop("clicked_bucket") or {}
        d.update(
            {
                k: clicked_bucket.get(k)
                for k in [
                    "loi_min",
                    "loi_max",
                    "user_payout_min",
                    "user_payout_max",
                ]
            }
        )

        # pymysql will complain about various values being in the dictionary
        #   that gets used as the connection.execute(..., args=) so we want to
        #   explicit about what comes back. pymysql tries to escape everything
        #   even if it isn't used in the actual query
        d["user_id"] = self.user_id

        d.pop("user", None)
        d.pop("wall_events", None)

        return d

    def append_wall_event(self, w: Wall) -> None:
        wall_events = self.wall_events + [w]
        # the assignment causes check_wall_events to run
        self.wall_events = wall_events

    def finalize_timeout(self, task_timeout_seconds: int = 5400) -> None:
        """Would usually be called on a session that has no status, presumably
        by some task, when this session has timed out. Results in setting
        of status.

        On a session that already has a status, this does nothing.
        """
        # We need the BP's default "task timeout". Assuming this is 90 min
        last_wall = self.wall_events[-1]
        if (
            last_wall.status is None
            and self.status is None
            and datetime.now(tz=timezone.utc)
            > self.started + timedelta(seconds=task_timeout_seconds)
        ):
            last_wall.status = Status.TIMEOUT
            self.status = Status.TIMEOUT

    def determine_session_status(self) -> Tuple[Status, StatusCode1]:
        """Given a list of wall events, determine what the session status
        should be. If this is called, it is because the Session is *over*,
        or it has timed out.

        Note: this does not support multiple completes within a session.
        This should be configurable or else it is very confusing... If I
        get a complete and then abandon, the BP will see it as an abandon,
        but we see a complete. We should only mark it as a complete if the
        BP will get a POST or we know they poll the statuses endpoint.
        """
        # # If there are no wall events, it is a GRL Fail
        # if len(self.wall_events) == 0:
        #     self.status = Status.FAIL
        #     self.status_code_1 = StatusCode1.SESSION_START_FAIL
        #     return None

        # The last wall event, regardless of GRS or external. If it is an
        #   abandon, the session's status is abandon
        self.finalize_timeout()
        last_wall = self.wall_events[-1]
        assert last_wall.status is not None, "Session is still active!"

        if last_wall.status in {Status.ABANDON, Status.TIMEOUT}:
            status = last_wall.status
            if last_wall.is_visible():
                status_code_1 = StatusCode1.BUYER_ABANDON
            else:
                status_code_1 = StatusCode1.GRS_ABANDON
            return status, status_code_1

        # The last non-GRS wall event
        last_wall = self.get_last_visible_wall()

        # If there are only hidden wall events, it is a SESSION_CONTINUE_FAIL
        if last_wall is None:
            return Status.FAIL, StatusCode1.SESSION_CONTINUE_FAIL

        # Report the status of the last wall event
        elif last_wall.status == Status.COMPLETE:
            return Status.COMPLETE, StatusCode1.COMPLETE

        elif last_wall.status == Status.FAIL:
            status = Status.FAIL
            status_code_1s = {x.status_code_1 for x in self.wall_events}

            if StatusCode1.BUYER_FAIL in status_code_1s:
                status_code_1 = StatusCode1.BUYER_FAIL
            elif StatusCode1.BUYER_QUALITY_FAIL in status_code_1s:
                status_code_1 = StatusCode1.BUYER_QUALITY_FAIL
            else:
                status_code_1 = last_wall.status_code_1

            if status_code_1 == StatusCode1.UNKNOWN:
                status_code_1 = StatusCode1.BUYER_FAIL
            elif status_code_1 in {
                StatusCode1.MARKETPLACE_FAIL,
                StatusCode1.GRS_QUALITY_FAIL,
            }:
                status_code_1 = StatusCode1.SESSION_CONTINUE_QUALITY_FAIL
            elif status_code_1 == StatusCode1.GRS_FAIL:
                status_code_1 = StatusCode1.SESSION_CONTINUE_FAIL
            return status, status_code_1

        return Status.FAIL, StatusCode1.BUYER_FAIL

    def get_last_visible_wall(self):
        return next(
            iter(filter(lambda x: x.is_visible(), self.wall_events[::-1])), None
        )

    def should_end_session(
        self, max_session_len: timedelta, max_session_hard_retry: int
    ) -> bool:

        now = datetime.now(tz=timezone.utc)
        last_wall = self.get_last_visible_wall()

        if last_wall and last_wall.status == Status.COMPLETE:
            return True

        if (now - self.started) > max_session_len:
            return True

        hard_retry_count = sum(not wall.is_soft_fail() for wall in self.wall_events)
        if hard_retry_count >= max_session_hard_retry:
            return True

        # Hard limit of 40 wall events per session
        if len(self.wall_events) >= 40:
            return True

        return False

    def determine_payments(
        self,
        thl_ledger_manager: Optional["ThlLedgerManager"] = None,
    ) -> Tuple[Decimal, Decimal, Decimal, Optional[Decimal]]:
        # How much we should get paid by the MPs for all completes in this
        #   session (usually 0 or 1 completes)
        thl_net: Decimal = Decimal(
            sum(wall.cpi for wall in self.wall_events if wall.is_visible_complete())
        )

        product = self.user.product
        # Handle brokerage product payouts
        bp_pay: Decimal = product.determine_bp_payment(thl_net)
        commission_amount: Decimal = thl_net - bp_pay

        # Some payout transformations may want this:
        user_wallet_balance = None
        if (
            product.payout_config.payout_transformation is not None
            and product.payout_config.payout_transformation.f
            == "payout_transformation_amt"
        ):
            assert thl_ledger_manager is not None
            amt = thl_ledger_manager.get_user_wallet_balance(user=self.user)
            user_wallet_balance = Decimal(amt / 100).quantize(Decimal("0.01"))
        user_pay: Optional[Decimal] = product.calculate_user_payment(
            bp_pay, user_wallet_balance=user_wallet_balance
        )

        return thl_net, commission_amount, bp_pay, user_pay

    def get_thl_net(self) -> Decimal:
        assert self.wall_events, "populate wall_events!"
        assert self.user.product, "prefetch user.product!"
        walls = [w for w in self.wall_events if w.source != Source.GRS]
        completed_walls = [
            w for w in walls if w.get_status_after_adjustment() == Status.COMPLETE
        ]

        if completed_walls:
            return Decimal(sum([w.get_cpi_after_adjustment() for w in completed_walls]))

        else:
            return Decimal(0)

    def determine_new_status_and_payouts(
        self,
    ) -> Tuple[Status, Decimal, Optional[Decimal]]:
        """Session is adjusted any time one of the wall events is. Assuming
        status adjustments happened on a session's wall events. Calculate
        if any status changes are need to the session.

        - It is possible that complicated outcomes occur. Such as, e.g.
            originally [Fail, Fail, Complete ($2)], the complete gets
            reversed (Session Adj to fail $0), then the 2nd fail gets
            changed to complete --> [Fail, Complete ($1), Fail] (Session Adj
            to Complete $1). But since it was originally a complete, the
            final status is Payout Adjustment ($2 -> $1).

        - In summary, possible outcomes: orig complete -> adj to fail, orig
            fail -> adj to complete, or orig complete -> payout adj. And
            also adjustments being reverted back to normal (complete -> adj
            to fail -> complete), etc.

        returns: status, bp_payout, Optional[user_payout]
        """
        assert self.wall_events, "populate wall_events!"
        assert self.user.product, "prefetch user.product!"

        product = self.user.product
        thl_net = self.get_thl_net()

        if thl_net:
            adjusted_payout = product.determine_bp_payment(thl_net)
            adjusted_user_payout = product.calculate_user_payment(adjusted_payout)
            return Status.COMPLETE, adjusted_payout, adjusted_user_payout

        else:
            if product.payout_config.payout_transformation is None:
                adjusted_user_payout = None
            else:
                adjusted_user_payout = Decimal(0)

            return Status.FAIL, Decimal(0), adjusted_user_payout

    def adjust_status(self) -> bool:
        """A complete can go to an adj_fail, or a payout adjustment. It can
        then go back to a complete.

        A session that was orig a failure, can go to adj_complete. But it
        cannot go to payout_adj.
        """
        adjusted_timestamp = max(
            [x.adjusted_timestamp for x in self.wall_events if x.adjusted_timestamp],
            default=None,
        )

        new_status, new_payout, new_user_payout = (
            self.determine_new_status_and_payouts()
        )
        current_status = self.get_status_after_adjustment()
        current_payout = self.get_payout_after_adjustment()
        original_payout = self.payout

        if (current_status == Status.FAIL and new_status == Status.FAIL) or (
            current_status == Status.COMPLETE
            and new_status == Status.COMPLETE
            and new_payout == current_payout
        ):
            # If the session is originally a complete, or a fail adjusted to
            #   complete, and we want to change it to complete and the payout
            #   is the same, (or is fail, or adjusted to fail, and we want to
            #   change to fail): do nothing.
            logger.info(f"adjust_status: session {self.uuid} is already {new_status}")
            return False

        if self.status == Status.COMPLETE:
            assert (
                self.adjusted_status != SessionAdjustedStatus.ADJUSTED_TO_COMPLETE
            ), "Can't have complete adj to complete"
            if self.adjusted_status in {
                None,
                SessionAdjustedStatus.PAYOUT_ADJUSTMENT,
            }:
                if new_status == Status.COMPLETE:
                    if original_payout == new_payout:
                        self.update(
                            adjusted_status=None,
                            adjusted_payout=None,
                            adjusted_user_payout=None,
                            adjusted_timestamp=adjusted_timestamp,
                        )

                    elif self.get_payout_after_adjustment() != new_payout:
                        # Complete -> Complete (different payout) OR
                        #   Complete -> Complete (different payout) -> Complete (different payout)
                        self.update(
                            adjusted_status=SessionAdjustedStatus.PAYOUT_ADJUSTMENT,
                            adjusted_payout=new_payout,
                            adjusted_timestamp=adjusted_timestamp,
                            adjusted_user_payout=new_user_payout,
                        )

                    else:
                        # Complete -> Complete (same payout). do nothing
                        raise ValueError("should never reach here")

                else:
                    # Complete -> Fail OR Complete -> Complete (diff payout) -> Fail
                    self.update(
                        adjusted_status=SessionAdjustedStatus.ADJUSTED_TO_FAIL,
                        adjusted_payout=new_payout,
                        adjusted_timestamp=adjusted_timestamp,
                        adjusted_user_payout=new_user_payout,
                    )
            else:
                # adj_status = adj to fail
                if new_status == Status.FAIL:
                    # Complete -> Fail -> Fail (do nothing)
                    raise ValueError("should never reach here")

                else:
                    # Complete -> Fail -> Complete
                    if original_payout == new_payout:
                        self.update(
                            adjusted_status=None,
                            adjusted_payout=None,
                            adjusted_timestamp=adjusted_timestamp,
                            adjusted_user_payout=None,
                        )
                    else:
                        # complete -> fail -> complete (different payout)
                        self.update(
                            adjusted_status=SessionAdjustedStatus.PAYOUT_ADJUSTMENT,
                            adjusted_payout=new_payout,
                            adjusted_timestamp=adjusted_timestamp,
                            adjusted_user_payout=new_user_payout,
                        )
        else:
            # originally a failure. possible adj_status -> {None, adj to complete}
            assert self.adjusted_status not in {
                SessionAdjustedStatus.ADJUSTED_TO_FAIL,
                SessionAdjustedStatus.PAYOUT_ADJUSTMENT,
            }, "Can't have fail adj to fail or payout adj"
            if self.adjusted_status == SessionAdjustedStatus.ADJUSTED_TO_COMPLETE:
                if new_status == Status.FAIL:
                    # Fail -> Complete -> Fail
                    self.update(
                        adjusted_status=None,
                        adjusted_payout=None,
                        adjusted_timestamp=adjusted_timestamp,
                        adjusted_user_payout=None,
                    )
                else:
                    # Fail -> Complete
                    if new_payout != self.adjusted_payout:
                        # Fail -> Complete -> Complete (new payout)
                        # If a session is originally Fail. And then its adjusted
                        #   to complete, and then a 2nd wall in that session is
                        #   also adj to complete, is the session adj to complete
                        #   or payout_adj? I'm sticking with adj to complete,
                        #   and the adj payout changed.
                        self.update(
                            adjusted_status=SessionAdjustedStatus.ADJUSTED_TO_COMPLETE,
                            adjusted_payout=new_payout,
                            adjusted_timestamp=adjusted_timestamp,
                            adjusted_user_payout=new_user_payout,
                        )
                    else:
                        # Fail -> Complete -> Complete (same payout)
                        raise ValueError("should never reach here")
            else:
                # adj status is None
                if new_status == Status.COMPLETE:
                    # Fail -> Complete
                    self.update(
                        adjusted_status=SessionAdjustedStatus.ADJUSTED_TO_COMPLETE,
                        adjusted_payout=new_payout,
                        adjusted_timestamp=adjusted_timestamp,
                        adjusted_user_payout=new_user_payout,
                    )
        return True

    def get_status_after_adjustment(self) -> Status:
        if self.adjusted_status in {
            SessionAdjustedStatus.ADJUSTED_TO_COMPLETE,
            SessionAdjustedStatus.PAYOUT_ADJUSTMENT,
        }:
            return Status.COMPLETE
        elif self.adjusted_status == SessionAdjustedStatus.ADJUSTED_TO_FAIL:
            return Status.FAIL
        elif self.status == Status.COMPLETE:
            return Status.COMPLETE
        else:
            return Status.FAIL

    def get_payout_after_adjustment(self) -> Decimal:
        if self.adjusted_status is not None:
            return self.adjusted_payout
        else:
            return self.payout or Decimal(0)

    def get_user_payout_after_adjustment(self) -> Optional[Decimal]:
        if self.adjusted_status is not None:
            return self.adjusted_user_payout
        else:
            return self.user_payout


def check_adjusted_status_consistent(
    status: Status,
    cpi: Decimal,
    adjusted_status: WallAdjustedStatus,
    adjusted_cpi: Decimal,
):
    if adjusted_status == WallAdjustedStatus.ADJUSTED_TO_COMPLETE:
        assert status != Status.COMPLETE, (
            "If a Wall was originally completed, reversed, and then re-reversed to complete,"
            "the adjusted_status should be null"
        )
        assert adjusted_cpi == cpi, "adjusted_cpi should be equal to the original cpi"

    elif adjusted_status == WallAdjustedStatus.ADJUSTED_TO_FAIL:
        assert (
            status == Status.COMPLETE
        ), "Wall.status must be COMPLETE for the adjusted_status to be ADJUSTED_TO_FAIL"
        assert (
            adjusted_cpi == 0
        ), "adjusted_cpi should be 0 if adjusted_status is ADJUSTED_TO_FAIL"

    elif adjusted_status == WallAdjustedStatus.CPI_ADJUSTMENT:
        # the original status is allowed to be anything
        # the adjusted cpi should be something different
        assert (
            adjusted_cpi != 0 and adjusted_cpi != cpi
        ), "If CPI_ADJUSTMENT, the adjusted_cpi should be different from the original cpi or 0"

    elif adjusted_status is None:
        assert adjusted_cpi is None, "incompatible adjusted values"


def check_adjusted_status_wall_consistent(
    status: Status,
    cpi: Optional[Decimal] = None,
    adjusted_status: Optional[WallAdjustedStatus] = None,
    adjusted_cpi: Optional[Decimal] = None,
    new_adjusted_status: Optional[WallAdjustedStatus] = None,
    new_adjusted_cpi: Optional[Decimal] = None,
) -> Tuple[bool, str]:
    """
    Raises an AssertionError if inconsistent.

    - status, cpi, adjusted_status, adjusted_cpi are the wall's CURRENT values
    - new_adjusted_status & new_adjusted_cpi are attempting to be set

    We are checking if the adjustment is allowed, based on the attempt's current status.
    """
    try:
        _check_adjusted_status_wall_consistent(
            status=status,
            cpi=cpi,
            adjusted_status=adjusted_status,
            adjusted_cpi=adjusted_cpi,
            new_adjusted_status=new_adjusted_status,
            new_adjusted_cpi=new_adjusted_cpi,
        )
    except AssertionError as e:
        return False, str(e)
    return True, ""


def _check_adjusted_status_wall_consistent(
    status: Status,
    cpi: Optional[Decimal] = None,
    adjusted_status: Optional[WallAdjustedStatus] = None,
    adjusted_cpi: Optional[Decimal] = None,
    new_adjusted_status: Optional[WallAdjustedStatus] = None,
    new_adjusted_cpi: Optional[Decimal] = None,
) -> None:
    """
    See check_adjusted_status_wall_consistent
    """
    # Check that we're actually changing something
    if adjusted_status == new_adjusted_status and adjusted_cpi == new_adjusted_cpi:
        raise AssertionError(f"attempt is already {adjusted_status=}, {adjusted_cpi=}")

    # adjusted_status/adjusted_cpi agreement
    check_adjusted_status_consistent(
        status=status,
        cpi=cpi,
        adjusted_status=new_adjusted_status,
        adjusted_cpi=new_adjusted_cpi,
    )

    # status / adjusted_status agreement
    if status == Status.COMPLETE:
        assert (
            new_adjusted_status != WallAdjustedStatus.ADJUSTED_TO_COMPLETE
        ), "adjusted status can't be ADJUSTED_TO_COMPLETE if the status is already COMPLETE"
    elif status == Status.FAIL:
        assert (
            new_adjusted_status != WallAdjustedStatus.ADJUSTED_TO_FAIL
        ), "adjusted status can't be ADJUSTED_TO_FAIL if the status is already FAIL"
    else:
        # status is None/timeout/abandon, which we treat as a fail anyway
        assert (
            new_adjusted_status != WallAdjustedStatus.ADJUSTED_TO_FAIL
        ), "attempt is already a failure"

    # adjusted_status / new_adjusted_status agreement
    if new_adjusted_status == WallAdjustedStatus.CPI_ADJUSTMENT:
        assert (
            new_adjusted_cpi != adjusted_cpi
        ), f"adjusted_cpi is already {adjusted_cpi}"
