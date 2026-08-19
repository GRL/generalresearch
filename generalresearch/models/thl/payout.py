from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Collection
from uuid import uuid4

from pydantic import (
    BaseModel,
    Field,
    PositiveInt,
    computed_field,
    field_validator,
    model_validator,
    ConfigDict,
)
from typing_extensions import Self

from generalresearch.currency import USDCent
from generalresearch.models.custom_types import AwareDatetimeISO, UUIDStr
from generalresearch.models.thl.definitions import PayoutStatus
from generalresearch.models.thl.ledger import OrderBy
from generalresearch.models.thl.wallet import PayoutType
from generalresearch.models.thl.wallet.cashout_method import (
    CashMailOrderData,
)
from generalresearch.redis_helper import RedisConfig


class PayoutEvent(BaseModel):
    """Base Pydantic Model to represent the `event_payout` table

    This table supports multiple different kinds of "Payouts":

    - UserPayoutEvent - A User (survey or task taker) is requesting to
        withdraw money from their balance

    - BusinessPayoutEvent - A Supplier gets paid out via ACH / Wire

    - BrokerageProductPayoutEvent - BusinessPayoutEvent is composed of
        multiple BrokerageProductPayoutEvents.
    """

    uuid: UUIDStr = Field(
        title="Payout Event Unique Identifier",
        default_factory=lambda: uuid4().hex,
        examples=["9453cd076713426cb68d05591c7145aa"],
    )

    debit_account_uuid: UUIDStr | None = Field(
        description="The LedgerAccount.uuid that money is being requested from. "
        "Thie User or Brokerage Product is retrievable through the "
        "LedgerAccount.reference_uuid",
        examples=["18298cb1583846fbb06e4747b5310693"],
    )

    cashout_method_uuid: UUIDStr | None= Field(
        description="References a row in the account_cashoutmethod table. This "
        "is the cashout method that was used to request this "
        "payout. (A cashout is the same thing as a payout)",
        examples=["a6dc1fc1bf934557b952f253dee12813"],
    )

    created: AwareDatetimeISO | None = Field(
        default = None
    )

    # In the smallest unit of the currency being transacted. For USD, this
    #   is cents.
    amount: PositiveInt = Field(
        lt=2**63 - 1,
        strict=True,
        description="The USDCent amount int. This cannot be 0 or negative",
        examples=[531],
    )

    status: PayoutStatus | None = Field(
        default=PayoutStatus.PENDING,
        description=PayoutStatus.as_openapi(),
        examples=[PayoutStatus.COMPLETE],
    )

    # Used for holding an external, payout-type-specific identifier
    ext_ref_id: str | None = Field(default=None)
    payout_type: PayoutType = Field(
        description=PayoutType.as_openapi(), examples=[PayoutType.ACH]
    )

    request_data: dict = Field(
        default_factory=dict,
        description="Stores payout-type-specific information that is used to "
        "request this payout from the external provider.",
    )

    order_data: dict | CashMailOrderData | None = Field(
        default=None,
        description="Stores payout-type-specific order information that is "
        "returned from the external payout provider.",
    )

    def update(
        self,
        status: PayoutStatus,
        ext_ref_id: str | None = None,
        order_data: dict | None = None,
    ) -> None:

        self.check_status_change_allowed(status)

        # These 3 things are the only modifiable attributes
        self.status = status
        self.ext_ref_id = ext_ref_id
        self.order_data = order_data

    def check_status_change_allowed(self, status: PayoutStatus) -> None:

        # We may not be changing the status when this method gets called. It's
        #   possible to be called when we're updating other attributes so
        #   allow immediate bypass if it isn't actually different.
        if self.status == status:
            return

        if self.status in {
            PayoutStatus.REJECTED,
            PayoutStatus.CANCELLED,
            PayoutStatus.COMPLETE,
        }:
            raise ValueError(f"status {self.status} is final. No changes allowed")

        if self.status == PayoutStatus.PENDING:
            assert status != PayoutStatus.PENDING, "status is already PENDING!"

        elif self.status == PayoutStatus.APPROVED:
            assert status in {
                PayoutStatus.FAILED,
                PayoutStatus.COMPLETE,
            }, f"status APPROVED can only be FAILED or COMPLETED, not {status}"

        elif self.status == PayoutStatus.FAILED:
            assert status in {
                PayoutStatus.CANCELLED,
                PayoutStatus.COMPLETE,
            }, f"status FAILED can only be CANCELLED or COMPLETED, not {status}"
        else:
            raise ValueError("this shouldn't happen")

    # --- ORM ---

    def model_dump_mysql(self, *args, **kwargs) -> dict:
        d = self.model_dump(mode="json", *args, **kwargs)

        if "created" in d:
            d["created"] = self.created.replace(tzinfo=None)

        if d.get("request_data") is not None:
            d["request_data"] = json.dumps(self.request_data)

        if d.get("order_data") is not None:
            if isinstance(self.order_data, dict):
                d["order_data"] = json.dumps(self.order_data)
            else:
                d["order_data"] = self.order_data.model_dump_json()

        return d


class UserPayoutEvent(PayoutEvent):
    """A user has requested to be paid from their wallet balance."""

    # These two fields are copied here from the LedgerAccount through the
    #   debit_account_uuid for convenience. They will get populated if the
    #   PayoutEventManager retrieves a PayoutEvent from the db.
    #   Requires joining on:
    #       - accounting_cashoutmethod
    #       - ledger_account
    account_reference_type: str | None = Field(default=None)
    account_reference_uuid: UUIDStr | None = Field(default=None)

    # By default, this will just be the cashout_method.name. This also is
    #   populated from the db and so does not need to be set (there is no
    #   `description` field in event_payout)
    description: str | None = Field(default=None)

    @field_validator("payout_type", mode="before")
    @classmethod
    def normalize_enum(cls, v):
        if isinstance(v, str):
            try:
                return PayoutType[v.upper()]
            except KeyError:
                raise ValueError(f"Invalid payout_type: {v}")
        return v


class BrokerageProductPayoutEvent(PayoutEvent):
    """The amount

    - created: When the Brokerage Product was paid out
    """

    product_id: UUIDStr = Field(
        description="The Brokerage Product that was paid out",
        examples=["1108d053e4fa47c5b0dbdcd03a7981e7"],
    )

    @computed_field(
        return_type=PayoutType,
        description=PayoutType.as_openapi(),
        examples=[PayoutType.ACH],
    )
    @property
    def method(self) -> PayoutType:
        return self.payout_type

    @computed_field(return_type=USDCent, examples=["$10,000.000"])
    @property
    def amount_usd(self) -> USDCent:
        return USDCent(self.amount)

    @computed_field(return_type=str, examples=["$10,000.000"])
    @property
    def amount_usd_str(self) -> str:
        return self.amount_usd.to_usd_str()

    # --- ORM ---

    @classmethod
    def from_payout_event(
        cls,
        pe: PayoutEvent,
        account_product_mapping: dict[UUIDStr, UUIDStr] | None = None,
        redis_config: RedisConfig | None = None,
    ) -> Self:
        # TODO!: prevent re-assignment, rework this...

        if account_product_mapping is None:
            rc = redis_config.create_redis_client()
            account_product_mapping: dict = rc.hgetall(name="pem:account_to_product")
            assert isinstance(account_product_mapping, dict)
            assert pe.uuid in account_product_mapping.keys()

        d = pe.model_dump()
        d["product_id"] = account_product_mapping[pe.debit_account_uuid]
        return cls.model_validate(d)

    @classmethod
    def from_payout_events(
        cls,
        payout_events: Collection[PayoutEvent],
        order_by=OrderBy,
        account_product_mapping: dict[UUIDStr, UUIDStr] | None = None,
        redis_config: RedisConfig | None = None,
    ) -> list[Self]:
        # TODO!: prevent re-assignment, rework this...

        if account_product_mapping is None:
            rc = redis_config.create_redis_client()
            account_product_mapping: dict = rc.hgetall(name="pem:account_to_product")
            assert isinstance(account_product_mapping, dict)

        res = []
        for pe in payout_events:
            res.append(
                cls.from_payout_event(
                    pe=pe, account_product_mapping=account_product_mapping
                )
            )

        match order_by:
            case OrderBy.ASC:
                sorted_list = sorted(res, key=lambda x: x.created, reverse=False)
            case OrderBy.DESC:
                sorted_list = sorted(res, key=lambda x: x.created, reverse=True)
            case _:
                raise ValueError("Invalid order provided..")

        return sorted_list


class BusinessPayoutEvent(BaseModel):
    """A single payout event to a supplier Business."""
    model_config = ConfigDict(validate_assignment=True)

    uuid: UUIDStr = Field(
        title="Supplier Payout Unique Identifier",
        examples=["9453cd076713426cb68d05591c7145aa"],
    )

    # Used for holding a *unique*, external, payout-type-specific identifier.
    ext_ref_id: str = Field(title="Unique external reference ID")

    business_id: UUIDStr = Field(
        description="The Business receiving this supplier payout.",
        examples=[uuid4().hex],
    )

    created: AwareDatetimeISO | None = Field(
        default=None
    )

    # In the smallest unit of the currency being transacted. For USD, this
    #   is cents.
    amount: PositiveInt = Field(
        lt=2**63 - 1,
        strict=True,
        title="Amount",
        description="The amount issued to the supplier.",
        examples=[1_982_343],
    )

    status: PayoutStatus = Field(
        default=PayoutStatus.PENDING,
        description=PayoutStatus.as_openapi(),
        examples=[PayoutStatus.COMPLETE],
    )

    payout_type: PayoutType = Field(
        description=PayoutType.as_openapi(), examples=[PayoutType.ACH]
    )

    request_data: dict | None = Field(
        default=None,
        description="Stores payout-type-specific information that is used to "
        "request this payout from the external provider.",
    )

    order_data: dict | None = Field(
        default=None,
        description="Stores payout-type-specific order information that is "
        "returned from the external payout provider.",
    )

    bp_payouts: list[BrokerageProductPayoutEvent] | None = Field(
        default=None,
        description="The list of Brokerage Product Payouts that this Business Payout includes",
        min_length=1,
    )

    @computed_field(
        title="Amount USD Str",
        description="The amount issued to the supplier as a USD string",
        examples=["$19,823.43"],
        return_type=str,
    )
    @property
    def amount_usd_str(self) -> str:
        return USDCent(self.amount).to_usd_str()

    # --- Validators ---

    @field_validator("payout_type", mode="before")
    @classmethod
    def normalize_payout_type(cls, v):
        if isinstance(v, str):
            try:
                return PayoutType[v.upper()]
            except KeyError:
                raise ValueError(f"Invalid payout_type: {v}")
        return v

    @field_validator("bp_payouts", mode="before")
    @classmethod
    def validate_bp_payouts_type(cls, v):
        """This can be a list of Instances or Python Dictionaries depending
        on how it's initialized.
        """

        if v is None:
            return v

        assert isinstance(v, list)
        return v

    @model_validator(mode="after")
    def validate_bp_payouts(self) -> Self:
        if not self.bp_payouts:
            return self

        bp_payout_amount = sum([p.amount for p in self.bp_payouts])
        if bp_payout_amount != self.amount:
            raise ValueError(
                "BusinessPayoutEvent.amount must equal the sum of "
                f"bp_payouts amounts ({self.amount=} {bp_payout_amount=})"
            )

        invalid_payout_types = [
            p.payout_type for p in self.bp_payouts if p.payout_type != self.payout_type
        ]
        if invalid_payout_types:
            raise ValueError(
                "All BrokerageProductPayoutEvent.payout_type values must equal "
                f"BusinessPayoutEvent.payout_type ({self.payout_type=})"
            )

        return self
