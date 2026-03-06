import json
from datetime import datetime, timezone
from typing import Dict, Optional, Collection, List
from uuid import uuid4

from pydantic import (
    BaseModel,
    Field,
    PositiveInt,
    computed_field,
    field_validator,
)

from generalresearch.currency import USDCent
from generalresearch.models.custom_types import UUIDStr, AwareDatetimeISO
from generalresearch.models.thl.definitions import PayoutStatus
from generalresearch.models.thl.wallet import PayoutType
from generalresearch.models.thl.wallet.cashout_method import (
    CashMailOrderData,
)


class PayoutEvent(BaseModel, validate_assignment=True):
    """A user has requested to be paid from their wallet balance."""

    uuid: UUIDStr = Field(
        default_factory=lambda: uuid4().hex,
        examples=["9453cd076713426cb68d05591c7145aa"],
    )

    # This is the LedgerAccount.uuid that this money is being requested
    #   from. The user/BP is retrievable through the LedgerAccount.reference_uuid
    debit_account_uuid: UUIDStr = Field(examples=["18298cb1583846fbb06e4747b5310693"])

    # These two fields are copied here from the LedgerAccount through the
    #   debit_account_uuid for convenience. They will get populated if the
    #   PayoutEventManager retrieves a PayoutEvent from the db.
    account_reference_type: Optional[str] = Field(default=None)
    account_reference_uuid: Optional[UUIDStr] = Field(default=None)

    # References a row in the account_cashoutmethod table. This is the
    #   cashout method that was used to request this payout. (A cashout is
    #   the same thing as a payout)
    cashout_method_uuid: UUIDStr = Field(examples=["a6dc1fc1bf934557b952f253dee12813"])

    # By default, this will just be the cashout_method.name. This also is
    #   populated from the db and so does not need to be set (there is no
    #   `description` field in event_payout)
    description: Optional[str] = Field(default=None)
    created: AwareDatetimeISO = Field(
        default_factory=lambda: datetime.now(tz=timezone.utc)
    )

    # In the smallest unit of the currency being transacted. For USD, this
    #   is cents.
    amount: PositiveInt = Field(
        lt=2**63 - 1,
        strict=True,
        description="The USDCent amount int. This cannot be 0 or negative",
        examples=[531],
    )

    status: PayoutStatus = Field(
        default=PayoutStatus.PENDING,
        description=PayoutStatus.as_openapi(),
        examples=[PayoutStatus.COMPLETE],
    )

    # Used for holding an external, payout-type-specific identifier
    ext_ref_id: Optional[str] = Field(default=None)
    payout_type: PayoutType = Field(
        description=PayoutType.as_openapi(), examples=[PayoutType.ACH]
    )

    # Stores payout-type-specific information that is used to request this
    #   payout from the external provider.
    request_data: Dict = Field(default_factory=dict)

    # Stores payout-type-specific order information that is returned from
    #   the external payout provider.
    order_data: Optional[Dict | CashMailOrderData] = Field(default=None)

    @field_validator("payout_type", mode="before")
    @classmethod
    def normalize_enum(cls, v):
        if isinstance(v, str):
            try:
                return PayoutType[v.upper()]
            except KeyError:
                raise ValueError(f"Invalid payout_type: {v}")
        return v

    def update(
        self,
        status: PayoutStatus,
        ext_ref_id: Optional[str] = None,
        order_data: Optional[Dict] = None,
    ) -> None:
        # These 3 things are the only modifiable attributes
        self.check_status_change_allowed(status)
        self.status = status
        self.ext_ref_id = ext_ref_id
        self.order_data = order_data

    def check_status_change_allowed(self, status: PayoutStatus) -> None:
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


class BPPayoutEvent(BaseModel):
    uuid: UUIDStr = Field(
        title="Brokerage Product Payout ID",
        description="Unique identifier for the Payout Event",
        examples=["9453cd076713426cb68d05591c7145aa"],
    )

    product_id: UUIDStr = Field(
        description="The Brokerage Product that was paid out",
        examples=["1108d053e4fa47c5b0dbdcd03a7981e7"],
    )

    created: AwareDatetimeISO = Field(
        description="When the Brokerage Product was paid out",
        default_factory=lambda: datetime.now(tz=timezone.utc),
    )

    amount: USDCent = Field(
        lt=2**63 - 1,
        strict=True,
        description="The USDCent amount int. This cannot be 0 or negative",
        examples=[531],
    )

    status: Optional[PayoutStatus] = Field(
        default=PayoutStatus.PENDING,
        description=PayoutStatus.as_openapi(),
        examples=[PayoutStatus.COMPLETE],
    )

    method: PayoutType = Field(
        title="Payout Method",
        description=PayoutType.as_openapi(),
        examples=[PayoutType.ACH],
    )

    @computed_field(return_type=str, examples=["$10,000.000"])
    @property
    def amount_usd(self) -> str:
        return self.amount.to_usd_str()

    @staticmethod
    def from_pe(
        payout_events: Collection[PayoutEvent],
        account_product_mapping: Dict[str, str],
        order_by="ASC",
    ) -> List["BPPayoutEvent"]:
        res = []
        for pe in payout_events:
            bp_pe = BPPayoutEvent.model_validate(
                {
                    "uuid": pe.uuid,
                    "product_id": account_product_mapping[pe.debit_account_uuid],
                    "created": pe.created,
                    "amount": USDCent(pe.amount),
                    "status": pe.status,
                    "method": pe.payout_type,
                }
            )
            res.append(bp_pe)

        match order_by:
            case "ASC":
                sorted_list = sorted(res, key=lambda x: x.created, reverse=False)
            case "DESC":
                sorted_list = sorted(res, key=lambda x: x.created, reverse=True)
            case _:
                raise ValueError("Invalid order provided..")

        return sorted_list
