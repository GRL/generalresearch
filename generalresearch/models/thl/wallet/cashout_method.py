from __future__ import annotations

import hashlib
import logging
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any, Literal, Self

from pydantic import (
    BaseModel,
    ConfigDict,
    EmailStr,
    Field,
    NonNegativeInt,
    PositiveInt,
    field_validator,
    model_validator,
)

from generalresearch.currency import USDCent
from generalresearch.models.custom_types import (
    AwareDatetimeISO,
    HttpsUrlStr,
    UUIDStr,
)
from generalresearch.models.legacy.api_status import StatusResponse
from generalresearch.models.thl.definitions import PayoutStatus
from generalresearch.models.thl.locales import CountryISO
from generalresearch.models.thl.user_identifiers import BPUIDStr
from generalresearch.models.thl.user_ref import UserRef
from generalresearch.models.thl.wallet.definitions import Currency, PayoutType

logger = logging.getLogger()

example_cashout_method = {
    "id": "941d489c3ce04eb39a0ddb7f8f75db74",
    "bpid": "6a3ddfb747344bbc93efadf1c3a16e1a",
    "bpuid": None,
    "currency": "USD",
    "data": {"terms": "...", "disclaimer": "..."},
    "description": "...",
    "image_url": "https://d30s7yzk2az89n.cloudfront.net/images/brands/b238587-1200w-326ppi.png",
    "max_value": 25000,
    "min_value": 500,
    "name": "Visa® Prepaid Card USD",
    "type": "TANGO",
}


class CashoutMethodBase(BaseModel):
    """
    A user can request a payout of their wallet balance via a cashout method. This is the way
    in which the money is paid. The terms cashout and payout are used interchangeably.
    """

    model_config = ConfigDict(json_schema_extra={"example": example_cashout_method})

    id: UUIDStr = Field(description="Unique ID for this cashout method")

    currency: Literal[Currency.USD] = Field(
        default=Currency.USD,
        description="The currency of the cashout. Only USD is supported.",
    )
    original_currency: Currency | None = Field(
        default=None,
        description="The base currency of the money paid out. This is used for "
        "e.g. sending an Amazon UK gift card",
    )
    # This also is used for the PayoutEvent.request_data
    data: (
        PaypalCashoutMethodData
        | TangoCashoutMethodData
        | CashMailCashoutMethodData
        | AmtCashoutMethodData
    ) = Field(discriminator="type")
    description: str = Field(
        description="The description of the cashout method.", default=""
    )
    image_url: HttpsUrlStr | None = Field(
        description="Link to an image to display", default=None
    )
    max_value: PositiveInt = Field(
        description="(In lowest unit of the original_currency), "
        "The maximum amount that can be cashed out in one transaction."
    )
    min_value: NonNegativeInt = Field(
        description="(In lowest unit of the original_currency), "
        "The minimum amount that can be cashed out in one transaction."
    )
    name: str = Field(description="A descriptive name for the cashout method.")
    # In the db, this is called "provider"
    type: PayoutType = Field(
        description=PayoutType.as_openapi_with_value_descriptions(),
    )
    ext_id: str | None = Field(
        default=None,
        description="An external ID. Can be shown to a user to disambiguate "
        "a user's possibly multiple methods",
    )
    usd_exchange_rate: float | None = Field(default=None)
    max_value_usd: USDCent | None = Field(
        default=None,
        description="(In lowest unit of USD), "
        "The maximum amount that can be cashed out in one transaction.",
    )
    min_value_usd: USDCent | None = Field(
        default=None,
        description="(In lowest unit of USD), "
        "The minimum amount that can be cashed out in one transaction.",
    )

    #
    # @property
    # def min_value_usd(self):
    #     if self.original_currency == Currency.USD:
    #         return self.min_value
    #     if self.usd_exchange_rate is None:
    #         return None
    #     return self.min_value * self.usd_exchange_rate

    @model_validator(mode="after")
    def validate_value_ranges(self) -> Self:
        if self.min_value > self.max_value:
            raise ValueError("min_value must not exceed max_value")
        if (
            self.min_value_usd is not None
            and self.max_value_usd is not None
            and self.min_value_usd > self.max_value_usd
        ):
            raise ValueError("min_value_usd must not exceed max_value_usd")
        return self

    def validate_requested_amount(self, amount: USDCent):
        """
        Check if 'amount' is a valid amount that can be requested.
        :param amount: The amount to be requested in USD Cents
        """
        amount = int(amount)
        if amount <= 0:
            raise ValueError("Amount must be positive")
        if self.original_currency not in {None, Currency.USD} and (
            self.min_value_usd is None or self.max_value_usd is None
        ):
            raise ValueError("USD limits are required for a foreign cashout method")
        min_value = self.min_value if self.min_value_usd is None else self.min_value_usd
        max_value = self.max_value if self.max_value_usd is None else self.max_value_usd
        if not min_value <= amount <= max_value:
            raise ValueError(
                f"Invalid amount requested: ${amount / 100:.2f}. Must be between"
                f" ${int(min_value) / 100:.2f} and ${int(max_value) / 100:.2f}"
            )
        if self.type == PayoutType.CASH_IN_MAIL and amount % 500 != 0:
            raise ValueError("Amount must be in increments of $5.00")
        return True


class CashoutMethod(CashoutMethodBase):
    user: UserRef | None = Field(
        default=None,
        description="If set, this cashout method is custom for this user. For example"
        "a user may have a paypal cashout method with their paypal"
        "email associated.",
    )
    last_updated: AwareDatetimeISO = Field(default_factory=lambda: datetime.now(tz=UTC))
    is_live: bool = Field(default=True)

    @model_validator(mode="after")
    def validate_user(self) -> Self:
        if self.type in {PayoutType.PAYPAL, PayoutType.CASH_IN_MAIL}:
            assert self.user is not None, (
                "user_id must be set for this cashout method type"
            )
        else:
            assert self.user is None, (
                "user_id must NOT be set for this cashout method type"
            )
        return self


class CashoutMethodOut(CashoutMethodBase):
    product_id: UUIDStr | None = Field(
        default=None, examples=["4fe381fb7186416cb443a38fa66c6557"]
    )

    product_user_id: str | None = Field(
        default=None,
        min_length=3,
        max_length=128,
        examples=["app-user-9329ebd"],
        description="A unique identifier for each user, which is set by the "
        "Supplier. It should not contain any sensitive information"
        "like email or names, and should avoid using any"
        "incrementing values.",
    )

    @classmethod
    def from_cashout_method(cls, cm: CashoutMethod) -> Self:
        d = cm.model_dump()
        if cm.user:
            d["product_id"] = cm.user.product_id
            d["product_user_id"] = cm.user.product_user_id
        return cls.model_validate(d)


class USDeliveryAddress(BaseModel):
    name_or_attn: str = Field(min_length=1, max_length=50)
    company: str | None = Field(
        default=None,
        min_length=1,
        max_length=50,
    )
    phone_number: str | None = Field(
        default=None,
        min_length=10,
        max_length=10,
        pattern=r"^[0-9]+$",
    )
    address: str = Field(min_length=1, max_length=100)
    city: str = Field(min_length=1, max_length=100)
    state: str = Field(min_length=1, max_length=2)
    postal_code: str = Field(min_length=1, max_length=10)
    country: CountryISO = Field(default="us")

    def md5sum(self) -> str:
        return hashlib.md5(self.model_dump_json().encode()).hexdigest()


class CashMailCashoutMethodData(BaseModel):
    type: Literal[PayoutType.CASH_IN_MAIL] = Field(default=PayoutType.CASH_IN_MAIL)

    delivery_address: USDeliveryAddress = Field(
        description="Delivery address where payment should be sent"
    )


class CashMailCashoutMethodRequestData(CashMailCashoutMethodData):
    pass


class PaypalCashoutMethodData(BaseModel):
    type: Literal[PayoutType.PAYPAL] = Field(default=PayoutType.PAYPAL)

    email: EmailStr = Field(
        description="Email address of the paypal user",
        examples=["test@example.com"],
    )


class PaypalCashoutMethodRequestData(BaseModel):
    email: EmailStr = Field(
        description="Email address of the paypal user",
        examples=["test@example.com"],
    )
    interface: Literal["api"] = Field(default="api")


class TangoCashoutMethodRequestData(BaseModel):
    accountIdentifier: str = Field()
    customerIdentifier: str = Field()
    utid: str = Field(description="tango utid")
    # This is not necessarily in USD. It is in whatever currency the card is in
    amount: Decimal = Field(description="Amount to be paid out (in USD, EUR, etc)")
    campaign: Literal["300large"]
    sendEmail: bool = Field(default=False)
    externalRefID: str = Field(description="External Ref ID")
    description: str = Field()


class TangoCashoutMethodData(BaseModel):
    type: Literal[PayoutType.TANGO] = Field(default=PayoutType.TANGO)
    utid: str = Field(description="tango utid")
    # TODO: Can't be CountryISOLike because it appears to be allcaps
    countries: list[str] = Field()
    value_type: Literal["variable", "fixed"] = Field()
    disclaimer: str = Field(default="")
    terms: str = Field(default="")

    @field_validator("countries", mode="after")
    def countries_case(cls, countries: list[str]) -> list[str]:
        return [x.lower() for x in countries]


class AmtCashoutMethodData(BaseModel):
    type: Literal[PayoutType.AMT] = Field(default=PayoutType.AMT)


class CashoutMethodsResponse(StatusResponse):
    cashout_methods: list[CashoutMethodOut] = Field()


class DeliveryStatus(StrEnum):
    PENDING = "Pending"
    SHIPPED = "Shipped"
    IN_TRANSIT = "In Transit"
    OUT_FOR_DELIVERY = "Out for Delivery"
    DELIVERED = "Delivered"
    RETURNED = "Returned"
    CANCELED = "Canceled"
    FAILED_ATTEMPT = "Failed Attempt"
    LOST = "Lost"


class ShippingCarrier(StrEnum):
    USPS = "USPS"
    FEDEX = "FedEx"
    UPS = "UPS"
    DHL = "DHL"


class ShippingMethod(StrEnum):
    STANDARD = "Standard"
    EXPRESS = "Express"
    TWO_DAY = "Two-Day"
    OVERNIGHT = "Overnight"
    SAME_DAY = "Same Day"


# This goes in the PayoutEvent.order_data
class CashMailOrderData(BaseModel):
    type: Literal[PayoutType.CASH_IN_MAIL] = Field(default=PayoutType.CASH_IN_MAIL)
    shipping_cost: PositiveInt | None = Field(
        description="(USD cents) The shipping cost. This amount get charged to the BP.",
        strict=True,
    )
    tracking_number: str | None = Field(
        default=None,
        min_length=1,
        max_length=50,
    )
    shipping_method: ShippingMethod | None = Field(
        default=None,
        min_length=1,
        max_length=50,
        description="Standard, express, etc.",
    )
    carrier: ShippingCarrier | None = Field(
        default=None,
        min_length=1,
        max_length=50,
        description="Name of the shipping company, e.g., USPS, FedEx, DHL",
    )
    ship_date: AwareDatetimeISO | None = Field(default=None)
    estimated_delivery_date: AwareDatetimeISO | None = Field(default=None)
    delivery_status: DeliveryStatus | None = Field(
        default=None,
        min_length=1,
        max_length=50,
        description="Current status of delivery, e.g., pending, in transit, delivered",
    )
    last_updated: AwareDatetimeISO | None = Field(
        default=None,
        description="Timestamp of the last status update",
    )


class CreateCashoutMethodRequest(BaseModel):
    bpuid: BPUIDStr = Field(
        description="(product_user_id) The user to create this cashout method for.",
        examples=["app-user-9329ebd"],
    )
    type: PayoutType = Field(
        description=PayoutType.as_openapi_with_value_descriptions(),
        examples=[PayoutType.PAYPAL],
    )


class CreatePayPalCashoutMethodRequest(
    PaypalCashoutMethodData,
    CreateCashoutMethodRequest,
):
    pass


class CreateCashMailCashoutMethodRequest(
    CashMailCashoutMethodData, CreateCashoutMethodRequest
):
    pass


class CashoutMethodResponse(StatusResponse):
    cashout_method: CashoutMethodOut = Field()


class CreateCashoutRequest(BaseModel):
    bpuid: BPUIDStr = Field(
        description="(product_user_id) The user requesting a cashout.",
        examples=["app-user-9329ebd"],
    )
    amount: PositiveInt = Field(
        description="(USD cents) The amount requested for the cashout.",
        strict=True,
        examples=[531],
    )
    cashout_method_id: UUIDStr = Field(
        description="Unique ID for the cashout method the cashout is being requested with.",
        examples=["941d489c3ce04eb39a0ddb7f8f75db74"],
    )


class CashoutRequestInfo(BaseModel):
    """See models.thl.payout: PayoutEvent. We've confused a CashOut and a
    Payout. This is used only in the API response.
    """

    id: UUIDStr | None = Field(
        description="Unique ID for this cashout. This may be NULL if the "
        "status is REJECTED or FAILED, which may happen if the "
        "request is invalid.",
        examples=["3ceb847aaf9f40f4bd15b2b5e083abf6"],
    )
    description: str = Field(
        description="This is the name of the cashout method.",
        examples=["Visa® Prepaid Card USD"],
    )
    message: str | None = Field(default=None)
    status: PayoutStatus | None = Field(
        default=PayoutStatus.PENDING,
        description=PayoutStatus.as_openapi(),
        examples=[PayoutStatus.PENDING],
    )
    transaction_info: dict[str, Any] | None = Field(default=None)
    product_id: UUIDStr = Field()
    product_user_id: BPUIDStr = Field()

    payout_type: PayoutType = Field(
        description=PayoutType.as_openapi(), examples=[PayoutType.ACH]
    )
    amount: PositiveInt = Field(
        lt=2**63 - 1,
        strict=True,
        description="The USDCent amount int. This cannot be 0 or negative",
        examples=[531],
    )


class CashoutRequestResponse(StatusResponse):
    cashout: CashoutRequestInfo = Field()


example_foreign_value = {
    "value": "138",
    "currency": "CAD",
    "value_string": "$1.38 CAD",
}


class CashoutMethodForeignValue(BaseModel):
    """
    Shows the expected value of a redemption in a foreign currency.
    """

    model_config = ConfigDict(json_schema_extra={"example": example_foreign_value})

    value: NonNegativeInt = Field(
        description="Value of the redemption in the currency's smallest unit."
    )
    currency: Currency = Field(
        description=Currency.as_openapi_with_value_descriptions()
    )
    value_string: str = Field(
        description="A string representation of the value in the currency."
    )


class CashoutMethodForeignValueResponse(StatusResponse):
    cashout_method_value: CashoutMethodForeignValue = Field()
