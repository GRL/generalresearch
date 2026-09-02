from __future__ import annotations

import logging
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, NonNegativeInt

from generalresearch.models.custom_types import UUIDStr
from generalresearch.models.legacy.api_status import StatusResponse
from generalresearch.models.thl.ledger import AccountType
from generalresearch.models.thl.payout_format import (
    PayoutFormatField,
    PayoutFormatType,
)

logger = logging.getLogger()

example_wallet_balance = {
    "amount": 123,
    "redeemable_amount": 100,
    "payout_format": "{payout*10:,.0f} Points",
    "amount_string": "1230 Points",
    "redeemable_amount_string": "1000 Points",
}


class UserWalletBalance(BaseModel):
    model_config = ConfigDict(json_schema_extra={"example": example_wallet_balance})

    # This can be negative (due to recons for instance), but shouldn't be often ...
    amount: int = Field(description="(USD cents) The amount in the user's wallet.")
    redeemable_amount: NonNegativeInt = Field(
        description="(USD cents) The amount in the user's wallet this is currently redeemable."
    )
    payout_format: PayoutFormatType = PayoutFormatField
    amount_string: str = Field(
        description="The 'amount' with the payout_format applied. Can be displayed to the user."
    )
    redeemable_amount_string: str = Field(
        description="The 'redeemable_amount' with the payout_format applied. Can be displayed to the user."
    )


class UserWalletBalanceResponse(StatusResponse):
    wallet: UserWalletBalance = Field()


class UserLedgerWallet(UserWalletBalance):
    """A user-owned ledger account exposed by the wallets endpoint."""

    account_uuid: UUIDStr = Field(
        description="A unique identifier for this Ledger Account",
        examples=["c3c3566b5b1b4961b63a5670a2dc923d"],
    )
    account_type: Literal[
        AccountType.USER_WALLET,
        AccountType.USER_ATTEMPT_CREDIT,
    ]
    currency: str = Field(default="USD", max_length=32)
    display_name: str = Field(
        max_length=64,
        description="Human-readable description of the Ledger Account",
    )


class UserLedgerWallets(BaseModel):
    wallets: list[UserLedgerWallet] = Field(default_factory=list)
