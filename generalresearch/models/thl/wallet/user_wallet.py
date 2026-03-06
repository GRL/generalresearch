from __future__ import annotations

import logging

from pydantic import BaseModel, Field, ConfigDict, NonNegativeInt

from generalresearch.models.legacy.api_status import StatusResponse
from generalresearch.models.thl.payout_format import (
    PayoutFormatType,
    PayoutFormatField,
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
