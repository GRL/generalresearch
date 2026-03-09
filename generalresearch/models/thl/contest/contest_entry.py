from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Union
from uuid import uuid4

from pydantic import (
    BaseModel,
    Field,
    computed_field,
    model_validator,
)

from generalresearch.currency import USDCent
from generalresearch.models.custom_types import AwareDatetimeISO, UUIDStr
from generalresearch.models.thl.contest.definitions import ContestEntryType
from generalresearch.models.thl.user import User


class ContestEntryCreate(BaseModel):
    entry_type: ContestEntryType = Field()
    # The meaning of this field is dictated by the contest's ContestEntryType
    amount: Union[USDCent, int] = Field(
        description="The amount of the entry in integer counts or USD Cents",
        gt=0,
    )

    # This is used in the Create Entry API. We'll look up the user and set
    # user_id. When we return this model in the API, user_id is excluded
    product_user_id: str = Field(
        min_length=3,
        max_length=128,
        examples=["app-user-9329ebd"],
        description="A unique identifier for each user, which is set by the "
        "Supplier. It should not contain any sensitive information"
        "like email or names, and should avoid using any"
        "incrementing values.",
    )


class ContestEntry(BaseModel):
    uuid: UUIDStr = Field(default_factory=lambda: uuid4().hex)
    created_at: AwareDatetimeISO = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    updated_at: AwareDatetimeISO = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    # entry_type and amount are the same as on ContestEntryCreate
    entry_type: ContestEntryType = Field()

    # The meaning of this field is dictated by the contest's ContestEntryType
    amount: Union[USDCent, int] = Field(
        description="The amount of the entry in integer counts or USD Cents",
        gt=0,
    )

    # user_id used internally, for DB joins/index
    user: User = Field(exclude=True)

    @model_validator(mode="before")
    @classmethod
    def validate_amount_type(cls, data: Dict) -> Dict:
        from generalresearch.models.thl.contest.definitions import (
            ContestEntryType,
        )

        amount = data.get("amount")
        entry_type = data.get("entry_type")

        if entry_type == ContestEntryType.COUNT:
            assert isinstance(amount, int) and not isinstance(
                amount, USDCent
            ), "amount must be int in ContestEntryType.COUNT"
        elif entry_type == ContestEntryType.CASH:
            # This may be coming from the DB, in which case it is an int.
            data["amount"] = USDCent(data["amount"])

        return data

    @computed_field()
    def amount_str(self) -> str:
        from generalresearch.models.thl.contest.definitions import (
            ContestEntryType,
        )

        if self.entry_type == ContestEntryType.COUNT:
            return str(self.amount)

        elif self.entry_type == ContestEntryType.CASH:
            return self.amount.to_usd_str()

        raise ValueError(f"Unknown entry_type: {self.entry_type}")

    @computed_field()
    @property
    def censored_product_user_id(self) -> str:
        from generalresearch.models.thl.contest.utils import (
            censor_product_user_id,
        )

        return censor_product_user_id(user=self.user)

    def model_dump_mysql(self, contest_id: int) -> Dict[str, Any]:
        data = self.model_dump(mode="json", exclude={"user"})
        data["contest_id"] = contest_id
        data["created_at"] = self.created_at
        data["updated_at"] = self.updated_at
        data["user_id"] = self.user.user_id
        return data
