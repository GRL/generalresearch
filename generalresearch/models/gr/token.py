from datetime import UTC, datetime

from pydantic import BaseModel, Field

from generalresearch.models.custom_types import AwareDatetimeISO


class TokenCreate(BaseModel):
    name: str = Field(default="Default")


class TokenCreated(BaseModel):
    id: str = Field()  # This is the public_id!
    name: str = Field()
    token: str = Field()  # Only shown on creation
    created_at: AwareDatetimeISO = Field(default_factory=lambda: datetime.now(tz=UTC))


class TokenSummary(BaseModel):
    """Returned when listing tokens - token value is hidden."""

    id: str = Field()  # This is the public_id!
    name: str = Field()
    is_active: bool = Field()
    revoked_at: AwareDatetimeISO | None = Field(default=None)
    last_used_at: AwareDatetimeISO | None = Field(default=None)
    created_at: AwareDatetimeISO = Field(default_factory=lambda: datetime.now(tz=UTC))
