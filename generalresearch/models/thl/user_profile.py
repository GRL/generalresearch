import hashlib
from typing import Optional, Dict, Any, List

from pydantic import (
    Field,
    BaseModel,
    ConfigDict,
    EmailStr,
    PositiveInt,
    computed_field,
)
from pydantic.json_schema import SkipJsonSchema
from typing_extensions import Self, Annotated

from generalresearch.models import MAX_INT32, Source
from generalresearch.models.custom_types import UUIDStr
from generalresearch.models.thl.user import User
from generalresearch.models.thl.user_streak import UserStreak


class UserMetadata(BaseModel):
    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    user_id: SkipJsonSchema[Optional[PositiveInt]] = Field(
        exclude=True, default=None, lt=MAX_INT32
    )

    email_address: Optional[EmailStr] = Field(
        default=None, examples=["contact@mail.com"]
    )

    @computed_field
    def email_md5(
        self,
    ) -> Annotated[
        Optional[str],
        Field(
            min_length=32,
            max_length=32,
            description="MD5 hash of the email address",
            examples=["053fc3d5575362159e0c782abec83ffa"],
        ),
    ]:
        if self.email_address is None:
            return None

        return hashlib.md5(self.email_address.encode("utf-8")).hexdigest()

    @computed_field
    def email_sha1(
        self,
    ) -> Annotated[
        Optional[str],
        Field(
            min_length=40,
            max_length=40,
            description="SHA1 hash of the email address",
            examples=["6280fb76135b3585c0c5403be04844a0f0bae726"],
        ),
    ]:
        if self.email_address is None:
            return None
        return hashlib.sha1(self.email_address.encode("utf-8")).hexdigest()

    @computed_field
    def email_sha256(
        self,
    ) -> Annotated[
        Optional[str],
        Field(
            min_length=64,
            max_length=64,
            description="SHA256 hash of the email address",
            examples=[
                "8a098233e750f08de87d6053c06a58724287f34372368b6dc28b7ad4a77f3d39"
            ],
        ),
    ]:
        if self.email_address is None:
            return None
        return hashlib.sha256(self.email_address.encode("utf-8")).hexdigest()

    def to_db(self) -> Dict[str, Any]:
        res = self.model_dump(mode="json")
        res["user_id"] = self.user_id
        return res

    @classmethod
    def from_db(cls, user_id, email_address, **kwargs) -> Self:
        # If the hashes are passed, just validate that they match
        obj = cls.model_validate({"user_id": user_id, "email_address": email_address})

        if kwargs.get("email_md5") is not None:
            assert obj.email_md5 == kwargs["email_md5"], "email_md5 mismatch"

        if kwargs.get("email_sha1") is not None:
            assert obj.email_sha1 == kwargs["email_sha1"], "email_sha1 mismatch"

        if kwargs.get("email_sha256") is not None:
            assert obj.email_sha256 == kwargs["email_sha256"], "email_sha256 mismatch"

        return obj


class UserProfile(UserMetadata):
    model_config = ConfigDict()

    user: User = Field()

    marketplace_pids: Dict[Source, UUIDStr] = Field(
        default_factory=dict,
        description="User's PID in marketplaces",
        examples=[
            {
                Source.CINT: "b507a2c00c3e481fb82f23655d142198",
                Source.DYNATA: "deffe922063e4b9980206a62c3df2fba",
                Source.INNOVATE: "1dd9bd986794444eb97cb921aee5663f",
            }
        ],
    )

    streaks: List[UserStreak] = Field(default_factory=list)
