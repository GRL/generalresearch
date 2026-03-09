from typing import Any, Dict, Optional
from uuid import uuid4

from pydantic import BaseModel, Field, PositiveInt, model_validator
from typing_extensions import Self

from generalresearch.models.custom_types import UUIDStr


class Category(BaseModel, frozen=True):
    id: Optional[PositiveInt] = Field(exclude=True, default=None)

    uuid: UUIDStr = Field(examples=[uuid4().hex])

    adwords_vertical_id: Optional[str] = Field(default=None, max_length=8)

    label: str = Field(max_length=255, examples=["Hair Loss"])

    # The path is '/' separated string, that shows the full hierarchy.
    #   e.g. "Hair Loss" has the path: "/Beauty & Fitness/Hair Care/Hair Loss"
    path: str = Field(
        pattern=r"^\/.*[^\/]$",
        examples=["/Beauty & Fitness/Hair Care/Hair Loss"],
    )

    parent_id: Optional[PositiveInt] = Field(default=None, exclude=True)
    parent_uuid: Optional[UUIDStr] = Field(default=None, examples=[uuid4().hex])

    @model_validator(mode="after")
    def check_path(self) -> Self:
        assert self.label in self.path, "invalid path"
        return self

    @model_validator(mode="after")
    def check_parent(self) -> Self:
        if self.id is not None:
            assert self.parent_id != self.id, "you can't be your own parent!"
        if self.uuid and self.parent_uuid:
            assert self.parent_uuid != self.uuid, "you can't be your own parent!"
        return self

    @property
    def root_label(self) -> str:
        # If path is "/Beauty & Fitness/Hair Care/Hair Loss", this returns "Beauty & Fitness"
        return self.path.split("/", 2)[1]

    @property
    def parent_path(self) -> Optional[str]:
        # If path is "/Beauty & Fitness/Hair Care/Hair Loss", this returns "/Beauty & Fitness/Hair Care"
        return self.path.rsplit("/", 1)[0] or None

    @property
    def is_root(self) -> bool:
        return self.parent_path is None

    def to_offerwall_api(self) -> Dict[str, Any]:
        return {
            "id": self.uuid,
            "label": self.label,
            "adwords_id": self.adwords_vertical_id,
            "adwords_label": self.label if self.adwords_vertical_id else None,
        }
