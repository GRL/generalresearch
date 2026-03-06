from datetime import datetime, timezone
from typing import Optional, Union, Dict
from uuid import uuid4

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    PositiveInt,
    model_validator,
    computed_field,
)
from typing_extensions import Self

from generalresearch.models import MAX_INT32
from generalresearch.models.custom_types import (
    UUIDStr,
    AwareDatetimeISO,
    CountryISOLike,
)
from generalresearch.models.thl.profiling.upk_property import (
    PropertyType,
    Cardinality,
)


class UpkQuestionAnswer(BaseModel):
    """ """

    model_config = ConfigDict(populate_by_name=True)

    user_id: PositiveInt = Field(lt=MAX_INT32)

    question_id: Optional[UUIDStr] = Field(
        examples=[uuid4().hex],
        description="The ID of the question that was asked in order to determine this",
        default=None,
    )
    session_id: Optional[UUIDStr] = Field(
        examples=[uuid4().hex],
        description="The thl_session in which the question was asked",
        default=None,
    )

    property_id: UUIDStr = Field(examples=[uuid4().hex])

    property_label: str = Field(max_length=255, examples=["educational_attainment"])

    prop_type: PropertyType = Field(
        default=PropertyType.UPK_ITEM,
        description=PropertyType.as_openapi_with_value_descriptions(),
    )

    cardinality: Cardinality = Field(
        default=Cardinality.ZERO_OR_ONE,
        description=Cardinality.as_openapi_with_value_descriptions(),
    )

    # ISO 3166-1 alpha-2 (two-letter codes, lowercase)
    country_iso: CountryISOLike = Field()

    created: AwareDatetimeISO = Field(
        default_factory=lambda: datetime.now(tz=timezone.utc)
    )

    # If the property is PropertyType.UPK_ITEM, it should have an item (and no value).
    # If the property is UPK_NUMERICAL or UPK_TEXT, it'll have a value (and no item).
    item_id: Optional[UUIDStr] = Field(
        default=None, examples=["497b1fedec464151b063cd5367643ffa"]
    )
    item_label: Optional[str] = Field(
        default=None, max_length=255, examples=["high_school_completion"]
    )
    value_text: Optional[str] = Field(
        default=None,
        max_length=1024,
    )
    value_num: Optional[float] = Field(
        default=None,
    )

    @computed_field
    @property
    def value(self) -> Optional[Union[str, float]]:
        if self.prop_type == PropertyType.UPK_ITEM:
            return self.item_label
        elif self.prop_type == PropertyType.UPK_TEXT:
            return self.value_text
        elif self.prop_type == PropertyType.UPK_NUMERICAL:
            return self.value_num

    @model_validator(mode="after")
    def check_value_vs_item(self) -> Self:
        if self.prop_type == PropertyType.UPK_ITEM:
            if not self.item_id or not self.item_label:
                raise ValueError("item_id and item_label must be provided for UPK_ITEM")
            if self.value_num is not None or self.value_text is not None:
                raise ValueError("value and value_text must be None for UPK_ITEM")

        elif self.prop_type in {
            PropertyType.UPK_NUMERICAL,
            PropertyType.UPK_TEXT,
        }:
            if self.item_id or self.item_label:
                raise ValueError("item_id and item_label must be None for non-UPK_ITEM")
            if self.prop_type == PropertyType.UPK_NUMERICAL and self.value_num is None:
                raise ValueError("value must be provided for UPK_NUMERICAL")
            if self.prop_type == PropertyType.UPK_TEXT and self.value_text is None:
                raise ValueError("value_text must be provided for UPK_TEXT")

        return self

    def model_dump_mysql(self) -> Dict:
        d = self.model_dump(mode="json")
        d["created"] = self.created
        return d
