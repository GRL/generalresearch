from enum import Enum
from functools import cached_property
from typing import List, Optional, Dict
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter

from generalresearch.models.custom_types import UUIDStr, CountryISOLike
from generalresearch.models.thl.category import Category
from generalresearch.utils.enum import ReprEnumMeta


class PropertyType(str, Enum, metaclass=ReprEnumMeta):
    # UserProfileKnowledge Item
    UPK_ITEM = "i"
    # UserProfileKnowledge Numerical
    UPK_NUMERICAL = "n"
    # UserProfileKnowledge Text
    UPK_TEXT = "x"

    # Not used
    # UPK_DATETIME = "a"
    # UPK_TIME = "t"
    # UPK_DATE = "d"


class Cardinality(str, Enum, metaclass=ReprEnumMeta):
    # Zero or More
    ZERO_OR_MORE = "*"
    # Zero or One
    ZERO_OR_ONE = "?"


class UpkItem(BaseModel):
    id: UUIDStr = Field(examples=["497b1fedec464151b063cd5367643ffa"])
    label: str = Field(max_length=255, examples=["high_school_completion"])
    description: Optional[str] = Field(
        max_length=1024, examples=["Completed high school"], default=None
    )


class UpkProperty(BaseModel):
    """
    This used to be called "QuestionInfo", which is a bad name,
        as this describes a UPK Property, like "educational_attainment",
        not the question that asks for your education.
    """

    model_config = ConfigDict(populate_by_name=True)

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

    gold_standard: bool = Field(
        default=False,
        description="A Gold-Standard question has been enumerated for all "
        "possible values (per country) as best as possible by GRL,"
        "allowing it to be mapped across inventory sources. A "
        "property not marked as Gold-Standard may have: 1) "
        "marketplace qid associations & 2) category associations, "
        "but doesn't have a defined 'range' (list of allowed items"
        "in a multiple choice question). "
        "This is used for exposing a user's profiling data & for"
        "the Nudge API.",
    )

    allowed_items: Optional[List[UpkItem]] = Field(default=None)

    categories: List[Category] = Field(default_factory=list)

    @cached_property
    def allowed_items_by_label(self) -> Dict[str, UpkItem]:
        return {i.label: i for i in self.allowed_items}

    @cached_property
    def allowed_items_by_id(self) -> Dict[UUIDStr, UpkItem]:
        return {i.id: i for i in self.allowed_items}


ProfilingInfo = TypeAdapter(List[UpkProperty])
