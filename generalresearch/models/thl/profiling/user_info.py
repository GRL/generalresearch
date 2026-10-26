from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field

from generalresearch.models.custom_types import AwareDatetimeISO
from generalresearch.models.definitions import Source
from generalresearch.models.thl.profiling.user_question_answer import (
    MarketplaceResearchProfileQuestion,
)
from generalresearch.models.thl.user_ref import UserRef


class UserProfileKnowledgeAnswer(BaseModel):
    # Returns {id, label, translation} when the prop_type is an item,
    #   and only {value} if it's a string/text (such as for postalcode)
    id: str | None = Field(default=None)
    label: str | None = Field(default=None)
    translation: str | None = Field(default=None)

    value: str | None = Field(default=None)


class UserProfileKnowledge(BaseModel):
    property_id: str = Field()
    property_label: str = Field()
    translation: str = Field()

    answer: list[UserProfileKnowledgeAnswer] = Field(default_factory=list)

    created: AwareDatetimeISO = Field(
        description="When the User submitted this Profiling data"
    )


class MarketProfileKnowledge(BaseModel):
    """
    This is used solely in API responses, so it is simplified.
    """

    source: Source = Field(
        max_length=16, description="Marketplace this question is from"
    )

    question_id: str = Field(examples=["gender", "1843", "gender_plus"])
    question_text: str | None = Field(default=None)

    answer: list[str] = Field(
        default_factory=list, examples=[["male"], ["7657644"], ["1"]]
    )
    answer_text: list[str] | None = Field(default=None)

    created: AwareDatetimeISO = Field(
        description="When the User submitted this Profiling data"
    )
    is_derived: bool = Field(
        default=False,
        description='What this answer derived from a "harmonized" / "gold-standard" question?',
    )

    @classmethod
    def from_MarketplaceResearchProfileQuestion(
        cls, q: MarketplaceResearchProfileQuestion
    ):
        return cls(
            source=q.source,
            question_id=q.question_code,
            answer=list(q.answer),
            created=q.timestamp,
            question_text=q.question_text,
            answer_text=list(q.answer_text) if q.answer_text is not None else None,
            is_derived=q.is_derived,
        )


class UserProfilingData(BaseModel):
    model_config = ConfigDict()

    user: UserRef = Field()

    user_profile_knowledge: list[UserProfileKnowledge] = Field(default_factory=list)

    marketplace_profile_knowledge: list[MarketProfileKnowledge] = Field(
        default_factory=list
    )


class UserInfo(UserProfilingData):
    # fsb endpoint uses this name. todo: rename it
    pass
