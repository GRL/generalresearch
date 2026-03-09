from __future__ import annotations

from typing import Any, Dict, Optional, Tuple

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    computed_field,
)

from generalresearch.models.custom_types import (
    AwareDatetimeISO,
    CountryISOLike,
    LanguageISOLike,
    UUIDStr,
)
from generalresearch.models.thl.profiling.upk_question import UpkQuestion


class Question(BaseModel):
    model_config = ConfigDict(validate_assignment=True)

    id: Optional[UUIDStr] = Field(default=None, alias="question_id")
    # ISO 3166-1 alpha-2 (two-letter codes, lowercase)
    country_iso: CountryISOLike = Field()
    # 3-char ISO 639-2/B, lowercase
    language_iso: LanguageISOLike = Field()

    property_code: Optional[str] = Field(
        default=None,
        description="What marketplace question this question links to",
        pattern=r"^[a-z]{1,2}\:.*",
    )
    data: UpkQuestion = Field()
    is_live: bool = Field()
    custom: Dict[str, Any] = Field(default_factory=dict)
    last_updated: AwareDatetimeISO = Field()

    @computed_field
    @property
    def md5sum(self) -> str:
        return self.data.md5sum

    def validate_question_answer(self, answer: Tuple[str, ...]) -> Tuple[bool, str]:
        return self.data.validate_question_answer(answer=answer)
