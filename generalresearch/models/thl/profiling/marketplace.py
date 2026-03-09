from abc import ABC, abstractmethod
from datetime import datetime, timezone
from functools import cached_property
from typing import Any, Dict, Optional, Set, Tuple

from pydantic import BaseModel, ConfigDict, Field, PositiveInt, computed_field

from generalresearch.models import MAX_INT32, Source
from generalresearch.models.custom_types import (
    AwareDatetimeISO,
    CountryISOLike,
    LanguageISOLike,
    UUIDStr,
)
from generalresearch.models.thl.locales import CountryISO, LanguageISO


class MarketplaceQuestion(BaseModel, ABC):
    model_config = ConfigDict(extra="allow")

    # ISO 3166-1 alpha-2 (two-letter codes, lowercase)
    country_iso: CountryISOLike = Field(frozen=True)

    # 3-char ISO 639-2/B, lowercase
    language_iso: LanguageISOLike = Field(frozen=True)

    # To avoid deleting questions, if a question no longer comes back in the
    #   API response (or in some cases, depending on how the question library
    #   is retrieved, if the question is not used by any live surveys), we'll
    #   mark it as not live.
    is_live: bool = Field(default=True)

    # This should be an "abstract field", but there is no way to do that, so
    #   just listing it here. It should be overridden by the implementation
    source: Source = Field()

    # Refers to a Category that we annotate. The info is stored in different
    #   dbs, so it may not be possible to retrieve the Category from the id,
    #   so we just store the id here.
    category_id: Optional[UUIDStr] = Field(default=None)

    # # This doesn't work
    # @property
    # @abstractmethod
    # def source(self) -> Source:
    #     ...

    @property
    @abstractmethod
    def internal_id(self) -> str:
        """This is the value that is used for this question within the
        marketplace. Typically, this is question_id. Innovate uses question_key.
        """
        ...

    @property
    def external_id(self) -> str:
        return f"{self.source.value}:{self.internal_id}"

    @property
    def _key(self) -> Tuple[str, CountryISOLike, LanguageISOLike]:
        """This uniquely identifies a question in a locale. There is a unique
        index on this in the db. e.g. (question_id, country_iso, language_iso)
        """
        return self.internal_id, self.country_iso, self.language_iso

    @abstractmethod
    def to_upk_question(self): ...

    @computed_field
    def num_options(self) -> Optional[int]:
        return len(self.options) if self.options is not None else None

    def __hash__(self):
        # We need this so this obj can be added into a set.
        return hash(self._key)

    def __repr__(self) -> str:
        # Fancy repr that only shows the first and last 3 options if the
        #   question has more than 6.
        repr_args = list(self.__repr_args__())
        for n, (k, v) in enumerate(repr_args):
            if k == "options":
                if v and len(v) > 6:
                    v = v[:3] + ["..."] + v[-3:]
                    repr_args[n] = ("options", v)
        join_str = ", "
        repr_str = join_str.join(
            repr(v) if a is None else f"{a}={v!r}" for a, v in repr_args
        )
        return f"{self.__repr_name__()}({repr_str})"


class MarketplaceUserQuestionAnswer(BaseModel):
    # This is optional b/c this model can be used for eligibility checks for
    #   "anonymous" users, which are represented by a list of question answers
    #   not associated with an actual user. No default b/c we must explicitly
    #   set the field to None.
    user_id: Optional[PositiveInt] = Field(lt=MAX_INT32)

    question_id: str = Field()

    # This is optional b/c we do not need it when writing these to the db. When
    #   these are fetched from the db for use in yield-management, we read this
    #   field from the marketplace's question table.
    #   This should be overloaded in each implementation !!!
    question_type: Optional[str] = Field(default=None)

    # This may be a pipe-separated string if the question_type is multi. Regex
    #   means any chars except capital letters
    option_id: str = Field(pattern=r"^[^A-Z]*$")
    created: AwareDatetimeISO = Field(
        default_factory=lambda: datetime.now(tz=timezone.utc)
    )
    country_iso: CountryISO = Field(frozen=True)
    language_iso: LanguageISO = Field(frozen=True)

    @cached_property
    def options_ids(self) -> Set[str]:
        return set(self.option_id.split("|"))

    @property
    def pre_code(self) -> str:
        return self.option_id

    def to_mysql(self) -> Dict[str, Any]:
        d = self.model_dump(mode="json", exclude={"question_type"})
        d["created"] = self.created.replace(tzinfo=None)
        return d
