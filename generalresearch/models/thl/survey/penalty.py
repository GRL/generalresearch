import abc
from datetime import timezone, datetime
from typing import List, Literal, Union

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter
from typing_extensions import Annotated

from generalresearch.models import Source
from generalresearch.models.custom_types import (
    UUIDStr,
    AwareDatetimeISO,
)


class SurveyPenalty(BaseModel, abc.ABC):
    """
    BP or Team-specific penalization to a survey, for the purpose of
    rate-limiting entrances from a BP or Team into a survey
    """

    model_config = ConfigDict(validate_assignment=True, extra="forbid")

    kind: Literal["bp", "team"]

    source: Source = Field()
    survey_id: str = Field(min_length=1, max_length=32)

    penalty: float = Field(ge=0, le=1)

    created: AwareDatetimeISO = Field(
        default_factory=lambda: datetime.now(tz=timezone.utc)
    )

    @property
    def sid(self):
        return f"{self.source.value}:{self.survey_id}"


class BPSurveyPenalty(SurveyPenalty):
    """
    BP-specific penalization to a survey, for the purpose of
    rate-limiting entrances from a BP into a survey
    """

    kind: Literal["bp"] = "bp"
    product_id: UUIDStr = Field(examples=["be40ff316fd4450dbaa53c13cc0cba04"])


class TeamSurveyPenalty(SurveyPenalty):
    """
    Team-specific penalization to a survey, for the purpose of
    rate-limiting entrances from a Team into a survey
    """

    kind: Literal["team"] = "team"
    team_id: UUIDStr = Field(examples=["2ac57f2264334af7874be56a06ef75db"])


Penalty = Annotated[
    Union[BPSurveyPenalty, TeamSurveyPenalty],
    Field(discriminator="kind"),
]
PenaltyListAdapter = TypeAdapter(List[Penalty])
