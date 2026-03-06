from enum import Enum

from pydantic import Field
from typing_extensions import Annotated

SagoQuestionIdType = Annotated[
    str, Field(min_length=1, max_length=16, pattern=r"^[0-9]+$")
]


class SagoStatus(str, Enum):
    LIVE = "LIVE"
    NOT_LIVE = "NOT_LIVE"
