from enum import Enum
from typing import Annotated

from pydantic import Field

SagoQuestionIdType = Annotated[
    str, Field(min_length=1, max_length=16, pattern=r"^[0-9]+$")
]


class SagoStatus(str, Enum):
    LIVE = "LIVE"
    NOT_LIVE = "NOT_LIVE"
