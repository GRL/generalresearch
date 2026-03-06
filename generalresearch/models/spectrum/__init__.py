from enum import Enum

from pydantic import Field
from typing_extensions import Annotated

SpectrumQuestionIdType = Annotated[
    str, Field(min_length=1, max_length=16, pattern=r"^[0-9]+$")
]


class SpectrumStatus(int, Enum):
    DRAFT = 11
    LIVE = 22
    PAUSED = 33
    CLOSED = 44
