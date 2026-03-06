from enum import Enum

from pydantic import StringConstraints
from typing_extensions import Annotated

# This is text-based, in lowercase. e.g. 'age', 'household_income'
MorningQuestionID = Annotated[
    str, StringConstraints(min_length=1, max_length=64, pattern=r"^[^A-Z]+$")
]


class MorningStatus(str, Enum):
    DRAFT = "draft"
    ACTIVE = "active"  # aka LIVE
    PAUSED = "paused"
    CLOSED = "closed"
