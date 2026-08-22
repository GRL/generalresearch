from enum import Enum
from typing import Annotated

from pydantic import StringConstraints

# This is text-based, in lowercase. e.g. 'age', 'household_income'
MorningQuestionID = Annotated[
    str, StringConstraints(min_length=1, max_length=64, pattern=r"^[^A-Z]+$")
]


class MorningStatus(str, Enum):
    DRAFT = "draft"
    ACTIVE = "active"  # aka LIVE
    PAUSED = "paused"
    CLOSED = "closed"
