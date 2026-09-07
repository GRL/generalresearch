from enum import StrEnum
from typing import Annotated

from pydantic import StringConstraints


class PrecisionStatus(StrEnum):
    # I made this up. They use isactive: "Yes" or "no", which I think is stupid
    OPEN = "open"
    CLOSED = "closed"


# Some questions are strings, like 'state', 'gender', and others are numeric
PrecisionQuestionID = Annotated[
    str, StringConstraints(min_length=1, max_length=32, pattern=r"^[^A-Z]+$")
]
