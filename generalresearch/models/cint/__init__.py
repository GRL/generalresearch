from typing import Annotated

from pydantic import Field

CintQuestionIdType = Annotated[
    str, Field(min_length=1, max_length=16, pattern=r"^[0-9]+$")
]
