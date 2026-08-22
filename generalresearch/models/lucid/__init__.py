from typing import Annotated

from pydantic import Field

LucidQuestionIdType = Annotated[
    str, Field(min_length=1, max_length=16, pattern=r"^[0-9]+$")
]
