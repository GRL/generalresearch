from pydantic import Field

from typing_extensions import Annotated

LucidQuestionIdType = Annotated[
    str, Field(min_length=1, max_length=16, pattern=r"^[0-9]+$")
]
