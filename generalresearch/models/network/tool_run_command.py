from typing import Dict

from pydantic import BaseModel, Field


class ToolRunCommand(BaseModel):
    # todo: expand with arguments specific for each tool
    command: str = Field()
    options: Dict[str, str | int] = Field(default_factory=dict)
