from typing import Dict, Optional

from pydantic import BaseModel, Field


class ToolRunCommand(BaseModel):
    # todo: expand with arguments specific for each tool
    command: str = Field()
    options: Dict[str, Optional[str | int]] = Field(default_factory=dict)
