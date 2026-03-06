from typing import Optional

from math import ceil
from pydantic import BaseModel, Field, computed_field


class Page(BaseModel):
    # Based on fastapi_pagination.Page
    page: int = Field(default=1, ge=1, description="Page number")
    size: int = Field(default=50, ge=1, le=100, description="Page size")
    total: Optional[int] = Field(
        default=None, ge=0, description="Total number of results"
    )

    @computed_field(description="Total number of pages")
    def pages(self) -> Optional[int]:
        if self.size == 0:
            return 0
        elif self.total is not None:
            return ceil(self.total / self.size)
        else:
            return None
