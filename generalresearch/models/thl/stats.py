from typing import Optional

from pydantic import BaseModel, Field, computed_field, model_validator


class StatisticalSummary(BaseModel):
    """
    Stores the five-number summary of a dataset. This consists of the minimum,
    first quartile (Q1), median (Q2), third quartile (Q3), and maximum.

    Mean is optional.
    """

    min: int = Field()
    max: int = Field()
    mean: Optional[int] = Field(default=None)
    q1: int = Field()
    q2: int = Field()
    q3: int = Field()

    @model_validator(mode="after")
    def check_values(self):
        assert self.max >= self.min, "invalid max/min"
        assert self.q1 >= self.min, "invalid q1/min"
        assert self.q2 >= self.q1, "invalid q1/q2"
        assert self.q3 >= self.q2, "invalid q2/q3"
        assert self.max >= self.q3, "invalid q3/max"
        return self

    @property
    def iqr(self) -> int:
        # Interquartile Range (IQR)
        return self.q3 - self.q1

    @computed_field
    @property
    def lower_whisker(self) -> int:
        return round(self.q1 - (1.5 * self.iqr))

    @computed_field
    @property
    def upper_whisker(self) -> int:
        return round(self.q3 + (1.5 * self.iqr))
