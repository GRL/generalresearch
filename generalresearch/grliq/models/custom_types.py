import annotated_types
from typing_extensions import Annotated

GrlIqScore = Annotated[int, annotated_types.Ge(0), annotated_types.Le(100)]
GrlIqAvgScore = Annotated[float, annotated_types.Ge(0), annotated_types.Le(100)]
GrlIqRate = Annotated[float, annotated_types.Ge(0), annotated_types.Le(1)]
