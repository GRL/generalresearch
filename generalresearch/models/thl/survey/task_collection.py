import copy
import json
import logging
from typing import List

import pandas as pd
import pandera
from pandera import DataFrameSchema
from pydantic import BaseModel, ConfigDict, Field, model_validator

from generalresearch.models.thl.survey import MarketplaceTask

logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.INFO)


class TaskCollection(BaseModel):
    """I'm calling this a task and not a survey or whatever b/c it will be
    exposed externally to this project and we don't care what the internal
    structure is.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    # overload this with the correct type!
    items: List[MarketplaceTask]
    df: pd.DataFrame = Field(default_factory=pd.DataFrame)

    # overload this with the correct schema!
    _schema: DataFrameSchema

    @model_validator(mode="after")
    def handle_df(self):
        df = self.to_df()
        try:
            df = self._schema.validate(df, lazy=True)
        except pandera.errors.SchemaErrors as exc:
            idx = exc.failure_cases["index"]
            if len(idx) >= len(df) * 0.10:
                raise exc
            logger.info(f"{self.__repr_name__()}:handle_df:{json.dumps(exc.message)}")
            df.drop(index=list(idx), inplace=True)
            # we need to redo the validation after removing failing rows!
            df = self._schema.validate(df)
        self.df = df
        return self

    def to_df(self) -> pd.DataFrame: ...


def create_empty_df_from_schema(schema: DataFrameSchema) -> pd.DataFrame:
    # Create an empty df from the schema. We have to do this or else a plain empty df
    #   will fail validating non-nullable columns b/c they don't have a default.
    schema = copy.deepcopy(schema)
    schema.coerce = True
    schema.add_missing_columns = True
    index = pd.Index([], name=schema.index.name, dtype=schema.index.dtype.type)
    empty_df = schema.coerce_dtype(pd.DataFrame(columns=[*schema.columns], index=index))
    return empty_df
