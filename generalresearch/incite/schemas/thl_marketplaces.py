import copy
from datetime import timedelta

import pandas as pd
from pandera import Check, Column, DataFrameSchema, Index

from generalresearch.incite.schemas import ARCHIVE_AFTER, ORDER_KEY

BIGINT = 9223372036854775807

SurveyHistorySchemaMeta = {
    "index": Index(
        name="id", dtype=int, checks=Check.between(min_value=0, max_value=BIGINT)
    ),
    "columns": {
        # "survey_id": # fill this in in implementations
        "key": Column(
            dtype="Int32",
            checks=Check.between(min_value=0, max_value=5),
            nullable=False,
        ),
        "value": Column(dtype="Int64", nullable=True),
        "date": Column(dtype=pd.DatetimeTZDtype(tz="UTC"), nullable=False),
    },
    "checks": [],
    "coerce": True,
    "metadata": {ORDER_KEY: "date", ARCHIVE_AFTER: timedelta(minutes=1)},
}

InnovateSurveyHistorySchemaDict = copy.deepcopy(SurveyHistorySchemaMeta)
InnovateSurveyHistorySchemaDict["columns"]["survey_id"] = Column(
    dtype=str, checks=Check.str_length(min_value=1, max_value=32), nullable=False
)
# global_conversion (5) is a float
InnovateSurveyHistorySchemaDict["columns"]["value"] = Column(dtype=float, nullable=True)
InnovateSurveyHistorySchema = DataFrameSchema(**InnovateSurveyHistorySchemaDict)

MorningSurveyTimeseriesSchemaDict = copy.deepcopy(SurveyHistorySchemaMeta)
MorningSurveyTimeseriesSchemaDict["columns"]["bid_id"] = Column(
    dtype=str, checks=Check.str_length(min_value=1, max_value=32), nullable=False
)
MorningSurveyTimeseriesSchema = DataFrameSchema(**MorningSurveyTimeseriesSchemaDict)

SagoSurveyHistorySchemaDict = copy.deepcopy(SurveyHistorySchemaMeta)
SagoSurveyHistorySchemaDict["columns"]["survey_id"] = Column(
    dtype=str, checks=Check.str_length(min_value=1, max_value=32), nullable=False
)
# They send us the client_conversion as a float
SagoSurveyHistorySchemaDict["columns"]["value"] = Column(dtype=float, nullable=True)
# We added 3 new keys: [3, 4, 5]. We don't need [0, 1, 2].
SagoSurveyHistorySchemaDict["columns"]["key"] = Column(
    dtype="Int32", checks=Check.between(min_value=0, max_value=5), nullable=False
)
SagoSurveyHistorySchema = DataFrameSchema(**SagoSurveyHistorySchemaDict)

SpectrumSurveyTimeseriesSchemaDict = copy.deepcopy(SurveyHistorySchemaMeta)
SpectrumSurveyTimeseriesSchemaDict["columns"]["survey_id"] = Column(
    dtype=str, checks=Check.str_length(min_value=1, max_value=32), nullable=False
)
# Keys 1 & 3 (ir) are floats
SpectrumSurveyTimeseriesSchemaDict["columns"]["value"] = Column(
    dtype=float, nullable=True
)
SpectrumSurveyTimeseriesSchema = DataFrameSchema(**SpectrumSurveyTimeseriesSchemaDict)
