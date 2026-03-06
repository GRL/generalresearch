from typing import Literal

from generalresearch.incite.collections import DFCollection, DFCollectionType
from generalresearch.incite.schemas.thl_marketplaces import (
    InnovateSurveyHistorySchema,
    MorningSurveyTimeseriesSchema,
    SagoSurveyHistorySchema,
    SpectrumSurveyTimeseriesSchema,
)


class InnovateSurveyHistoryCollection(DFCollection):
    data_type: Literal[DFCollectionType.INNOVATE_SURVEY_HISTORY] = (
        DFCollectionType.INNOVATE_SURVEY_HISTORY
    )
    _schema = InnovateSurveyHistorySchema


class MorningSurveyTimeseriesCollection(DFCollection):
    data_type: Literal[DFCollectionType.MORNING_SURVEY_TIMESERIES] = (
        DFCollectionType.MORNING_SURVEY_TIMESERIES
    )
    _schema = MorningSurveyTimeseriesSchema


class SagoSurveyHistoryCollection(DFCollection):
    data_type: Literal[DFCollectionType.SAGO_SURVEY_HISTORY] = (
        DFCollectionType.SAGO_SURVEY_HISTORY
    )
    _schema = SagoSurveyHistorySchema


class SpectrumSurveyTimeseriesCollection(DFCollection):
    data_type: Literal[DFCollectionType.SPECTRUM_SURVEY_TIMESERIES] = (
        DFCollectionType.SPECTRUM_SURVEY_TIMESERIES
    )
    _schema = SpectrumSurveyTimeseriesSchema
