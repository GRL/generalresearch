from datetime import datetime, timezone

from generalresearch.incite.base import GRLDatasets
from generalresearch.incite.collections import DFCollectionType
from generalresearch.incite.collections.thl_marketplaces import (
    InnovateSurveyHistoryCollection,
    MorningSurveyTimeseriesCollection,
    SagoSurveyHistoryCollection,
    SpectrumSurveyTimeseriesCollection,
)
from generalresearch.incite.collections.thl_web import (
    SessionDFCollection,
    WallDFCollection,
    UserDFCollection,
    TaskAdjustmentDFCollection,
    LedgerDFCollection,
)
from generalresearch.incite.mergers import MergeType
from generalresearch.incite.mergers.foundations.enriched_session import (
    EnrichedSessionMerge,
)
from generalresearch.incite.mergers.foundations.enriched_task_adjust import (
    EnrichedTaskAdjustMerge,
)
from generalresearch.incite.mergers.foundations.enriched_wall import (
    EnrichedWallMerge,
)
from generalresearch.incite.mergers.foundations.user_id_product import (
    UserIdProductMerge,
)
from generalresearch.incite.mergers.pop_ledger import PopLedgerMerge
from generalresearch.incite.mergers.ym_survey_wall import YMSurveyWallMerge
from generalresearch.pg_helper import PostgresConfig
from generalresearch.sql_helper import SqlHelper


# --- THL Web --- #


def session_df_collection(
    ds: "GRLDatasets", pg_config: PostgresConfig
) -> SessionDFCollection:
    return SessionDFCollection(
        offset="37h",
        pg_config=pg_config,
        start=datetime(year=2022, month=5, day=3, hour=12, tzinfo=timezone.utc),
        archive_path=ds.archive_path(enum_type=DFCollectionType.SESSION),
    )


def wall_df_collection(
    ds: "GRLDatasets", pg_config: PostgresConfig
) -> WallDFCollection:
    return WallDFCollection(
        offset="49h",
        pg_config=pg_config,
        start=datetime(year=2022, month=5, day=3, hour=12, tzinfo=timezone.utc),
        archive_path=ds.archive_path(enum_type=DFCollectionType.WALL),
    )


def user_df_collection(
    ds: "GRLDatasets", pg_config: PostgresConfig
) -> UserDFCollection:
    return UserDFCollection(
        offset="73h",
        pg_config=pg_config,
        start=datetime(year=2016, month=7, day=13, hour=1, tzinfo=timezone.utc),
        archive_path=ds.archive_path(enum_type=DFCollectionType.USER),
    )


def task_df_collection(
    ds: "GRLDatasets", pg_config: PostgresConfig
) -> TaskAdjustmentDFCollection:
    return TaskAdjustmentDFCollection(
        offset="48h",
        pg_config=pg_config,
        start=datetime(year=2022, month=7, day=16, hour=0, tzinfo=timezone.utc),
        archive_path=ds.archive_path(enum_type=DFCollectionType.TASK_ADJUSTMENT),
    )


def ledger_df_collection(
    ds: "GRLDatasets", pg_config: PostgresConfig
) -> LedgerDFCollection:
    return LedgerDFCollection(
        offset="12d",
        pg_config=pg_config,
        # thl_web:ledger_transaction - 1st record is 2018-03-14 20:22:17.408232
        start=datetime(year=2018, month=3, day=14, hour=0, tzinfo=timezone.utc),
        archive_path=ds.archive_path(enum_type=DFCollectionType.LEDGER),
    )


# --- Marketplace Specifics --- #
def innovate_survey_history_collection(
    ds: "GRLDatasets", sql_helper: SqlHelper
) -> InnovateSurveyHistoryCollection:
    return InnovateSurveyHistoryCollection(
        offset="12h",
        sql_helper=sql_helper,
        start=datetime(year=2024, month=3, day=1, hour=0, tzinfo=timezone.utc),
        archive_path=ds.archive_path(
            enum_type=DFCollectionType.INNOVATE_SURVEY_HISTORY
        ),
    )


def morning_survey_ts_collection(
    ds: "GRLDatasets", sql_helper: SqlHelper
) -> MorningSurveyTimeseriesCollection:
    return MorningSurveyTimeseriesCollection(
        offset="12h",
        sql_helper=sql_helper,
        start=datetime(year=2024, month=3, day=1, hour=0, tzinfo=timezone.utc),
        archive_path=ds.archive_path(
            enum_type=DFCollectionType.MORNING_SURVEY_TIMESERIES
        ),
    )


def sago_survey_history_collection(
    ds: "GRLDatasets", sql_helper: SqlHelper
) -> SagoSurveyHistoryCollection:
    return SagoSurveyHistoryCollection(
        offset="12h",
        sql_helper=sql_helper,
        start=datetime(year=2024, month=3, day=1, hour=0, tzinfo=timezone.utc),
        archive_path=ds.archive_path(enum_type=DFCollectionType.SAGO_SURVEY_HISTORY),
    )


def spectrum_survey_ts_collection(
    ds: "GRLDatasets", sql_helper: SqlHelper
) -> SpectrumSurveyTimeseriesCollection:
    return SpectrumSurveyTimeseriesCollection(
        offset="12h",
        sql_helper=sql_helper,
        start=datetime(year=2024, month=3, day=1, hour=0, tzinfo=timezone.utc),
        archive_path=ds.archive_path(
            enum_type=DFCollectionType.SPECTRUM_SURVEY_TIMESERIES
        ),
    )


# --- Mergers: Foundations --- #
def user_id_product(ds: "GRLDatasets") -> UserIdProductMerge:
    return UserIdProductMerge(
        start=datetime(year=2010, month=1, day=1, tzinfo=timezone.utc),
        offset=None,
        archive_path=ds.archive_path(enum_type=MergeType.USER_ID_PRODUCT),
    )


def enriched_session(ds: "GRLDatasets") -> EnrichedSessionMerge:
    return EnrichedSessionMerge(
        start=datetime(year=2023, month=5, day=1, tzinfo=timezone.utc),
        offset="14d",
        archive_path=ds.archive_path(enum_type=MergeType.ENRICHED_SESSION),
    )


def enriched_wall(ds: "GRLDatasets") -> EnrichedWallMerge:
    return EnrichedWallMerge(
        # start=datetime(year=2022, month=5, day=1, tzinfo=timezone.utc),
        start=datetime(year=2023, month=7, day=23, tzinfo=timezone.utc),
        offset="14d",
        archive_path=ds.archive_path(enum_type=MergeType.ENRICHED_WALL),
    )


def enriched_task_adjust(ds: "GRLDatasets") -> EnrichedTaskAdjustMerge:
    return EnrichedTaskAdjustMerge(
        start=datetime(year=2010, month=1, day=1, tzinfo=timezone.utc),
        offset=None,
        archive_path=ds.archive_path(enum_type=MergeType.ENRICHED_TASK_ADJUST),
    )


# --- Mergers: Others --- #
def pop_ledger(ds: "GRLDatasets") -> PopLedgerMerge:
    return PopLedgerMerge(
        # thl_web:ledger_transaction - 1st record is 2018-03-14 20:22:17.408232
        start=datetime(year=2018, month=3, day=14, hour=0, tzinfo=timezone.utc),
        offset="30d",
        archive_path=ds.archive_path(enum_type=MergeType.POP_LEDGER),
    )


def ym_survey_wall(ds: "GRLDatasets") -> YMSurveyWallMerge:
    return YMSurveyWallMerge(
        start=None,
        offset="10D",
        archive_path=ds.archive_path(enum_type=MergeType.YM_SURVEY_WALL),
    )
