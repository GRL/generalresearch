from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Callable, Optional

import pytest

from generalresearch.pg_helper import PostgresConfig
from test_utils.conftest import clear_directory

if TYPE_CHECKING:
    from generalresearch.incite.base import DFCollectionType, GRLDatasets
    from generalresearch.incite.collections import DFCollection
    from generalresearch.incite.collections.thl_web import (
        AuditLogDFCollection,
        LedgerDFCollection,
        SessionDFCollection,
        TaskAdjustmentDFCollection,
        UserDFCollection,
        WallDFCollection,
    )


@pytest.fixture
def user_collection(
    mnt_filepath: "GRLDatasets",
    offset: str,
    duration: timedelta,
    start: datetime,
    thl_web_rr: PostgresConfig,
) -> "UserDFCollection":
    from generalresearch.incite.collections.thl_web import (
        DFCollectionType,
        UserDFCollection,
    )

    return UserDFCollection(
        start=start,
        finished=start + duration,
        offset=offset,
        pg_config=thl_web_rr,
        archive_path=mnt_filepath.archive_path(enum_type=DFCollectionType.USER),
    )


@pytest.fixture
def wall_collection(
    mnt_filepath: "GRLDatasets",
    offset: str,
    duration: timedelta,
    start: datetime,
    thl_web_rr: PostgresConfig,
) -> "WallDFCollection":
    from generalresearch.incite.collections.thl_web import (
        DFCollectionType,
        WallDFCollection,
    )

    return WallDFCollection(
        start=start,
        finished=start + duration if duration else None,
        offset=offset,
        pg_config=thl_web_rr,
        archive_path=mnt_filepath.archive_path(enum_type=DFCollectionType.WALL),
    )


@pytest.fixture
def session_collection(
    mnt_filepath: "GRLDatasets",
    offset: str,
    duration: timedelta,
    start: datetime,
    thl_web_rr: PostgresConfig,
) -> "SessionDFCollection":
    from generalresearch.incite.collections.thl_web import (
        DFCollectionType,
        SessionDFCollection,
    )

    return SessionDFCollection(
        start=start,
        finished=start + duration if duration else None,
        offset=offset,
        pg_config=thl_web_rr,
        archive_path=mnt_filepath.archive_path(enum_type=DFCollectionType.SESSION),
    )


# IPInfoDFCollection
# IPHistoryDFCollection
# IPHistoryWSDFCollection

# @pytest.fixture
# def ip_history_collection(mnt_filepath, offset, duration, start,
#                           thl_web_rw) -> IPHistoryDFCollection:
#     return IPHistoryDFCollection(
#         start=start,
#         finished=start + duration,
#         offset=offset,
#         pg_config=thl_web_rw,
#         archive_path=mnt_filepath.archive_path(enum_type=DFCollectionType.IP_HISTORY),
#     )


@pytest.fixture
def task_adj_collection(
    mnt_filepath: "GRLDatasets",
    offset: str,
    duration: Optional[timedelta],
    start: datetime,
    thl_web_rr: PostgresConfig,
) -> "TaskAdjustmentDFCollection":
    from generalresearch.incite.collections.thl_web import (
        DFCollectionType,
        TaskAdjustmentDFCollection,
    )

    return TaskAdjustmentDFCollection(
        start=start,
        finished=start + duration if duration else duration,
        offset=offset,
        pg_config=thl_web_rr,
        archive_path=mnt_filepath.archive_path(
            enum_type=DFCollectionType.TASK_ADJUSTMENT
        ),
    )


@pytest.fixture
def auditlog_collection(
    mnt_filepath: "GRLDatasets",
    offset: str,
    duration: timedelta,
    start: datetime,
    thl_web_rr: PostgresConfig,
) -> "AuditLogDFCollection":
    from generalresearch.incite.collections.thl_web import (
        AuditLogDFCollection,
        DFCollectionType,
    )

    return AuditLogDFCollection(
        start=start,
        finished=start + duration,
        offset=offset,
        pg_config=thl_web_rr,
        archive_path=mnt_filepath.archive_path(enum_type=DFCollectionType.LEDGER),
    )


@pytest.fixture
def ledger_collection(
    mnt_filepath: "GRLDatasets",
    offset: str,
    duration: timedelta,
    start: datetime,
    thl_web_rr: PostgresConfig,
) -> "LedgerDFCollection":
    from generalresearch.incite.collections.thl_web import (
        DFCollectionType,
        LedgerDFCollection,
    )

    return LedgerDFCollection(
        start=start,
        finished=start + duration if duration else duration,
        offset=offset,
        pg_config=thl_web_rr,
        archive_path=mnt_filepath.archive_path(enum_type=DFCollectionType.LEDGER),
    )


@pytest.fixture
def rm_ledger_collection(
    ledger_collection: "LedgerDFCollection",
) -> Callable[..., None]:

    def _inner():
        clear_directory(ledger_collection.archive_path)

    return _inner


# --------------------------
#      Generic / Base
# --------------------------


@pytest.fixture
def df_collection(
    mnt_filepath: "GRLDatasets",
    df_collection_data_type: "DFCollectionType",
    offset: str,
    duration: timedelta,
    utc_90days_ago: datetime,
    thl_web_rr: PostgresConfig,
) -> "DFCollection":
    from generalresearch.incite.collections import DFCollection

    start = utc_90days_ago.replace(microsecond=0)

    return DFCollection(
        data_type=df_collection_data_type,
        archive_path=mnt_filepath.archive_path(enum_type=df_collection_data_type),
        offset=offset,
        pg_config=thl_web_rr,
        start=start,
        finished=start + duration,
    )
