from datetime import timedelta, datetime
from typing import TYPE_CHECKING, Optional, Callable

import pytest

from test_utils.incite.conftest import mnt_filepath
from test_utils.conftest import clear_directory

if TYPE_CHECKING:
    from generalresearch.incite.collections import DFCollection
    from generalresearch.incite.base import GRLDatasets, DFCollectionType
    from generalresearch.incite.collections.thl_web import LedgerDFCollection
    from generalresearch.incite.collections.thl_web import (
        WallDFCollection,
        SessionDFCollection,
        TaskAdjustmentDFCollection,
        UserDFCollection,
        AuditLogDFCollection,
    )


@pytest.fixture(scope="function")
def user_collection(
    mnt_filepath: "GRLDatasets",
    offset: str,
    duration: timedelta,
    start: datetime,
    thl_web_rr,
) -> "UserDFCollection":
    from generalresearch.incite.collections.thl_web import (
        UserDFCollection,
        DFCollectionType,
    )

    return UserDFCollection(
        start=start,
        finished=start + duration,
        offset=offset,
        pg_config=thl_web_rr,
        archive_path=mnt_filepath.archive_path(enum_type=DFCollectionType.USER),
    )


@pytest.fixture(scope="function")
def wall_collection(
    mnt_filepath: "GRLDatasets",
    offset: str,
    duration: timedelta,
    start: datetime,
    thl_web_rr,
) -> "WallDFCollection":
    from generalresearch.incite.collections.thl_web import (
        WallDFCollection,
        DFCollectionType,
    )

    return WallDFCollection(
        start=start,
        finished=start + duration if duration else None,
        offset=offset,
        pg_config=thl_web_rr,
        archive_path=mnt_filepath.archive_path(enum_type=DFCollectionType.WALL),
    )


@pytest.fixture(scope="function")
def session_collection(
    mnt_filepath: "GRLDatasets",
    offset: str,
    duration: timedelta,
    start: datetime,
    thl_web_rr,
) -> "SessionDFCollection":
    from generalresearch.incite.collections.thl_web import (
        SessionDFCollection,
        DFCollectionType,
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


@pytest.fixture(scope="function")
def task_adj_collection(
    mnt_filepath: "GRLDatasets",
    offset: str,
    duration: Optional[timedelta],
    start: datetime,
    thl_web_rr,
) -> "TaskAdjustmentDFCollection":
    from generalresearch.incite.collections.thl_web import (
        TaskAdjustmentDFCollection,
        DFCollectionType,
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


@pytest.fixture(scope="function")
def auditlog_collection(
    mnt_filepath: "GRLDatasets",
    offset: str,
    duration: timedelta,
    start: datetime,
    thl_web_rr,
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


@pytest.fixture(scope="function")
def ledger_collection(
    mnt_filepath: "GRLDatasets",
    offset: str,
    duration: timedelta,
    start: datetime,
    thl_web_rr,
) -> "LedgerDFCollection":
    from generalresearch.incite.collections.thl_web import (
        LedgerDFCollection,
        DFCollectionType,
    )

    return LedgerDFCollection(
        start=start,
        finished=start + duration if duration else duration,
        offset=offset,
        pg_config=thl_web_rr,
        archive_path=mnt_filepath.archive_path(enum_type=DFCollectionType.LEDGER),
    )


@pytest.fixture(scope="function")
def rm_ledger_collection(ledger_collection) -> Callable:
    def _rm_ledger_collection():
        clear_directory(ledger_collection.archive_path)

    return _rm_ledger_collection


# --------------------------
#      Generic / Base
# --------------------------


@pytest.fixture(scope="function")
def df_collection(
    mnt_filepath,
    df_collection_data_type: "DFCollectionType",
    offset,
    duration,
    utc_90days_ago,
    thl_web_rr,
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
