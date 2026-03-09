from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Callable

import pytest

from test_utils.conftest import clear_directory

if TYPE_CHECKING:
    from generalresearch.incite.base import GRLDatasets
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
    from generalresearch.incite.mergers.pop_ledger import (
        PopLedgerMerge,
        PopLedgerMergeItem,
    )
    from generalresearch.incite.mergers.ym_survey_wall import (
        YMSurveyWallMerge,
        YMSurveyWallMergeCollectionItem,
    )
    from generalresearch.incite.mergers.ym_wall_summary import (
        YMWallSummaryMerge,
        YMWallSummaryMergeItem,
    )


# --------------------------
#      Merges
# --------------------------


@pytest.fixture
def rm_pop_ledger_merge(pop_ledger_merge: "PopLedgerMerge") -> Callable[..., None]:

    def _inner():
        clear_directory(pop_ledger_merge.archive_path)

    return _inner


@pytest.fixture
def pop_ledger_merge(
    mnt_filepath: "GRLDatasets",
    offset: str,
    start: datetime,
    duration: timedelta,
) -> "PopLedgerMerge":

    from generalresearch.incite.mergers import MergeType
    from generalresearch.incite.mergers.pop_ledger import PopLedgerMerge

    return PopLedgerMerge(
        start=start,
        finished=start + duration if duration else None,
        offset=offset,
        archive_path=mnt_filepath.archive_path(enum_type=MergeType.POP_LEDGER),
    )


@pytest.fixture
def pop_ledger_merge_item(
    start: datetime,
    pop_ledger_merge: "PopLedgerMerge",
) -> "PopLedgerMergeItem":

    from generalresearch.incite.mergers.pop_ledger import PopLedgerMergeItem

    return PopLedgerMergeItem(
        start=start,
        _collection=pop_ledger_merge,
    )


@pytest.fixture
def ym_survey_wall_merge(
    mnt_filepath: "GRLDatasets",
    start: datetime,
) -> "YMSurveyWallMerge":
    from generalresearch.incite.mergers import MergeType
    from generalresearch.incite.mergers.ym_survey_wall import YMSurveyWallMerge

    return YMSurveyWallMerge(
        start=None,
        offset="10D",
        archive_path=mnt_filepath.archive_path(enum_type=MergeType.YM_SURVEY_WALL),
    )


@pytest.fixture
def ym_survey_wall_merge_item(
    start: datetime, ym_survey_wall_merge: "YMSurveyWallMerge"
) -> "YMSurveyWallMergeCollectionItem":
    from generalresearch.incite.mergers.ym_survey_wall import (
        YMSurveyWallMergeCollectionItem,
    )

    return YMSurveyWallMergeCollectionItem(
        start=start,
        _collection=pop_ledger_merge,
    )


@pytest.fixture
def ym_wall_summary_merge(
    mnt_filepath: "GRLDatasets",
    offset: str,
    duration: timedelta,
    start: datetime,
) -> "YMWallSummaryMerge":
    from generalresearch.incite.mergers import MergeType
    from generalresearch.incite.mergers.ym_wall_summary import YMWallSummaryMerge

    return YMWallSummaryMerge(
        start=start,
        finished=start + duration,
        offset=offset,
        archive_path=mnt_filepath.archive_path(enum_type=MergeType.POP_LEDGER),
    )


def ym_wall_summary_merge_item(
    start: datetime, ym_wall_summary_merge: "YMWallSummaryMerge"
) -> "YMWallSummaryMergeItem":
    from generalresearch.incite.mergers.ym_wall_summary import (
        YMWallSummaryMergeItem,
    )

    return YMWallSummaryMergeItem(
        start=start,
        _collection=pop_ledger_merge,
    )


# --------------------------
#      Merges: Foundations
# --------------------------


@pytest.fixture
def enriched_session_merge(
    mnt_filepath: "GRLDatasets",
    offset: str,
    duration: timedelta,
    start: datetime,
) -> "EnrichedSessionMerge":
    from generalresearch.incite.mergers import MergeType
    from generalresearch.incite.mergers.foundations.enriched_session import (
        EnrichedSessionMerge,
    )

    return EnrichedSessionMerge(
        start=start,
        finished=start + duration if duration else None,
        offset=offset,
        archive_path=mnt_filepath.archive_path(enum_type=MergeType.ENRICHED_SESSION),
    )


@pytest.fixture
def enriched_task_adjust_merge(
    mnt_filepath: "GRLDatasets",
    offset: str,
    duration: timedelta,
    start: datetime,
) -> "EnrichedTaskAdjustMerge":
    from generalresearch.incite.mergers import MergeType
    from generalresearch.incite.mergers.foundations.enriched_task_adjust import (
        EnrichedTaskAdjustMerge,
    )

    return EnrichedTaskAdjustMerge(
        start=start,
        finished=start + duration,
        offset=offset,
        archive_path=mnt_filepath.archive_path(
            enum_type=MergeType.ENRICHED_TASK_ADJUST
        ),
    )


@pytest.fixture
def enriched_wall_merge(
    mnt_filepath: "GRLDatasets",
    offset: str,
    duration: timedelta,
    start: datetime,
) -> "EnrichedWallMerge":
    from generalresearch.incite.mergers import MergeType
    from generalresearch.incite.mergers.foundations.enriched_wall import (
        EnrichedWallMerge,
    )

    return EnrichedWallMerge(
        start=start,
        finished=start + duration if duration else None,
        offset=offset,
        archive_path=mnt_filepath.archive_path(enum_type=MergeType.ENRICHED_WALL),
    )


@pytest.fixture
def user_id_product_merge(
    mnt_filepath: "GRLDatasets",
    duration: timedelta,
    offset: str,
    start: datetime,
) -> "UserIdProductMerge":
    from generalresearch.incite.mergers import MergeType
    from generalresearch.incite.mergers.foundations.user_id_product import (
        UserIdProductMerge,
    )

    return UserIdProductMerge(
        start=start,
        finished=start + duration,
        offset=None,
        archive_path=mnt_filepath.archive_path(enum_type=MergeType.USER_ID_PRODUCT),
    )


# --------------------------
#      Generic / Base
# --------------------------


@pytest.fixture
def merge_collection(
    mnt_filepath: "GRLDatasets",
    merge_type: "MergeType",
    offset: str,
    duration: timedelta,
    start: datetime,
):
    from generalresearch.incite.mergers import MergeCollection

    return MergeCollection(
        merge_type=merge_type,
        start=start,
        finished=start + duration,
        offset=offset,
        archive_path=mnt_filepath.archive_path(enum_type=merge_type),
    )
