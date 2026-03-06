from datetime import datetime, timezone, timedelta
from os.path import join as pjoin
from pathlib import Path
from random import choice as randchoice
from shutil import rmtree
from typing import Callable, TYPE_CHECKING, Optional
from uuid import uuid4

import pytest
from _pytest.fixtures import SubRequest
from faker import Faker

from test_utils.managers.ledger.conftest import session_with_tx_factory
from test_utils.models.conftest import session_factory

if TYPE_CHECKING:
    from generalresearch.models.thl.user import User
    from generalresearch.incite.base import GRLDatasets
    from generalresearch.incite.mergers import MergeType
    from generalresearch.incite.collections import (
        DFCollection,
        DFCollectionType,
        DFCollectionItem,
    )

fake = Faker()


@pytest.fixture(scope="function")
def mnt_gr_api_dir(request: SubRequest, settings):
    p = Path(settings.mnt_gr_api_dir)
    p.mkdir(parents=True, exist_ok=True)

    from generalresearch.models.admin.request import ReportType

    for e in list(ReportType):
        Path(pjoin(p, e.value)).mkdir(exist_ok=True)

    def tmp_file_teardown():
        assert "/mnt/" not in str(p), (
            "Under no condition, testing or otherwise should we have code delete "
            " any folders or potential data on a network mount"
        )

        rmtree(p)

    request.addfinalizer(tmp_file_teardown)

    return p


@pytest.fixture(scope="function")
def event_report_request(utc_hour_ago, start):
    from generalresearch.models.admin.request import (
        ReportRequest,
        ReportType,
    )

    return ReportRequest.model_validate(
        {
            "report_type": ReportType.POP_EVENT,
            "interval": "5min",
            "start": start,
        }
    )


@pytest.fixture(scope="function")
def session_report_request(utc_hour_ago, start):
    from generalresearch.models.admin.request import (
        ReportRequest,
        ReportType,
    )

    return ReportRequest.model_validate(
        {
            "report_type": ReportType.POP_SESSION,
            "interval": "5min",
            "start": start,
        }
    )


@pytest.fixture(scope="function")
def mnt_filepath(request: SubRequest) -> "GRLDatasets":
    """Creates a temporary file path for all DFCollections & Mergers parquet
    files.
    """
    from generalresearch.incite.base import GRLDatasets, NFSMount

    instance = GRLDatasets(
        data_src=Path(pjoin("/tmp", f"test-{uuid4().hex[:12]}")),
        incite=NFSMount(point="thl-incite"),
    )

    def tmp_file_teardown():
        assert "/mnt/" not in str(instance.data_src), (
            "Under no condition, testing or otherwise should we have code delete "
            " any folders or potential data on a network mount"
        )

        rmtree(instance.data_src)

    request.addfinalizer(tmp_file_teardown)

    return instance


@pytest.fixture(scope="function")
def start(utc_90days_ago) -> "datetime":
    s = utc_90days_ago.replace(microsecond=0)
    return s


@pytest.fixture(scope="function")
def offset() -> str:
    return "15min"


@pytest.fixture(scope="function")
def duration() -> Optional["timedelta"]:
    return timedelta(hours=1)


@pytest.fixture(scope="function")
def df_collection_data_type() -> "DFCollectionType":
    from generalresearch.incite.collections import DFCollectionType

    return DFCollectionType.TEST


@pytest.fixture(scope="function")
def merge_type() -> "MergeType":
    from generalresearch.incite.mergers import MergeType

    return MergeType.TEST


@pytest.fixture(scope="function")
def incite_item_factory(
    session_factory,
    product,
    user_factory,
    session_with_tx_factory,
) -> Callable:
    def _incite_item_factory(
        item: "DFCollectionItem",
        observations: int = 3,
        user: Optional["User"] = None,
    ):
        from generalresearch.incite.collections import (
            DFCollection,
            DFCollectionType,
        )
        from generalresearch.models.thl.session import Source

        collection: DFCollection = item._collection
        data_type: DFCollectionType = collection.data_type

        for idx in range(5):
            item_time = fake.date_time_between(
                start_date=item.start, end_date=item.finish, tzinfo=timezone.utc
            )

            match data_type:
                case DFCollectionType.USER:
                    user_factory(product=product, created=item_time)

                case DFCollectionType.LEDGER:
                    session_with_tx_factory(started=item_time, user=user)

                case DFCollectionType.WALL:
                    u = (
                        user
                        if user
                        else user_factory(product=product, created=item_time)
                    )
                    session_factory(
                        user=u,
                        started=item_time,
                        wall_source=randchoice(list(Source)),
                    )

                case DFCollectionType.SESSION:
                    u = (
                        user
                        if user
                        else user_factory(product=product, created=item_time)
                    )
                    session_factory(
                        user=u,
                        started=item_time,
                        wall_source=randchoice(list(Source)),
                    )

                case _:
                    raise ValueError("Unsupported DFCollectionItem")

        return None

    return _incite_item_factory
