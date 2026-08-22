from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime, timedelta, timezone
from uuid import uuid4

import pytest
from pydantic import PostgresDsn

from generalresearch.config import GRLBaseSettings
from generalresearch.grliq.managers import DUMMY_GRLIQ_DATA
from generalresearch.grliq.managers.forensic_data import (
    GrlIqDataManager,
)
from generalresearch.grliq.managers.forensic_events import (
    GrlIqEventManager,
)
from generalresearch.grliq.managers.forensic_results import (
    GrlIqCategoryResultsReader,
)
from generalresearch.grliq.models.forensic_data import GrlIqData
from generalresearch.pg_helper import PostgresConfig

# === Miscellaneous ===


@pytest.fixture(scope="function")
def mnt_grliq_archive_dir(settings: GRLBaseSettings) -> str | None:
    return settings.mnt_grliq_archive_dir


@pytest.fixture(scope="session")
def grliq_db(postgres_instance: PostgresDsn) -> PostgresConfig:
    # TODO: This will need to specificy a different DATABASE on the
    #   Postgres SERVER. That selection process will also need to
    #   selectively migrate only the tables from grliq

    return PostgresConfig(
        dsn=postgres_instance,
        connect_timeout=1,
        statement_timeout=5,
    )


# === Managers ===


@pytest.fixture(scope="session")
def grliq_dm(grliq_db: PostgresConfig) -> GrlIqDataManager:
    assert grliq_db.dsn.path
    assert "/unittest-" in grliq_db.dsn.path
    return GrlIqDataManager(postgres_config=grliq_db)


@pytest.fixture(scope="session")
def grliq_em(grliq_db: PostgresConfig) -> GrlIqEventManager:
    assert grliq_db.dsn.path
    assert "/unittest-" in grliq_db.dsn.path

    from generalresearch.grliq.managers.forensic_events import (
        GrlIqEventManager,
    )

    return GrlIqEventManager(postgres_config=grliq_db)


@pytest.fixture(scope="session")
def grliq_crr(grliq_db: PostgresConfig) -> GrlIqCategoryResultsReader:
    assert grliq_db.dsn.path
    assert "/unittest-" in grliq_db.dsn.path

    return GrlIqCategoryResultsReader(postgres_config=grliq_db)


# === Models ===


@pytest.fixture(scope="function")
def grliq_data() -> GrlIqData:
    from generalresearch.grliq.managers import DUMMY_GRLIQ_DATA

    g: GrlIqData = DUMMY_GRLIQ_DATA[1]["data"]

    g.id = None
    g.uuid = uuid4().hex
    g.created_at = datetime.now(tz=UTC)
    g.timestamp = g.created_at - timedelta(seconds=10)
    return g


@pytest.fixture
def grliq_data_factory(grliq_dm: GrlIqDataManager) -> Callable[..., GrlIqData]:

    def _inner(
        is_attempt_allowed: bool = True,
        product_id: str | None = None,
        product_user_id: str | None = None,
        uuid: str | None = None,
        mid: str | None = None,
        created_at: datetime | None = None,
    ) -> GrlIqData:
        """
        Creates a dummy record in the db with a GrlIqData (data), GrlIqCheckerResults (result_data),
            and GrlIqForensicCategoryResult (category_results)
        :param is_attempt_allowed: Whether the attempt is allowed.
        :param product_id: product_id of user
        :param product_user_id:  product_user_id of user
        :param uuid: uuid for the grliq data record
        :param mid: the thl_session:uuid / mid for the attempt.
        :return:
        """
        import copy

        res: GrlIqData = copy.deepcopy(DUMMY_GRLIQ_DATA[int(is_attempt_allowed)])

        product_id = product_id or uuid4().hex
        product_user_id = product_user_id or uuid4().hex
        uuid = uuid or uuid4().hex
        mid = mid or uuid4().hex
        created_at = created_at or datetime.now(tz=UTC)

        res["data"].product_id = product_id
        res["data"].product_user_id = product_user_id
        res["data"].uuid = uuid
        res["data"].mid = mid
        res["data"].created_at = created_at
        res["result_data"].uuid = uuid
        res["category_result"].uuid = uuid

        return grliq_dm.create(
            iq_data=res["data"],
            result_data=res["result_data"],
            category_result=res["category_result"],
            fraud_score=res["category_result"].fraud_score,
            is_attempt_allowed=res["category_result"].is_attempt_allowed(),
        )

    return _inner
