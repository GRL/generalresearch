from __future__ import annotations

import random
from collections.abc import Callable
from datetime import datetime
from typing import TYPE_CHECKING
from uuid import uuid4

import pytest

from generalresearch.managers.thl.cashout_method import (
    CashoutMethodManager,
)
from generalresearch.managers.thl.user_streak import (
    UserStreakManager,
)
from generalresearch.models.definitions import Source
from generalresearch.models.thl.wallet.cashout_method import (
    CashoutMethod,
    TangoCashoutMethodData,
)
from generalresearch.models.thl.wallet.definitions import Currency, PayoutType

if TYPE_CHECKING:
    from generalresearch.managers.spectrum.survey import SpectrumSurveyManager
    from generalresearch.managers.thl.buyer import BuyerManager
    from generalresearch.managers.thl.ipinfo import (
        GeoIpInfoManager,
        IPGeonameManager,
    )
    from generalresearch.managers.thl.userhealth import (
        AuditLogManager,
        IPRecordManager,
        UserIpHistoryManager,
    )
    from generalresearch.models.thl.user import User
    from generalresearch.pg_helper import PostgresConfig
    from generalresearch.redis_helper import RedisConfig
    from generalresearch.sql_helper import SqlHelper

# === THL ===


@pytest.fixture(scope="session")
def audit_log_manager(thl_web_rw: PostgresConfig) -> AuditLogManager:
    assert thl_web_rw.dsn.path
    assert "/unittest-" in thl_web_rw.dsn.path

    from generalresearch.managers.thl.userhealth import AuditLogManager

    return AuditLogManager(pg_config=thl_web_rw)


@pytest.fixture(scope="session")
def ip_geoname_manager(thl_web_rw: PostgresConfig) -> IPGeonameManager:
    assert thl_web_rw.dsn.path
    assert "/unittest-" in thl_web_rw.dsn.path

    from generalresearch.managers.thl.ipinfo import IPGeonameManager

    return IPGeonameManager(pg_config=thl_web_rw)


@pytest.fixture(scope="session")
def ip_record_manager(
    thl_web_rw: PostgresConfig, thl_redis_config: RedisConfig
) -> IPRecordManager:
    assert thl_web_rw.dsn.path
    assert "/unittest-" in thl_web_rw.dsn.path

    from generalresearch.managers.thl.userhealth import IPRecordManager

    return IPRecordManager(pg_config=thl_web_rw, redis_config=thl_redis_config)


@pytest.fixture(scope="session")
def user_iphistory_manager(
    thl_web_rw: PostgresConfig, thl_redis_config: RedisConfig
) -> UserIpHistoryManager:
    assert thl_web_rw.dsn.path
    assert "/unittest-" in thl_web_rw.dsn.path

    from generalresearch.managers.thl.userhealth import (
        UserIpHistoryManager,
    )

    return UserIpHistoryManager(pg_config=thl_web_rw, redis_config=thl_redis_config)


@pytest.fixture(scope="function")
def user_iphistory_manager_clear_cache(user_iphistory_manager, user: User):
    # On successive py-test/jenkins runs, the cache may contain
    #   the previous run's info (keyed under the same user_id)
    user_iphistory_manager.delete_user_ip_history_cache(user_id=user.user_id)
    yield
    user_iphistory_manager.delete_user_ip_history_cache(user_id=user.user_id)


@pytest.fixture(scope="session")
def geoipinfo_manager(
    thl_web_rw: PostgresConfig, thl_redis_config: RedisConfig
) -> GeoIpInfoManager:
    assert thl_web_rw.dsn.path
    assert "/unittest-" in thl_web_rw.dsn.path

    from generalresearch.managers.thl.ipinfo import GeoIpInfoManager

    return GeoIpInfoManager(pg_config=thl_web_rw, redis_config=thl_redis_config)


@pytest.fixture(scope="session")
def cashout_method_manager(thl_web_rw: PostgresConfig) -> CashoutMethodManager:
    assert thl_web_rw.dsn.path
    assert "/unittest-" in thl_web_rw.dsn.path

    return CashoutMethodManager(pg_config=thl_web_rw)


@pytest.fixture(scope="session")
def event_manager(thl_redis_config: RedisConfig):
    from generalresearch.managers.events import EventManager

    return EventManager(redis_config=thl_redis_config)


@pytest.fixture(scope="session")
def user_streak_manager(thl_web_rw: PostgresConfig) -> UserStreakManager:
    assert thl_web_rw.dsn.path
    assert "/unittest-" in thl_web_rw.dsn.path

    return UserStreakManager(pg_config=thl_web_rw)


@pytest.fixture(scope="session")
def uqa_db_index(thl_web_rw: PostgresConfig):
    # There were some custom indices created not through django.
    # Make sure the index used in the index hint exists
    assert thl_web_rw.dsn.path
    assert "/unittest-" in thl_web_rw.dsn.path

    # query = f"""create index idx_user_id
    # on `{thl_web_rw.db}`.marketplace_userquestionanswer (user_id);"""
    # try:
    #     thl_web_rw.execute_sql_query(query, commit=True)
    # except pymysql.OperationalError as e:
    #     if "Duplicate key name 'idx_user_id'" not in str(e):
    #         raise


@pytest.fixture(scope="session")
def delete_cashoutmethod_db(thl_web_rw: PostgresConfig) -> Callable[..., None]:
    def _delete_cashoutmethod_db():
        thl_web_rw.execute_write(
            query="DELETE FROM accounting_cashoutmethod;",
        )

    return _delete_cashoutmethod_db


@pytest.fixture(scope="session")
def setup_cashoutmethod_db(
    cashout_method_manager: CashoutMethodManager,
    delete_cashoutmethod_db: Callable[..., None],
    example_tango_cashout_methods: list[CashoutMethod],
) -> Callable[..., None]:

    def _inner():
        delete_cashoutmethod_db()

        for x in example_tango_cashout_methods:
            cashout_method_manager.create(x)

    return _inner


@pytest.fixture(scope="session")
def random_ext_id_factory(base: str = "U02") -> Callable[..., str]:

    def _inner() -> str:
        suffix = random.randint(0, 99999)
        return f"{base}{suffix:05d}"

    return _inner


@pytest.fixture(scope="session")
def example_tango_cashout_methods(
    random_ext_id_factory: Callable[..., str],
) -> list[CashoutMethod]:
    return [
        CashoutMethod(
            id=uuid4().hex,
            last_updated=datetime.fromisoformat("2021-06-23T20:45:38.239182Z"),
            is_live=True,
            type=PayoutType.TANGO,
            ext_id='U025035',
            name="Safeway eGift Card $25",
            data=TangoCashoutMethodData(
                value_type="fixed", countries=["US"], utid='U025035'
            ),
            user=None,
            image_url="https://d30s7yzk2az89n.cloudfront.net/images/brands/b694446-1200w-326ppi.png",
            original_currency=Currency.USD,
            min_value=2500,
            max_value=2500,
        ),
        CashoutMethod(
            id=uuid4().hex,
            last_updated=datetime.fromisoformat("2021-06-23T20:45:38.239182Z"),
            is_live=True,
            type=PayoutType.TANGO,
            ext_id='U006961',
            name="Amazon.it Gift Certificate",
            data=TangoCashoutMethodData(
                value_type="variable", countries=["IT"], utid="U006961"
            ),
            user=None,
            image_url="https://d30s7yzk2az89n.cloudfront.net/images/brands/b405753-1200w-326ppi.png",
            original_currency=Currency.EUR,
            min_value=1,
            max_value=10000,
        ),
    ]


# === THL: Marketplaces ===


@pytest.fixture(scope="session")
def spectrum_survey_manager(spectrum_rw: SqlHelper) -> SpectrumSurveyManager:
    from generalresearch.managers.spectrum.survey import (
        SpectrumSurveyManager,
    )

    return SpectrumSurveyManager(sql_helper=spectrum_rw)


@pytest.fixture(scope="session")
def delete_buyers_surveys(
    thl_web_rw: PostgresConfig, buyer_manager: BuyerManager
) -> Callable[..., None]:

    def _inner():
        # assert "/unittest-" in thl_web_rw.dsn.path
        thl_web_rw.execute_write(
            """
        DELETE FROM marketplace_surveystat
        WHERE survey_id IN (
            SELECT id
            FROM marketplace_survey
            WHERE source = %(source)s
        );""",
            params={"source": Source.TESTING.value},
        )
        thl_web_rw.execute_write(
            """
        DELETE FROM marketplace_survey
        WHERE buyer_id IN (
            SELECT id
            FROM marketplace_buyer
            WHERE source = %(source)s
        );""",
            params={"source": Source.TESTING.value},
        )
        thl_web_rw.execute_write(
            """
        DELETE from marketplace_buyer
        WHERE source=%(source)s;
        """,
            params={"source": Source.TESTING.value},
        )
        buyer_manager.populate_caches()

    return _inner
