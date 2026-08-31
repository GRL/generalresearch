from __future__ import annotations

from collections.abc import Callable

import pytest

from generalresearch.managers.gr.business import (
    BusinessAddressManager,
    BusinessBankAccountManager,
    BusinessManager,
)
from generalresearch.managers.gr.team import (
    MembershipManager,
    TeamManager,
)
from generalresearch.managers.spectrum.survey import SpectrumSurveyManager
from generalresearch.managers.thl.buyer import BuyerManager
from generalresearch.managers.thl.cashout_method import (
    CashoutMethodManager,
)
from generalresearch.managers.thl.ipinfo import (
    GeoIpInfoManager,
    IPGeonameManager,
    IPInformationManager,
)
from generalresearch.managers.thl.user_streak import (
    UserStreakManager,
)
from generalresearch.managers.thl.userhealth import (
    AuditLogManager,
    IPRecordManager,
    UserIpHistoryManager,
)
from generalresearch.models import Source
from generalresearch.models.thl.wallet.cashout_method import CashoutMethod
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
def ip_information_manager(thl_web_rw: PostgresConfig) -> IPInformationManager:
    assert thl_web_rw.dsn.path
    assert "/unittest-" in thl_web_rw.dsn.path

    from generalresearch.managers.thl.ipinfo import IPInformationManager

    return IPInformationManager(pg_config=thl_web_rw)


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
def user_iphistory_manager_clear_cache(user_iphistory_manager, user):
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

    # TODO: convert these ids into instances to use.
    # settings.amt_bonus_cashout_method_id
    # settings.amt_assignment_cashout_method_id

    # cashout_method_manager.create(AMT_ASSIGNMENT_CASHOUT_METHOD)
    # cashout_method_manager.create(AMT_BONUS_CASHOUT_METHOD)
    # raise NotImplementedError("Need to implement setup_cashoutmethod_db")

    return _inner


# === THL: Marketplaces ===


@pytest.fixture(scope="session")
def spectrum_survey_manager(spectrum_rw: SqlHelper) -> SpectrumSurveyManager:
    from generalresearch.managers.spectrum.survey import (
        SpectrumSurveyManager,
    )

    return SpectrumSurveyManager(sql_helper=spectrum_rw)


# === GR ===
@pytest.fixture(scope="session")
def business_manager(
    gr_db: PostgresConfig, gr_redis_config: RedisConfig
) -> BusinessManager:
    from generalresearch.redis_helper import RedisConfig

    assert gr_db.dsn.path
    assert "/unittest-" in gr_db.dsn.path
    assert isinstance(gr_redis_config, RedisConfig)

    from generalresearch.managers.gr.business import BusinessManager

    return BusinessManager(
        pg_config=gr_db,
        redis_config=gr_redis_config,
    )


@pytest.fixture(scope="session")
def business_address_manager(gr_db: PostgresConfig) -> BusinessAddressManager:
    assert gr_db.dsn.path
    assert "/unittest-" in gr_db.dsn.path

    from generalresearch.managers.gr.business import BusinessAddressManager

    return BusinessAddressManager(pg_config=gr_db)


@pytest.fixture(scope="session")
def business_bank_account_manager(
    gr_db: PostgresConfig,
) -> BusinessBankAccountManager:
    assert gr_db.dsn.path
    assert "/unittest-" in gr_db.dsn.path

    from generalresearch.managers.gr.business import (
        BusinessBankAccountManager,
    )

    return BusinessBankAccountManager(pg_config=gr_db)


@pytest.fixture(scope="session")
def team_manager(gr_db: PostgresConfig, gr_redis_config: RedisConfig) -> TeamManager:
    assert gr_db.dsn.path
    assert "/unittest-" in gr_db.dsn.path

    from generalresearch.managers.gr.team import TeamManager

    return TeamManager(pg_config=gr_db, redis_config=gr_redis_config)


@pytest.fixture(scope="session")
def membership_manager(gr_db: PostgresConfig) -> MembershipManager:
    assert gr_db.dsn.path
    assert "/unittest-" in gr_db.dsn.path

    from generalresearch.managers.gr.team import MembershipManager

    return MembershipManager(pg_config=gr_db)


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
