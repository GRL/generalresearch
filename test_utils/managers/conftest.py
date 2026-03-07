from typing import TYPE_CHECKING, Callable

import pymysql
import pytest

from generalresearch.managers.base import Permission
from generalresearch.models import Source
from test_utils.managers.cashout_methods import (
    EXAMPLE_TANGO_CASHOUT_METHODS,
)

if TYPE_CHECKING:
    from generalresearch.grliq.managers.forensic_data import (
        GrlIqDataManager,
    )
    from generalresearch.grliq.managers.forensic_events import (
        GrlIqEventManager,
    )
    from generalresearch.grliq.managers.forensic_results import (
        GrlIqCategoryResultsReader,
    )
    from generalresearch.managers.gr.authentication import (
        GRTokenManager,
        GRUserManager,
    )
    from generalresearch.managers.gr.business import (
        BusinessAddressManager,
        BusinessBankAccountManager,
        BusinessManager,
    )
    from generalresearch.managers.gr.team import (
        MembershipManager,
        TeamManager,
    )
    from generalresearch.managers.thl.contest_manager import ContestManager
    from generalresearch.managers.thl.ipinfo import (
        GeoIpInfoManager,
        IPGeonameManager,
        IPInformationManager,
    )
    from generalresearch.managers.thl.ledger_manager.ledger import (
        LedgerAccountManager,
        LedgerManager,
        LedgerTransactionManager,
    )
    from generalresearch.managers.thl.ledger_manager.thl_ledger import (
        ThlLedgerManager,
    )
    from generalresearch.managers.thl.maxmind import MaxmindManager
    from generalresearch.managers.thl.maxmind.basic import (
        MaxmindBasicManager,
    )
    from generalresearch.managers.thl.payout import (
        BrokerageProductPayoutEventManager,
        BusinessPayoutEventManager,
        PayoutEventManager,
        UserPayoutEventManager,
    )
    from generalresearch.managers.thl.product import ProductManager
    from generalresearch.managers.thl.session import SessionManager
    from generalresearch.managers.thl.task_adjustment import (
        TaskAdjustmentManager,
    )
    from generalresearch.managers.thl.user_manager.user_manager import (
        UserManager,
    )
    from generalresearch.managers.thl.user_manager.user_metadata_manager import (
        UserMetadataManager,
    )
    from generalresearch.managers.thl.userhealth import (
        AuditLogManager,
        IPGeonameManager,
        IPInformationManager,
        IPRecordManager,
        UserIpHistoryManager,
    )
    from generalresearch.managers.thl.wall import (
        WallCacheManager,
        WallManager,
    )


# === THL ===


@pytest.fixture(scope="session")
def ltxm(thl_web_rw, thl_redis_config) -> "LedgerTransactionManager":
    assert "/unittest-" in thl_web_rw.dsn.path

    from generalresearch.managers.thl.ledger_manager.ledger import (
        LedgerTransactionManager,
    )

    return LedgerTransactionManager(
        sql_helper=thl_web_rw,
        permissions=[Permission.CREATE, Permission.READ],
        testing=True,
        redis_config=thl_redis_config,
    )


@pytest.fixture(scope="session")
def lam(thl_web_rw, thl_redis_config) -> "LedgerAccountManager":
    assert "/unittest-" in thl_web_rw.dsn.path

    from generalresearch.managers.thl.ledger_manager.ledger import (
        LedgerAccountManager,
    )

    return LedgerAccountManager(
        pg_config=thl_web_rw,
        permissions=[Permission.CREATE, Permission.READ],
        testing=True,
        redis_config=thl_redis_config,
    )


@pytest.fixture(scope="session")
def lm(thl_web_rw, thl_redis_config) -> "LedgerManager":
    assert "/unittest-" in thl_web_rw.dsn.path

    from generalresearch.managers.thl.ledger_manager.ledger import (
        LedgerManager,
    )

    return LedgerManager(
        pg_config=thl_web_rw,
        permissions=[
            Permission.CREATE,
            Permission.READ,
            Permission.UPDATE,
            Permission.DELETE,
        ],
        testing=True,
        redis_config=thl_redis_config,
    )


@pytest.fixture(scope="session")
def thl_lm(thl_web_rw, thl_redis_config) -> "ThlLedgerManager":
    assert "/unittest-" in thl_web_rw.dsn.path

    from generalresearch.managers.thl.ledger_manager.thl_ledger import (
        ThlLedgerManager,
    )

    return ThlLedgerManager(
        pg_config=thl_web_rw,
        permissions=[
            Permission.CREATE,
            Permission.READ,
            Permission.UPDATE,
            Permission.DELETE,
        ],
        testing=True,
        redis_config=thl_redis_config,
    )


@pytest.fixture(scope="session")
def payout_event_manager(thl_web_rw, thl_redis_config) -> "PayoutEventManager":
    assert "/unittest-" in thl_web_rw.dsn.path

    from generalresearch.managers.thl.payout import PayoutEventManager

    return PayoutEventManager(
        pg_config=thl_web_rw,
        permissions=[Permission.CREATE, Permission.READ],
        redis_config=thl_redis_config,
    )


@pytest.fixture(scope="session")
def user_payout_event_manager(thl_web_rw, thl_redis_config) -> "UserPayoutEventManager":
    assert "/unittest-" in thl_web_rw.dsn.path

    from generalresearch.managers.thl.payout import UserPayoutEventManager

    return UserPayoutEventManager(
        pg_config=thl_web_rw,
        permissions=[Permission.CREATE, Permission.READ],
        redis_config=thl_redis_config,
    )


@pytest.fixture(scope="session")
def brokerage_product_payout_event_manager(
    thl_web_rw, thl_redis_config
) -> "BrokerageProductPayoutEventManager":
    assert "/unittest-" in thl_web_rw.dsn.path

    from generalresearch.managers.thl.payout import (
        BrokerageProductPayoutEventManager,
    )

    return BrokerageProductPayoutEventManager(
        pg_config=thl_web_rw,
        permissions=[Permission.CREATE, Permission.READ],
        redis_config=thl_redis_config,
    )


@pytest.fixture(scope="session")
def business_payout_event_manager(
    thl_web_rw, thl_redis_config
) -> "BusinessPayoutEventManager":
    assert "/unittest-" in thl_web_rw.dsn.path

    from generalresearch.managers.thl.payout import (
        BusinessPayoutEventManager,
    )

    return BusinessPayoutEventManager(
        pg_config=thl_web_rw,
        permissions=[Permission.CREATE, Permission.READ],
        redis_config=thl_redis_config,
    )


@pytest.fixture(scope="session")
def product_manager(thl_web_rw) -> "ProductManager":
    assert "/unittest-" in thl_web_rw.dsn.path

    from generalresearch.managers.thl.product import ProductManager

    return ProductManager(pg_config=thl_web_rw)


@pytest.fixture(scope="session")
def user_manager(settings, thl_web_rw, thl_web_rr) -> "UserManager":
    assert "/unittest-" in thl_web_rw.dsn.path
    assert "/unittest-" in thl_web_rr.dsn.path

    from generalresearch.managers.thl.user_manager.user_manager import (
        UserManager,
    )

    return UserManager(
        pg_config=thl_web_rw,
        pg_config_rr=thl_web_rr,
        redis=settings.redis,
    )


@pytest.fixture(scope="session")
def user_metadata_manager(thl_web_rw) -> "UserMetadataManager":
    assert "/unittest-" in thl_web_rw.dsn.path

    from generalresearch.managers.thl.user_manager.user_metadata_manager import (
        UserMetadataManager,
    )

    return UserMetadataManager(pg_config=thl_web_rw)


@pytest.fixture(scope="session")
def session_manager(thl_web_rw) -> "SessionManager":
    assert "/unittest-" in thl_web_rw.dsn.path

    from generalresearch.managers.thl.session import SessionManager

    return SessionManager(pg_config=thl_web_rw)


@pytest.fixture(scope="session")
def wall_manager(thl_web_rw) -> "WallManager":
    assert "/unittest-" in thl_web_rw.dsn.path

    from generalresearch.managers.thl.wall import WallManager

    return WallManager(pg_config=thl_web_rw)


@pytest.fixture(scope="session")
def wall_cache_manager(thl_web_rw, thl_redis_config) -> "WallCacheManager":
    # assert "/unittest-" in thl_web_rw.dsn.path

    from generalresearch.managers.thl.wall import WallCacheManager

    return WallCacheManager(pg_config=thl_web_rw, redis_config=thl_redis_config)


@pytest.fixture(scope="session")
def task_adjustment_manager(thl_web_rw) -> "TaskAdjustmentManager":
    # assert "/unittest-" in thl_web_rw.dsn.path

    from generalresearch.managers.thl.task_adjustment import (
        TaskAdjustmentManager,
    )

    return TaskAdjustmentManager(pg_config=thl_web_rw)


@pytest.fixture(scope="session")
def contest_manager(thl_web_rw) -> "ContestManager":
    assert "/unittest-" in thl_web_rw.dsn.path

    from generalresearch.managers.thl.contest_manager import ContestManager

    return ContestManager(
        pg_config=thl_web_rw,
        permissions=[
            Permission.CREATE,
            Permission.READ,
            Permission.UPDATE,
            Permission.DELETE,
        ],
    )


@pytest.fixture(scope="session")
def category_manager(thl_web_rw):
    assert "/unittest-" in thl_web_rw.dsn.path
    from generalresearch.managers.thl.category import CategoryManager

    return CategoryManager(pg_config=thl_web_rw)


@pytest.fixture(scope="session")
def buyer_manager(thl_web_rw):
    # assert "/unittest-" in thl_web_rw.dsn.path
    from generalresearch.managers.thl.buyer import BuyerManager

    return BuyerManager(pg_config=thl_web_rw)


@pytest.fixture(scope="session")
def survey_manager(thl_web_rw):
    # assert "/unittest-" in thl_web_rw.dsn.path
    from generalresearch.managers.thl.survey import SurveyManager

    return SurveyManager(pg_config=thl_web_rw)


@pytest.fixture(scope="session")
def surveystat_manager(thl_web_rw):
    # assert "/unittest-" in thl_web_rw.dsn.path
    from generalresearch.managers.thl.survey import SurveyStatManager

    return SurveyStatManager(pg_config=thl_web_rw)


@pytest.fixture(scope="session")
def surveypenalty_manager(thl_redis_config):
    from generalresearch.managers.thl.survey_penalty import SurveyPenaltyManager

    return SurveyPenaltyManager(redis_config=thl_redis_config)


@pytest.fixture(scope="session")
def upk_schema_manager(thl_web_rw):
    assert "/unittest-" in thl_web_rw.dsn.path
    from generalresearch.managers.thl.profiling.schema import (
        UpkSchemaManager,
    )

    return UpkSchemaManager(pg_config=thl_web_rw)


@pytest.fixture(scope="session")
def user_upk_manager(thl_web_rw, thl_redis_config):
    assert "/unittest-" in thl_web_rw.dsn.path
    from generalresearch.managers.thl.profiling.user_upk import (
        UserUpkManager,
    )

    return UserUpkManager(pg_config=thl_web_rw, redis_config=thl_redis_config)


@pytest.fixture(scope="session")
def question_manager(thl_web_rw, thl_redis_config):
    assert "/unittest-" in thl_web_rw.dsn.path
    from generalresearch.managers.thl.profiling.question import (
        QuestionManager,
    )

    return QuestionManager(pg_config=thl_web_rw)


@pytest.fixture(scope="session")
def uqa_manager(thl_web_rw, thl_redis_config):
    assert "/unittest-" in thl_web_rw.dsn.path
    from generalresearch.managers.thl.profiling.uqa import UQAManager

    return UQAManager(redis_config=thl_redis_config, pg_config=thl_web_rw)


@pytest.fixture(scope="function")
def uqa_manager_clear_cache(uqa_manager, user):
    # On successive py-test/jenkins runs, the cache may contain
    #   the previous run's info (keyed under the same user_id)
    uqa_manager.clear_cache(user)
    yield
    uqa_manager.clear_cache(user)


@pytest.fixture(scope="session")
def audit_log_manager(thl_web_rw) -> "AuditLogManager":
    assert "/unittest-" in thl_web_rw.dsn.path

    from generalresearch.managers.thl.userhealth import AuditLogManager

    return AuditLogManager(pg_config=thl_web_rw)


@pytest.fixture(scope="session")
def ip_geoname_manager(thl_web_rw) -> "IPGeonameManager":
    assert "/unittest-" in thl_web_rw.dsn.path

    from generalresearch.managers.thl.ipinfo import IPGeonameManager

    return IPGeonameManager(pg_config=thl_web_rw)


@pytest.fixture(scope="session")
def ip_information_manager(thl_web_rw) -> "IPInformationManager":
    assert "/unittest-" in thl_web_rw.dsn.path

    from generalresearch.managers.thl.ipinfo import IPInformationManager

    return IPInformationManager(pg_config=thl_web_rw)


@pytest.fixture(scope="session")
def ip_record_manager(thl_web_rw, thl_redis_config) -> "IPRecordManager":
    assert "/unittest-" in thl_web_rw.dsn.path

    from generalresearch.managers.thl.userhealth import IPRecordManager

    return IPRecordManager(pg_config=thl_web_rw, redis_config=thl_redis_config)


@pytest.fixture(scope="session")
def user_iphistory_manager(thl_web_rw, thl_redis_config) -> "UserIpHistoryManager":
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
def geoipinfo_manager(thl_web_rw, thl_redis_config) -> "GeoIpInfoManager":
    assert "/unittest-" in thl_web_rw.dsn.path

    from generalresearch.managers.thl.ipinfo import GeoIpInfoManager

    return GeoIpInfoManager(pg_config=thl_web_rw, redis_config=thl_redis_config)


@pytest.fixture(scope="session")
def maxmind_basic_manager() -> "MaxmindBasicManager":
    from generalresearch.managers.thl.maxmind.basic import (
        MaxmindBasicManager,
    )

    return MaxmindBasicManager(data_dir="/tmp/")


@pytest.fixture(scope="session")
def maxmind_manager(thl_web_rw, thl_redis_config) -> "MaxmindManager":
    assert "/unittest-" in thl_web_rw.dsn.path

    from generalresearch.managers.thl.maxmind import MaxmindManager

    return MaxmindManager(pg_config=thl_web_rw, redis_config=thl_redis_config)


@pytest.fixture(scope="session")
def cashout_method_manager(thl_web_rw):
    assert "/unittest-" in thl_web_rw.dsn.path
    from generalresearch.managers.thl.cashout_method import (
        CashoutMethodManager,
    )

    return CashoutMethodManager(pg_config=thl_web_rw)


@pytest.fixture(scope="session")
def event_manager(thl_redis_config):
    from generalresearch.managers.events import EventManager

    return EventManager(redis_config=thl_redis_config)


@pytest.fixture(scope="session")
def user_streak_manager(thl_web_rw):
    assert "/unittest-" in thl_web_rw.dsn.path
    from generalresearch.managers.thl.user_streak import (
        UserStreakManager,
    )

    return UserStreakManager(pg_config=thl_web_rw)


@pytest.fixture(scope="session")
def uqa_db_index(thl_web_rw):
    # There were some custom indices created not through django.
    # Make sure the index used in the index hint exists
    assert "/unittest-" in thl_web_rw.dsn.path

    # query = f"""create index idx_user_id
    # on `{thl_web_rw.db}`.marketplace_userquestionanswer (user_id);"""
    # try:
    #     thl_web_rw.execute_sql_query(query, commit=True)
    # except pymysql.OperationalError as e:
    #     if "Duplicate key name 'idx_user_id'" not in str(e):
    #         raise
    return None


@pytest.fixture(scope="session")
def delete_cashoutmethod_db(thl_web_rw) -> Callable:
    def _delete_cashoutmethod_db():
        thl_web_rw.execute_write(
            query="DELETE FROM accounting_cashoutmethod;",
        )

    return _delete_cashoutmethod_db


@pytest.fixture(scope="session")
def setup_cashoutmethod_db(settings, cashout_method_manager, delete_cashoutmethod_db):
    settings.amt_

    delete_cashoutmethod_db()
    for x in EXAMPLE_TANGO_CASHOUT_METHODS:
        cashout_method_manager.create(x)
    cashout_method_manager.create(AMT_ASSIGNMENT_CASHOUT_METHOD)
    cashout_method_manager.create(AMT_BONUS_CASHOUT_METHOD)
    return None


# === THL: Marketplaces ===


@pytest.fixture(scope="session")
def spectrum_manager(spectrum_rw):
    from generalresearch.managers.spectrum.survey import (
        SpectrumSurveyManager,
    )

    return SpectrumSurveyManager(sql_helper=spectrum_rw)


# === GR ===
@pytest.fixture(scope="session")
def business_manager(gr_db, gr_redis_config) -> "BusinessManager":
    from generalresearch.redis_helper import RedisConfig

    assert "/unittest-" in gr_db.dsn.path
    assert isinstance(gr_redis_config, RedisConfig)

    from generalresearch.managers.gr.business import BusinessManager

    return BusinessManager(
        pg_config=gr_db,
        redis_config=gr_redis_config,
    )


@pytest.fixture(scope="session")
def business_address_manager(gr_db) -> "BusinessAddressManager":
    assert "/unittest-" in gr_db.dsn.path

    from generalresearch.managers.gr.business import BusinessAddressManager

    return BusinessAddressManager(pg_config=gr_db)


@pytest.fixture(scope="session")
def business_bank_account_manager(gr_db) -> "BusinessBankAccountManager":
    assert "/unittest-" in gr_db.dsn.path

    from generalresearch.managers.gr.business import (
        BusinessBankAccountManager,
    )

    return BusinessBankAccountManager(pg_config=gr_db)


@pytest.fixture(scope="session")
def team_manager(gr_db, gr_redis_config) -> "TeamManager":
    assert "/unittest-" in gr_db.dsn.path

    from generalresearch.managers.gr.team import TeamManager

    return TeamManager(pg_config=gr_db, redis_config=gr_redis_config)


@pytest.fixture(scope="session")
def gr_um(gr_db, gr_redis_config) -> "GRUserManager":
    assert "/unittest-" in gr_db.dsn.path

    from generalresearch.managers.gr.authentication import GRUserManager

    return GRUserManager(pg_config=gr_db, redis_config=gr_redis_config)


@pytest.fixture(scope="session")
def gr_tm(gr_db) -> "GRTokenManager":
    assert "/unittest-" in gr_db.dsn.path

    from generalresearch.managers.gr.authentication import GRTokenManager

    return GRTokenManager(pg_config=gr_db)


@pytest.fixture(scope="session")
def membership_manager(gr_db) -> "MembershipManager":
    assert "/unittest-" in gr_db.dsn.path

    from generalresearch.managers.gr.team import MembershipManager

    return MembershipManager(pg_config=gr_db)


# === GRL IQ ===


@pytest.fixture(scope="session")
def grliq_dm(grliq_db) -> "GrlIqDataManager":
    assert "/unittest-" in grliq_db.dsn.path

    from generalresearch.grliq.managers.forensic_data import (
        GrlIqDataManager,
    )

    return GrlIqDataManager(postgres_config=grliq_db)


@pytest.fixture(scope="session")
def grliq_em(grliq_db) -> "GrlIqEventManager":
    assert "/unittest-" in grliq_db.dsn.path

    from generalresearch.grliq.managers.forensic_events import (
        GrlIqEventManager,
    )

    return GrlIqEventManager(postgres_config=grliq_db)


@pytest.fixture(scope="session")
def grliq_crr(grliq_db) -> "GrlIqCategoryResultsReader":
    assert "/unittest-" in grliq_db.dsn.path

    from generalresearch.grliq.managers.forensic_results import (
        GrlIqCategoryResultsReader,
    )

    return GrlIqCategoryResultsReader(postgres_config=grliq_db)


@pytest.fixture(scope="session")
def delete_buyers_surveys(thl_web_rw, buyer_manager):
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
