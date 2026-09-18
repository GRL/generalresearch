from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from typing import TYPE_CHECKING
from unittest.mock import Mock
from uuid import uuid4

import faker
import pytest

from generalresearch.managers.thl.ipinfo import GeoIpInfoManager
from generalresearch.managers.thl.userhealth import (
    AuditLogManager,
    IPRecordManager,
    UserIpHistoryManager,
)
from generalresearch.models.thl.ipinfo import (
    GeoIPInformation,
)
from generalresearch.models.thl.user_iphistory import (
    IPRecord,
    UserIPHistory,
)
from generalresearch.models.thl.userhealth import AuditLog, AuditLogLevel

if TYPE_CHECKING:
    from generalresearch.models.thl.product import Product
    from generalresearch.models.thl.user import User
    from generalresearch.pg_helper import PostgresConfig
    from generalresearch.redis_helper import RedisConfig

fake = faker.Faker()


class TestAuditLog:
    def test_init(self, thl_web_rr: PostgresConfig, audit_log_manager: AuditLogManager):
        alm = AuditLogManager(pg_config=thl_web_rr)

        assert isinstance(alm, AuditLogManager)
        assert isinstance(audit_log_manager, AuditLogManager)
        assert alm.pg_config.db == thl_web_rr.db
        assert audit_log_manager.pg_config.db == thl_web_rr.db

    @pytest.mark.parametrize(
        argnames="level",
        argvalues=list(AuditLogLevel),
    )
    def test_create(
        self, audit_log_manager: AuditLogManager, user: User, level: AuditLogLevel
    ):
        instance = audit_log_manager.create(
            user_id=user.user_id, level=level, event_type=uuid4().hex
        )
        assert isinstance(instance, AuditLog)
        assert instance.id != 0

    def test_get_by_id(self, audit_log: AuditLog, audit_log_manager: AuditLogManager):

        with pytest.raises(expected_exception=Exception) as cm:
            audit_log_manager.get_by_id(auditlog_id=999_999_999_999)
        assert "No AuditLog with id of " in str(cm.value)

        assert isinstance(audit_log, AuditLog)
        res = audit_log_manager.get_by_id(auditlog_id=audit_log.id)
        assert isinstance(res, AuditLog)
        assert res.id == audit_log.id
        assert res.created.tzinfo == UTC

    def test_filter_by_product(
        self,
        user_factory: Callable[..., User],
        product_factory: Callable[..., Product],
        audit_log_factory: Callable[..., AuditLog],
        audit_log_manager: AuditLogManager,
    ):
        p1 = product_factory()
        p2 = product_factory()

        audit_log_factory(user_id=user_factory(product=p1).user_id)
        audit_log_factory(user_id=user_factory(product=p1).user_id)
        audit_log_factory(user_id=user_factory(product=p1).user_id)

        res = audit_log_manager.filter_by_product(product=p2)
        assert isinstance(res, list)
        assert len(res) == 0

        res = audit_log_manager.filter_by_product(product=p1)
        assert isinstance(res, list)
        assert len(res) == 3

        audit_log_factory(user_id=user_factory(product=p2).user_id)
        res = audit_log_manager.filter_by_product(product=p2)
        assert isinstance(res, list)
        assert isinstance(res[0], AuditLog)
        assert len(res) == 1

    def test_filter_by_user_id(
        self,
        user_factory: Callable[..., User],
        product: Product,
        audit_log_factory: Callable[..., AuditLog],
        audit_log_manager: AuditLogManager,
    ):
        u1 = user_factory(product=product)
        u2 = user_factory(product=product)

        audit_log_factory(user_id=u1.user_id)
        audit_log_factory(user_id=u1.user_id)
        audit_log_factory(user_id=u1.user_id)

        res = audit_log_manager.filter_by_user_id(user_id=u1.user_id)
        assert isinstance(res, list)
        assert len(res) == 3

        res = audit_log_manager.filter_by_user_id(user_id=u2.user_id)
        assert isinstance(res, list)
        assert len(res) == 0

        audit_log_factory(user_id=u2.user_id)

        res = audit_log_manager.filter_by_user_id(user_id=u2.user_id)
        assert isinstance(res, list)
        assert isinstance(res[0], AuditLog)
        assert len(res) == 1

    def test_filter(
        self,
        user_factory: Callable[..., User],
        product_factory: Callable[..., Product],
        audit_log_factory: Callable[..., AuditLog],
        audit_log_manager: AuditLogManager,
    ):
        p1 = product_factory()
        p2 = product_factory()
        p3 = product_factory()

        u1 = user_factory(product=p1)
        u2 = user_factory(product=p2)
        u3 = user_factory(product=p3)

        with pytest.raises(expected_exception=AssertionError) as cm:
            audit_log_manager.filter(user_ids=[])
        assert "must pass at least 1 user_id" in str(cm.value)

        with pytest.raises(expected_exception=AssertionError) as cm:
            audit_log_manager.filter(user_ids=[u1, u2, u3])
        assert "must pass user_id as int" in str(cm.value)

        res = audit_log_manager.filter(user_ids=[u1.user_id, u2.user_id, u3.user_id])
        assert isinstance(res, list)
        assert len(res) == 0

        audit_log_factory(user_id=u1.user_id)

        res = audit_log_manager.filter(user_ids=[u1.user_id, u2.user_id, u3.user_id])
        assert isinstance(res, list)
        assert isinstance(res[0], AuditLog)
        assert len(res) == 1

    def test_filter_count(
        self,
        user_factory: Callable[..., User],
        product_factory: Callable[..., Product],
        audit_log_factory: Callable[..., AuditLog],
        audit_log_manager: AuditLogManager,
    ):
        p1 = product_factory()
        p2 = product_factory()
        p3 = product_factory()

        u1 = user_factory(product=p1)
        u2 = user_factory(product=p2)
        u3 = user_factory(product=p3)

        with pytest.raises(expected_exception=AssertionError) as cm:
            audit_log_manager.filter(user_ids=[])
        assert "must pass at least 1 user_id" in str(cm.value)

        with pytest.raises(expected_exception=AssertionError) as cm:
            audit_log_manager.filter(user_ids=[u1, u2, u3])
        assert "must pass user_id as int" in str(cm.value)

        res = audit_log_manager.filter_count(
            user_ids=[u1.user_id, u2.user_id, u3.user_id]
        )
        assert isinstance(res, int)
        assert res == 0

        audit_log_factory(user_id=u1.user_id, level=20)

        res = audit_log_manager.filter_count(
            user_ids=[u1.user_id, u2.user_id, u3.user_id]
        )
        assert isinstance(res, int)
        assert res == 1

        res = audit_log_manager.filter_count(
            user_ids=[u1.user_id, u2.user_id, u3.user_id],
            created_after=datetime.now(tz=UTC),
        )
        assert isinstance(res, int)
        assert res == 0

        res = audit_log_manager.filter_count(
            user_ids=[u1.user_id], event_type_like="offerwall-enter.%%"
        )
        assert res == 1

        audit_log_factory(user_id=u1.user_id, level=50)
        res = audit_log_manager.filter_count(
            user_ids=[u1.user_id],
            event_type_like="offerwall-enter.%%",
            level_ge=10,
        )
        assert res == 2

        res = audit_log_manager.filter_count(
            user_ids=[u1.user_id], event_type_like="poop.%", level_ge=10
        )
        assert res == 0


class TestIPRecordManager:
    def test_init(
        self,
        thl_web_rr: PostgresConfig,
        thl_redis_config: RedisConfig,
        ip_record_manager: IPRecordManager,
    ):
        instance = IPRecordManager(
            pg_config=thl_web_rr,
            redis_config=thl_redis_config,
        )
        assert isinstance(instance, IPRecordManager)
        assert isinstance(ip_record_manager, IPRecordManager)

    def test_create(
        self,
        ip_record_manager: IPRecordManager,
        user: User,
        ip_record_factory: Callable[..., IPRecord],
    ):
        ip = fake.ipv4_public()

        instance = ip_record_factory(user=user, ip=ip)
        assert isinstance(instance, IPRecord)

        assert isinstance(instance.forwarded_ips, list)
        assert isinstance(instance.forwarded_ip_records, list)
        assert isinstance(instance.forwarded_ip_records[0], IPRecord)
        assert isinstance(instance.forwarded_ips[0], str)

        assert instance.created == instance.forwarded_ip_records[0].created

        ipr1 = ip_record_manager.filter_ip_records(filter_ips=[instance.ip])
        assert isinstance(ipr1, list)
        assert instance.model_dump_json() == ipr1[0].model_dump_json()

    def test_prefetch_info(
        self,
        ip_record_factory: Callable[..., IPRecord],
        user: User,
        thl_web_rr: PostgresConfig,
        thl_redis_config: RedisConfig,
    ):
        # No more prefetch info here. Moved into UserIPHistory.enrich_ips
        pass


@pytest.mark.usefixtures("user_iphistory_manager_clear_cache")
class TestUserIpHistoryManager:
    def test_init(
        self,
        thl_web_rr: PostgresConfig,
        thl_redis_config: RedisConfig,
        geoip_info_manager: GeoIpInfoManager,
        user_iphistory_manager: UserIpHistoryManager,
    ):
        instance = UserIpHistoryManager(
            pg_config=thl_web_rr,
            redis_config=thl_redis_config,
            geoip_info_manager=geoip_info_manager,
        )
        assert isinstance(instance, UserIpHistoryManager)
        assert isinstance(user_iphistory_manager, UserIpHistoryManager)

    def test_latest_record_and_enrich(
        self,
        user_iphistory_manager: UserIpHistoryManager,
        user: User,
        ip_record_factory: Callable[..., IPRecord],
        geoip_information_factory: Callable[..., GeoIPInformation],
        geoip_info_manager: GeoIpInfoManager,
    ):
        ip = fake.ipv4_public()
        information = geoip_information_factory(
            ip=ip, is_anonymous=True, country_iso="de"
        )
        lookup_results = {ip: information}
        geoip_info_manager.get_multi = Mock(
            side_effect=lambda ip_addresses: {
                address: lookup_results[address] for address in ip_addresses
            }
        )

        ipr1 = ip_record_factory(user=user, ip=ip)
        ipr = user_iphistory_manager.get_user_latest_ip_record(
            user=user,
        )
        assert ipr.ip == ipr1.ip
        assert ipr.is_anonymous
        assert isinstance(ipr.information, GeoIPInformation)

        assert (
            user_iphistory_manager.get_user_latest_country(
                user=user,
            )
            == "de"
        )

        ip2 = fake.ipv6()
        ipr2: IPRecord = ip_record_factory(user=user, ip=ip2)
        lookup_results[ipr2.ip] = geoip_information_factory(
            ip=ipr2.ip,
            country_iso="us",
            is_anonymous=False,
        )

        ipr = user_iphistory_manager.get_user_latest_ip_record(
            user=user,
        )
        assert ipr.ip == ipr2.ip
        assert isinstance(ipr.information, GeoIPInformation)
        assert ipr.information is not None
        assert not ipr.is_anonymous

        assert (
            user_iphistory_manager.get_user_latest_country(
                user=user,
            )
            == "us"
        )

        iph = user_iphistory_manager.get_user_ip_history(user=user)
        assert isinstance(iph, UserIPHistory)
        assert isinstance(iph.ips, list)
        assert iph.ips[0].information is not None
        assert iph.ips[1].information is not None
        assert iph.ips[0].country_iso == "us"
        assert iph.ips[1].country_iso == "de"
        assert not iph.ips[0].is_anonymous
        assert iph.ips[1].is_anonymous
        # ordered by created DESCENDING!!!!!!!!!!!!!!1
        assert iph.ips[0].ip == ipr2.ip
        assert iph.ips[1].ip == ipr1.ip

    def test_virgin(
        self,
        user: User,
        user_iphistory_manager: UserIpHistoryManager,
        ip_record_factory: Callable[..., IPRecord],
    ):
        iph = user_iphistory_manager.get_user_ip_history(user=user)
        assert len(iph.ips) == 0

        ip_record_factory(user=user, ip=fake.ipv4_public())
        iph = user_iphistory_manager.get_user_ip_history(user=user)
        assert len(iph.ips) == 1
