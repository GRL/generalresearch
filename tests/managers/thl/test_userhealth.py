from datetime import timezone, datetime
from uuid import uuid4

import faker
import pytest

from generalresearch.managers.thl.userhealth import (
    IPRecordManager,
    UserIpHistoryManager,
)
from generalresearch.models.thl.ipinfo import GeoIPInformation
from generalresearch.models.thl.user_iphistory import (
    IPRecord,
)
from generalresearch.models.thl.userhealth import AuditLogLevel, AuditLog

fake = faker.Faker()


class TestAuditLog:

    def test_init(self, thl_web_rr, audit_log_manager):
        from generalresearch.managers.thl.userhealth import AuditLogManager

        alm = AuditLogManager(pg_config=thl_web_rr)

        assert isinstance(alm, AuditLogManager)
        assert isinstance(audit_log_manager, AuditLogManager)
        assert alm.pg_config.db == thl_web_rr.db
        assert audit_log_manager.pg_config.db == thl_web_rr.db

    @pytest.mark.parametrize(
        argnames="level",
        argvalues=list(AuditLogLevel),
    )
    def test_create(self, audit_log_manager, user, level):
        instance = audit_log_manager.create(
            user_id=user.user_id, level=level, event_type=uuid4().hex
        )
        assert isinstance(instance, AuditLog)
        assert instance.id != 1

    def test_get_by_id(self, audit_log, audit_log_manager):
        from generalresearch.models.thl.userhealth import AuditLog

        with pytest.raises(expected_exception=Exception) as cm:
            audit_log_manager.get_by_id(auditlog_id=999_999_999_999)
        assert "No AuditLog with id of " in str(cm.value)

        assert isinstance(audit_log, AuditLog)
        res = audit_log_manager.get_by_id(auditlog_id=audit_log.id)
        assert isinstance(res, AuditLog)
        assert res.id == audit_log.id
        assert res.created.tzinfo == timezone.utc

    def test_filter_by_product(
        self,
        user_factory,
        product_factory,
        audit_log_factory,
        audit_log_manager,
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
        self, user_factory, product, audit_log_factory, audit_log_manager
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
        user_factory,
        product_factory,
        audit_log_factory,
        audit_log_manager,
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
        user_factory,
        product_factory,
        audit_log_factory,
        audit_log_manager,
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
            created_after=datetime.now(tz=timezone.utc),
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

    def test_init(self, thl_web_rr, thl_redis_config, ip_record_manager):
        instance = IPRecordManager(pg_config=thl_web_rr, redis_config=thl_redis_config)
        assert isinstance(instance, IPRecordManager)
        assert isinstance(ip_record_manager, IPRecordManager)

    def test_create(self, ip_record_manager, user, ip_information):
        instance = ip_record_manager.create_dummy(
            user_id=user.user_id, ip=ip_information.ip
        )
        assert isinstance(instance, IPRecord)

        assert isinstance(instance.forwarded_ips, list)
        assert isinstance(instance.forwarded_ip_records[0], IPRecord)
        assert isinstance(instance.forwarded_ips[0], str)

        assert instance.created == instance.forwarded_ip_records[0].created

        ipr1 = ip_record_manager.filter_ip_records(filter_ips=[instance.ip])
        assert isinstance(ipr1, list)
        assert instance.model_dump_json() == ipr1[0].model_dump_json()

    def test_prefetch_info(
        self,
        ip_record_factory,
        ip_information_factory,
        ip_geoname,
        user,
        thl_web_rr,
        thl_redis_config,
    ):

        ip = fake.ipv4_public()
        ip_information_factory(ip=ip, geoname=ip_geoname)
        ipr: IPRecord = ip_record_factory(user_id=user.user_id, ip=ip)

        assert ipr.information is None
        assert len(ipr.forwarded_ip_records) >= 1
        fipr = ipr.forwarded_ip_records[0]
        assert fipr.information is None

        ipr.prefetch_ipinfo(
            pg_config=thl_web_rr,
            redis_config=thl_redis_config,
            include_forwarded=True,
        )
        assert isinstance(ipr.information, GeoIPInformation)
        assert ipr.information.ip == ipr.ip == ip
        assert fipr.information is None, "the ipinfo doesn't exist in the db yet"

        ip_information_factory(ip=fipr.ip, geoname=ip_geoname)
        ipr.prefetch_ipinfo(
            pg_config=thl_web_rr,
            redis_config=thl_redis_config,
            include_forwarded=True,
        )
        assert fipr.information is not None


@pytest.mark.usefixtures("user_iphistory_manager_clear_cache")
class TestUserIpHistoryManager:
    def test_init(self, thl_web_rr, thl_redis_config, user_iphistory_manager):
        instance = UserIpHistoryManager(
            pg_config=thl_web_rr, redis_config=thl_redis_config
        )
        assert isinstance(instance, UserIpHistoryManager)
        assert isinstance(user_iphistory_manager, UserIpHistoryManager)

    def test_latest_record(
        self,
        user_iphistory_manager,
        user,
        ip_record_factory,
        ip_information_factory,
        ip_geoname,
    ):
        ip = fake.ipv4_public()
        ip_information_factory(ip=ip, geoname=ip_geoname, is_anonymous=True)
        ipr1: IPRecord = ip_record_factory(user_id=user.user_id, ip=ip)

        ipr = user_iphistory_manager.get_user_latest_ip_record(user=user)
        assert ipr.ip == ipr1.ip
        assert ipr.is_anonymous
        assert ipr.information.lookup_prefix == "/32"

        ip = fake.ipv6()
        ip_information_factory(ip=ip, geoname=ip_geoname)
        ipr2: IPRecord = ip_record_factory(user_id=user.user_id, ip=ip)

        ipr = user_iphistory_manager.get_user_latest_ip_record(user=user)
        assert ipr.ip == ipr2.ip
        assert ipr.information.lookup_prefix == "/64"
        assert ipr.information is not None
        assert not ipr.is_anonymous

        country_iso = user_iphistory_manager.get_user_latest_country(user=user)
        assert country_iso == ip_geoname.country_iso

        iph = user_iphistory_manager.get_user_ip_history(user_id=user.user_id)
        assert iph.ips[0].information is not None
        assert iph.ips[1].information is not None
        assert iph.ips[0].country_iso == country_iso
        assert iph.ips[0].is_anonymous
        assert iph.ips[0].ip == ipr1.ip
        assert iph.ips[1].ip == ipr2.ip

    def test_virgin(self, user, user_iphistory_manager, ip_record_factory):
        iph = user_iphistory_manager.get_user_ip_history(user_id=user.user_id)
        assert len(iph.ips) == 0

        ip_record_factory(user_id=user.user_id, ip=fake.ipv4_public())
        iph = user_iphistory_manager.get_user_ip_history(user_id=user.user_id)
        assert len(iph.ips) == 1

    def test_out_of_order(
        self,
        ip_record_factory,
        user,
        user_iphistory_manager,
        ip_information_factory,
        ip_geoname,
    ):
        # Create the user-ip association BEFORE the ip even exists in the ipinfo table
        ip = fake.ipv4_public()
        ip_record_factory(user_id=user.user_id, ip=ip)
        iph = user_iphistory_manager.get_user_ip_history(user_id=user.user_id)
        assert len(iph.ips) == 1
        ipr = iph.ips[0]
        assert ipr.information is None
        assert not ipr.is_anonymous

        ip_information_factory(ip=ip, geoname=ip_geoname, is_anonymous=True)
        iph = user_iphistory_manager.get_user_ip_history(user_id=user.user_id)
        assert len(iph.ips) == 1
        ipr = iph.ips[0]
        assert ipr.information is not None
        assert ipr.is_anonymous

    def test_out_of_order_ipv6(
        self,
        ip_record_factory,
        user,
        user_iphistory_manager,
        ip_information_factory,
        ip_geoname,
    ):
        # Create the user-ip association BEFORE the ip even exists in the ipinfo table
        ip = fake.ipv6()
        ip_record_factory(user_id=user.user_id, ip=ip)
        iph = user_iphistory_manager.get_user_ip_history(user_id=user.user_id)
        assert len(iph.ips) == 1
        ipr = iph.ips[0]
        assert ipr.information is None
        assert not ipr.is_anonymous

        ip_information_factory(ip=ip, geoname=ip_geoname, is_anonymous=True)
        iph = user_iphistory_manager.get_user_ip_history(user_id=user.user_id)
        assert len(iph.ips) == 1
        ipr = iph.ips[0]
        assert ipr.information is not None
        assert ipr.is_anonymous
