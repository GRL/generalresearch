import json
import logging
from typing import Callable

import geoip2.models
import pytest
from faker import Faker
from faker.providers.address.en_US import Provider as USAddressProvider

from generalresearch.managers.thl.ipinfo import GeoIpInfoManager
from generalresearch.managers.thl.maxmind import MaxmindManager
from generalresearch.managers.thl.maxmind.basic import (
    MaxmindBasicManager,
)
from generalresearch.models.thl.ipinfo import (
    GeoIPInformation,
    normalize_ip,
)
from generalresearch.models.thl.maxmind.definitions import UserType

fake = Faker()

US_STATES = {x.lower() for x in USAddressProvider.states}

IP_v4_INDIA = "106.203.146.157"
IP_v6_INDIA = "2402:3a80:4649:de3f:0:24:74aa:b601"
IP_v4_US = "174.218.60.101"
IP_v6_US = "2600:1700:ece0:9410:55d:faf3:c15d:6e4"
IP_v6_US_SAME_64 = "2600:1700:ece0:9410:55d:faf3:c15d:aaaa"


@pytest.fixture(scope="session")
def delete_ipinfo(thl_web_rw) -> Callable:
    def _delete_ipinfo(ip):
        thl_web_rw.execute_write(
            query="DELETE FROM thl_geoname WHERE geoname_id IN (SELECT geoname_id FROM thl_ipinformation WHERE ip = %s);",
            params=[ip],
        )
        thl_web_rw.execute_write(
            query="DELETE FROM thl_ipinformation WHERE ip = %s;",
            params=[ip],
        )

    return _delete_ipinfo


class TestMaxmindBasicManager:

    def test_init(self, maxmind_basic_manager):

        assert isinstance(maxmind_basic_manager, MaxmindBasicManager)

    def test_get_basic_ip_information(self, maxmind_basic_manager):
        ip = IP_v4_INDIA
        maxmind_basic_manager.run_update_geoip_db()

        res1 = maxmind_basic_manager.get_basic_ip_information(ip_address=ip)
        assert isinstance(res1, geoip2.models.Country)
        assert res1.country.iso_code == "IN"
        assert res1.country.name == "India"

        res2 = maxmind_basic_manager.get_basic_ip_information(
            ip_address=fake.ipv4_private()
        )
        assert res2 is None

    def test_get_country_iso_from_ip_geoip2db(self, maxmind_basic_manager):
        ip = IP_v4_INDIA
        maxmind_basic_manager.run_update_geoip_db()

        res1 = maxmind_basic_manager.get_country_iso_from_ip_geoip2db(ip=ip)
        assert res1 == "in"

        res2 = maxmind_basic_manager.get_country_iso_from_ip_geoip2db(
            ip=fake.ipv4_private()
        )
        assert res2 is None

    def test_get_basic_ip_information_ipv6(self, maxmind_basic_manager):
        ip = IP_v6_INDIA
        maxmind_basic_manager.run_update_geoip_db()

        res1 = maxmind_basic_manager.get_basic_ip_information(ip_address=ip)
        assert isinstance(res1, geoip2.models.Country)
        assert res1.country.iso_code == "IN"
        assert res1.country.name == "India"


class TestMaxmindManager:

    def test_init(self, thl_web_rr, thl_redis_config, maxmind_manager: MaxmindManager):
        instance = MaxmindManager(pg_config=thl_web_rr, redis_config=thl_redis_config)
        assert isinstance(instance, MaxmindManager)
        assert isinstance(maxmind_manager, MaxmindManager)

    def test_create_basic(
        self,
        maxmind_manager: MaxmindManager,
        geoipinfo_manager: GeoIpInfoManager,
        delete_ipinfo,
    ):
        # This is (currently) an IP in India, and so it should only do the basic lookup
        ip = IP_v4_INDIA
        delete_ipinfo(ip)
        geoipinfo_manager.clear_cache(ip)
        assert geoipinfo_manager.get_cache(ip) is None
        assert geoipinfo_manager.get_mysql_if_exists(ip) is None

        maxmind_manager.run_ip_information(ip, force_insights=False)
        # Check that it is in the cache and in mysql
        res = geoipinfo_manager.get_cache(ip)
        assert res.ip == ip
        assert res.basic
        res = geoipinfo_manager.get_mysql(ip)
        assert res.ip == ip
        assert res.basic

    def test_create_basic_ipv6(
        self,
        maxmind_manager: MaxmindManager,
        geoipinfo_manager: GeoIpInfoManager,
        delete_ipinfo,
    ):
        # This is (currently) an IP in India, and so it should only do the basic lookup
        ip = IP_v6_INDIA
        normalized_ip, lookup_prefix = normalize_ip(ip)
        delete_ipinfo(ip)
        geoipinfo_manager.clear_cache(ip)
        delete_ipinfo(normalized_ip)
        geoipinfo_manager.clear_cache(normalized_ip)
        assert geoipinfo_manager.get_cache(ip) is None
        assert geoipinfo_manager.get_cache(normalized_ip) is None
        assert geoipinfo_manager.get_mysql_if_exists(ip) is None
        assert geoipinfo_manager.get_mysql_if_exists(normalized_ip) is None

        maxmind_manager.run_ip_information(ip, force_insights=False)

        # Check that it is in the cache
        res = geoipinfo_manager.get_cache(ip)
        # The looked up IP (/128) is returned,
        assert res.ip == ip
        assert res.lookup_prefix == "/64"
        assert res.basic

        # ... but the normalized version was stored (/64)
        assert geoipinfo_manager.get_cache_raw(ip) is None
        res = json.loads(geoipinfo_manager.get_cache_raw(normalized_ip))
        assert res["ip"] == normalized_ip

        # Check mysql
        res = geoipinfo_manager.get_mysql(ip)
        assert res.ip == ip
        assert res.lookup_prefix == "/64"
        assert res.basic
        with pytest.raises(AssertionError):
            geoipinfo_manager.get_mysql_raw(ip)
        res = geoipinfo_manager.get_mysql_raw(normalized_ip)
        assert res["ip"] == normalized_ip

    def test_create_insights(
        self,
        maxmind_manager: MaxmindManager,
        geoipinfo_manager: GeoIpInfoManager,
        delete_ipinfo,
    ):
        # This is (currently) an IP in the US, so it should do insights
        ip = IP_v4_US
        delete_ipinfo(ip)
        geoipinfo_manager.clear_cache(ip)
        assert geoipinfo_manager.get_cache(ip) is None
        assert geoipinfo_manager.get_mysql_if_exists(ip) is None

        res1 = maxmind_manager.run_ip_information(ip, force_insights=False)
        assert isinstance(res1, GeoIPInformation)

        # Check that it is in the cache and in mysql
        res2 = geoipinfo_manager.get_cache(ip)
        assert isinstance(res2, GeoIPInformation)
        assert res2.ip == ip
        assert not res2.basic

        res3 = geoipinfo_manager.get_mysql(ip)
        assert isinstance(res3, GeoIPInformation)
        assert res3.ip == ip
        assert not res3.basic
        assert res3.is_anonymous is False
        assert res3.subdivision_1_name.lower() in US_STATES
        # this might change ...
        assert res3.user_type == UserType.CELLULAR

        assert res1 == res2 == res3, "runner, cache, mysql all return same instance"

    def test_create_insights_ipv6(
        self,
        maxmind_manager: MaxmindManager,
        geoipinfo_manager: GeoIpInfoManager,
        delete_ipinfo,
    ):
        # This is (currently) an IP in the US, so it should do insights
        ip = IP_v6_US
        normalized_ip, lookup_prefix = normalize_ip(ip)
        delete_ipinfo(ip)
        geoipinfo_manager.clear_cache(ip)
        delete_ipinfo(normalized_ip)
        geoipinfo_manager.clear_cache(normalized_ip)
        assert geoipinfo_manager.get_cache(ip) is None
        assert geoipinfo_manager.get_cache(normalized_ip) is None
        assert geoipinfo_manager.get_mysql_if_exists(ip) is None
        assert geoipinfo_manager.get_mysql_if_exists(normalized_ip) is None

        res1 = maxmind_manager.run_ip_information(ip, force_insights=False)
        assert isinstance(res1, GeoIPInformation)
        assert res1.lookup_prefix == "/64"

        # Check that it is in the cache and in mysql
        res2 = geoipinfo_manager.get_cache(ip)
        assert isinstance(res2, GeoIPInformation)
        assert res2.ip == ip
        assert not res2.basic

        res3 = geoipinfo_manager.get_mysql(ip)
        assert isinstance(res3, GeoIPInformation)
        assert res3.ip == ip
        assert not res3.basic
        assert res3.is_anonymous is False
        assert res3.subdivision_1_name.lower() in US_STATES
        # this might change ...
        assert res3.user_type == UserType.RESIDENTIAL

        assert res1 == res2 == res3, "runner, cache, mysql all return same instance"

    def test_get_or_create_ip_information(self, maxmind_manager):
        ip = IP_v4_US

        res1 = maxmind_manager.get_or_create_ip_information(ip_address=ip)
        assert isinstance(res1, GeoIPInformation)

        res2 = maxmind_manager.get_or_create_ip_information(
            ip_address=fake.ipv4_private()
        )
        assert res2 is None

    def test_get_or_create_ip_information_ipv6(
        self, maxmind_manager, delete_ipinfo, geoipinfo_manager, caplog
    ):
        ip = IP_v6_US
        normalized_ip, lookup_prefix = normalize_ip(ip)
        delete_ipinfo(normalized_ip)
        geoipinfo_manager.clear_cache(normalized_ip)

        with caplog.at_level(logging.INFO):
            res1 = maxmind_manager.get_or_create_ip_information(ip_address=ip)
        assert isinstance(res1, GeoIPInformation)
        assert res1.ip == ip
        # It looks up in insight using the normalize IP!
        assert f"get_insights_ip_information: {normalized_ip}" in caplog.text

        # And it should NOT do the lookup again with an ipv6 in the same /64 block!
        ip = IP_v6_US_SAME_64
        caplog.clear()
        with caplog.at_level(logging.INFO):
            res2 = maxmind_manager.get_or_create_ip_information(ip_address=ip)
        assert isinstance(res2, GeoIPInformation)
        assert res2.ip == ip
        assert "get_insights_ip_information" not in caplog.text

    def test_run_ip_information(self, maxmind_manager):
        ip = IP_v4_US

        res = maxmind_manager.run_ip_information(ip_address=ip)
        assert isinstance(res, GeoIPInformation)
        assert res.country_name == "United States"
        assert res.country_iso == "us"
