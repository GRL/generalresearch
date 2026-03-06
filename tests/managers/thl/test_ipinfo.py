import faker

from generalresearch.managers.thl.ipinfo import (
    IPGeonameManager,
    IPInformationManager,
    GeoIpInfoManager,
)
from generalresearch.models.thl.ipinfo import IPGeoname, IPInformation

fake = faker.Faker()


class TestIPGeonameManager:

    def test_init(self, thl_web_rr, ip_geoname_manager: IPGeonameManager):

        instance = IPGeonameManager(pg_config=thl_web_rr)
        assert isinstance(instance, IPGeonameManager)
        assert isinstance(ip_geoname_manager, IPGeonameManager)

    def test_create(self, ip_geoname_manager: IPGeonameManager):

        instance = ip_geoname_manager.create_dummy()

        assert isinstance(instance, IPGeoname)

        res = ip_geoname_manager.fetch_geoname_ids(filter_ids=[instance.geoname_id])

        assert res[0].model_dump_json() == instance.model_dump_json()


class TestIPInformationManager:

    def test_init(self, thl_web_rr, ip_information_manager: IPInformationManager):
        instance = IPInformationManager(pg_config=thl_web_rr)
        assert isinstance(instance, IPInformationManager)
        assert isinstance(ip_information_manager, IPInformationManager)

    def test_create(self, ip_information_manager: IPInformationManager):
        instance = ip_information_manager.create_dummy()

        assert isinstance(instance, IPInformation)

        res = ip_information_manager.fetch_ip_information(filter_ips=[instance.ip])

        assert res[0].model_dump_json() == instance.model_dump_json()

    def test_prefetch_geoname(self, ip_information, ip_geoname, thl_web_rr):
        assert isinstance(ip_information, IPInformation)

        assert ip_information.geoname_id == ip_geoname.geoname_id
        assert ip_information.geoname is None

        ip_information.prefetch_geoname(pg_config=thl_web_rr)
        assert isinstance(ip_information.geoname, IPGeoname)


class TestGeoIpInfoManager:
    def test_init(
        self, thl_web_rr, thl_redis_config, geoipinfo_manager: GeoIpInfoManager
    ):
        instance = GeoIpInfoManager(pg_config=thl_web_rr, redis_config=thl_redis_config)
        assert isinstance(instance, GeoIpInfoManager)
        assert isinstance(geoipinfo_manager, GeoIpInfoManager)

    def test_multi(self, ip_information_factory, ip_geoname, geoipinfo_manager):
        ip = fake.ipv4_public()
        ip_information_factory(ip=ip, geoname=ip_geoname)
        ips = [ip]

        # This only looks up in redis. They don't exist yet
        res = geoipinfo_manager.get_cache_multi(ip_addresses=ips)
        assert res == {ip: None}

        # Looks up in redis, if not exists, looks in mysql, then sets
        #   the caches that didn't exist.
        res = geoipinfo_manager.get_multi(ip_addresses=ips)
        assert res[ip] is not None

        ip2 = fake.ipv4_public()
        ip_information_factory(ip=ip2, geoname=ip_geoname)
        ips = [ip, ip2]
        res = geoipinfo_manager.get_cache_multi(ip_addresses=ips)
        assert res[ip] is not None
        assert res[ip2] is None
        res = geoipinfo_manager.get_multi(ip_addresses=ips)
        assert res[ip] is not None
        assert res[ip2] is not None
        res = geoipinfo_manager.get_cache_multi(ip_addresses=ips)
        assert res[ip] is not None
        assert res[ip2] is not None

    def test_multi_ipv6(self, ip_information_factory, ip_geoname, geoipinfo_manager):
        ip = fake.ipv6()
        # Make another IP that will be in the same /64 block.
        ip2 = ip[:-1] + "a" if ip[-1] != "a" else ip[:-1] + "b"
        ip_information_factory(ip=ip, geoname=ip_geoname)
        ips = [ip, ip2]
        print(f"{ips=}")

        # This only looks up in redis. They don't exist yet
        res = geoipinfo_manager.get_cache_multi(ip_addresses=ips)
        assert res == {ip: None, ip2: None}

        # Looks up in redis, if not exists, looks in mysql, then sets
        #   the caches that didn't exist.
        res = geoipinfo_manager.get_multi(ip_addresses=ips)
        assert res[ip].ip == ip
        assert res[ip].lookup_prefix == "/64"
        assert res[ip2].ip == ip2
        assert res[ip2].lookup_prefix == "/64"
        # they should be the same basically, except for the ip

    def test_doesnt_exist(self, geoipinfo_manager):
        ip = fake.ipv4_public()
        res = geoipinfo_manager.get_multi(ip_addresses=[ip])
        assert res == {ip: None}
