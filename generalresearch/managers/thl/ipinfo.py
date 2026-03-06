import ipaddress
from decimal import Decimal
from random import randint
from typing import List, Optional, Dict, Collection

import faker
import pymysql
from more_itertools import chunked
from psycopg import Cursor
from pydantic import PositiveInt

from generalresearch.managers.base import (
    PostgresManager,
    PostgresManagerWithRedis,
)
from generalresearch.models.custom_types import (
    IPvAnyAddressStr,
    CountryISOLike,
)
from generalresearch.models.thl.ipinfo import (
    IPGeoname,
    GeoIPInformation,
    IPInformation,
    normalize_ip,
)
from generalresearch.models.thl.maxmind.definitions import UserType
from generalresearch.pg_helper import PostgresConfig

fake = faker.Faker()


class IPGeonameManager(PostgresManager):

    def create_dummy(
        self,
        geoname_id: Optional[PositiveInt] = None,
        continent_code: Optional[str] = None,
        continent_name: Optional[str] = None,
        country_iso: Optional[str] = None,
        country_name: Optional[str] = None,
        subdivision_1_iso: Optional[str] = None,
        subdivision_1_name: Optional[str] = None,
        subdivision_2_iso: Optional[str] = None,
        subdivision_2_name: Optional[str] = None,
        city_name: Optional[str] = None,
        metro_code: Optional[int] = None,
        time_zone: Optional[str] = None,
        is_in_european_union: Optional[bool] = None,
    ) -> IPGeoname:
        return self.create(
            geoname_id=geoname_id or randint(1, 999_999_999),
            continent_code=continent_code or "na",
            continent_name=continent_name or "North America",
            country_iso=country_iso or "us",
            country_name=country_name or "United States",
            subdivision_1_iso=subdivision_1_iso or "fl",
            subdivision_1_name=subdivision_1_name or "Florida",
            subdivision_2_iso=subdivision_2_iso,
            subdivision_2_name=subdivision_2_name,
            city_name=city_name,
            metro_code=metro_code,
            time_zone=time_zone,
            is_in_european_union=is_in_european_union,
        )

    def create_basic(
        self,
        geoname_id: PositiveInt,
        is_in_european_union: bool,
        country_iso: CountryISOLike,
        country_name: str,
        continent_code: str,
        continent_name: str,
    ) -> IPGeoname:
        instance = IPGeoname.model_validate(
            {
                "geoname_id": geoname_id,
                "country_iso": country_iso,
                "is_in_european_union": is_in_european_union,
                "country_name": country_name,
                "continent_code": continent_code,
                "continent_name": continent_name,
            }
        )
        self.pg_config.execute_write(
            query=f"""
            INSERT INTO thl_geoname (
                geoname_id, country_iso, is_in_european_union, country_name,
                continent_code, continent_name, updated
            )
            VALUES (
                %(geoname_id)s, %(country_iso)s, %(is_in_european_union)s, %(country_name)s,
                %(continent_code)s, %(continent_name)s, %(updated)s
             )
            ON CONFLICT (geoname_id) DO NOTHING;
            """,
            params=instance.model_dump(mode="json"),
        )
        return instance

    def create_or_update(self, ipgeo: IPGeoname):
        keys = list(ipgeo.model_fields.keys())
        data = ipgeo.model_dump_mysql()

        keys_str = ", ".join(keys)
        values_str = ", ".join([f"%({k})s" for k in keys])
        update_cols = set(keys) - {"geoname_id"}
        update_str = ", ".join([f"{k} = EXCLUDED.{k}" for k in update_cols])

        query = f"""
        INSERT INTO thl_geoname ({keys_str})
        VALUES ({values_str})
        ON CONFLICT (geoname_id)
        DO UPDATE SET {update_str}
        """
        self.pg_config.execute_write(query=query, params=data)

    def create(
        self,
        geoname_id: PositiveInt,
        continent_code: str,
        continent_name: str,
        country_iso: Optional[str],
        country_name: Optional[str] = None,
        subdivision_1_iso: Optional[str] = None,
        subdivision_1_name: Optional[str] = None,
        subdivision_2_iso: Optional[str] = None,
        subdivision_2_name: Optional[str] = None,
        city_name: Optional[str] = None,
        metro_code: Optional[int] = None,
        time_zone: Optional[str] = None,
        is_in_european_union: Optional[bool] = None,
    ) -> IPGeoname:

        instance = IPGeoname.model_validate(
            {
                "geoname_id": geoname_id,
                "continent_code": continent_code,
                "continent_name": continent_name,
                "country_iso": country_iso,
                "country_name": country_name,
                "subdivision_1_iso": subdivision_1_iso,
                "subdivision_1_name": subdivision_1_name,
                "subdivision_2_iso": subdivision_2_iso,
                "subdivision_2_name": subdivision_2_name,
                "city_name": city_name,
                "metro_code": metro_code,
                "time_zone": time_zone,
                "is_in_european_union": is_in_european_union,
            }
        )

        self.pg_config.execute_write(
            query=f"""
            INSERT INTO thl_geoname
                (   geoname_id, continent_code, continent_name, 
                    country_iso, country_name,
                    subdivision_1_iso, subdivision_1_name, 
                    subdivision_2_iso, subdivision_2_name,
                    city_name, metro_code, time_zone, is_in_european_union, 
                    updated
                )
            VALUES (
                    %(geoname_id)s, %(continent_code)s, %(continent_name)s,
                    %(country_iso)s, %(country_name)s, 
                    %(subdivision_1_iso)s, %(subdivision_1_name)s, 
                    %(subdivision_2_iso)s, %(subdivision_2_name)s,
                    %(city_name)s, %(metro_code)s, %(time_zone)s, %(is_in_european_union)s,
                    %(updated)s
                )
            ON CONFLICT (geoname_id) DO NOTHING;
            """,
            params=instance.model_dump(mode="json"),
        )

        return instance

    def get_by_id(self, geoname_id: PositiveInt) -> "IPGeoname":
        return self.fetch_geoname_ids(filter_ids=[geoname_id])[0]

    def fetch_geoname_ids(
        self,
        filter_ids: List[PositiveInt],
    ) -> List[IPGeoname]:

        if len(filter_ids) == 0:
            return []

        with self.pg_config.make_connection() as sql_connection:
            sql_connection: pymysql.Connection
            with sql_connection.cursor() as c:
                res = []
                for chunk in chunked(filter_ids, 500):
                    res.extend(
                        self.fetch_geoname_ids_(
                            c=c,
                            filter_ids=chunk,
                        )
                    )
        return res

    def fetch_geoname_ids_(
        self,
        c: Cursor,
        filter_ids: List[PositiveInt],
    ) -> List[IPGeoname]:

        assert len(filter_ids) <= 500, "chunk me"

        c.execute(
            query=f"""
                SELECT  g.geoname_id,
                        g.continent_code, g.continent_name,
                        g.country_iso, g.country_name,
                        g.subdivision_1_iso, g.subdivision_1_name,
                        g.subdivision_2_iso, g.subdivision_2_name,
                        g.city_name, g.metro_code,
                        g.time_zone, g.is_in_european_union,
                        g.updated                        
                FROM thl_geoname AS g
                WHERE g.geoname_id = ANY(%s);
            """,
            params=[filter_ids],
        )
        return [IPGeoname.from_mysql(i) for i in c.fetchall()]


class IPInformationManager(PostgresManager):

    def create_dummy(
        self,
        ip: Optional[IPvAnyAddressStr] = None,
        geoname_id: Optional[PositiveInt] = None,
        country_iso: Optional[str] = None,
        registered_country_iso: Optional[str] = None,
        is_anonymous: Optional[bool] = None,
        is_anonymous_vpn: Optional[bool] = None,
        is_hosting_provider: Optional[bool] = None,
        is_public_proxy: Optional[bool] = None,
        is_tor_exit_node: Optional[bool] = None,
        is_residential_proxy: Optional[bool] = None,
        autonomous_system_number: Optional[PositiveInt] = None,
        autonomous_system_organization: Optional[str] = None,
        domain: Optional[str] = None,
        isp: Optional[str] = None,
        mobile_country_code: Optional[str] = None,
        mobile_network_code: Optional[str] = None,
        network: Optional[str] = None,
        organization: Optional[str] = None,
        static_ip_score: Optional[float] = None,
        user_type: Optional[UserType] = None,
        postal_code: Optional[str] = None,
        latitude: Optional[Decimal] = None,
        longitude: Optional[Decimal] = None,
        accuracy_radius: Optional[int] = None,
    ) -> "IPInformation":
        return self.create(
            ip=ip or fake.ipv4_public(),
            geoname_id=geoname_id,
            country_iso=country_iso or fake.country_code(),
            registered_country_iso=registered_country_iso,
            is_anonymous=is_anonymous,
            is_anonymous_vpn=is_anonymous_vpn,
            is_hosting_provider=is_hosting_provider,
            is_public_proxy=is_public_proxy,
            is_tor_exit_node=is_tor_exit_node,
            is_residential_proxy=is_residential_proxy,
            autonomous_system_number=autonomous_system_number,
            autonomous_system_organization=autonomous_system_organization,
            domain=domain,
            isp=isp,
            mobile_country_code=mobile_country_code,
            mobile_network_code=mobile_network_code,
            network=network,
            organization=organization,
            static_ip_score=static_ip_score,
            user_type=user_type,
            postal_code=postal_code,
            latitude=latitude,
            longitude=longitude,
            accuracy_radius=accuracy_radius,
        )

    def create_basic(
        self,
        ip: IPvAnyAddressStr,
        geoname_id: PositiveInt,
        country_iso: str,
        registered_country_iso: str,
    ) -> IPInformation:
        instance = IPInformation.model_validate(
            {
                "ip": ip,
                "geoname_id": geoname_id,
                "country_iso": country_iso,
                "registered_country_iso": registered_country_iso,
            }
        )
        instance.normalize_ip()
        self.pg_config.execute_write(
            query=f"""
            INSERT INTO thl_ipinformation
            (ip, country_iso, registered_country_iso, geoname_id, updated)
            VALUES (%(ip)s, %(country_iso)s, %(registered_country_iso)s, %(geoname_id)s, %(updated)s)
            ON CONFLICT (ip) DO NOTHING;
            """,
            params=instance.model_dump(mode="json"),
        )
        return instance

    def create(
        self,
        ip: IPvAnyAddressStr,
        geoname_id: Optional[PositiveInt] = None,
        country_iso: Optional[str] = None,
        registered_country_iso: Optional[str] = None,
        is_anonymous: Optional[bool] = None,
        is_anonymous_vpn: Optional[bool] = None,
        is_hosting_provider: Optional[bool] = None,
        is_public_proxy: Optional[bool] = None,
        is_tor_exit_node: Optional[bool] = None,
        is_residential_proxy: Optional[bool] = None,
        autonomous_system_number: Optional[PositiveInt] = None,
        autonomous_system_organization: Optional[str] = None,
        domain: Optional[str] = None,
        isp: Optional[str] = None,
        mobile_country_code: Optional[str] = None,
        mobile_network_code: Optional[str] = None,
        network: Optional[str] = None,
        organization: Optional[str] = None,
        static_ip_score: Optional[float] = None,
        user_type: Optional[UserType] = None,
        postal_code: Optional[str] = None,
        latitude: Optional[Decimal] = None,
        longitude: Optional[Decimal] = None,
        accuracy_radius: Optional[int] = None,
    ) -> "IPInformation":

        instance = IPInformation.model_validate(
            {
                "ip": ip,
                "geoname_id": geoname_id,
                "country_iso": country_iso,
                "registered_country_iso": registered_country_iso,
                "is_anonymous": is_anonymous,
                "is_anonymous_vpn": is_anonymous_vpn,
                "is_hosting_provider": is_hosting_provider,
                "is_public_proxy": is_public_proxy,
                "is_tor_exit_node": is_tor_exit_node,
                "is_residential_proxy": is_residential_proxy,
                "autonomous_system_number": autonomous_system_number,
                "autonomous_system_organization": autonomous_system_organization,
                "domain": domain,
                "isp": isp,
                "mobile_country_code": mobile_country_code,
                "mobile_network_code": mobile_network_code,
                "network": network,
                "organization": organization,
                "static_ip_score": static_ip_score,
                "user_type": user_type,
                "postal_code": postal_code,
                "latitude": latitude,
                "longitude": longitude,
                "accuracy_radius": accuracy_radius,
            }
        )
        instance.normalize_ip()

        self.pg_config.execute_write(
            query=f"""
            INSERT INTO thl_ipinformation
                (   ip, geoname_id, 
                    country_iso, registered_country_iso, 
                    is_anonymous, is_anonymous_vpn,
                    is_hosting_provider, is_public_proxy, 
                    is_tor_exit_node, is_residential_proxy,
                    autonomous_system_number, autonomous_system_organization, 
                    domain, isp, 
                    mobile_country_code, mobile_network_code,
                    network, organization, static_ip_score,
                    user_type, postal_code, latitude, longitude,
                    accuracy_radius,
                    updated
                )
            VALUES (
                    %(ip)s, %(geoname_id)s, 
                    %(country_iso)s, %(registered_country_iso)s,
                    %(is_anonymous)s, %(is_anonymous_vpn)s, 
                    %(is_hosting_provider)s, %(is_public_proxy)s, 
                    %(is_tor_exit_node)s, %(is_residential_proxy)s,
                    %(autonomous_system_number)s, %(autonomous_system_organization)s, 
                    %(domain)s, %(isp)s,
                    %(mobile_country_code)s, %(mobile_network_code)s,
                    %(network)s, %(organization)s, %(static_ip_score)s,
                    %(user_type)s, %(postal_code)s, %(latitude)s, %(longitude)s, 
                    %(accuracy_radius)s,
                    %(updated)s
                )
            ON CONFLICT (ip) DO NOTHING;
            """,
            params=instance.model_dump(mode="json"),
        )

        return instance

    def create_or_update(self, ipinfo: IPInformation):
        ipinfo.normalize_ip()
        keys = [key for key, field in ipinfo.model_fields.items() if not field.exclude]
        data = ipinfo.model_dump_mysql()

        keys_str = ", ".join(keys)
        values_str = ", ".join([f"%({k})s" for k in keys])
        update_cols = set(keys) - {"ip"}
        update_str = ", ".join([f"{k} = EXCLUDED.{k}" for k in update_cols])

        query = f"""
        INSERT INTO thl_ipinformation ({keys_str})
        VALUES ({values_str})
        ON CONFLICT (ip) DO UPDATE
        SET {update_str}
        """
        self.pg_config.execute_write(query, params=data)

    def get_ip_info(self, ip: IPvAnyAddressStr) -> Optional["IPInformation"]:
        res = self.fetch_ip_information(filter_ips=[ip])
        if len(res) != 1:
            return None

        return res[0]

    def fetch_ip_information(
        self,
        filter_ips: List[IPvAnyAddressStr],
    ) -> List["IPInformation"]:

        if len(filter_ips) == 0:
            return []

        with self.pg_config.make_connection() as conn:
            with conn.cursor() as c:
                res = []
                for chunk in chunked(filter_ips, 500):
                    res.extend(
                        self.fetch_ip_information_(
                            c=c,
                            filter_ips=chunk,
                        )
                    )
        return res

    def fetch_ip_information_(
        self,
        c: Cursor,
        filter_ips: List[IPvAnyAddressStr],
    ) -> List["IPInformation"]:
        """
        IPs are converted to normalized form (/64 network exploded) for DB lookup,
            and are then matched back to the original queried form for return.
        e.g. '2600:1700:ece0:9410:055d:faf3:c15d:06e4' is passed in,
            it gets converted to '2600:1700:ece0:9410:0000:0000:0000:0000' for db lookup,
            the result gets
        """

        assert len(filter_ips) <= 500, "chunk me"
        normalized_ip_lookup = {ip: normalize_ip(ip)[0] for ip in filter_ips}
        normalized_ips = set(normalized_ip_lookup.values())

        c.execute(
            query=f"""
                SELECT  i.ip, i.geoname_id,
                        i.country_iso, i.registered_country_iso,
                        i.is_anonymous, i.is_anonymous_vpn, i.is_hosting_provider,
                        i.is_public_proxy, i.is_tor_exit_node, i.is_residential_proxy,
                        i.autonomous_system_number, i.autonomous_system_organization,
                        i.domain, i.isp,
                        i.mobile_country_code, i.mobile_network_code,
                        i.network, i.organization,
                        i.static_ip_score, i.user_type, i.postal_code,
                        i.latitude, i.longitude, i.accuracy_radius,
                        i.updated 

                FROM thl_ipinformation AS i
                WHERE i.ip = ANY(%s)
            """,
            params=[list(normalized_ips)],
        )

        return [IPInformation.from_mysql(i) for i in c.fetchall()]

    @staticmethod
    def monitor_ipinformation(pg_config: PostgresConfig) -> None:
        """Continually check our IPInformation table to ensure location information
        is being saved properly"""

        # Check the percentage of IPs that don't have a country_iso in the past 12 hours
        # Split query is 1000x faster
        query = """
        SELECT COUNT(*) AS numerator
        FROM thl_ipinformation
        WHERE updated >= NOW() - INTERVAL '12 hours'
          AND country_iso IS NULL;
        """
        numerator = list(pg_config.execute_sql_query(query=query))[0]["numerator"]

        query = f"""
        SELECT COUNT(1) AS denominator
        FROM thl_ipinformation
        WHERE updated >= NOW() - INTERVAL '12 hours'
        """
        denominator = list(pg_config.execute_sql_query(query=query))[0]["denominator"]
        if denominator == 0:
            pass
        percent_empty = numerator / (denominator or 1)
        # TODO: Post to telegraf / grafana

        return


class GeoIpInfoManager(PostgresManagerWithRedis):

    def get(self, ip_address: IPvAnyAddressStr) -> Optional[GeoIPInformation]:
        res = self.get_cache(ip_address)
        if res:
            return res
        res = self.get_mysql_if_exists(ip_address)
        if res:
            self.set_cache(res)
        return res

    def get_multi(
        self, ip_addresses: Collection[IPvAnyAddressStr]
    ) -> Dict[IPvAnyAddressStr, Optional[GeoIPInformation]]:
        if not ip_addresses:
            return {}
        # To deploy this, we still have (for the next 28 days) users who's
        #   ipv6 history was looked up and saved using the full /128. We need
        #   to pull those if the /64 doesn't exist.
        # See notes in get_cache_multi &  get_mysql_multi
        res = self.get_cache_multi(ip_addresses=ip_addresses)
        missing_ips = {k for k, v in res.items() if v is None and k in ip_addresses}
        res_mysql = self.get_mysql_multi(ips=missing_ips)
        self.set_cache_multi({k: v for k, v in res_mysql.items() if v})
        res.update(res_mysql)
        return res

    def set_cache_multi(
        self, ipinfo_map: Dict[IPvAnyAddressStr, GeoIPInformation]
    ) -> None:
        """Set multiple GeoIPInformation objects in Redis in one call."""
        if not ipinfo_map:
            return

        pipe = self.redis_client.pipeline(transaction=False)
        expire_seconds = 3 * 24 * 3600
        for ip, ipinfo in ipinfo_map.items():
            pipe.set(
                self.get_cache_key(ip),
                ipinfo.model_dump_json(),
                ex=expire_seconds,
            )
        pipe.execute()

    @staticmethod
    def compress_ip(ip: str) -> str:
        """
        To support looking up an ip in the db before we switched
        to using the exploded form. (remove me 28 days after 2025-11-15)
        """
        addr = ipaddress.ip_address(ip)
        if addr.version == 4:
            return str(addr)
        return addr.compressed

    def get_cache_multi(
        self, ip_addresses: Collection[IPvAnyAddressStr]
    ) -> Dict[IPvAnyAddressStr, Optional[GeoIPInformation]]:
        """Get multiple GeoIPInformation objects from Redis in one call.

        Returns a dict mapping IP address -> GeoIPInformation (or None if not in cache).
        """
        if not ip_addresses:
            return {}
        # We must do it like this b/c we could have multiple /128 ips that normalize
        #   to the same normalized /64 ip, and we don't want to "loose" them.
        ip_norm_lookup = {ip: normalize_ip(ip) for ip in ip_addresses}
        normalized_ips = {v[0] for v in ip_norm_lookup.values()}
        # also lookup exact matches (can remove this 28 days from 2025-11-15)
        normalized_ips.update(ip_addresses)
        # also lookup compressed form ... (remove me also)
        normalized_ips.update({self.compress_ip(ip) for ip in ip_addresses})

        keys = [self.get_cache_key(ip) for ip in normalized_ips]
        res = self.redis_client.mget(keys)
        res = [GeoIPInformation.model_validate_json(raw) for raw in res if raw]
        gs = {x.ip: x for x in res}

        res2 = dict()
        for ip, (normalized_ip, lookup_prefix) in ip_norm_lookup.items():
            if normalized_ip not in gs:
                # try the non-normalized (remove me also 28 days from 2025-11-15)
                if ip in gs:
                    res2[ip] = gs[ip].model_copy()
                    continue
                res2[ip] = None
                continue
            g = gs[normalized_ip]
            g.ip = ip
            g.lookup_prefix = lookup_prefix
            res2[g.ip] = g.model_copy()
        return res2

    def get_cache_key(self, ip_address: IPvAnyAddressStr) -> str:
        return self.cache_prefix + f"thl:GeoIpInfoManager:{ip_address}"

    def clear_cache(self, ip_address: IPvAnyAddressStr) -> None:
        # typically for testing
        self.redis_client.delete(self.get_cache_key(ip_address=ip_address))

    def set_cache(self, ipinfo: GeoIPInformation):
        ipinfo = ipinfo.model_copy()
        ipinfo.normalize_ip()
        data = ipinfo.model_dump_json()
        return self.redis_client.set(
            self.get_cache_key(ip_address=ipinfo.ip), data, ex=3 * 24 * 3600
        )

    def get_cache(self, ip_address: IPvAnyAddressStr) -> Optional[GeoIPInformation]:
        normalized_ip, lookup_prefix = normalize_ip(ip_address)
        res: str = self.get_cache_raw(normalized_ip)
        if not res:
            return None
        g = GeoIPInformation.model_validate_json(res)
        g.ip = ip_address
        g.lookup_prefix = lookup_prefix
        return g

    def get_cache_raw(self, ip_address: IPvAnyAddressStr) -> str:
        return self.redis_client.get(self.get_cache_key(ip_address=ip_address))

    def get_mysql_if_exists(self, ip_address: IPvAnyAddressStr):
        try:
            return self.get_mysql(ip_address=ip_address)
        except AssertionError:
            return None

    def get_mysql_raw(self, ip_address: IPvAnyAddressStr):
        query = """
            SELECT
                geo.geoname_id,
                geo.continent_name,
                LOWER(geo.continent_code) AS continent_code,
                geo.country_name,
                LOWER(geo.country_iso) AS geo_country_iso,
                geo.subdivision_1_iso,
                geo.subdivision_1_name,
                geo.subdivision_2_iso,
                geo.subdivision_2_name,
                geo.city_name,
                geo.metro_code,
                geo.time_zone,
                geo.is_in_european_union,
                LOWER(ipinfo.country_iso) AS country_iso,
                ipinfo.registered_country_iso,
                ipinfo.is_anonymous,
                ipinfo.is_anonymous_vpn,
                ipinfo.is_hosting_provider,
                ipinfo.is_public_proxy,
                ipinfo.is_tor_exit_node,
                ipinfo.is_residential_proxy,
                ipinfo.autonomous_system_number,
                ipinfo.autonomous_system_organization,
                ipinfo.domain,
                ipinfo.isp,
                ipinfo.mobile_country_code,
                ipinfo.mobile_network_code,
                ipinfo.network,
                ipinfo.organization,
                ipinfo.static_ip_score,
                ipinfo.user_type,
                ipinfo.postal_code,
                CAST(ipinfo.latitude AS float) AS latitude,
                CAST(ipinfo.longitude AS float) AS longitude,
                ipinfo.accuracy_radius,
                ipinfo.ip,
                ipinfo.updated
            FROM thl_ipinformation AS ipinfo
            LEFT JOIN thl_geoname AS geo
                ON ipinfo.geoname_id = geo.geoname_id
            WHERE ipinfo.ip = %s
        """
        res = self.pg_config.execute_sql_query(query=query, params=[ip_address])
        assert len(res) == 1
        d = res[0]
        if d.get("geo_country_iso") and (d["geo_country_iso"] != d["country_iso"]):
            raise ValueError(
                f'mismatch between ipinfo country {d["country_iso"]} and geoname country {d["geo_country_iso"]}'
            )
        return d

    def get_mysql(self, ip_address: IPvAnyAddressStr):
        normalized_ip, lookup_prefix = normalize_ip(ip_address)
        d = self.get_mysql_raw(normalized_ip)
        g = GeoIPInformation.from_mysql(d)
        g.ip = ip_address
        g.lookup_prefix = lookup_prefix
        return g

    def recreate_cache(self, ip_address: IPvAnyAddressStr) -> GeoIPInformation:
        res = self.get_mysql(ip_address)
        self.set_cache(res)
        return res

    def get_mysql_multi(
        self,
        ips: Collection[IPvAnyAddressStr],
    ) -> Dict[IPvAnyAddressStr, Optional[GeoIPInformation]]:

        if len(ips) == 0:
            return {}

        with self.pg_config.make_connection() as sql_connection:
            sql_connection: pymysql.Connection
            with sql_connection.cursor() as c:
                res = {}
                for chunk in chunked(ips, 500):
                    inner = self.get_mysql_multi_chunk(
                        c=c,
                        ips=chunk,
                    )
                    res.update(inner)
        return res

    def get_mysql_multi_chunk(
        self,
        c: Cursor,
        ips: List[IPvAnyAddressStr],
    ) -> Dict[IPvAnyAddressStr, Optional[GeoIPInformation]]:

        assert len(ips) <= 500, "chunk me"

        # We must do it like this b/c we could have multiple /128 ips that normalize
        #   to the same normalized /64 ip, and we don't want to "loose" them.
        ip_norm_lookup = {ip: normalize_ip(ip) for ip in ips}
        normalized_ips = {v[0] for v in ip_norm_lookup.values()}
        # also lookup exact matches (can remove this 28 days from 2025-11-15)
        normalized_ips.update(ips)
        # also lookup compressed form ... (remove me also)
        normalized_ips.update({self.compress_ip(ip) for ip in ips})

        c.execute(
            query=f"""
            SELECT
                geo.geoname_id,
                geo.continent_name,
                LOWER(geo.continent_code) AS continent_code,
                geo.country_name,
                LOWER(geo.country_iso) AS geo_country_iso,
                geo.subdivision_1_iso,
                geo.subdivision_1_name,
                geo.subdivision_2_iso,
                geo.subdivision_2_name,
                geo.city_name,
                geo.metro_code,
                geo.time_zone,
                geo.is_in_european_union,
                LOWER(ipinfo.country_iso) AS country_iso,
                ipinfo.registered_country_iso,
                ipinfo.is_anonymous,
                ipinfo.is_anonymous_vpn,
                ipinfo.is_hosting_provider,
                ipinfo.is_public_proxy,
                ipinfo.is_tor_exit_node,
                ipinfo.is_residential_proxy,
                ipinfo.autonomous_system_number,
                ipinfo.autonomous_system_organization,
                ipinfo.domain,
                ipinfo.isp,
                ipinfo.mobile_country_code,
                ipinfo.mobile_network_code,
                ipinfo.network,
                ipinfo.organization,
                ipinfo.static_ip_score,
                ipinfo.user_type,
                ipinfo.postal_code,
                CAST(ipinfo.latitude AS float) AS latitude,
                CAST(ipinfo.longitude AS float) AS longitude,
                ipinfo.accuracy_radius,
                ipinfo.ip,
                ipinfo.updated
            FROM thl_ipinformation AS ipinfo
            LEFT JOIN thl_geoname AS geo
                ON ipinfo.geoname_id = geo.geoname_id
            WHERE ipinfo.ip = ANY(%s)
            """,
            params=[list(normalized_ips)],
        )

        res = c.fetchall()
        for d in res:
            if d.get("geo_country_iso") and (d["geo_country_iso"] != d["country_iso"]):
                raise ValueError(
                    f'mismatch between ipinfo country {d["country_iso"]} and geoname country {d["geo_country_iso"]}'
                )
        gs = [GeoIPInformation.from_mysql(i) for i in res]
        gs = {g.ip: g for g in gs}
        res2 = dict()
        for ip, (normalized_ip, lookup_prefix) in ip_norm_lookup.items():
            if normalized_ip not in gs:
                # also can remove 28 days after 2025-11-15
                if ip in gs:
                    res2[ip] = gs[ip].model_copy()
                    continue
                res2[ip] = None
                continue
            g = gs[normalized_ip]
            g.ip = ip
            g.lookup_prefix = lookup_prefix
            res2[g.ip] = g.model_copy()
        return res2
