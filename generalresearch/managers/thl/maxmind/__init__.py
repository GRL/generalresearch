from typing import Collection, Optional

import geoip2.models

from generalresearch.managers.base import (
    Permission,
    PostgresManagerWithRedis,
)
from generalresearch.managers.thl.ipinfo import (
    GeoIpInfoManager,
    IPGeonameManager,
    IPInformationManager,
)
from generalresearch.managers.thl.maxmind.basic import MaxmindBasicManager
from generalresearch.managers.thl.maxmind.insights import (
    get_insights_ip_information,
    should_call_insights,
)
from generalresearch.models.custom_types import IPvAnyAddressStr
from generalresearch.models.thl.ipinfo import (
    GeoIPInformation,
    IPGeoname,
    IPInformation,
    normalize_ip,
)
from generalresearch.pg_helper import PostgresConfig
from generalresearch.redis_helper import RedisConfig


class MaxmindManager(PostgresManagerWithRedis):
    def __init__(
        self,
        maxmind_account_id: str,
        maxmind_license_key: str,
        pg_config: PostgresConfig,
        redis_config: RedisConfig,
        permissions: Collection[Permission] = None,
    ):
        self.ipinfo_manager = IPInformationManager(pg_config=pg_config)
        self.ipgeo_manager = IPGeonameManager(pg_config=pg_config)
        self.geoipinfo_manager = GeoIpInfoManager(
            pg_config=pg_config, redis_config=redis_config
        )

        self.basic_maxmind_manager = MaxmindBasicManager(
            data_dir="/tmp/",
            maxmind_account_id=maxmind_account_id,
            maxmind_license_key=maxmind_license_key,
        )

        self.maxmind_account_id = maxmind_account_id
        self.maxmind_license_key = maxmind_license_key

        super().__init__(
            pg_config=pg_config,
            redis_config=redis_config,
            permissions=permissions,
        )

    def store_basic_ip_information(self, res: geoip2.models.Country) -> None:
        geoname_id = res.country.geoname_id
        assert geoname_id, "Must have a Geoname ID to store"

        res_geo = self.ipgeo_manager.fetch_geoname_ids(filter_ids=[geoname_id])
        if len(res_geo) == 0:
            self.ipgeo_manager.create_basic(
                geoname_id=geoname_id,
                is_in_european_union=res.country.is_in_european_union,
                country_iso=res.country.iso_code,
                country_name=res.country.name,
                continent_name=res.continent.name,
                continent_code=res.continent.code,
            )

        self.ipinfo_manager.create_basic(
            ip=res.traits.ip_address,
            country_iso=res.country.iso_code,
            registered_country_iso=res.registered_country.iso_code,
            geoname_id=geoname_id,
        )

    def store_insights_ip_information(self, res: geoip2.models.Insights) -> None:
        ipinfo = IPInformation.from_insights(res)
        geoname_id = ipinfo.geoname_id
        res_geo = self.ipgeo_manager.fetch_geoname_ids([geoname_id])
        if len(res_geo) == 0:
            ipgeo = IPGeoname.from_insights(res)
            self.ipgeo_manager.create_or_update(ipgeo=ipgeo)
        self.ipinfo_manager.create_or_update(ipinfo=ipinfo)

        return None

    def get_or_create_ip_information(
        self,
        ip_address: IPvAnyAddressStr,
        force_insights: bool = False,
    ) -> Optional[GeoIPInformation]:
        """
        This is the 'top-level' IP handling call.

        - Check to see if we already 'know about' this IP. If so, return
            it. Otherwise:
        - Lookup basic or detailed info. Cache the result. maxmind lookup
            happens synchronously. If `pool`, the db operation happens async
            and we don't necessarily return the insights info.
        """
        res = self.geoipinfo_manager.get(ip_address)
        if res and (
            (force_insights is True and res.basic is False) or (force_insights is False)
        ):
            return res
        return self.run_ip_information(ip_address, force_insights=force_insights)

    def run_ip_information(
        self,
        ip_address: IPvAnyAddressStr,
        force_insights: bool = False,
    ) -> Optional[GeoIPInformation]:
        """
        Assumes this IP is "unknown" to us (not in the ipinformation table).
        Quick lookup IP using geoip2.Database. If its "good", lookup detailed
        info. Run db update.
        """
        # Quick lookup IP using geoip2.database
        basic_res = self.basic_maxmind_manager.get_basic_ip_information(ip_address)
        if basic_res is None:
            # IP is not 'valid'. We do nothing because if we see it again, it'll just hit the
            #   geoip2.database (and redis and mysql_rr) which is ok... so no biggie.
            return None

        if force_insights or should_call_insights(res=basic_res):
            # IP is valid and country is good. Look up insights.
            return self.get_and_store_insights(ip_address)

        else:
            # IP is valid, but from a spammy country.
            self.store_basic_ip_information(res=basic_res)
            return self.geoipinfo_manager.get(ip_address)

    def get_and_store_insights(
        self,
        ip_address: IPvAnyAddressStr,
    ) -> GeoIPInformation:

        rc = self.redis_client
        normalized_ip, lookup_prefix = normalize_ip(ip_address)
        # Protect the actual calling of this with a lock
        with rc.lock(f"insights-lock:{normalized_ip}", timeout=2, blocking_timeout=1):
            # Check again we don't have it (or it is only the basic that is cached)
            res = self.geoipinfo_manager.get_cache(ip_address=ip_address)
            if res is not None and res.basic is False:
                return res

            res_mm = get_insights_ip_information(
                ip_address=normalized_ip,
                maxmind_account_id=self.maxmind_account_id,
                maxmind_license_key=self.maxmind_license_key,
            )
            self.store_insights_ip_information(res_mm)
            res = self.geoipinfo_manager.recreate_cache(ip_address)

        return res
