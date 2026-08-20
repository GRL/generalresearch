from __future__ import annotations

from decimal import Decimal
from random import randint
from typing import Callable

import faker
from pydantic import PositiveInt

from generalresearch.managers.thl.ipinfo import IPGeonameManager, IPInformationManager
from generalresearch.models.custom_types import IPvAnyAddressStr
from generalresearch.models.thl.ipinfo import IPGeoname, IPInformation, UserType

fake = faker.Faker()


def ipgeoname_factory(ipgeoname_manager: IPGeonameManager) -> Callable[..., IPGeoname]:

    def _inner(
        geoname_id: PositiveInt | None = None,
        continent_code: str | None = None,
        continent_name: str | None = None,
        country_iso: str | None = None,
        country_name: str | None = None,
        subdivision_1_iso: str | None = None,
        subdivision_1_name: str | None = None,
        subdivision_2_iso: str | None = None,
        subdivision_2_name: str | None = None,
        city_name: str | None = None,
        metro_code: int | None = None,
        time_zone: str | None = None,
        is_in_european_union: bool | None = None,
    ) -> IPGeoname:

        return ipgeoname_manager.create(
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

    return _inner


def ipinformation_factory(
    ipinformation_manager: IPInformationManager,
) -> Callable[..., IPInformation]:

    def _inner(
        ip: IPvAnyAddressStr | None = None,
        geoname_id: PositiveInt | None = None,
        country_iso: str | None = None,
        registered_country_iso: str | None = None,
        is_anonymous: bool | None = None,
        is_anonymous_vpn: bool | None = None,
        is_hosting_provider: bool | None = None,
        is_public_proxy: bool | None = None,
        is_tor_exit_node: bool | None = None,
        is_residential_proxy: bool | None = None,
        autonomous_system_number: PositiveInt | None = None,
        autonomous_system_organization: str | None = None,
        domain: str | None = None,
        isp: str | None = None,
        mobile_country_code: str | None = None,
        mobile_network_code: str | None = None,
        network: str | None = None,
        organization: str | None = None,
        static_ip_score: float | None = None,
        user_type: UserType | None = None,
        postal_code: str | None = None,
        latitude: Decimal | None = None,
        longitude: Decimal | None = None,
        accuracy_radius: int | None = None,
    ) -> IPInformation:

        return ipinformation_manager.create(
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

    return _inner
