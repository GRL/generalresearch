import ipaddress
from datetime import datetime, timezone
from typing import Any, Dict, Literal, Optional, Tuple

import geoip2.models
from faker import Faker
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    PositiveInt,
    PrivateAttr,
    field_validator,
)
from typing_extensions import Self

from generalresearch.models.custom_types import (
    AwareDatetimeISO,
    CountryISOLike,
    IPvAnyAddressStr,
)
from generalresearch.models.thl.maxmind.definitions import UserType
from generalresearch.pg_helper import PostgresConfig

fake = Faker()

PrefixLength = Literal["/128", "/64", "/32"]


def normalize_ip(ip: IPvAnyAddressStr) -> Tuple[str, PrefixLength]:
    """
    Normalize an IP address for MySQL storage.

    - IPv4: returned unchanged
    - IPv6: converted to its /64 network address and returned
            in fully expanded (exploded) form
    Returns:
        (ip, lookup_prefix)
    """
    addr = ipaddress.ip_address(ip)
    if addr.version == 4:
        return ip, "/32"
    net64 = ipaddress.IPv6Network((addr, 64), strict=False)
    return net64.network_address.exploded, "/64"


class IPGeoname(BaseModel):
    geoname_id: PositiveInt = Field()

    continent_code: Optional[str] = Field(default=None, max_length=2)
    continent_name: Optional[str] = Field(default=None, max_length=32)

    country_iso: CountryISOLike = Field(
        description="The ISO code of the country associated with the IP address.",
        examples=[fake.country_code().lower()],
    )
    country_name: Optional[str] = Field(default=None, max_length=64)

    subdivision_1_iso: Optional[str] = Field(
        default=None,
        description="The ISO code of the primary subdivision (e.g., state or province).",
        max_length=3,
    )
    subdivision_1_name: Optional[str] = Field(
        default=None,
        description="The name of the primary subdivision (e.g., state or province).",
        max_length=255,
    )
    subdivision_2_iso: Optional[str] = Field(
        default=None,
        description="The ISO code of the secondary subdivision (if applicable).",
        max_length=3,
    )
    subdivision_2_name: Optional[str] = Field(
        default=None,
        description="The name of the secondary subdivision (if applicable).",
        max_length=255,
    )

    city_name: Optional[str] = Field(
        default=None,
        max_length=255,
        description="The name of the city associated with the IP address.",
        examples=[fake.city()],
    )
    metro_code: Optional[int] = Field(default=None)

    time_zone: Optional[str] = Field(
        default=None,
        max_length=60,
        description="The time zone associated with the geographical location.",
        examples=[fake.timezone()],
    )
    is_in_european_union: Optional[bool] = Field(default=None)

    updated: AwareDatetimeISO = Field(
        default_factory=lambda: datetime.now(tz=timezone.utc),
    )

    @field_validator(
        "country_iso",
        "continent_code",
        "subdivision_1_iso",
        "subdivision_2_iso",
        mode="before",
    )
    def make_lower(cls, value: Optional[str]) -> Optional[str]:
        if value is not None:
            return value.lower()

        return value

    # --- ORM ---
    def model_dump_mysql(self) -> Dict[str, Any]:
        d = self.model_dump(mode="json")
        d["updated"] = self.updated
        return d

    @classmethod
    def from_mysql(cls, d: Dict[str, Any]) -> Self:
        d["updated"] = d["updated"].replace(tzinfo=timezone.utc)

        return cls.model_validate(d)

    @classmethod
    def from_insights(cls, res: geoip2.models.Insights) -> Self:
        geoname_id = res.city.geoname_id
        # Some ips don't have city level specificity. grab the first subdivision if it exists
        if geoname_id is None and len(res.subdivisions) > 0:
            geoname_id = res.subdivisions[0].geoname_id
        elif geoname_id is None:
            # No city, no subdivision, use the country
            geoname_id = res.country.geoname_id
        # Some ips have a city but no subdivisions (41.33.89.99)
        d = {
            "geoname_id": geoname_id,
            "continent_code": res.continent.code,
            "continent_name": res.continent.name,
            "country_iso": res.country.iso_code,
            "country_name": res.country.name,
            "city_name": res.city.name,
            "metro_code": res.location.metro_code,
            "time_zone": res.location.time_zone,
            "is_in_european_union": res.country.is_in_european_union,
            "subdivision_1_iso": None,
            "subdivision_1_name": None,
            "subdivision_2_iso": None,
            "subdivision_2_name": None,
        }
        if len(res.subdivisions) > 0:
            d.update(
                {
                    "subdivision_1_iso": res.subdivisions[0].iso_code,
                    "subdivision_1_name": res.subdivisions[0].name,
                }
            )
        if len(res.subdivisions) > 1:
            d.update(
                {
                    "subdivision_2_iso": res.subdivisions[1].iso_code,
                    "subdivision_2_name": res.subdivisions[1].name,
                }
            )
        return cls.model_validate(d)


class IPInformation(BaseModel):
    ip: IPvAnyAddressStr = Field()
    # This doesn't get stored in mysql/redis, b/c we only look up by the normalized ip
    lookup_prefix: Optional[PrefixLength] = Field(default=None, exclude=True)

    geoname_id: Optional[PositiveInt] = Field(default=None)

    country_iso: CountryISOLike = Field(
        description="The ISO code of the country associated with the IP address.",
        examples=[fake.country_code().lower()],
    )

    registered_country_iso: Optional[CountryISOLike] = Field(
        default=None,
        description="The ISO code of the country where the IP address is "
        "registered.",
        examples=[fake.country_code().lower()],
    )
    is_anonymous: Optional[bool] = Field(
        default=None,
        description="Indicates whether the IP address is associated with an "
        "anonymous source (e.g., VPN, proxy).",
        examples=[False],
    )
    is_anonymous_vpn: Optional[bool] = Field(default=None)
    is_hosting_provider: Optional[bool] = Field(default=None)
    is_public_proxy: Optional[bool] = Field(default=None)
    is_tor_exit_node: Optional[bool] = Field(default=None)
    is_residential_proxy: Optional[bool] = Field(default=None)

    autonomous_system_number: Optional[PositiveInt] = Field(default=None)
    autonomous_system_organization: Optional[str] = Field(default=None, max_length=255)

    domain: Optional[str] = Field(default=None, max_length=255)
    isp: Optional[str] = Field(
        default=None,
        description="The Internet Service Provider associated with the " "IP address.",
        examples=["Comcast"],
    )

    mobile_country_code: Optional[str] = Field(default=None, max_length=3)
    mobile_network_code: Optional[str] = Field(default=None, max_length=3)

    network: Optional[str] = Field(default=None, max_length=56)
    organization: Optional[str] = Field(default=None, max_length=255)

    static_ip_score: Optional[float] = Field(
        default=None,
        description="A score indicating the likelihood that the IP address is static.",
    )
    user_type: Optional[UserType] = Field(
        default=None,
        description="The type of user associated with the IP address "
        "(e.g., 'residential', 'business').",
        examples=[UserType.SCHOOL],
    )
    postal_code: Optional[str] = Field(
        default=None,
        description="The postal code associated with the IP address.",
        examples=[fake.postcode()],
    )

    latitude: Optional[float] = Field(
        description="The latitude coordinate of the IP address location.",
        default=None,
        examples=[float(fake.latitude())],
    )
    longitude: Optional[float] = Field(
        description="The longitude coordinate of the IP address location.",
        default=None,
        examples=[float(fake.longitude())],
    )

    accuracy_radius: Optional[int] = Field(
        default=None,
        description="The approximate radius of accuracy for the latitude "
        "and longitude, in kilometers.",
        examples=[fake.random_int(min=25, max=250)],
    )

    updated: AwareDatetimeISO = Field(
        default_factory=lambda: datetime.now(tz=timezone.utc),
    )

    _geoname: Optional[IPGeoname] = PrivateAttr(default=None)

    @field_validator("country_iso", "registered_country_iso", mode="before")
    def make_lower(cls, value: Optional[str]) -> Optional[str]:
        if value is not None:
            return value.lower()

        return value

    @property
    def basic(self) -> bool:
        # This could be almost any field, but we're checking here if maxmind
        #   insights was run on this record. If not, then most of the optional
        #   fields will be None
        return self.is_anonymous is None

    @property
    def geoname(self) -> Optional["IPGeoname"]:
        return self._geoname or None

    def normalize_ip(self):
        normalized_ip, lookup_prefix = normalize_ip(self.ip)
        self.ip = normalized_ip
        self.lookup_prefix = lookup_prefix
        return None

    # --- prefetch_* ---
    def prefetch_geoname(
        self,
        pg_config: PostgresConfig,
    ) -> None:
        if self.geoname_id is None:
            raise ValueError("Must provide geoname_id")

        from generalresearch.managers.thl.ipinfo import IPGeonameManager

        ip_gm = IPGeonameManager(pg_config=pg_config)

        self._geoname = ip_gm.get_by_id(geoname_id=self.geoname_id)

        return None

    # --- ORM ---
    def model_dump_mysql(self):
        d = self.model_dump(mode="json", exclude={"geoname"})
        d["updated"] = self.updated
        return d

    @classmethod
    def from_mysql(cls, d: Dict) -> Self:
        d["updated"] = d["updated"].replace(tzinfo=timezone.utc)

        return cls.model_validate(d)

    @classmethod
    def from_insights(cls, res: geoip2.models.Insights) -> Self:
        geoname_id = res.city.geoname_id
        # Some ips don't have city level specificity. grab the first subdivision if it exists
        if geoname_id is None and len(res.subdivisions) > 0:
            geoname_id = res.subdivisions[0].geoname_id
        elif geoname_id is None:
            # No city, no subdivision, use the country
            geoname_id = res.country.geoname_id
        return cls.model_validate(
            {
                "ip": res.traits.ip_address,
                "network": str(res.traits.network),
                "geoname_id": geoname_id,
                "country_iso": res.country.iso_code.upper(),
                "registered_country_iso": (
                    res.registered_country.iso_code.upper()
                    if res.registered_country.iso_code
                    else None
                ),
                "is_anonymous": res.traits.is_anonymous,
                "is_anonymous_vpn": res.traits.is_anonymous_vpn,
                "is_hosting_provider": res.traits.is_hosting_provider,
                "is_public_proxy": res.traits.is_public_proxy,
                "is_tor_exit_node": res.traits.is_tor_exit_node,
                "is_residential_proxy": res.traits.is_residential_proxy,
                "autonomous_system_number": res.traits.autonomous_system_number,
                "autonomous_system_organization": res.traits.autonomous_system_organization,
                "domain": res.traits.domain,
                "isp": res.traits.isp,
                "mobile_country_code": res.traits.mobile_country_code,
                "mobile_network_code": res.traits.mobile_network_code,
                "organization": res.traits.organization,
                "static_ip_score": res.traits.static_ip_score,
                "user_type": res.traits.user_type,
                # IP-specific location that may be different for different IPs in the same City
                "postal_code": res.postal.code,
                "latitude": res.location.latitude,
                "longitude": res.location.longitude,
                "accuracy_radius": res.location.accuracy_radius,
            }
        )


class GeoIPInformation(IPInformation, IPGeoname):
    model_config = ConfigDict(extra="ignore")
