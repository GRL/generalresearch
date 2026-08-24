from __future__ import annotations

import ipaddress
from datetime import UTC, datetime
from typing import Any, Literal, Self

from faker import Faker
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    PositiveInt,
    PrivateAttr,
    field_validator,
)

from generalresearch.models.custom_types import (
    AwareDatetimeISO,
    CountryISOLike,
    IPvAnyAddressStr,
)
from generalresearch.models.thl.maxmind.definitions import UserType
from generalresearch.pg_helper import PostgresConfig

fake = Faker()

PrefixLength = Literal["/128", "/64", "/32"]


def normalize_ip(ip: IPvAnyAddressStr) -> tuple[str, PrefixLength]:
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

    continent_code: str | None = Field(default=None, max_length=2)
    continent_name: str | None = Field(default=None, max_length=32)

    country_iso: CountryISOLike = Field(
        description="The ISO code of the country associated with the IP address.",
        examples=[fake.country_code().lower()],
    )
    country_name: str | None = Field(default=None, max_length=64)

    subdivision_1_iso: str | None = Field(
        default=None,
        description="The ISO code of the primary subdivision (e.g., state or province).",
        max_length=3,
    )
    subdivision_1_name: str | None = Field(
        default=None,
        description="The name of the primary subdivision (e.g., state or province).",
        max_length=255,
    )
    subdivision_2_iso: str | None = Field(
        default=None,
        description="The ISO code of the secondary subdivision (if applicable).",
        max_length=3,
    )
    subdivision_2_name: str | None = Field(
        default=None,
        description="The name of the secondary subdivision (if applicable).",
        max_length=255,
    )

    city_name: str | None = Field(
        default=None,
        max_length=255,
        description="The name of the city associated with the IP address.",
        examples=[fake.city()],
    )
    metro_code: int | None = Field(default=None)

    time_zone: str | None = Field(
        default=None,
        max_length=60,
        description="The time zone associated with the geographical location.",
        examples=[fake.timezone()],
    )
    is_in_european_union: bool | None = Field(default=None)

    updated: AwareDatetimeISO = Field(
        default_factory=lambda: datetime.now(tz=UTC),
    )

    @field_validator(
        "country_iso",
        "continent_code",
        "subdivision_1_iso",
        "subdivision_2_iso",
        mode="before",
    )
    def make_lower(cls, value: str | None) -> str | None:
        if value is not None:
            return value.lower()

        return value

    # --- ORM ---
    def model_dump_mysql(self) -> dict[str, Any]:
        d = self.model_dump(mode="json")
        d["updated"] = self.updated
        return d

    @classmethod
    def from_mysql(cls, d: dict[str, Any]) -> Self:
        d["updated"] = d["updated"].replace(tzinfo=UTC)

        return cls.model_validate(d)


class IPInformation(BaseModel):
    ip: IPvAnyAddressStr = Field()
    # This doesn't get stored in mysql/redis, b/c we only look up by the normalized ip
    lookup_prefix: PrefixLength | None = Field(default=None, exclude=True)

    geoname_id: PositiveInt | None = Field(default=None)

    country_iso: CountryISOLike = Field(
        description="The ISO code of the country associated with the IP address.",
        examples=[fake.country_code().lower()],
    )

    registered_country_iso: CountryISOLike | None = Field(
        default=None,
        description="The ISO code of the country where the IP address is "
        "registered.",
        examples=[fake.country_code().lower()],
    )
    is_anonymous: bool | None = Field(
        default=None,
        description="Indicates whether the IP address is associated with an "
        "anonymous source (e.g., VPN, proxy).",
        examples=[False],
    )
    is_anonymous_vpn: bool | None = Field(default=None)
    is_hosting_provider: bool | None = Field(default=None)
    is_public_proxy: bool | None = Field(default=None)
    is_tor_exit_node: bool | None = Field(default=None)
    is_residential_proxy: bool | None = Field(default=None)

    autonomous_system_number: PositiveInt | None = Field(default=None)
    autonomous_system_organization: str | None = Field(default=None, max_length=255)

    domain: str | None = Field(default=None, max_length=255)
    isp: str | None = Field(
        default=None,
        description="The Internet Service Provider associated with the " "IP address.",
        examples=["Comcast"],
    )

    mobile_country_code: str | None = Field(default=None, max_length=3)
    mobile_network_code: str | None = Field(default=None, max_length=3)

    network: str | None = Field(default=None, max_length=56)
    organization: str | None = Field(default=None, max_length=255)

    static_ip_score: float | None = Field(
        default=None,
        description="A score indicating the likelihood that the IP address is static.",
    )
    user_type: UserType | None = Field(
        default=None,
        description="The type of user associated with the IP address "
        "(e.g., 'residential', 'business').",
        examples=[UserType.SCHOOL],
    )
    postal_code: str | None = Field(
        default=None,
        description="The postal code associated with the IP address.",
        examples=[fake.postcode()],
    )

    latitude: float | None = Field(
        description="The latitude coordinate of the IP address location.",
        default=None,
        examples=[float(fake.latitude())],
    )
    longitude: float | None = Field(
        description="The longitude coordinate of the IP address location.",
        default=None,
        examples=[float(fake.longitude())],
    )

    accuracy_radius: int | None = Field(
        default=None,
        description="The approximate radius of accuracy for the latitude "
        "and longitude, in kilometers.",
        examples=[fake.random_int(min=25, max=250)],
    )

    updated: AwareDatetimeISO = Field(
        default_factory=lambda: datetime.now(tz=UTC),
    )

    _geoname: IPGeoname | None = PrivateAttr(default=None)

    @field_validator("country_iso", "registered_country_iso", mode="before")
    def make_lower(cls, value: str | None) -> str | None:
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
    def geoname(self) -> IPGeoname | None:
        return self._geoname or None

    def normalize_ip(self):
        normalized_ip, lookup_prefix = normalize_ip(self.ip)
        self.ip = normalized_ip
        self.lookup_prefix = lookup_prefix

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

    # --- ORM ---
    def model_dump_mysql(self):
        d = self.model_dump(mode="json", exclude={"geoname"})
        d["updated"] = self.updated
        return d

    @classmethod
    def from_mysql(cls, d: dict) -> Self:
        d["updated"] = d["updated"].replace(tzinfo=UTC)

        return cls.model_validate(d)


class GeoIPInformation(IPInformation, IPGeoname):
    model_config = ConfigDict(extra="ignore")
