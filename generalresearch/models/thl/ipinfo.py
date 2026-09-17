from __future__ import annotations

import ipaddress
from datetime import UTC, datetime
from typing import Any, Literal, Self

from faker import Faker
from grip_client import AccessType
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    PositiveInt,
    field_validator,
    IPvAnyAddress,
)

from generalresearch.models.custom_types import (
    AwareDatetimeISO,
    CountryISOLike,
    IPvAnyAddressStr,
)

fake = Faker()

PrefixLength = Literal["/128", "/64", "/32"]


def normalize_ip(ip: str | IPvAnyAddress) -> tuple[str, PrefixLength]:
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
        return addr.exploded, "/32"
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
    """
    Fields we'll always pull from GRIP's mmdb files at minimum
    """

    ip: IPvAnyAddressStr = Field()

    country_iso: CountryISOLike | None = Field(
        description="The ISO code of the country associated with the IP address.",
        examples=[fake.country_code().lower()],
    )

    is_anonymous: bool | None = Field(
        default=None,
        description="Indicates whether the IP address is associated with an "
        "anonymous source (e.g., VPN, proxy).",
        examples=[False],
    )

    autonomous_system_number: PositiveInt | None = Field(default=None)
    autonomous_system_organization: str | None = Field(default=None, max_length=255)

    access_type: AccessType | None = Field(
        default=None,
        description="The type of user associated with the IP address "
        "(e.g., 'residential', 'business').",
        examples=[AccessType.RESIDENTIAL],
    )


class GeoIPInformation(IPInformation):
    model_config = ConfigDict(extra="ignore")
