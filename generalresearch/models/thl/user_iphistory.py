from __future__ import annotations

import ipaddress
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Self

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    PositiveInt,
    field_validator,
)

from generalresearch.models.custom_types import (
    AwareDatetimeISO,
    CountryISOLike,
    IPvAnyAddressStr,
)
from generalresearch.models.thl.ipinfo import GeoIPInformation, normalize_ip
from generalresearch.models.thl.user_ref import UserRef

if TYPE_CHECKING:
    from grip_client.enums import AccessType

    from generalresearch.managers.thl.ipinfo import GeoIpInfoManager


class UserIPRecord(BaseModel):
    model_config = ConfigDict(validate_assignment=True)

    ip: IPvAnyAddressStr = Field()
    created: AwareDatetimeISO = Field()
    information: GeoIPInformation | None = Field(default=None, exclude=True)

    @property
    def country_iso(self) -> CountryISOLike | None:
        return self.information.country_iso if self.information else None

    @property
    def is_anonymous(self) -> bool:
        # Default to false if information is not looked up
        return (self.information.is_anonymous if self.information else False) or False

    @property
    def access_type(self) -> AccessType | None:
        return self.information.access_type if self.information else None


class IPRecord(BaseModel):
    user_id: PositiveInt = Field()
    ip: IPvAnyAddressStr = Field()
    created: AwareDatetimeISO = Field()

    # On a top-level, this should be an empty list if there are no forwarded_ip.
    #   Within a forwarded_ip record, this should be None.
    forwarded_ip_records: list[IPRecord] | None = Field(default=None, description="")

    information: GeoIPInformation | None = Field(default=None)

    @property
    def forwarded_ips(self) -> list[IPvAnyAddressStr] | None:
        return (
            [x.ip for x in self.forwarded_ip_records]
            if self.forwarded_ip_records is not None
            else None
        )

    def ip_changed(
        self, ip: IPvAnyAddressStr, forwarded_ips: list[IPvAnyAddressStr]
    ) -> bool:
        return not (ip == self.ip and forwarded_ips == self.forwarded_ips)

    # --- ORM ---
    @classmethod
    def from_mysql(cls, d: dict) -> Self:
        created = d["created"].replace(tzinfo=UTC)

        d["created"] = created
        d["forwarded_ip_records"] = []

        for fip in [
            d.get("forwarded_ip1"),
            d.get("forwarded_ip2"),
            d.get("forwarded_ip3"),
            d.get("forwarded_ip4"),
            d.get("forwarded_ip5"),
            d.get("forwarded_ip6"),
        ]:
            if fip:
                d["forwarded_ip_records"].append(
                    {
                        "user_id": d["user_id"],
                        "ip": fip,
                        "created": created,
                        "forwarded_ip_records": None,
                    }
                )

        return cls.model_validate(d)


class UserIPHistory(BaseModel):
    model_config = ConfigDict(validate_assignment=True)

    user: UserRef = Field()

    # In thl-gprc, we run "audit_ip_history()", and so a user should
    #   get blocked after 100 IP switches or 30 unique IPs
    # Sorted created DESC
    ips: list[UserIPRecord] | None = Field(
        default=None,
        description="These are any IP addresses that came in ",
        max_length=101,
    )

    ips_ws: list[IPRecord] | None = Field(
        default=None, description="These are any IP addresses that came in "
    )

    ips_dns: list[IPRecord] | None = Field(
        default=None, description="These are any IP addresses that came in "
    )

    @field_validator("ips", mode="after")
    @classmethod
    def ips_timestamp(cls, ips):
        if ips is None:
            return None
        cutoff = datetime.now(tz=UTC) - timedelta(days=28)
        return sorted(
            [x for x in ips if x.created > cutoff],
            key=lambda x: x.created,
            reverse=True,
        )

    def enrich_ips(self, geoip_info_manager: GeoIpInfoManager) -> None:
        if not self.ips:
            return
        ip_addresses = {x.ip for x in self.ips if x.information is None}
        res = geoip_info_manager.get_multi(ip_addresses=ip_addresses)
        for x in self.ips:
            if res.get(x.ip):
                x.information = res[x.ip]

    def collapse_ip_records(self):
        """
        - Records where sequential ipv6 addresses are in the same /64 block,
            just keep the last one.
        - If a user has a new ip b/c they've simply alternated between a ipv4
            and ipv6, only keep the most recent 1 of each version.
        """
        records = self.ips.copy()

        res = []
        last_ipv4 = None
        last_ipv6 = None

        # Iterate through (most recent first)
        for record in records:
            ip = ipaddress.ip_address(record.ip)
            if ip.version == 4:
                if last_ipv4 and last_ipv4 == record.ip:
                    continue
                last_ipv4 = record.ip
                res.append(record)
            elif ip.version == 6:
                normalized_ip, _ = normalize_ip(ip)
                # If the latest ipv6 is the same /64 block as an older one,
                #   discard the older one.
                if last_ipv6 and last_ipv6 == normalized_ip:
                    continue
                last_ipv6 = normalized_ip
                res.append(record)
            else:
                raise ValueError("we've ripped a hole in the universe")

        return res
