import ipaddress
from datetime import timezone, datetime, timedelta
from typing import List, Optional, Dict

from faker import Faker
from pydantic import (
    BaseModel,
    Field,
    ConfigDict,
    PositiveInt,
    field_validator,
)
from typing_extensions import Self

from generalresearch.models.custom_types import (
    AwareDatetimeISO,
    IPvAnyAddressStr,
    CountryISOLike,
)
from generalresearch.models.thl.ipinfo import (
    GeoIPInformation,
    normalize_ip,
)
from generalresearch.models.thl.maxmind.definitions import UserType
from generalresearch.models.thl.user import User
from generalresearch.pg_helper import PostgresConfig
from generalresearch.redis_helper import RedisConfig

fake = Faker()


class UserIPRecord(BaseModel):
    model_config = ConfigDict(validate_assignment=True)

    ip: IPvAnyAddressStr = Field()
    created: AwareDatetimeISO = Field()
    information: Optional[GeoIPInformation] = Field(default=None, exclude=True)

    @property
    def country_iso(self) -> Optional[CountryISOLike]:
        return self.information.country_iso if self.information else None

    @property
    def is_anonymous(self) -> bool:
        # default to False even if insights is not looked up
        return (
            self.information.is_anonymous
            if self.information
            and self.information.basic is False
            and self.information.is_anonymous is not None
            else False
        )

    @property
    def user_type(self) -> Optional[UserType]:
        return self.information.user_type if self.information else None

    @property
    def subdivision_1_iso(self) -> Optional[str]:
        return self.information.subdivision_1_iso if self.information else None

    @property
    def subdivision_2_iso(self) -> Optional[str]:
        return self.information.subdivision_2_iso if self.information else None


class IPRecord(BaseModel):
    user_id: PositiveInt = Field()
    ip: IPvAnyAddressStr = Field()
    created: AwareDatetimeISO = Field()

    # On a top-level, this should be an empty list if there are no forwarded_ip.
    #   Within a forwarded_ip record, this should be None.
    forwarded_ip_records: Optional[List["IPRecord"]] = Field(
        default=None, description=""
    )

    information: Optional[GeoIPInformation] = Field(default=None)

    @property
    def forwarded_ips(self) -> Optional[List[IPvAnyAddressStr]]:
        return (
            [x.ip for x in self.forwarded_ip_records]
            if self.forwarded_ip_records is not None
            else None
        )

    def ip_changed(
        self, ip: IPvAnyAddressStr, forwarded_ips: List[IPvAnyAddressStr]
    ) -> bool:
        return not (ip == self.ip and forwarded_ips == self.forwarded_ips)

    # --- prefetch_* ---
    def prefetch_ipinfo(
        self,
        pg_config: PostgresConfig,
        redis_config: RedisConfig,
        include_forwarded: bool = True,
    ) -> None:
        from generalresearch.managers.thl.ipinfo import GeoIpInfoManager

        m = GeoIpInfoManager(pg_config=pg_config, redis_config=redis_config)

        if include_forwarded:
            ips = {self.ip}
            ips.update(set(self.forwarded_ips))
            res = m.get_multi(ips)
            self.information = res.get(self.ip)
            for x in self.forwarded_ip_records:
                x.information = res.get(x.ip)
        else:
            self.information = m.get(ip_address=self.ip)
        return None

    # --- ORM ---
    @classmethod
    def from_mysql(cls, d: Dict) -> Self:
        created = d["created"].replace(tzinfo=timezone.utc)

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

    user_id: PositiveInt = Field()

    # In thl-gprc, we run "audit_ip_history()", and so a user should
    #   get blocked after 100 IP switches or 30 unique IPs
    # Sorted created DESC
    ips: Optional[List[UserIPRecord]] = Field(
        default=None,
        description="These are any IP addresses that came in ",
        max_length=101,
    )

    ips_ws: Optional[List[IPRecord]] = Field(
        default=None, description="These are any IP addresses that came in "
    )

    ips_dns: Optional[List[IPRecord]] = Field(
        default=None, description="These are any IP addresses that came in "
    )

    # -- prefetch_ fields
    user: Optional[User] = Field(default=None)

    @field_validator("ips", mode="after")
    @classmethod
    def ips_timestamp(cls, ips):
        if ips is None:
            return None
        cutoff = datetime.now(tz=timezone.utc) - timedelta(days=28)
        return sorted(
            [x for x in ips if x.created > cutoff],
            key=lambda x: x.created,
            reverse=True,
        )

    def prefetch_user(
        self,
        pg_config: PostgresConfig,
        redis_config: RedisConfig,
        pg_config_rr: PostgresConfig,
    ) -> None:
        from generalresearch.managers.thl.user_manager.user_manager import (
            UserManager,
        )

        um = UserManager(
            pg_config=pg_config,
            pg_config_rr=pg_config_rr,
            redis=redis_config.dsn,
        )
        self.user = um.get_user(user_id=self.user_id)

        return None

    def enrich_ips(self, pg_config: PostgresConfig, redis_config: RedisConfig) -> None:
        from generalresearch.managers.thl.ipinfo import GeoIpInfoManager

        m = GeoIpInfoManager(pg_config=pg_config, redis_config=redis_config)

        ip_addresses = {x.ip for x in self.ips if x.information is None}
        res = m.get_multi(ip_addresses=ip_addresses)
        for x in self.ips:
            if res.get(x.ip):
                x.information = res[x.ip]

        return None

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
