from collections.abc import Collection

from grip_client import GRIPMMDBClient

from generalresearch.managers.base import Manager
from generalresearch.models.custom_types import IPvAnyAddressStr
from generalresearch.models.thl.ipinfo import GeoIPInformation


class GeoIpInfoManager(Manager):
    def __init__(self, grip_token: str, **kwargs) -> None:
        self.grip_mmdb = GRIPMMDBClient(token=grip_token, **kwargs)

    def get(self, ip_address: IPvAnyAddressStr) -> GeoIPInformation:
        d = self.get_multi({ip_address})
        return d[ip_address]

    def get_multi(
        self, ip_addresses: Collection[IPvAnyAddressStr]
    ) -> dict[IPvAnyAddressStr, GeoIPInformation]:
        if not ip_addresses:
            return {}
        ips = set(ip_addresses)
        d = {ip: self.grip_mmdb.lookup(ip) for ip in ips}
        d = {
            ip: GeoIPInformation(
                ip=ip,
                country_iso=v.country.country_iso.lower()
                if v.country.country_iso
                else None,
                is_anonymous=v.anonymous.is_anonymous,
                autonomous_system_number=v.asn.asn,
                autonomous_system_organization=v.asn.network_operator,
                access_type=v.asn.access_type,
            )
            for ip, v in d.items()
        }
        return d
