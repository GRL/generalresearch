from enum import StrEnum
from ipaddress import ip_address, ip_network
from typing import Optional

CGNAT_NET = ip_network("100.64.0.0/10")


class IPProtocol(StrEnum):
    TCP = "tcp"
    UDP = "udp"
    SCTP = "sctp"
    IP = "ip"
    ICMP = "icmp"
    ICMPv6 = "icmpv6"

    def to_number(self) -> int:
        # https://www.iana.org/assignments/protocol-numbers/protocol-numbers.xhtml
        return {
            self.TCP: 6,
            self.UDP: 17,
            self.SCTP: 132,
            self.IP: 4,
            self.ICMP: 1,
            self.ICMPv6: 58,
        }[self]


class IPKind(StrEnum):
    PUBLIC = "public"
    PRIVATE = "private"
    CGNAT = "carrier_nat"
    LOOPBACK = "loopback"
    LINK_LOCAL = "link_local"
    MULTICAST = "multicast"
    RESERVED = "reserved"
    UNSPECIFIED = "unspecified"


def get_ip_kind(ip: Optional[str]) -> Optional[IPKind]:
    if not ip:
        return None

    ip_obj = ip_address(ip)

    if ip_obj in CGNAT_NET:
        return IPKind.CGNAT

    if ip_obj.is_loopback:
        return IPKind.LOOPBACK

    if ip_obj.is_link_local:
        return IPKind.LINK_LOCAL

    if ip_obj.is_multicast:
        return IPKind.MULTICAST

    if ip_obj.is_unspecified:
        return IPKind.UNSPECIFIED

    if ip_obj.is_private:
        return IPKind.PRIVATE

    if ip_obj.is_reserved:
        return IPKind.RESERVED

    if ip_obj.is_global:
        return IPKind.PUBLIC

    return None
