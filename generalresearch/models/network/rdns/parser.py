import ipaddress
import re
from typing import TYPE_CHECKING

from generalresearch.models.network.rdns.result import RDNSResult

if TYPE_CHECKING:
    from generalresearch.models.custom_types import IPvAnyAddressStr

PTR_RE = re.compile(r"\sPTR\s+([^\s]+)\.")


def parse_rdns_output(ip: IPvAnyAddressStr, raw: str) -> RDNSResult:
    hostnames: list[str] = []

    for line in raw.splitlines():
        m = PTR_RE.search(line)
        if m:
            hostnames.append(m.group(1))

    return RDNSResult(
        ip=ipaddress.ip_address(ip),
        hostnames=hostnames,
    )
