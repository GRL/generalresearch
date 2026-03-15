import ipaddress
import re
from typing import List

from generalresearch.models.custom_types import IPvAnyAddressStr
from generalresearch.models.network.rdns.result import RDNSResult

PTR_RE = re.compile(r"\sPTR\s+([^\s]+)\.")


def parse_rdns_output(ip: IPvAnyAddressStr, raw:str):
    hostnames: List[str] = []

    for line in raw.splitlines():
        m = PTR_RE.search(line)
        if m:
            hostnames.append(m.group(1))

    return RDNSResult(
        ip=ipaddress.ip_address(ip),
        hostnames=hostnames,
    )
