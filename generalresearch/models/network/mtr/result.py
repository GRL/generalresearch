import re
from functools import cached_property
from ipaddress import ip_address
from typing import List, Optional

import tldextract
from pydantic import (
    Field,
    field_validator,
    BaseModel,
    ConfigDict,
    model_validator,
    computed_field,
)

from generalresearch.models.network.definitions import IPProtocol, get_ip_kind, IPKind

HOST_RE = re.compile(r"^(?P<hostname>.+?) \((?P<ip>[^)]+)\)$")


class MTRHop(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    hop: int = Field(alias="count")
    host: str
    asn: Optional[int] = Field(default=None, alias="ASN")

    loss_pct: float = Field(alias="Loss%")
    sent: int = Field(alias="Snt")

    last_ms: float = Field(alias="Last")
    avg_ms: float = Field(alias="Avg")
    best_ms: float = Field(alias="Best")
    worst_ms: float = Field(alias="Wrst")
    stdev_ms: float = Field(alias="StDev")

    hostname: Optional[str] = Field(
        default=None, examples=["fixed-187-191-8-145.totalplay.net"]
    )
    ip: Optional[str] = None

    @field_validator("asn", mode="before")
    @classmethod
    def normalize_asn(cls, v: str):
        if v is None or v == "AS???":
            return None
        if type(v) is int:
            return v
        return int(v.replace("AS", ""))

    @model_validator(mode="after")
    def parse_host(self):
        host = self.host.strip()

        # hostname (ip)
        m = HOST_RE.match(host)
        if m:
            self.hostname = m.group("hostname")
            self.ip = m.group("ip")
            return self

        # ip only
        try:
            ip_address(host)
            self.ip = host
            self.hostname = None
            return self
        except ValueError:
            pass

        # hostname only
        self.hostname = host
        self.ip = None
        return self

    @cached_property
    def ip_kind(self) -> Optional[IPKind]:
        return get_ip_kind(self.ip)

    @cached_property
    def icmp_rate_limited(self):
        if self.avg_ms == 0:
            return False
        return self.stdev_ms > self.avg_ms or self.worst_ms > self.best_ms * 10

    @computed_field(examples=["totalplay.net"])
    @cached_property
    def domain(self) -> Optional[str]:
        if self.hostname:
            return tldextract.extract(self.hostname).top_domain_under_public_suffix

    def model_dump_postgres(self, run_id: int):
        # Writes for the network_mtrhop table
        d = {"mtr_run_id": run_id}
        data = self.model_dump(
            mode="json",
            include={
                "hop",
                "ip",
                "domain",
                "asn",
            },
        )
        d.update(data)
        return d


class MTRResult(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    source: str = Field(description="Hostname of the system running mtr.", alias="src")
    destination: str = Field(
        description="Destination hostname or IP being traced.", alias="dst"
    )
    tos: int = Field(description="IP Type-of-Service (TOS) value used for probes.")
    tests: int = Field(description="Number of probes sent per hop.")
    psize: int = Field(description="Probe packet size in bytes.")
    bitpattern: str = Field(description="Payload byte pattern used in probes (hex).")

    # Protocol used for the traceroute
    protocol: IPProtocol = Field(default=IPProtocol.ICMP)
    # The target port number for TCP/SCTP/UDP traces
    port: Optional[int] = Field(default=None)

    hops: List[MTRHop] = Field()

    def model_dump_postgres(self):
        # Writes for the network_mtr table
        d = self.model_dump(
            mode="json",
            include={"port"},
        )
        d["protocol"] = self.protocol.to_number()
        d["parsed"] = self.model_dump_json(indent=0)
        return d

    def print_report(self) -> None:
        print(
            f"MTR Report → {self.destination} {self.protocol.name} {self.port or ''}\n"
        )
        host_max_len = max(len(h.host) for h in self.hops)

        header = (
            f"{'Hop':>3}  "
            f"{'Host':<{host_max_len}} "
            f"{'Kind':<10} "
            f"{'ASN':<8} "
            f"{'Loss%':>6} {'Sent':>5} "
            f"{'Last':>7} {'Avg':>7} {'Best':>7} {'Worst':>7} {'StDev':>7}"
        )
        print(header)
        print("-" * len(header))

        for hop in self.hops:
            print(
                f"{hop.hop:>3}  "
                f"{hop.host:<{host_max_len}} "
                f"{hop.ip_kind or '???':<10} "
                f"{hop.asn or '???':<8} "
                f"{hop.loss_pct:6.1f} "
                f"{hop.sent:5d} "
                f"{hop.last_ms:7.1f} "
                f"{hop.avg_ms:7.1f} "
                f"{hop.best_ms:7.1f} "
                f"{hop.worst_ms:7.1f} "
                f"{hop.stdev_ms:7.1f}"
            )
