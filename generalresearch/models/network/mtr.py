import json
import re
import subprocess
from ipaddress import ip_address
from typing import List, Optional, Dict

from pydantic import Field, field_validator, BaseModel, ConfigDict, model_validator

from generalresearch.models.network.definitions import IPProtocol, get_ip_kind, IPKind


class MTRHop(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    hop: int = Field(alias="count")
    host: str
    asn: Optional[str] = Field(default=None, alias="ASN")

    loss_pct: float = Field(alias="Loss%")
    sent: int = Field(alias="Snt")

    last_ms: float = Field(alias="Last")
    avg_ms: float = Field(alias="Avg")
    best_ms: float = Field(alias="Best")
    worst_ms: float = Field(alias="Wrst")
    stdev_ms: float = Field(alias="StDev")

    hostname: Optional[str] = None
    ip: Optional[str] = None

    @field_validator("asn")
    @classmethod
    def normalize_asn(cls, v):
        if v == "AS???":
            return None
        return v

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

    @property
    def ip_kind(self) -> Optional[IPKind]:
        return get_ip_kind(self.ip)

    @property
    def icmp_rate_limited(self):
        if self.avg_ms == 0:
            return False
        return self.stdev_ms > self.avg_ms or self.worst_ms > self.best_ms * 10


class MTRReport(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    source: str = Field(description="Hostname of the system running mtr.", alias="src")
    destination: str = Field(
        description="Destination hostname or IP being traced.", alias="dst"
    )
    tos: int = Field(description="IP Type-of-Service (TOS) value used for probes.")
    tests: int = Field(description="Number of probes sent per hop.")
    psize: int = Field(description="Probe packet size in bytes.")
    bitpattern: str = Field(description="Payload byte pattern used in probes (hex).")

    hops: List[MTRHop] = Field()

    def print_report(self) -> None:
        print(f"MTR Report → {self.destination}\n")
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


HOST_RE = re.compile(r"^(?P<hostname>.+?) \((?P<ip>[^)]+)\)$")

SUPPORTED_PROTOCOLS = {
    IPProtocol.TCP,
    IPProtocol.UDP,
    IPProtocol.SCTP,
    IPProtocol.ICMP,
}
PROTOCOLS_W_PORT = {IPProtocol.TCP, IPProtocol.UDP, IPProtocol.SCTP}


def get_mtr_command(
    ip: str,
    protocol: Optional[IPProtocol] = None,
    port: Optional[int] = None,
    report_cycles: int = 10,
) -> List[str]:
    # https://manpages.ubuntu.com/manpages/focal/man8/mtr.8.html
    # e.g. "mtr -r -c 2 -b -z -j -T -P 443 74.139.70.149"
    args = ["mtr", "--report", "--show-ips", "--aslookup", "--json"]
    if report_cycles is not None:
        args.extend(["-c", str(int(report_cycles))])
    if port is not None:
        if protocol is None:
            protocol = IPProtocol.TCP
        assert protocol in PROTOCOLS_W_PORT, "port only allowed for TCP/SCTP/UDP traces"
        args.extend(["--port", str(int(port))])
    if protocol:
        assert protocol in SUPPORTED_PROTOCOLS, f"unsupported protocol: {protocol}"
        # default is ICMP (no args)
        arg_map = {
            IPProtocol.TCP: "--tcp",
            IPProtocol.UDP: "--udp",
            IPProtocol.SCTP: "--sctp",
        }
        if protocol in arg_map:
            args.append(arg_map[protocol])
    args.append(ip)
    return args


def get_mtr_version() -> str:
    proc = subprocess.run(
        ["mtr", "-v"],
        capture_output=True,
        text=True,
        check=False,
    )
    # e.g. mtr 0.95
    ver_str = proc.stdout.strip()
    return ver_str.split(" ", 1)[1]


def run_mtr(
    ip: str,
    protocol: Optional[IPProtocol] = None,
    port: Optional[int] = None,
    report_cycles: int = 10,
) -> MTRReport:
    args = get_mtr_command(
        ip=ip, protocol=protocol, port=port, report_cycles=report_cycles
    )
    proc = subprocess.run(
        args,
        capture_output=True,
        text=True,
        check=False,
    )
    raw = proc.stdout.strip()
    data = parse_raw_output(raw)
    return MTRReport.model_validate(data)


def parse_raw_output(raw: str) -> Dict:
    data = json.loads(raw)["report"]
    data.update(data.pop("mtr"))
    data["hops"] = data.pop("hubs")
    return data


def load_example():
    s = open(
        "/home/gstupp/projects/generalresearch/generalresearch/models/network/mtr_fatbeam.json",
        "r",
    ).read()
    data = parse_raw_output(s)
    return MTRReport.model_validate(data)
