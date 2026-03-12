import subprocess
from typing import List, Optional

from generalresearch.models.network.definitions import IPProtocol
from generalresearch.models.network.mtr.parser import parse_mtr_output
from generalresearch.models.network.mtr.result import MTRResult

SUPPORTED_PROTOCOLS = {
    IPProtocol.TCP,
    IPProtocol.UDP,
    IPProtocol.SCTP,
    IPProtocol.ICMP,
}
PROTOCOLS_W_PORT = {IPProtocol.TCP, IPProtocol.UDP, IPProtocol.SCTP}


def build_mtr_command(
    ip: str,
    protocol: Optional[IPProtocol] = None,
    port: Optional[int] = None,
    report_cycles: int = 10,
) -> str:
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
    return " ".join(args)


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
) -> MTRResult:
    args = build_mtr_command(
        ip=ip, protocol=protocol, port=port, report_cycles=report_cycles
    )
    proc = subprocess.run(
        args.split(" "),
        capture_output=True,
        text=True,
        check=False,
    )
    raw = proc.stdout.strip()
    return parse_mtr_output(raw, protocol=protocol, port=port)
