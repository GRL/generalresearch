import subprocess
from typing import Optional, List

from generalresearch.models.network.nmap.parser import parse_nmap_xml
from generalresearch.models.network.nmap.result import NmapResult
from generalresearch.models.network.tool_run_command import NmapRunCommand


def build_nmap_command(
    ip: str,
    no_ping: bool = True,
    enable_advanced: bool = True,
    timing: int = 4,
    ports: Optional[str] = None,
    top_ports: Optional[int] = None,
) -> str:
    # e.g. "nmap -Pn -T4 -A --top-ports 1000 -oX - scanme.nmap.org"
    # https://linux.die.net/man/1/nmap
    args = ["nmap"]
    assert 0 <= timing <= 5
    args.append(f"-T{timing}")
    if no_ping:
        args.append("-Pn")
    if enable_advanced:
        args.append("-A")
    if ports is not None:
        assert top_ports is None
        args.extend(["-p", ports])
    if top_ports is not None:
        assert ports is None
        args.extend(["--top-ports", str(top_ports)])

    args.extend(["-oX", "-", ip])
    return " ".join(args)


def run_nmap(config: NmapRunCommand) -> NmapResult:
    cmd = config.to_command_str()
    args = cmd.split(" ")
    proc = subprocess.run(
        args,
        capture_output=True,
        text=True,
        check=False,
    )
    raw = proc.stdout.strip()
    return parse_nmap_xml(raw)
