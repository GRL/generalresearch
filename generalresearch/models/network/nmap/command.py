import subprocess
from typing import Optional, List

from generalresearch.models.network.nmap.parser import parse_nmap_xml
from generalresearch.models.network.nmap.result import NmapResult


def build_nmap_command(ip: str, top_ports: Optional[int] = 1000) -> List[str]:
    # e.g. "nmap -Pn -T4 -A --top-ports 1000 -oX - scanme.nmap.org"
    # https://linux.die.net/man/1/nmap
    args = ["nmap", "-Pn", "-T4", "-A", "--top-ports", str(int(top_ports)), "-oX", "-"]
    args.append(ip)
    return args


def run_nmap(ip: str, top_ports: Optional[int] = 1000) -> NmapResult:
    args = build_nmap_command(ip=ip, top_ports=top_ports)
    proc = subprocess.run(
        args,
        capture_output=True,
        text=True,
        check=False,
    )
    raw = proc.stdout.strip()
    return parse_nmap_xml(raw)
