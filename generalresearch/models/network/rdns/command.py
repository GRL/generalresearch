import subprocess

from generalresearch.models.network.rdns.parser import parse_rdns_output
from generalresearch.models.network.rdns.result import RDNSResult
from generalresearch.models.network.tool_run_command import RDNSRunCommand


def run_rdns(config: RDNSRunCommand) -> RDNSResult:
    cmd = config.to_command_str()
    args = cmd.split(" ")
    proc = subprocess.run(
        args,
        capture_output=True,
        text=True,
        check=False,
    )
    raw = proc.stdout.strip()
    return parse_rdns_output(ip=config.options.ip, raw=raw)


def build_rdns_command(ip: str) -> str:
    # e.g. dig +noall +answer -x 1.2.3.4
    return " ".join(["dig", "+noall", "+answer", "-x", ip])


def get_dig_version() -> str:
    proc = subprocess.run(
        ["dig", "-v"],
        capture_output=True,
        text=True,
        check=False,
    )
    # e.g. DiG 9.18.39-0ubuntu0.22.04.2-Ubuntu
    ver_str = proc.stderr.strip()
    return ver_str.split("-", 1)[0].split(" ", 1)[1]
