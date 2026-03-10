import ipaddress
import json
from functools import cached_property

from pydantic import BaseModel, Field, model_validator, computed_field
from typing import Optional, List

from typing_extensions import Self

from generalresearch.models.custom_types import IPvAnyAddressStr
import subprocess
import re
from typing import List
import ipaddress
import tldextract


class RDNSResult(BaseModel):

    ip: IPvAnyAddressStr = Field()

    hostnames: List[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_hostname_prop(self):
        assert len(self.hostnames) == self.hostname_count
        if self.hostnames:
            assert self.hostnames[0] == self.primary_hostname
            assert self.primary_domain in self.primary_hostname
        return self

    @computed_field(examples=["fixed-187-191-8-145.totalplay.net"])
    @cached_property
    def primary_hostname(self) -> Optional[str]:
        if self.hostnames:
            return self.hostnames[0]

    @computed_field(examples=[1])
    @cached_property
    def hostname_count(self) -> int:
        return len(self.hostnames)

    @computed_field(examples=["totalplay"])
    @cached_property
    def primary_domain(self) -> Optional[str]:
        if self.primary_hostname:
            return tldextract.extract(self.primary_hostname).top_domain_under_public_suffix

    def model_dump_postgres(self):
        # Writes for the network_rdnsresult table
        d = self.model_dump(
            mode="json",
            include={"primary_hostname", "primary_domain", "hostname_count"},
        )
        d["hostnames"] = json.dumps(self.hostnames)
        return d

    @classmethod
    def from_dig(cls, ip: str, raw_output: str) -> Self:
        hostnames: List[str] = []

        for line in raw_output.splitlines():
            m = PTR_RE.search(line)
            if m:
                hostnames.append(m.group(1))

        return cls(
            ip=ipaddress.ip_address(ip),
            hostnames=hostnames,
        )


PTR_RE = re.compile(r"\sPTR\s+([^\s]+)\.")


def dig_rdns(ip: str) -> RDNSResult:
    args = get_dig_rdns_command(ip).split(" ")
    proc = subprocess.run(
        args,
        capture_output=True,
        text=True,
        check=False,
    )
    raw = proc.stdout.strip()
    return RDNSResult.from_dig(ip=ip, raw_output=raw)


def get_dig_rdns_command(ip: str):
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
