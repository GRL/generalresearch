from typing import Dict, Optional, Literal

from pydantic import BaseModel, Field

from generalresearch.models.custom_types import IPvAnyAddressStr
from generalresearch.models.network.definitions import IPProtocol


class ToolRunCommand(BaseModel):
    command: str = Field()
    options: Dict[str, Optional[str | int]] = Field(default_factory=dict)


class NmapRunCommandOptions(BaseModel):
    ip: IPvAnyAddressStr
    top_ports: Optional[int] = Field(default=1000)
    ports: Optional[str] = Field(default=None)
    no_ping: bool = Field(default=True)
    enable_advanced: bool = Field(default=True)
    timing: int = Field(default=4)


class NmapRunCommand(ToolRunCommand):
    command: Literal["nmap"] = Field(default="nmap")
    options: NmapRunCommandOptions = Field()

    def to_command_str(self):
        from generalresearch.models.network.nmap.command import build_nmap_command

        options = self.options
        return build_nmap_command(**options.model_dump())


class RDNSRunCommandOptions(BaseModel):
    ip: IPvAnyAddressStr


class RDNSRunCommand(ToolRunCommand):
    command: Literal["dig"] = Field(default="dig")
    options: RDNSRunCommandOptions = Field()

    def to_command_str(self):
        from generalresearch.models.network.rdns.command import build_rdns_command

        options = self.options
        return build_rdns_command(**options.model_dump())


class MTRRunCommandOptions(BaseModel):
    ip: IPvAnyAddressStr = Field()
    protocol: IPProtocol = Field(default=IPProtocol.ICMP)
    port: Optional[int] = Field(default=None)
    report_cycles: int = Field(default=10)


class MTRRunCommand(ToolRunCommand):
    command: Literal["mtr"] = Field(default="mtr")
    options: MTRRunCommandOptions = Field()

    def to_command_str(self):
        from generalresearch.models.network.mtr.command import build_mtr_command

        options = self.options
        return build_mtr_command(**options.model_dump())
