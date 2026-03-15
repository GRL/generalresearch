from enum import StrEnum
from typing import Optional, Literal
from uuid import uuid4

from pydantic import BaseModel, Field, PositiveInt

from generalresearch.models.custom_types import (
    AwareDatetimeISO,
    IPvAnyAddressStr,
    UUIDStr,
)
from generalresearch.models.network.mtr.result import MTRResult
from generalresearch.models.network.nmap.result import NmapResult
from generalresearch.models.network.rdns.result import RDNSResult
from generalresearch.models.network.tool_run_command import (
    ToolRunCommand,
    NmapRunCommand,
    RDNSRunCommand,
    MTRRunCommand,
)


class ToolClass(StrEnum):
    PORT_SCAN = "port_scan"
    RDNS = "rdns"
    PING = "ping"
    TRACEROUTE = "traceroute"


class ToolName(StrEnum):
    NMAP = "nmap"
    RUSTMAP = "rustmap"
    DIG = "dig"
    PING = "ping"
    TRACEROUTE = "traceroute"
    MTR = "mtr"


class Status(StrEnum):
    SUCCESS = "success"
    FAILED = "failed"
    TIMEOUT = "timeout"
    ERROR = "error"


class ToolRun(BaseModel):
    """
    A run of a networking tool against one host/ip.
    """

    id: Optional[PositiveInt] = Field(default=None)

    ip: IPvAnyAddressStr = Field()
    scan_group_id: UUIDStr = Field(default_factory=lambda: uuid4().hex)
    tool_class: ToolClass = Field()
    tool_name: ToolName = Field()
    tool_version: str = Field()

    started_at: AwareDatetimeISO = Field()
    finished_at: Optional[AwareDatetimeISO] = Field(default=None)
    status: Optional[Status] = Field(default=None)

    raw_command: str = Field()

    config: ToolRunCommand = Field()

    def model_dump_postgres(self):
        d = self.model_dump(mode="json", exclude={"config"})
        d["config"] = self.config.model_dump_json()
        return d


class NmapRun(ToolRun):
    tool_class: Literal[ToolClass.PORT_SCAN] = Field(default=ToolClass.PORT_SCAN)
    tool_name: Literal[ToolName.NMAP] = Field(default=ToolName.NMAP)
    config: NmapRunCommand = Field()

    parsed: NmapResult = Field()

    def model_dump_postgres(self):
        d = super().model_dump_postgres()
        d["run_id"] = self.id
        d.update(self.parsed.model_dump_postgres())
        return d


class RDNSRun(ToolRun):
    tool_class: Literal[ToolClass.RDNS] = Field(default=ToolClass.RDNS)
    tool_name: Literal[ToolName.DIG] = Field(default=ToolName.DIG)
    config: RDNSRunCommand = Field()

    parsed: RDNSResult = Field()

    def model_dump_postgres(self):
        d = super().model_dump_postgres()
        d["run_id"] = self.id
        d.update(self.parsed.model_dump_postgres())
        return d


class MTRRun(ToolRun):
    tool_class: Literal[ToolClass.TRACEROUTE] = Field(default=ToolClass.TRACEROUTE)
    tool_name: Literal[ToolName.MTR] = Field(default=ToolName.MTR)
    config: MTRRunCommand = Field()

    facility_id: int = Field(default=1)
    source_ip: IPvAnyAddressStr = Field()
    parsed: MTRResult = Field()

    def model_dump_postgres(self):
        d = super().model_dump_postgres()
        d["run_id"] = self.id
        d["source_ip"] = self.source_ip
        d["facility_id"] = self.facility_id
        d.update(self.parsed.model_dump_postgres())
        return d
