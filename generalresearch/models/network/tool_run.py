from datetime import datetime, timezone
from enum import StrEnum
from typing import Optional, Tuple
from uuid import uuid4

from pydantic import BaseModel, Field, PositiveInt

from generalresearch.models.custom_types import (
    AwareDatetimeISO,
    IPvAnyAddressStr,
    UUIDStr,
)
from generalresearch.models.network.nmap import NmapRun
from generalresearch.models.network.rdns import (
    RDNSResult,
    get_dig_version,
    dig_rdns,
    get_dig_rdns_command,
)
from generalresearch.models.network.mtr import (
    MTRReport,
    get_mtr_version,
    run_mtr,
    get_mtr_command,
)
from generalresearch.models.network.tool_utils import ToolRunCommand


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


class PortScanRun(ToolRun):
    parsed: NmapRun = Field()

    def model_dump_postgres(self):
        d = super().model_dump_postgres()
        d["run_id"] = self.id
        d.update(self.parsed.model_dump_postgres())
        return d


class RDnsRun(ToolRun):
    parsed: RDNSResult = Field()

    def model_dump_postgres(self):
        d = super().model_dump_postgres()
        d["run_id"] = self.id
        d.update(self.parsed.model_dump_postgres())
        return d


class MtrRun(ToolRun):
    facility_id: int = Field(default=1)
    source_ip: IPvAnyAddressStr = Field()
    parsed: MTRReport = Field()

    def model_dump_postgres(self):
        d = super().model_dump_postgres()
        d["run_id"] = self.id
        d["source_ip"] = self.source_ip
        d["facility_id"] = self.facility_id
        d.update(self.parsed.model_dump_postgres())
        return d


def new_tool_run_from_nmap(
    nmap_run: NmapRun, scan_group_id: Optional[UUIDStr] = None
) -> PortScanRun:
    assert nmap_run.exit_status == "success"
    return PortScanRun(
        tool_name=ToolName.NMAP,
        tool_class=ToolClass.PORT_SCAN,
        tool_version=nmap_run.version,
        status=Status.SUCCESS,
        ip=nmap_run.target_ip,
        started_at=nmap_run.started_at,
        finished_at=nmap_run.finished_at,
        raw_command=nmap_run.command_line,
        scan_group_id=scan_group_id or uuid4().hex,
        config=ToolRunCommand.from_raw_command(nmap_run.command_line),
        parsed=nmap_run,
    )


def run_dig(ip: str, scan_group_id: Optional[UUIDStr] = None) -> RDnsRun:
    started_at = datetime.now(tz=timezone.utc)
    tool_version = get_dig_version()
    rdns_result = dig_rdns(ip)
    finished_at = datetime.now(tz=timezone.utc)
    raw_command = get_dig_rdns_command(ip)

    return RDnsRun(
        tool_name=ToolName.DIG,
        tool_class=ToolClass.RDNS,
        tool_version=tool_version,
        status=Status.SUCCESS,
        ip=ip,
        started_at=started_at,
        finished_at=finished_at,
        raw_command=raw_command,
        scan_group_id=scan_group_id or uuid4().hex,
        config=ToolRunCommand.from_raw_command(raw_command),
        parsed=rdns_result,
    )


def mtr_tool_run(ip: str, scan_group_id: Optional[UUIDStr] = None) -> MtrRun:
    started_at = datetime.now(tz=timezone.utc)
    tool_version = get_mtr_version()
    result = run_mtr(ip)
    finished_at = datetime.now(tz=timezone.utc)
    raw_command = " ".join(get_mtr_command(ip))

    return MtrRun(
        tool_name=ToolName.MTR,
        tool_class=ToolClass.TRACEROUTE,
        tool_version=tool_version,
        status=Status.SUCCESS,
        ip=ip,
        started_at=started_at,
        finished_at=finished_at,
        raw_command=raw_command,
        scan_group_id=scan_group_id or uuid4().hex,
        config=ToolRunCommand.from_raw_command(raw_command),
        parsed=result,
    )
