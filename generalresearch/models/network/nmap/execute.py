from typing import Optional
from uuid import uuid4

from generalresearch.models.custom_types import UUIDStr
from generalresearch.models.network.nmap.command import run_nmap
from generalresearch.models.network.tool_run import NmapRun, ToolName, ToolClass, Status
from generalresearch.models.network.tool_run_command import (
    NmapRunCommand,
    NmapRunCommandOptions,
)


def execute_nmap(
    ip: str,
    top_ports: Optional[int] = 1000,
    ports: Optional[str] = None,
    no_ping: bool = True,
    enable_advanced: bool = True,
    timing: int = 4,
    scan_group_id: Optional[UUIDStr] = None,
):
    config = NmapRunCommand(
        options=NmapRunCommandOptions(
            top_ports=top_ports,
            ports=ports,
            no_ping=no_ping,
            enable_advanced=enable_advanced,
            timing=timing,
            ip=ip,
        )
    )
    result = run_nmap(config)
    assert result.exit_status == "success"
    assert result.target_ip == ip, f"{result.target_ip=}, {ip=}"
    assert result.command_line == config.to_command_str()

    run = NmapRun(
        tool_name=ToolName.NMAP,
        tool_class=ToolClass.PORT_SCAN,
        tool_version=result.version,
        status=Status.SUCCESS,
        ip=ip,
        started_at=result.started_at,
        finished_at=result.finished_at,
        raw_command=result.command_line,
        scan_group_id=scan_group_id or uuid4().hex,
        config=config,
        parsed=result,
    )
    return run
