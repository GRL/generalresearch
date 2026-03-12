from typing import Optional
from uuid import uuid4

from generalresearch.models.custom_types import UUIDStr
from generalresearch.models.network.nmap.command import run_nmap
from generalresearch.models.network.tool_run import NmapRun, ToolName, ToolClass, Status
from generalresearch.models.network.tool_run_command import ToolRunCommand


def execute_nmap(
    ip: str, top_ports: Optional[int] = 1000, scan_group_id: Optional[UUIDStr] = None
):
    result = run_nmap(ip=ip, top_ports=top_ports)
    assert result.exit_status == "success"
    assert result.target_ip == ip

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
        config=ToolRunCommand(command="nmap", options={'top_ports': top_ports}),
        parsed=result,
    )
    return run