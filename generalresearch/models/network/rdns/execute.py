from datetime import datetime, timezone
from typing import Optional
from uuid import uuid4

from generalresearch.models.custom_types import UUIDStr
from generalresearch.models.network.rdns.command import (
    run_rdns,
    get_dig_version,
    build_rdns_command,
)
from generalresearch.models.network.tool_run import (
    ToolName,
    ToolClass,
    Status,
    RDNSRun,
)
from generalresearch.models.network.tool_run_command import ToolRunCommand


def execute_rdns(ip: str, scan_group_id: Optional[UUIDStr] = None):
    started_at = datetime.now(tz=timezone.utc)
    tool_version = get_dig_version()
    result = run_rdns(ip=ip)
    finished_at = datetime.now(tz=timezone.utc)
    raw_command = build_rdns_command(ip)

    run = RDNSRun(
        tool_name=ToolName.DIG,
        tool_class=ToolClass.RDNS,
        tool_version=tool_version,
        status=Status.SUCCESS,
        ip=ip,
        started_at=started_at,
        finished_at=finished_at,
        raw_command=raw_command,
        scan_group_id=scan_group_id or uuid4().hex,
        config=ToolRunCommand(command="dig", options={}),
        parsed=result,
    )

    return run
