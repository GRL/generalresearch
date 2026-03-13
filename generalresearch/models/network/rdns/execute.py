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
from generalresearch.models.network.tool_run_command import (
    RDNSRunCommand,
    RDNSRunCommandOptions,
)


def execute_rdns(ip: str, scan_group_id: Optional[UUIDStr] = None):
    started_at = datetime.now(tz=timezone.utc)
    tool_version = get_dig_version()
    config = RDNSRunCommand(options=RDNSRunCommandOptions(ip=ip))
    result = run_rdns(config)
    finished_at = datetime.now(tz=timezone.utc)

    run = RDNSRun(
        tool_name=ToolName.DIG,
        tool_class=ToolClass.RDNS,
        tool_version=tool_version,
        status=Status.SUCCESS,
        ip=ip,
        started_at=started_at,
        finished_at=finished_at,
        raw_command=config.to_command_str(),
        scan_group_id=scan_group_id or uuid4().hex,
        config=config,
        parsed=result,
    )

    return run
