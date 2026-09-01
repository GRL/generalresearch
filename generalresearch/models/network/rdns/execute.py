from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

from generalresearch.models.custom_types import UUIDStr
from generalresearch.models.network.rdns.command import (
    get_dig_version,
    run_rdns,
)
from generalresearch.models.network.tool_run import (
    RDNSRun,
    Status,
    ToolClass,
    ToolName,
)
from generalresearch.models.network.tool_run_command import (
    RDNSRunCommand,
    RDNSRunCommandOptions,
)


def execute_rdns(ip: str, scan_group_id: UUIDStr | None = None):
    started_at = datetime.now(tz=UTC)
    tool_version = get_dig_version()
    config = RDNSRunCommand(options=RDNSRunCommandOptions(ip=ip))
    result = run_rdns(config)
    finished_at = datetime.now(tz=UTC)

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
