from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

from generalresearch.models.custom_types import UUIDStr
from generalresearch.models.network.definitions import IPProtocol
from generalresearch.models.network.mtr.command import (
    get_mtr_version,
    run_mtr,
)
from generalresearch.models.network.tool_run import (
    MTRRun,
    Status,
    ToolClass,
    ToolName,
)
from generalresearch.models.network.tool_run_command import (
    MTRRunCommand,
    MTRRunCommandOptions,
)
from generalresearch.models.network.utils import get_source_ip


def execute_mtr(
    ip: str,
    scan_group_id: UUIDStr | None = None,
    protocol: IPProtocol | None = IPProtocol.ICMP,
    port: int | None = None,
    report_cycles: int = 10,
) -> MTRRun:
    config = MTRRunCommand(
        options=MTRRunCommandOptions(
            ip=ip,
            report_cycles=report_cycles,
            protocol=protocol,
            port=port,
        ),
    )

    started_at = datetime.now(tz=UTC)
    tool_version = get_mtr_version()
    result = run_mtr(config)
    finished_at = datetime.now(tz=UTC)

    return MTRRun(
        tool_name=ToolName.MTR,
        tool_class=ToolClass.TRACEROUTE,
        tool_version=tool_version,
        status=Status.SUCCESS,
        ip=ip,
        started_at=started_at,
        finished_at=finished_at,
        raw_command=config.to_command_str(),
        scan_group_id=scan_group_id or uuid4().hex,
        config=config,
        parsed=result,
        source_ip=get_source_ip(),
        facility_id=1,
    )
