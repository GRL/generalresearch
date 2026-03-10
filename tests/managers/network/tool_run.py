import os
from datetime import datetime, timezone
from uuid import uuid4

import faker
import pytest

from generalresearch.models.network.definitions import IPProtocol
from generalresearch.models.network.mtr import (
    get_mtr_version,
    parse_raw_output,
    MTRReport,
    get_mtr_command,
)
from generalresearch.models.network.tool_run import (
    new_tool_run_from_nmap,
    run_dig,
    MtrRun,
    ToolName,
    ToolClass,
    Status,
)
from generalresearch.models.network.tool_utils import ToolRunCommand

fake = faker.Faker()


def test_create_tool_run_from_nmap(nmap_run, toolrun_manager):
    scan_group_id = uuid4().hex
    run = new_tool_run_from_nmap(nmap_run, scan_group_id=scan_group_id)

    toolrun_manager.create_portscan_run(run)

    run_out = toolrun_manager.get_portscan_run(run.id)

    assert run == run_out


def test_create_tool_run_from_dig_fixture(reverse_dns_run, toolrun_manager):

    toolrun_manager.create_rdns_run(reverse_dns_run)

    run_out = toolrun_manager.get_rdns_run(reverse_dns_run.id)

    assert reverse_dns_run == run_out


def test_run_dig(toolrun_manager):
    reverse_dns_run = run_dig(ip="65.19.129.53")

    toolrun_manager.create_rdns_run(reverse_dns_run)

    run_out = toolrun_manager.get_rdns_run(reverse_dns_run.id)

    assert reverse_dns_run == run_out


def test_run_dig_empty(toolrun_manager):
    reverse_dns_run = run_dig(ip=fake.ipv6())

    toolrun_manager.create_rdns_run(reverse_dns_run)

    run_out = toolrun_manager.get_rdns_run(reverse_dns_run.id)

    assert reverse_dns_run == run_out


@pytest.fixture(scope="session")
def mtr_report(request) -> MTRReport:
    fp = os.path.join(request.config.rootpath, "data/mtr_fatbeam.json")
    with open(fp, "r") as f:
        s = f.read()
    data = parse_raw_output(s)
    data["port"] = 443
    data["protocol"] = IPProtocol.TCP
    return MTRReport.model_validate(data)


def test_create_tool_run_from_mtr(toolrun_manager, mtr_report):
    started_at = datetime.now(tz=timezone.utc)
    tool_version = get_mtr_version()

    ip = mtr_report.destination

    finished_at = datetime.now(tz=timezone.utc)
    raw_command = " ".join(get_mtr_command(ip))

    run = MtrRun(
        tool_name=ToolName.MTR,
        tool_class=ToolClass.TRACEROUTE,
        tool_version=tool_version,
        status=Status.SUCCESS,
        ip=ip,
        started_at=started_at,
        finished_at=finished_at,
        raw_command=raw_command,
        scan_group_id=uuid4().hex,
        config=ToolRunCommand.from_raw_command(raw_command),
        parsed=mtr_report,
        source_ip="1.1.1.1"
    )

    toolrun_manager.create_mtr_run(run)

    run_out = toolrun_manager.get_mtr_run(run.id)

    assert run == run_out
