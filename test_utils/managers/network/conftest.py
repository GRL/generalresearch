import os
from datetime import timedelta, datetime, timezone
from uuid import uuid4

import pytest

from generalresearch.managers.network.label import IPLabelManager
from generalresearch.managers.network.tool_run import ToolRunManager
from generalresearch.models.network.definitions import IPProtocol
from generalresearch.models.network.mtr.command import build_mtr_command
from generalresearch.models.network.mtr.parser import parse_mtr_output
from generalresearch.models.network.nmap.parser import parse_nmap_xml
from generalresearch.models.network.rdns.command import build_rdns_command
from generalresearch.models.network.rdns.parser import parse_rdns_output
from generalresearch.models.network.tool_run import NmapRun, Status, RDNSRun, MTRRun
from generalresearch.models.network.tool_run_command import ToolRunCommand


@pytest.fixture(scope="session")
def scan_group_id():
    return uuid4().hex


@pytest.fixture(scope="session")
def iplabel_manager(thl_web_rw) -> IPLabelManager:
    assert "/unittest-" in thl_web_rw.dsn.path

    return IPLabelManager(pg_config=thl_web_rw)


@pytest.fixture(scope="session")
def toolrun_manager(thl_web_rw) -> ToolRunManager:
    assert "/unittest-" in thl_web_rw.dsn.path

    return ToolRunManager(pg_config=thl_web_rw)


@pytest.fixture(scope="session")
def nmap_raw_output(request) -> str:
    fp = os.path.join(request.config.rootpath, "data/nmaprun1.xml")
    with open(fp, "r") as f:
        data = f.read()
    return data


@pytest.fixture(scope="session")
def nmap_result(nmap_raw_output):
    return parse_nmap_xml(nmap_raw_output)


@pytest.fixture(scope="session")
def nmap_run(nmap_result, scan_group_id):
    r = nmap_result
    return NmapRun(
        tool_version=r.version,
        status=Status.SUCCESS,
        ip=r.target_ip,
        started_at=r.started_at,
        finished_at=r.finished_at,
        raw_command=r.command_line,
        scan_group_id=scan_group_id,
        config=ToolRunCommand(command="nmap"),
        parsed=r,
    )


@pytest.fixture(scope="session")
def dig_raw_output():
    return "156.32.33.45.in-addr.arpa. 300	IN	PTR	scanme.nmap.org."


@pytest.fixture(scope="session")
def rdns_result(dig_raw_output):
    return parse_rdns_output(ip="45.33.32.156", raw=dig_raw_output)


@pytest.fixture(scope="session")
def rdns_run(rdns_result, scan_group_id):
    r = rdns_result
    ip = "45.33.32.156"
    utc_now = datetime.now(tz=timezone.utc)
    return RDNSRun(
        tool_version="1.2.3",
        status=Status.SUCCESS,
        ip=ip,
        started_at=utc_now,
        finished_at=utc_now + timedelta(seconds=1),
        raw_command=build_rdns_command(ip=ip),
        scan_group_id=scan_group_id,
        config=ToolRunCommand(command="dig"),
        parsed=r,
    )


@pytest.fixture(scope="session")
def mtr_raw_output(request):
    fp = os.path.join(request.config.rootpath, "data/mtr_fatbeam.json")
    with open(fp, "r") as f:
        data = f.read()
    return data


@pytest.fixture(scope="session")
def mtr_result(mtr_raw_output):
    return parse_mtr_output(mtr_raw_output, port=443, protocol=IPProtocol.TCP)


@pytest.fixture(scope="session")
def mtr_run(mtr_result, scan_group_id):
    r = mtr_result
    utc_now = datetime.now(tz=timezone.utc)

    return MTRRun(
        tool_version="1.2.3",
        status=Status.SUCCESS,
        ip=r.destination,
        started_at=utc_now,
        finished_at=utc_now + timedelta(seconds=1),
        raw_command=build_mtr_command(
            ip=r.destination, protocol=IPProtocol.TCP, port=443
        ),
        scan_group_id=scan_group_id,
        config=ToolRunCommand(command="mtr"),
        parsed=r,
        facility_id=1,
        source_ip="1.2.3.4",
    )
