import os
from datetime import datetime, timezone
from typing import Callable, TYPE_CHECKING
from uuid import uuid4

import pytest

from generalresearch.managers.network.label import IPLabelManager
from generalresearch.managers.network.nmap import NmapManager
from generalresearch.managers.network.tool_run import ToolRunManager
from generalresearch.models.network.rdns import (
    RDNSResult,
    get_dig_version,
    get_dig_rdns_command,
)
from generalresearch.models.network.tool_run import (
    RDnsRun,
    ToolName,
    ToolClass,
    Status,
)
from generalresearch.models.network.tool_utils import ToolRunCommand
from generalresearch.models.network.xml_parser import NmapXmlParser


@pytest.fixture(scope="session")
def iplabel_manager(thl_web_rw) -> IPLabelManager:
    assert "/unittest-" in thl_web_rw.dsn.path

    return IPLabelManager(pg_config=thl_web_rw)


@pytest.fixture(scope="session")
def nmap_manager(thl_web_rw) -> NmapManager:
    assert "/unittest-" in thl_web_rw.dsn.path

    return NmapManager(pg_config=thl_web_rw)


@pytest.fixture(scope="session")
def toolrun_manager(thl_web_rw) -> ToolRunManager:
    assert "/unittest-" in thl_web_rw.dsn.path

    return ToolRunManager(pg_config=thl_web_rw)


@pytest.fixture(scope="session")
def nmap_xml_str(request) -> str:
    fp = os.path.join(request.config.rootpath, "data/nmaprun1.xml")
    with open(fp, "r") as f:
        data = f.read()
    return data


@pytest.fixture(scope="session")
def nmap_run(nmap_xml_str):
    return NmapXmlParser.parse_xml(nmap_xml_str)


@pytest.fixture(scope="session")
def raw_dig_output():
    return "156.32.33.45.in-addr.arpa. 300	IN	PTR	scanme.nmap.org."


@pytest.fixture(scope="session")
def reverse_dns_run(raw_dig_output):
    ip = "45.33.32.156"
    rdns_result = RDNSResult.from_dig(ip=ip, raw_output=raw_dig_output)
    scan_group_id = uuid4().hex
    started_at = datetime.now(tz=timezone.utc)
    tool_version = get_dig_version()
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
