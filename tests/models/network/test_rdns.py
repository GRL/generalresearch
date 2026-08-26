from __future__ import annotations

import faker

from generalresearch.managers.network.tool_run import ToolRunManager
from generalresearch.models.network.rdns.execute import execute_rdns
from generalresearch.models.network.tool_run import ToolClass, ToolName

fake = faker.Faker()


def test_execute_rdns_grl(toolrun_manager: ToolRunManager):
    ip = "65.19.129.53"
    run = execute_rdns(ip=ip)
    assert run.tool_name == ToolName.DIG
    assert run.tool_class == ToolClass.RDNS
    assert run.ip == ip
    result = run.parsed
    assert result.primary_hostname == "in1-smtp.grlengine.com"
    assert result.primary_domain == "grlengine.com"
    assert result.hostname_count == 1

    toolrun_manager.create_rdns_run(run)


def test_execute_rdns_none(toolrun_manager: ToolRunManager):
    ip = fake.ipv6()
    run = execute_rdns(ip)
    result = run.parsed

    assert result.primary_hostname is None
    assert result.primary_domain is None
    assert result.hostname_count == 0
    assert result.hostnames == []

    toolrun_manager.create_rdns_run(run)
