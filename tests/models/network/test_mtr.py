from generalresearch.models.network.mtr.execute import execute_mtr
import faker

from generalresearch.models.network.tool_run import ToolName, ToolClass

fake = faker.Faker()


def test_execute_mtr(toolrun_manager):
    ip = "65.19.129.53"

    run = execute_mtr(ip=ip, report_cycles=3)
    assert run.tool_name == ToolName.MTR
    assert run.tool_class == ToolClass.TRACEROUTE
    assert run.ip == ip
    result = run.parsed

    last_hop = result.hops[-1]
    assert last_hop.asn == 6939
    assert last_hop.domain == "grlengine.com"

    last_hop_1 = result.hops[-2]
    assert last_hop_1.asn == 6939
    assert last_hop_1.domain == "he.net"

    toolrun_manager.create_mtr_run(run)
