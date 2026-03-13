import subprocess

from generalresearch.models.network.definitions import IPProtocol
from generalresearch.models.network.nmap.execute import execute_nmap
import faker

from generalresearch.models.network.nmap.result import PortState
from generalresearch.models.network.tool_run import ToolName, ToolClass

fake = faker.Faker()


def resolve(host):
    return subprocess.check_output(["dig", host, "+short"]).decode().strip()


def test_execute_nmap_scanme(toolrun_manager):
    ip = resolve("scanme.nmap.org")

    run = execute_nmap(ip=ip, top_ports=None, ports="20-30", enable_advanced=False)
    assert run.tool_name == ToolName.NMAP
    assert run.tool_class == ToolClass.PORT_SCAN
    assert run.ip == ip
    result = run.parsed

    port22 = result._port_index[(IPProtocol.TCP, 22)]
    assert port22.state == PortState.OPEN

    toolrun_manager.create_nmap_run(run)
