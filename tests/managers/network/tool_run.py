from uuid import uuid4

import faker

from generalresearch.models.network.tool_run import (
    new_tool_run_from_nmap,
    run_dig,
)
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