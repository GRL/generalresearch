from uuid import uuid4

from generalresearch.models.network.tool_run import new_tool_run_from_nmap


def test_new_tool_run_from_nmap(nmap_run):
    scan_group_id = uuid4().hex
    run, scan = new_tool_run_from_nmap(nmap_run, scan_group_id=scan_group_id)
