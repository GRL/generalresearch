import os

import pytest

from generalresearch.models.network.nmap.parser import parse_nmap_xml

@pytest.fixture
def nmap_raw_output_2(request) -> str:
    fp = os.path.join(request.config.rootpath, "data/nmaprun2.xml")
    with open(fp, "r") as f:
        data = f.read()
    return data


def test_nmap_xml_parser(nmap_raw_output, nmap_raw_output_2):
    n = parse_nmap_xml(nmap_raw_output)
    assert n.tcp_open_ports == [61232]
    assert len(n.trace.hops) == 18

    n = parse_nmap_xml(nmap_raw_output_2)
    assert n.tcp_open_ports == [22, 80, 9929, 31337]
    assert n.trace is None
