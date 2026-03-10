import os

import pytest

from generalresearch.models.network.xml_parser import NmapXmlParser


@pytest.fixture
def nmap_xml_str(request) -> str:
    fp = os.path.join(request.config.rootpath, "data/nmaprun1.xml")
    with open(fp, "r") as f:
        data = f.read()
    return data


@pytest.fixture
def nmap_xml_str2(request) -> str:
    fp = os.path.join(request.config.rootpath, "data/nmaprun2.xml")
    with open(fp, "r") as f:
        data = f.read()
    return data


def test_nmap_xml_parser(nmap_xml_str, nmap_xml_str2):
    p = NmapXmlParser()
    n = p.parse_xml(nmap_xml_str)
    assert n.tcp_open_ports == [61232]
    assert len(n.trace.hops) == 18

    n = p.parse_xml(nmap_xml_str2)
    assert n.tcp_open_ports == [22, 80, 9929, 31337]
    assert n.trace is None
