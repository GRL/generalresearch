from __future__ import annotations

import os
from typing import TYPE_CHECKING

import pytest

from generalresearch.models.network.nmap.parser import parse_nmap_xml
from generalresearch.models.network.nmap.result import NmapTrace

if TYPE_CHECKING:
    from generalresearch.models.network.nmap.result import NmapResult


@pytest.fixture
def nmap_raw_output_2(request) -> str:
    fp = os.path.join(request.config.rootpath, "data/nmaprun2.xml")
    with open(fp) as f:
        data = f.read()
    return data


def test_nmap_xml_parser(nmap_raw_output: str, nmap_raw_output_2: str):
    n: NmapResult = parse_nmap_xml(nmap_raw_output)
    assert n.tcp_open_ports == [61232]

    assert isinstance(n.trace, NmapTrace)
    assert len(n.trace.hops) == 18

    n = parse_nmap_xml(nmap_raw_output_2)
    assert n.tcp_open_ports == [22, 80, 9929, 31337]
    assert n.trace is None
