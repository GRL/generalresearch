import xml.etree.cElementTree as ET
from datetime import datetime, timezone
from typing import List, Dict, Any, Tuple, Optional

from generalresearch.models.network.nmap import (
    NmapHostname,
    NmapRun,
    NmapPort,
    PortState,
    PortStateReason,
    NmapService,
    NmapScript,
    NmapPortStats,
    NmapScanType,
    NmapProtocol,
    NmapHostState,
    NmapHostStatusReason,
    NmapHostScript,
    NmapOSMatch,
    NmapOSClass,
    NmapTrace,
    NmapTraceHop,
    NmapTraceProtocol,
    NmapScanInfo,
)


class NmapParserException(Exception):
    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return self.msg


class NmapXmlParser:
    """
    Example: https://nmap.org/book/output-formats-xml-output.html
    Full DTD: https://nmap.org/book/nmap-dtd.html
    """

    @classmethod
    def parse_xml(cls, nmap_data: str) -> NmapRun:
        """
        Expects a full nmap scan report.
        """

        try:
            root = ET.fromstring(nmap_data)
        except Exception as e:
            emsg = "Wrong XML structure: cannot parse data: {0}".format(e)
            raise NmapParserException(emsg)

        if root.tag != "nmaprun":
            raise NmapParserException("Unpexpected data structure for XML " "root node")
        return cls._parse_xml_nmaprun(root)

    @classmethod
    def _parse_xml_nmaprun(cls, root: ET.Element) -> NmapRun:
        """
        This method parses out a full nmap scan report from its XML root
        node: <nmaprun>. We expect there is only 1 host in this report!

        :param root: Element from xml.ElementTree (top of XML the document)
        """
        cls._validate_nmap_root(root)
        host_count = len(root.findall(".//host"))
        assert host_count == 1, f"Expected 1 host, got {host_count}"

        xml_str = ET.tostring(root, encoding="unicode").replace("\n", "")
        nmap_data = {"raw_xml": xml_str}
        nmap_data.update(cls._parse_nmaprun(root))

        nmap_data["scan_infos"] = [
            cls._parse_scaninfo(scaninfo_el)
            for scaninfo_el in root.findall(".//scaninfo")
        ]

        nmap_data.update(cls._parse_runstats(root))

        nmap_data.update(cls._parse_xml_host(root.find(".//host")))

        return NmapRun.model_validate(nmap_data)

    @classmethod
    def _validate_nmap_root(cls, root: ET.Element) -> None:
        allowed = {
            "scaninfo",
            "host",
            "runstats",
            "verbose",
            "debugging",
        }

        found = {child.tag for child in root}
        unexpected = found - allowed
        if unexpected:
            raise ValueError(
                f"Unexpected top-level tags in nmap XML: {sorted(unexpected)}"
            )

    @classmethod
    def _parse_scaninfo(cls, scaninfo_el: ET.Element) -> NmapScanInfo:
        data = dict()
        data["type"] = NmapScanType(scaninfo_el.attrib["type"])
        data["protocol"] = NmapProtocol(scaninfo_el.attrib["protocol"])
        data["num_services"] = scaninfo_el.attrib["numservices"]
        data["services"] = scaninfo_el.attrib["services"]
        return NmapScanInfo.model_validate(data)

    @classmethod
    def _parse_runstats(cls, root: ET.Element) -> Dict:
        runstats = root.find("runstats")
        if runstats is None:
            return {}

        finished = runstats.find("finished")
        if finished is None:
            return {}

        finished_at = None
        ts = finished.attrib.get("time")
        if ts:
            finished_at = datetime.fromtimestamp(int(ts), tz=timezone.utc)

        return {
            "finished_at": finished_at,
            "exit_status": finished.attrib.get("exit"),
        }

    @classmethod
    def _parse_nmaprun(cls, nmaprun_el: ET.Element) -> Dict:
        nmap_data = dict()
        nmaprun = dict(nmaprun_el.attrib)
        nmap_data["command_line"] = nmaprun["args"]
        nmap_data["started_at"] = datetime.fromtimestamp(
            float(nmaprun["start"]), tz=timezone.utc
        )
        nmap_data["version"] = nmaprun["version"]
        nmap_data["xmloutputversion"] = nmaprun["xmloutputversion"]
        return nmap_data

    @classmethod
    def _parse_xml_host(cls, host_el: ET.Element) -> Dict:
        """
        Receives a <host> XML tag representing a scanned host with
        its services.
        """
        data = dict()

        # <status state="up" reason="user-set" reason_ttl="0"/>
        status_el = host_el.find("status")
        data["host_state"] = NmapHostState(status_el.attrib["state"])
        data["host_state_reason"] = NmapHostStatusReason(status_el.attrib["reason"])
        host_state_reason_ttl = status_el.attrib.get("reason_ttl")
        if host_state_reason_ttl:
            data["host_state_reason_ttl"] = int(host_state_reason_ttl)

        # <address addr="108.171.53.1" addrtype="ipv4"/>
        address_el = host_el.find("address")
        data["target_ip"] = address_el.attrib["addr"]

        data["hostnames"] = cls._parse_hostnames(host_el.find("hostnames"))

        data["ports"], data["port_stats"] = cls._parse_xml_ports(host_el.find("ports"))

        uptime = host_el.find("uptime")
        if uptime is not None:
            data["uptime_seconds"] = int(uptime.attrib["seconds"])

        distance = host_el.find("distance")
        if distance is not None:
            data["distance"] = int(distance.attrib["value"])

        tcpsequence = host_el.find("tcpsequence")
        if tcpsequence is not None:
            data["tcp_sequence_index"] = int(tcpsequence.attrib["index"])
            data["tcp_sequence_difficulty"] = tcpsequence.attrib["difficulty"]
        ipidsequence = host_el.find("ipidsequence")
        if ipidsequence is not None:
            data["ipid_sequence_class"] = ipidsequence.attrib["class"]
        tcptssequence = host_el.find("tcptssequence")
        if tcptssequence is not None:
            data["tcp_timestamp_class"] = tcptssequence.attrib["class"]

        times_elem = host_el.find("times")
        if times_elem is not None:
            data.update(
                {
                    "srtt_us": int(times_elem.attrib.get("srtt", 0)) or None,
                    "rttvar_us": int(times_elem.attrib.get("rttvar", 0)) or None,
                    "timeout_us": int(times_elem.attrib.get("to", 0)) or None,
                }
            )

        hostscripts_el = host_el.find("hostscript")
        if hostscripts_el is not None:
            data["host_scripts"] = [
                NmapHostScript(id=el.attrib["id"], output=el.attrib.get("output"))
                for el in hostscripts_el.findall("script")
            ]

        data["os_matches"] = cls._parse_os_matches(host_el)

        data["trace"] = cls._parse_trace(host_el)

        return data

    @classmethod
    def _parse_os_matches(cls, host_el: ET.Element) -> List[NmapOSMatch] | None:
        os_elem = host_el.find("os")
        if os_elem is None:
            return None

        matches: List[NmapOSMatch] = []

        for m in os_elem.findall("osmatch"):
            classes: List[NmapOSClass] = []

            for c in m.findall("osclass"):
                cpes = [e.text.strip() for e in c.findall("cpe") if e.text]

                classes.append(
                    NmapOSClass(
                        vendor=c.attrib.get("vendor"),
                        osfamily=c.attrib.get("osfamily"),
                        osgen=c.attrib.get("osgen"),
                        accuracy=(
                            int(c.attrib["accuracy"]) if "accuracy" in c.attrib else None
                        ),
                        cpe=cpes or None,
                    )
                )

            matches.append(
                NmapOSMatch(
                    name=m.attrib["name"],
                    accuracy=int(m.attrib["accuracy"]),
                    classes=classes,
                )
            )

        return matches or None

    @classmethod
    def _parse_hostnames(cls, hostnames_el: ET.Element) -> List[NmapHostname]:
        """
        Parses the hostnames element.
        e.g. <hostnames>
        <hostname name="108-171-53-1.aceips.com" type="PTR"/>
        </hostnames>
        """
        return [cls._parse_hostname(hname) for hname in hostnames_el.findall("hostname")]

    @classmethod
    def _parse_hostname(cls, hostname_el: ET.Element) -> NmapHostname:
        """
        Parses the hostname element.
        e.g. <hostname name="108-171-53-1.aceips.com" type="PTR"/>

        :param hostname_el: <hostname> XML tag from a nmap scan
        """
        return NmapHostname.model_validate(dict(hostname_el.attrib))

    @classmethod
    def _parse_xml_ports(
        cls, ports_elem: ET.Element
    ) -> Tuple[List[NmapPort], NmapPortStats]:
        """
        Parses the list of scanned services from a targeted host.
        """
        ports: List[NmapPort] = []
        stats = NmapPortStats()

        # handle extraports first
        for e in ports_elem.findall("extraports"):
            state = PortState(e.attrib["state"])
            count = int(e.attrib["count"])

            key = state.value.replace("|", "_")
            setattr(stats, key, getattr(stats, key) + count)

        for port_elem in ports_elem.findall("port"):
            port = cls._parse_xml_port(port_elem)
            ports.append(port)
            key = port.state.value.replace("|", "_")
            setattr(stats, key, getattr(stats, key) + 1)
        return ports, stats

    @classmethod
    def _parse_xml_service(cls, service_elem: ET.Element) -> NmapService:
        svc = {
            "name": service_elem.attrib.get("name"),
            "product": service_elem.attrib.get("product"),
            "version": service_elem.attrib.get("version"),
            "extrainfo": service_elem.attrib.get("extrainfo"),
            "method": service_elem.attrib.get("method"),
            "conf": (
                int(service_elem.attrib["conf"])
                if "conf" in service_elem.attrib
                else None
            ),
            "cpe": [e.text.strip() for e in service_elem.findall("cpe")],
        }

        return NmapService.model_validate(svc)

    @classmethod
    def _parse_xml_script(cls, script_elem: ET.Element) -> NmapScript:
        output = script_elem.attrib.get("output")
        if output:
            output = output.strip()
        script = {
            "id": script_elem.attrib["id"],
            "output": output,
        }

        elements: Dict[str, Any] = {}

        # handle <elem key="...">value</elem>
        for elem in script_elem.findall(".//elem"):
            key = elem.attrib.get("key")
            if key:
                elements[key.strip()] = elem.text.strip()

        script["elements"] = elements
        return NmapScript.model_validate(script)

    @classmethod
    def _parse_xml_port(cls, port_elem: ET.Element) -> NmapPort:
        """
        <port protocol="tcp" portid="61232">
            <state state="open" reason="syn-ack" reason_ttl="47"/>
            <service name="socks5" extrainfo="Username/password authentication required" method="probed" conf="10"/>
            <script id="socks-auth-info" output="&#xa;  Username and password">
                <table>
                    <elem key="name">Username and password</elem>
                    <elem key="method">2</elem>
                </table>
            </script>
        </port>
        """
        state_elem = port_elem.find("state")

        port = {
            "port": int(port_elem.attrib["portid"]),
            "protocol": port_elem.attrib["protocol"],
            "state": PortState(state_elem.attrib["state"]),
            "reason": (
                PortStateReason(state_elem.attrib["reason"])
                if "reason" in state_elem.attrib
                else None
            ),
            "reason_ttl": (
                int(state_elem.attrib["reason_ttl"])
                if "reason_ttl" in state_elem.attrib
                else None
            ),
        }

        service_elem = port_elem.find("service")
        if service_elem is not None:
            port["service"] = cls._parse_xml_service(service_elem)

        port["scripts"] = []
        for script_elem in port_elem.findall("script"):
            port["scripts"].append(cls._parse_xml_script(script_elem))

        return NmapPort.model_validate(port)

    @classmethod
    def _parse_trace(cls, host_elem: ET.Element) -> Optional[NmapTrace]:
        trace_elem = host_elem.find("trace")
        if trace_elem is None:
            return None

        port_attr = trace_elem.attrib.get("port")
        proto_attr = trace_elem.attrib.get("proto")

        hops: List[NmapTraceHop] = []

        for hop_elem in trace_elem.findall("hop"):
            ttl = hop_elem.attrib.get("ttl")
            if ttl is None:
                continue  # ttl is required by the DTD but guard anyway

            rtt = hop_elem.attrib.get("rtt")
            ipaddr = hop_elem.attrib.get("ipaddr")
            host = hop_elem.attrib.get("host")

            hops.append(
                NmapTraceHop(
                    ttl=int(ttl),
                    ipaddr=ipaddr,
                    rtt_ms=float(rtt) if rtt is not None else None,
                    host=host,
                )
            )

        return NmapTrace(
            port=int(port_attr) if port_attr is not None else None,
            protocol=NmapTraceProtocol(proto_attr) if proto_attr is not None else None,
            hops=hops,
        )

