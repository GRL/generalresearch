import json
from datetime import timedelta
from enum import StrEnum
from functools import cached_property
from typing import Dict, Any, Literal, List, Optional, Tuple, Set

from pydantic import computed_field, BaseModel, Field

from generalresearch.models.custom_types import AwareDatetimeISO, IPvAnyAddressStr
from generalresearch.models.network.definitions import IPProtocol


class PortState(StrEnum):
    OPEN = "open"
    CLOSED = "closed"
    FILTERED = "filtered"
    UNFILTERED = "unfiltered"
    OPEN_FILTERED = "open|filtered"
    CLOSED_FILTERED = "closed|filtered"
    # Added by me, does not get returned. Used for book-keeping
    NOT_SCANNED = "not_scanned"


class PortStateReason(StrEnum):
    SYN_ACK = "syn-ack"
    RESET = "reset"
    CONN_REFUSED = "conn-refused"
    NO_RESPONSE = "no-response"
    SYN = "syn"
    FIN = "fin"

    ICMP_NET_UNREACH = "net-unreach"
    ICMP_HOST_UNREACH = "host-unreach"
    ICMP_PROTO_UNREACH = "proto-unreach"
    ICMP_PORT_UNREACH = "port-unreach"

    ADMIN_PROHIBITED = "admin-prohibited"
    HOST_PROHIBITED = "host-prohibited"
    NET_PROHIBITED = "net-prohibited"

    ECHO_REPLY = "echo-reply"
    TIME_EXCEEDED = "time-exceeded"


class NmapScanType(StrEnum):
    SYN = "syn"
    CONNECT = "connect"
    ACK = "ack"
    WINDOW = "window"
    MAIMON = "maimon"
    FIN = "fin"
    NULL = "null"
    XMAS = "xmas"
    UDP = "udp"
    SCTP_INIT = "sctpinit"
    SCTP_COOKIE_ECHO = "sctpcookieecho"


class NmapHostState(StrEnum):
    UP = "up"
    DOWN = "down"
    UNKNOWN = "unknown"


class NmapHostStatusReason(StrEnum):
    USER_SET = "user-set"
    SYN_ACK = "syn-ack"
    RESET = "reset"
    ECHO_REPLY = "echo-reply"
    ARP_RESPONSE = "arp-response"
    NO_RESPONSE = "no-response"
    NET_UNREACH = "net-unreach"
    HOST_UNREACH = "host-unreach"
    PROTO_UNREACH = "proto-unreach"
    PORT_UNREACH = "port-unreach"
    ADMIN_PROHIBITED = "admin-prohibited"
    LOCALHOST_RESPONSE = "localhost-response"


class NmapOSClass(BaseModel):
    vendor: str = None
    osfamily: str = None
    osgen: Optional[str] = None
    accuracy: int = None
    cpe: Optional[List[str]] = None


class NmapOSMatch(BaseModel):
    name: str
    accuracy: int
    classes: List[NmapOSClass] = Field(default_factory=list)

    @property
    def best_class(self) -> Optional[NmapOSClass]:
        if not self.classes:
            return None
        return max(self.classes, key=lambda m: m.accuracy)


class NmapScript(BaseModel):
    """
    <script id="socks-auth-info" output="&#xa;  Username and password">
        <table>
            <elem key="name">Username and password</elem>
            <elem key="method">2</elem>
        </table>
    </script>
    """

    id: str
    output: Optional[str] = None
    elements: Dict[str, Any] = Field(default_factory=dict)


class NmapService(BaseModel):
    # <service name="socks5" extrainfo="Username/password authentication required" method="probed" conf="10"/>
    name: Optional[str] = None
    product: Optional[str] = None
    version: Optional[str] = None
    extrainfo: Optional[str] = None
    method: Optional[str] = None
    conf: Optional[int] = None
    cpe: List[str] = Field(default_factory=list)

    def model_dump_postgres(self):
        d = self.model_dump(mode="json")
        d["service_name"] = self.name
        return d


class NmapPort(BaseModel):
    port: int = Field()
    protocol: IPProtocol = Field()
    # Closed ports will not have a NmapPort record
    state: PortState = Field()
    reason: Optional[PortStateReason] = Field(default=None)
    reason_ttl: Optional[int] = Field(default=None)

    service: Optional[NmapService] = None
    scripts: List[NmapScript] = Field(default_factory=list)

    def model_dump_postgres(self, run_id: int):
        # Writes for the network_portscanport table
        d = {"port_scan_id": run_id}
        data = self.model_dump(
            mode="json",
            include={
                "port",
                "state",
                "reason",
                "reason_ttl",
            },
        )
        d.update(data)
        d["protocol"] = self.protocol.to_number()
        if self.service:
            d.update(self.service.model_dump_postgres())
        return d


class NmapHostScript(BaseModel):
    id: str = Field()
    output: Optional[str] = Field(default=None)


class NmapTraceHop(BaseModel):
    """
    One hop observed during Nmap's traceroute.

    Example XML:
    <hop ttl="7" ipaddr="62.115.192.20" rtt="17.17" host="gdl-b2-link.ip.twelve99.net"/>
    """

    ttl: int = Field()

    ipaddr: Optional[str] = Field(
        default=None,
        description="IP address of the responding router or host",
    )

    rtt_ms: Optional[float] = Field(
        default=None,
        description="Round-trip time in milliseconds for the probe reaching this hop.",
    )

    host: Optional[str] = Field(
        default=None,
        description="Reverse DNS hostname for the hop if Nmap resolved one.",
    )


class NmapTrace(BaseModel):
    """
    Traceroute information collected by Nmap.

    Nmap performs a single traceroute per host using probes matching the scan
    type (typically TCP) directed at a chosen destination port.

    Example XML:
        <trace port="61232" proto="tcp">
            <hop ttl="1" ipaddr="192.168.86.1" rtt="3.83"/>
            ...
        </trace>
    """

    port: Optional[int] = Field(
        default=None,
        description="Destination port used for traceroute probes (may be absent depending on scan type).",
    )
    protocol: Optional[IPProtocol] = Field(
        default=None,
        description="Transport protocol used for the traceroute probes (tcp, udp, etc.).",
    )

    hops: List[NmapTraceHop] = Field(
        default_factory=list,
        description="Ordered list of hops observed during the traceroute.",
    )

    @property
    def destination(self) -> Optional[NmapTraceHop]:
        return self.hops[-1] if self.hops else None


class NmapHostname(BaseModel):
    # <hostname name="108-171-53-1.aceips.com" type="PTR"/>
    name: str
    type: Optional[Literal["PTR", "user"]] = None


class NmapPortStats(BaseModel):
    """
    This is counts across all protocols scanned (tcp/udp)
    """

    open: int = 0
    closed: int = 0
    filtered: int = 0
    unfiltered: int = 0
    open_filtered: int = 0
    closed_filtered: int = 0


class NmapScanInfo(BaseModel):
    """
    We could have multiple protocols in one run.
    <scaninfo type="syn" protocol="tcp" numservices="983" services="22-1000,1100,3389,11000,61232"/>
    <scaninfo type="syn" protocol="udp" numservices="983" services="1100"/>
    """

    type: NmapScanType = Field()
    protocol: IPProtocol = Field()
    num_services: int = Field()
    services: str = Field()

    @cached_property
    def port_set(self) -> Set[int]:
        """
        Expand the Nmap services string into a set of port numbers.
        Example:
            "22-25,80,443" -> {22,23,24,25,80,443}
        """
        ports: Set[int] = set()
        for part in self.services.split(","):
            if "-" in part:
                start, end = part.split("-", 1)
                ports.update(range(int(start), int(end) + 1))
            else:
                ports.add(int(part))
        return ports


class NmapRun(BaseModel):
    """
    A Nmap Run. Expects that we've only scanned ONE host.
    """

    command_line: str = Field()
    started_at: AwareDatetimeISO = Field()
    version: str = Field()
    xmloutputversion: Literal["1.04"] = Field()

    scan_infos: List[NmapScanInfo] = Field(min_length=1)

    # comes from <runstats>
    finished_at: Optional[AwareDatetimeISO] = Field(default=None)
    exit_status: Optional[Literal["success", "error"]] = Field(default=None)

    #####
    # Everything below here is from within the *single* host we've scanned
    #####

    # <status state="up" reason="user-set" reason_ttl="0"/>
    host_state: NmapHostState = Field()
    host_state_reason: NmapHostStatusReason = Field()
    host_state_reason_ttl: Optional[int] = None

    # <address addr="108.171.53.1" addrtype="ipv4"/>
    target_ip: IPvAnyAddressStr = Field()

    hostnames: List[NmapHostname] = Field()

    ports: List[NmapPort] = []
    port_stats: NmapPortStats = Field()

    # <uptime seconds="4063775" lastboot="Fri Jan 16 12:12:06 2026"/>
    uptime_seconds: Optional[int] = Field(default=None)
    # <distance value="11"/>
    distance: Optional[int] = Field(description="approx number of hops", default=None)

    # <tcpsequence index="263" difficulty="Good luck!">
    tcp_sequence_index: Optional[int] = None
    tcp_sequence_difficulty: Optional[str] = None

    # <ipidsequence class="All zeros">
    ipid_sequence_class: Optional[str] = None

    # <tcptssequence class="1000HZ" >
    tcp_timestamp_class: Optional[str] = None

    # <times srtt="54719" rttvar="23423" to="148411"/>
    srtt_us: Optional[int] = Field(
        default=None, description="smoothed RTT estimate (microseconds µs)"
    )
    rttvar_us: Optional[int] = Field(
        default=None, description="RTT variance (microseconds µs)"
    )
    timeout_us: Optional[int] = Field(
        default=None, description="probe timeout (microseconds µs)"
    )

    os_matches: Optional[List[NmapOSMatch]] = Field(default=None)

    host_scripts: List[NmapHostScript] = Field(default_factory=list)

    trace: Optional[NmapTrace] = Field(default=None)

    raw_xml: Optional[str] = None

    @computed_field
    @property
    def last_boot(self) -> Optional[AwareDatetimeISO]:
        if self.uptime_seconds:
            return self.started_at - timedelta(seconds=self.uptime_seconds)

    @property
    def scan_info_tcp(self):
        return next(
            filter(lambda x: x.protocol == IPProtocol.TCP, self.scan_infos), None
        )

    @property
    def scan_info_udp(self):
        return next(
            filter(lambda x: x.protocol == IPProtocol.UDP, self.scan_infos), None
        )

    @property
    def latency_ms(self) -> Optional[float]:
        return self.srtt_us / 1000 if self.srtt_us is not None else None

    @property
    def best_os_match(self) -> Optional[NmapOSMatch]:
        if not self.os_matches:
            return None
        return max(self.os_matches, key=lambda m: m.accuracy)

    def filter_ports(self, protocol: IPProtocol, state: PortState) -> List[NmapPort]:
        return [p for p in self.ports if p.protocol == protocol and p.state == state]

    @property
    def tcp_open_ports(self) -> List[int]:
        """
        Returns a list of open TCP port numbers.
        """
        return [
            p.port
            for p in self.filter_ports(protocol=IPProtocol.TCP, state=PortState.OPEN)
        ]

    @property
    def udp_open_ports(self) -> List[int]:
        """
        Returns a list of open UDP port numbers.
        """
        return [
            p.port
            for p in self.filter_ports(protocol=IPProtocol.UDP, state=PortState.OPEN)
        ]

    @cached_property
    def _port_index(self) -> Dict[Tuple[IPProtocol, int], NmapPort]:
        return {(p.protocol, p.port): p for p in self.ports}

    def get_port_state(
        self, port: int, protocol: IPProtocol = IPProtocol.TCP
    ) -> PortState:
        # Explicit (only if scanned and not closed)
        if (protocol, port) in self._port_index:
            return self._port_index[(protocol, port)].state

        # Check if we even scanned it
        scaninfo = next((s for s in self.scan_infos if s.protocol == protocol), None)
        if scaninfo and port in scaninfo.port_set:
            return PortState.CLOSED

        # We didn't scan it
        return PortState.NOT_SCANNED

    def model_dump_postgres(self):
        # Writes for the network_portscan table
        d = dict()
        data = self.model_dump(
            mode="json",
            include={
                "started_at",
                "host_state",
                "host_state_reason",
                "distance",
                "uptime_seconds",
                "raw_xml",
            },
        )
        d.update(data)
        d["ip"] = self.target_ip
        d["xml_version"] = self.xmloutputversion
        d["latency_ms"] = self.latency_ms
        d["last_boot"] = self.last_boot
        d["parsed"] = self.model_dump_json(indent=0)
        d["open_tcp_ports"] = json.dumps(self.tcp_open_ports)
        return d
