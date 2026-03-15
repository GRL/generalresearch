from uuid import uuid4
from django.utils import timezone
from django.contrib.postgres.indexes import GistIndex, GinIndex

from django.db import models

from generalresearch.thl_django.fields import CIDRField


#######
# ** Signals **
# ToolRun
# PortScan
# PortScanPort
# RDNSResult
# Traceroute
# TracerouteHop

# ** Features **
# IPFeatureSnapshot

# ** Labels **
# IPLabel

# ** Predictions **
# IPPrediction
#######


class ToolRun(models.Model):
    """
    Represents one execution of one tool against one target
    """

    id = models.BigAutoField(primary_key=True)

    # The *Target* IP.
    # Should correspond to an IP we already have in the thl_ipinformation table
    ip = models.GenericIPAddressField()

    # Logical grouping of multiple scans (fast scan + deep scan + rdns + trace, etc.)
    scan_group_id = models.UUIDField(default=uuid4)

    class ToolClass(models.TextChoices):
        PORT_SCAN = "port_scan"
        RDNS = "rdns"
        PING = "ping"
        TRACEROUTE = "traceroute"

    tool_class = models.CharField(
        max_length=32,
        choices=ToolClass.choices,
    )

    # Actual binary used (e.g. nmap vs rustmap)
    tool_name = models.CharField(
        max_length=64,
    )

    tool_version = models.CharField(
        max_length=32,
        null=True,
    )

    started_at = models.DateTimeField()
    finished_at = models.DateTimeField(null=True)

    class Status(models.TextChoices):
        SUCCESS = "success"
        FAILED = "failed"
        TIMEOUT = "timeout"
        ERROR = "error"

    status = models.CharField(
        max_length=16,
        choices=Status.choices,
        default=Status.SUCCESS,
    )

    # Raw CLI invocation
    raw_command = models.TextField()
    # Parsed arguments / normalized config
    config = models.JSONField(null=True)

    class Meta:
        db_table = "network_toolrun"
        indexes = [
            models.Index(fields=["started_at"]),
            models.Index(fields=["scan_group_id"]),
            models.Index(fields=["ip", "-started_at"]),
        ]


class RDNSResult(models.Model):
    run = models.OneToOneField(
        ToolRun,
        on_delete=models.CASCADE,
        related_name="rdns",
        primary_key=True,
    )

    # denormalized from ToolRun for query speed
    ip = models.GenericIPAddressField()
    started_at = models.DateTimeField()
    scan_group_id = models.UUIDField()

    primary_hostname = models.CharField(max_length=255, null=True)
    primary_domain = models.CharField(max_length=255, null=True)
    hostname_count = models.PositiveIntegerField(default=0)
    hostnames = models.JSONField(default=list)

    class Meta:
        db_table = "network_rdnsresult"
        indexes = [
            models.Index(fields=["ip", "-started_at"]),
            models.Index(fields=["scan_group_id"]),
            models.Index(fields=["primary_hostname"]),
            models.Index(fields=["primary_domain"]),
        ]


class PortScan(models.Model):
    run = models.OneToOneField(
        ToolRun,
        on_delete=models.CASCADE,
        related_name="port_scan",
        primary_key=True,
    )

    # denormalized from ToolRun for query speed
    ip = models.GenericIPAddressField()
    started_at = models.DateTimeField()
    scan_group_id = models.UUIDField()

    xml_version = models.CharField(max_length=8)
    host_state = models.CharField(max_length=16)
    host_state_reason = models.CharField(max_length=32)

    latency_ms = models.FloatField(null=True)
    distance = models.IntegerField(null=True)

    uptime_seconds = models.IntegerField(null=True)
    last_boot = models.DateTimeField(null=True)

    # Full parsed output
    parsed = models.JSONField()

    # Can be inferred through a join, but will make common queries easier
    open_tcp_ports = models.JSONField(default=list)
    open_udp_ports = models.JSONField(default=list)

    class Meta:
        db_table = "network_portscan"
        indexes = [
            models.Index(fields=["scan_group_id"]),
            models.Index(fields=["ip", "-started_at"]),
            GinIndex(fields=["open_tcp_ports"]),
            GinIndex(fields=["open_udp_ports"]),
        ]


class PortScanPort(models.Model):
    id = models.BigAutoField(primary_key=True)
    port_scan = models.ForeignKey(
        PortScan,
        on_delete=models.CASCADE,
        related_name="ports",
    )

    # https://www.iana.org/assignments/protocol-numbers/protocol-numbers.xhtml
    protocol = models.PositiveSmallIntegerField(default=6)
    # nullable b/c ICMP doesn't use ports
    port = models.PositiveIntegerField(null=True)

    state = models.CharField(max_length=20)

    reason = models.CharField(max_length=32, null=True)
    reason_ttl = models.IntegerField(null=True)

    service_name = models.CharField(max_length=64, null=True)

    class Meta:
        db_table = "network_portscanport"
        constraints = [
            models.UniqueConstraint(
                fields=["port_scan", "protocol", "port"],
                name="unique_port_per_scan",
            ),
        ]
        indexes = [
            models.Index(fields=["port", "protocol", "state"]),
            models.Index(fields=["state"]),
            models.Index(fields=["service_name"]),
        ]


class MTR(models.Model):
    run = models.OneToOneField(
        ToolRun,
        on_delete=models.CASCADE,
        related_name="mtr",
        primary_key=True,
    )

    # denormalized from ToolRun for query speed
    ip = models.GenericIPAddressField()
    started_at = models.DateTimeField()
    scan_group_id = models.UUIDField()

    # Source performing the trace
    source_ip = models.GenericIPAddressField()
    facility_id = models.PositiveIntegerField()

    # IANA protocol numbers (1=ICMP, 6=TCP, 17=UDP)
    protocol = models.PositiveSmallIntegerField()
    # nullable b/c ICMP doesn't use ports
    port = models.PositiveIntegerField(null=True)

    # Full parsed output
    parsed = models.JSONField()

    class Meta:
        db_table = "network_mtr"
        indexes = [
            models.Index(fields=["ip", "-started_at"]),
            models.Index(fields=["scan_group_id"]),
        ]


class MTRHop(models.Model):
    id = models.BigAutoField(primary_key=True)
    mtr_run = models.ForeignKey(
        MTR,
        on_delete=models.CASCADE,
        related_name="hops",
    )

    hop = models.PositiveSmallIntegerField()
    ip = models.GenericIPAddressField(null=True)

    domain = models.CharField(max_length=255, null=True)
    asn = models.PositiveIntegerField(null=True)

    class Meta:
        db_table = "network_mtrhop"
        constraints = [
            models.UniqueConstraint(
                fields=["mtr_run", "hop"],
                name="unique_hop_per_run",
            )
        ]
        indexes = [
            models.Index(fields=["ip"]),
            models.Index(fields=["asn"]),
            models.Index(fields=["domain"]),
        ]


class IPLabel(models.Model):
    """
    Stores *ground truth* about an IP at a specific time.
    Used for model training and evaluation.
    """

    id = models.BigAutoField(primary_key=True)

    ip = CIDRField()

    labeled_at = models.DateTimeField(default=timezone.now)
    created_at = models.DateTimeField(auto_now_add=True)

    label_kind = models.CharField(max_length=32)

    source = models.CharField(max_length=32)

    confidence = models.FloatField(default=1.0)

    provider = models.CharField(
        max_length=128,
        null=True,
        help_text="Proxy/VPN provider if known (e.g. geonode, brightdata)",
    )

    metadata = models.JSONField(null=True)

    class Meta:
        db_table = "network_iplabel"
        indexes = [
            GistIndex(fields=["ip"]),
            models.Index(fields=["-labeled_at"]),
            models.Index(fields=["ip", "-labeled_at"]),
            models.Index(fields=["label_kind"]),
        ]
        constraints = [
            models.UniqueConstraint(
                fields=["ip", "label_kind", "source", "labeled_at"],
                name="unique_ip_label_event",
            )
        ]


# #########
# # Below here Copied/pasted from chatgpt, todo: evaluate this
# #########
#
#
# class IPFeatureSnapshot(models.Model):
#     """
#     Example features:
#     open_proxy_port
#     rdns_residential_score
#     distance
#     asn_type
#     latency
#     mobile_network_likelihood
#     """
#
#     ip = models.GenericIPAddressField(db_index=True)
#
#     scan_group_id = models.UUIDField(db_index=True)
#
#     computed_at = models.DateTimeField(auto_now_add=True)
#
#     features = models.JSONField()
#
#     class Meta:
#         db_table = "network_ip_feature_snapshot"
#         indexes = [
#             models.Index(fields=["ip", "-computed_at"]),
#             models.Index(fields=["scan_group_id"]),
#         ]
#
#
# class IPPrediction(models.Model):
#
#     ip = models.GenericIPAddressField(db_index=True)
#
#     scan_group_id = models.UUIDField(db_index=True)
#
#     predicted_at = models.DateTimeField(auto_now_add=True)
#
#     model_version = models.CharField(max_length=32)
#
#     risk_score = models.FloatField()
#
#     feature_scores = models.JSONField()
#
#     metadata = models.JSONField(default=dict)
#
#     class Meta:
#         db_table = "network_ip_prediction"
#         indexes = [
#             models.Index(fields=["ip", "-predicted_at"]),
#             models.Index(fields=["scan_group_id"]),
#             models.Index(fields=["risk_score"]),
#         ]
