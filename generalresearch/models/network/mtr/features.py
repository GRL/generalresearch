from typing import List, Dict

from pydantic import BaseModel, ConfigDict, Field

from generalresearch.models.network.definitions import IPKind
from generalresearch.models.network.mtr import MTRHop


class MTRFeatures(BaseModel):
    model_config = ConfigDict()

    hop_count: int = Field()

    public_hop_count: int
    private_hop_count: int

    unique_asn_count: int
    asn_transition_count: int

    missing_hop_count: int
    missing_hop_ratio: float

    # typical for mobile (vs residential)
    private_hops_after_public: int

    has_cgnat: bool


def trim_local_hops(hops: List[MTRHop]) -> List[MTRHop]:
    start = 0
    for i, h in enumerate(hops):
        if h.ip_kind == IPKind.PUBLIC:
            start = i
            break
    return hops[start:]


def extract_mtr_features(hops: List[MTRHop]) -> Dict[str, float | int | bool | None]:
    features: Dict[str, float | int | bool | None] = {}

    if not hops:
        return {}

    hops = trim_local_hops(hops)

    features["hop_count"] = len(hops)

    private_hops = 0
    public_hops = 0
    for h in hops:
        if not h.ip:
            continue
        if h.ip_kind == IPKind.PUBLIC:
            public_hops += 1
        else:
            private_hops += 1
    features["private_hop_count"] = private_hops
    features["public_hop_count"] = public_hops

    # -----------------------
    # ASN structure
    # -----------------------

    asns = [h.asn for h in hops if h.asn]

    features["unique_asn_count"] = len(set(asns))

    asn_changes = 0
    for a, b in zip(asns, asns[1:]):
        if a != b:
            asn_changes += 1

    features["asn_transition_count"] = asn_changes

    # -----------------------
    # Missing hops
    # -----------------------

    missing_hops = sum(1 for h in hops if h.ip is None)

    features["missing_hop_count"] = missing_hops
    features["missing_hop_ratio"] = missing_hops / len(hops)

    # -----------------------
    # Packet loss
    # -----------------------

    lossy_hops = sum(1 for h in hops if h.loss_pct > 0)

    features["lossy_hop_count"] = lossy_hops
    features["max_loss_pct"] = max(h.loss_pct for h in hops)

    # -----------------------
    # Latency stats
    # -----------------------

    avg_rtts = [h.avg_ms for h in hops if h.avg_ms > 0]

    if avg_rtts:
        features["destination_rtt"] = avg_rtts[-1]
        features["mean_rtt"] = sum(avg_rtts) / len(avg_rtts)
        features["max_rtt"] = max(avg_rtts)
    else:
        features["destination_rtt"] = None
        features["mean_rtt"] = None
        features["max_rtt"] = None

    # -----------------------
    # RTT jumps
    # -----------------------

    rtt_jumps = []

    for a, b in zip(hops, hops[1:]):
        if a.avg_ms > 0 and b.avg_ms > 0:
            rtt_jumps.append(b.avg_ms - a.avg_ms)

    if rtt_jumps:
        features["max_rtt_jump"] = max(rtt_jumps)
        features["mean_rtt_jump"] = sum(rtt_jumps) / len(rtt_jumps)
    else:
        features["max_rtt_jump"] = None
        features["mean_rtt_jump"] = None

    # -----------------------
    # Jitter
    # -----------------------

    stdevs = [h.stdev_ms for h in hops if h.stdev_ms > 0]

    if stdevs:
        features["max_jitter"] = max(stdevs)
        features["mean_jitter"] = sum(stdevs) / len(stdevs)
    else:
        features["max_jitter"] = None
        features["mean_jitter"] = None

    # -----------------------
    # Route completion
    # -----------------------

    last = hops[-1]

    features["destination_reached"] = last.ip is not None and last.loss_pct < 100

    return features
