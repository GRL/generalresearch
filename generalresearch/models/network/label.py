from __future__ import annotations

import ipaddress
from enum import StrEnum
from typing import Optional, List

from pydantic import (
    BaseModel,
    Field,
    computed_field,
    field_validator,
    ConfigDict,
    IPvAnyNetwork,
)

from generalresearch.models.custom_types import (
    AwareDatetimeISO,
    now_utc_factory,
)


class IPTrustClass(StrEnum):
    TRUSTED = "trusted"
    UNTRUSTED = "untrusted"
    # Note: use case of unknown is for e.g. Spur says this IP is a residential proxy
    #   on 2026-1-1, and then has no annotation a month later. It doesn't mean
    #   the IP is TRUSTED, but we want to record that Spur now doesn't claim UNTRUSTED.
    UNKNOWN = "unknown"


class IPLabelKind(StrEnum):
    # --- UNTRUSTED ---
    RESIDENTIAL_PROXY = "residential_proxy"
    DATACENTER_PROXY = "datacenter_proxy"
    ISP_PROXY = "isp_proxy"
    MOBILE_PROXY = "mobile_proxy"
    PROXY = "proxy"
    HOSTING = "hosting"
    VPN = "vpn"
    RELAY = "relay"
    TOR_EXIT = "tor_exit"
    BAD_ACTOR = "bad_actor"
    # --- TRUSTED ---
    TRUSTED_USER = "trusted_user"
    # --- UNKNOWN ---
    UNKNOWN = "unknown"


class IPLabelSource(StrEnum):
    # We got this IP from our own use of a proxy service
    INTERNAL_USE = "internal_use"

    # An external "security" service flagged this IP
    SPUR = "spur"
    IPINFO = "ipinfo"
    MAXMIND = "maxmind"

    MANUAL = "manual"


class IPLabel(BaseModel):
    """
    Stores *ground truth* about an IP at a specific time.
    To be used for model training and evaluation.
    """

    model_config = ConfigDict(validate_assignment=True)

    ip: IPvAnyNetwork = Field()

    labeled_at: AwareDatetimeISO = Field(default_factory=now_utc_factory)
    created_at: Optional[AwareDatetimeISO] = Field(default=None)

    label_kind: IPLabelKind = Field()
    source: IPLabelSource = Field()

    confidence: float = Field(default=1.0, ge=0.0, le=1.0)

    # Optionally, if this is untrusted, which service is providing the proxy/vpn service
    provider: Optional[str] = Field(
        default=None, examples=["geonode", "gecko"], max_length=128
    )

    metadata: Optional[IPLabelMetadata] = Field(default=None)

    @field_validator("ip", mode="before")
    @classmethod
    def normalize_and_validate_network(cls, v):
        net = ipaddress.ip_network(v, strict=False)

        if isinstance(net, ipaddress.IPv6Network):
            if net.prefixlen > 64:
                raise ValueError("IPv6 network must be /64 or larger")

        return net

    @field_validator("provider", mode="before")
    @classmethod
    def provider_format(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return v
        return v.lower().strip()

    @computed_field()
    @property
    def trust_class(self) -> IPTrustClass:
        if self.label_kind == IPLabelKind.UNKNOWN:
            return IPTrustClass.UNKNOWN
        if self.label_kind == IPLabelKind.TRUSTED_USER:
            return IPTrustClass.TRUSTED
        return IPTrustClass.UNTRUSTED

    def model_dump_postgres(self):
        d = self.model_dump(mode="json")
        d["metadata"] = self.metadata.model_dump_json() if self.metadata else None
        return d


class IPLabelMetadata(BaseModel):
    """
    To be expanded. Just for storing some things from Spur for now
    """

    model_config = ConfigDict(validate_assignment=True, extra="allow")

    services: Optional[List[str]] = Field(min_length=1, examples=[["RDP"]])
