import ipaddress

import faker
import pytest
from psycopg.errors import UniqueViolation
from pydantic import ValidationError

from generalresearch.managers.network.label import IPLabelManager
from generalresearch.models.network.label import (
    IPLabel,
    IPLabelKind,
    IPLabelSource,
    IPLabelMetadata,
)
from generalresearch.models.thl.ipinfo import normalize_ip

fake = faker.Faker()


@pytest.fixture
def ip_label(utc_now) -> IPLabel:
    ip = ipaddress.IPv6Network((fake.ipv6(), 64), strict=False)
    return IPLabel(
        label_kind=IPLabelKind.VPN,
        labeled_at=utc_now,
        source=IPLabelSource.INTERNAL_USE,
        provider="GeoNodE",
        created_at=utc_now,
        ip=ip,
        metadata=IPLabelMetadata(services=["RDP"])
    )


def test_model(utc_now):
    ip = fake.ipv4_public()
    lbl = IPLabel(
        label_kind=IPLabelKind.VPN,
        labeled_at=utc_now,
        source=IPLabelSource.INTERNAL_USE,
        provider="GeoNodE",
        created_at=utc_now,
        ip=ip,
    )
    assert lbl.ip.prefixlen == 32
    print(f"{lbl.ip=}")

    ip = ipaddress.IPv4Network((ip, 24), strict=False)
    lbl = IPLabel(
        label_kind=IPLabelKind.VPN,
        labeled_at=utc_now,
        source=IPLabelSource.INTERNAL_USE,
        provider="GeoNodE",
        created_at=utc_now,
        ip=ip,
    )
    print(f"{lbl.ip=}")

    with pytest.raises(ValidationError, match="IPv6 network must be /64 or larger"):
        IPLabel(
            label_kind=IPLabelKind.VPN,
            labeled_at=utc_now,
            source=IPLabelSource.INTERNAL_USE,
            provider="GeoNodE",
            created_at=utc_now,
            ip=fake.ipv6(),
        )

    ip = ipaddress.IPv6Network((fake.ipv6(), 64), strict=False)
    lbl = IPLabel(
        label_kind=IPLabelKind.VPN,
        labeled_at=utc_now,
        source=IPLabelSource.INTERNAL_USE,
        provider="GeoNodE",
        created_at=utc_now,
        ip=ip,
    )
    print(f"{lbl.ip=}")

    ip = ipaddress.IPv6Network((ip.network_address, 48), strict=False)
    lbl = IPLabel(
        label_kind=IPLabelKind.VPN,
        labeled_at=utc_now,
        source=IPLabelSource.INTERNAL_USE,
        provider="GeoNodE",
        created_at=utc_now,
        ip=ip,
    )
    print(f"{lbl.ip=}")


def test_create(iplabel_manager: IPLabelManager, ip_label: IPLabel):
    iplabel_manager.create(ip_label)

    with pytest.raises(
        UniqueViolation, match="duplicate key value violates unique constraint"
    ):
        iplabel_manager.create(ip_label)


def test_filter(iplabel_manager: IPLabelManager, ip_label: IPLabel, utc_hour_ago):
    res = iplabel_manager.filter(ips=[ip_label.ip])
    assert len(res) == 0

    iplabel_manager.create(ip_label)
    res = iplabel_manager.filter(ips=[ip_label.ip])
    assert len(res) == 1

    out = res[0]
    assert out == ip_label

    res = iplabel_manager.filter(ips=[ip_label.ip], labeled_after=utc_hour_ago)
    assert len(res) == 1

    ip_label2 = ip_label.model_copy()
    ip_label2.ip = fake.ipv4_public()
    iplabel_manager.create(ip_label2)
    res = iplabel_manager.filter(ips=[ip_label.ip, ip_label2.ip])
    assert len(res) == 2


def test_filter_network(
    iplabel_manager: IPLabelManager, ip_label: IPLabel, utc_hour_ago
):
    print(ip_label)
    ip_label = ip_label.model_copy()
    ip_label.ip = ipaddress.IPv6Network((fake.ipv6(), 64), strict=False)

    iplabel_manager.create(ip_label)
    res = iplabel_manager.filter(ips=[ip_label.ip])
    assert len(res) == 1

    out = res[0]
    assert out == ip_label

    res = iplabel_manager.filter(ips=[ip_label.ip], labeled_after=utc_hour_ago)
    assert len(res) == 1

    ip_label2 = ip_label.model_copy()
    ip_label2.ip = fake.ipv4_public()
    iplabel_manager.create(ip_label2)
    res = iplabel_manager.filter(ips=[ip_label.ip, ip_label2.ip])
    assert len(res) == 2


def test_network(iplabel_manager: IPLabelManager, utc_now):
    # This is a fully-specific /128 ipv6 address.
    # e.g. '51b7:b38d:8717:6c5b:cd3e:f5c3:3aba:17d'
    ip = fake.ipv6()
    # Generally, we'd want to annotate the /64 network
    # e.g. '51b7:b38d:8717:6c5b::/64'
    ip_64 = ipaddress.IPv6Network((ip, 64), strict=False)

    label = IPLabel(
        label_kind=IPLabelKind.VPN,
        labeled_at=utc_now,
        source=IPLabelSource.INTERNAL_USE,
        provider="GeoNodE",
        created_at=utc_now,
        ip=ip_64,
    )
    iplabel_manager.create(label)

    # If I query for the /128 directly, I won't find it
    res = iplabel_manager.filter(ips=[ip])
    assert len(res) == 0

    # If I query for the /64 network I will
    res = iplabel_manager.filter(ips=[ip_64])
    assert len(res) == 1

    # Or, I can query for the /128 ip IN a network
    res = iplabel_manager.filter(ip_in_network=ip)
    assert len(res) == 1


def test_label_cidr_and_ipinfo(
    iplabel_manager: IPLabelManager, ip_information_factory, ip_geoname, utc_now
):
    # We have network_iplabel.ip as a cidr col and
    # thl_ipinformation.ip as a inet col. Make sure we can join appropriately
    ip = fake.ipv6()
    ip_information_factory(ip=ip, geoname=ip_geoname)
    # We normalize for storage into ipinfo table
    ip_norm, prefix = normalize_ip(ip)

    # Test with a larger network
    ip_48 = ipaddress.IPv6Network((ip, 48), strict=False)
    print(f"{ip=}")
    print(f"{ip_norm=}")
    print(f"{ip_48=}")
    label = IPLabel(
        label_kind=IPLabelKind.VPN,
        labeled_at=utc_now,
        source=IPLabelSource.INTERNAL_USE,
        provider="GeoNodE",
        created_at=utc_now,
        ip=ip_48,
    )
    iplabel_manager.create(label)

    res = iplabel_manager.test_join(ip_norm)
    print(res)
