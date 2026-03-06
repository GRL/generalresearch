from datetime import timezone, datetime, timedelta

from generalresearch.models.thl.user_iphistory import (
    UserIPHistory,
    UserIPRecord,
)


def test_collapse_ip_records():
    # This does not exist in a db, so we do not need fixtures/ real user ids, whatever
    now = datetime.now(tz=timezone.utc) - timedelta(days=1)
    # Gets stored most recent first. This is reversed, but the validator will order it
    records = [
        UserIPRecord(ip="1.2.3.5", created=now + timedelta(minutes=1)),
        UserIPRecord(
            ip="1e5c:de49:165a:6aa0:4f89:1433:9af7:aaaa",
            created=now + timedelta(minutes=2),
        ),
        UserIPRecord(
            ip="1e5c:de49:165a:6aa0:4f89:1433:9af7:bbbb",
            created=now + timedelta(minutes=3),
        ),
        UserIPRecord(ip="1.2.3.5", created=now + timedelta(minutes=4)),
        UserIPRecord(
            ip="1e5c:de49:165a:6aa0:4f89:1433:9af7:cccc",
            created=now + timedelta(minutes=5),
        ),
        UserIPRecord(
            ip="6666:de49:165a:6aa0:4f89:1433:9af7:aaaa",
            created=now + timedelta(minutes=6),
        ),
        UserIPRecord(ip="1.2.3.6", created=now + timedelta(minutes=7)),
    ]
    iph = UserIPHistory(user_id=1, ips=records)
    res = iph.collapse_ip_records()

    # We should be left with one of the 1.2.3.5 ipv4s,
    #   and only the 1e5c::cccc and the 6666 ipv6 addresses
    assert len(res) == 4
    assert [x.ip for x in res] == [
        "1.2.3.6",
        "6666:de49:165a:6aa0:4f89:1433:9af7:aaaa",
        "1e5c:de49:165a:6aa0:4f89:1433:9af7:cccc",
        "1.2.3.5",
    ]
