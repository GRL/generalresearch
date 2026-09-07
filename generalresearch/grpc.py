from __future__ import annotations

from datetime import UTC, datetime, timedelta

from google.protobuf.duration_pb2 import Duration
from google.protobuf.timestamp_pb2 import Timestamp


def timestamp_from_datetime(dt: datetime) -> Timestamp:
    ts = Timestamp()
    ts.FromDatetime(dt)
    return ts


def timestamp_from_datetime_nullable(dt: datetime | None) -> Timestamp:
    ts = Timestamp()
    if dt:
        ts.FromDatetime(dt)
    return ts


def timestamp_to_datetime(ts: Timestamp) -> datetime:
    return datetime.fromtimestamp(ts.seconds + ts.nanos / 1e9, tz=UTC)


def timestamp_to_datetime_nullable(ts: Timestamp) -> datetime | None:
    # grpc has no None. If a google.protobuf.Timestamp field is not set, it gets interpreted as timestamp 0
    default = datetime.fromtimestamp(0, tz=UTC)
    d = datetime.fromtimestamp(ts.seconds + ts.nanos / 1e9, tz=UTC)
    return None if d == default else d


def timestamp_to_json_nullable(ts: Timestamp) -> str | None:
    # 1) grpc converts a null timestamp to '1970-01-01T00:00:00Z'. Not what we want...
    # 2) grpc uses different formatting for the microseconds depending on if it's divisible by 0, 3, or 6 digits.
    #   I don't understand why anyone would want to do this ...
    # This forces 6 digit microsecond (even if it is .000000), uses a Z for utc (grpc Timestamp does not
    #   support timezones and so is always UTC) and handles None properly.
    dt = timestamp_to_datetime_nullable(ts)
    dt = dt.isoformat(timespec="microseconds") + "Z" if dt else None
    return dt


def duration_from_timedelta(td: timedelta) -> Duration:
    d = Duration()
    d.FromTimedelta(td)
    return d
