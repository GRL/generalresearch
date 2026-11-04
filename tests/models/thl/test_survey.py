from datetime import UTC, datetime, time
from unittest.mock import patch
from zoneinfo import ZoneInfo

import pytest

from generalresearch.models.definitions import Source
from generalresearch.models.thl.survey.model import Survey, TimeInterval


@pytest.mark.parametrize(
    ("hour", "minute", "expected"),
    [
        (6, 59, False),
        (7, 0, True),
        (12, 0, True),
        (22, 0, True),
        (22, 1, False),
    ],
)
def test_is_currently_active_in_worker_local_time(hour, minute, expected):
    interval = TimeInterval(start=time(7), end=time(22), timezone=None)
    worker_time = datetime(
        2026, 1, 15, hour, minute, tzinfo=ZoneInfo("America/Chicago")
    )

    assert interval.is_currently_active(worker_time) is expected


@pytest.mark.parametrize(
    ("hour", "expected"),
    [
        (21, False),
        (22, True),
        (23, True),
        (0, True),
        (6, True),
        (7, False),
    ],
)
def test_is_currently_active_for_interval_crossing_midnight(hour, expected):
    interval = TimeInterval(start=time(22), end=time(6), timezone=None)
    worker_time = datetime(2026, 1, 15, hour, tzinfo=UTC)

    assert interval.is_currently_active(worker_time) is expected


def test_is_currently_active_converts_to_interval_timezone():
    interval = TimeInterval(
        start=time(7),
        end=time(9),
        timezone="America/New_York",
    )
    # 08:00 in New York on this date.
    worker_time = datetime(
        2026, 1, 15, 7, tzinfo=ZoneInfo("America/Chicago")
    )

    assert interval.is_currently_active(worker_time) is True


def test_is_currently_active_requires_aware_worker_time():
    interval = TimeInterval(start=time(7), end=time(22), timezone=None)

    with pytest.raises(AssertionError):
        interval.is_currently_active(datetime(2026, 1, 15, 12))


def test_is_currently_active_requires_worker_timezone_for_local_interval():
    interval = TimeInterval(start=time(7), end=time(22), timezone=None)

    assert interval.is_currently_active_by_worker_tz(worker_tz=None) is False


@pytest.mark.parametrize(
    ("interval_timezone", "worker_timezone"),
    [
        (None, ZoneInfo("America/Chicago")),
        ("America/New_York", None),
    ],
)
def test_is_currently_active_supports_interval_crossing_midnight(
    interval_timezone, worker_timezone
):
    interval = TimeInterval(
        start=time(22),
        end=time(6),
        timezone=interval_timezone,
    )
    current_timezone = (
        ZoneInfo(interval_timezone) if interval_timezone else worker_timezone
    )
    current_time = datetime(2026, 1, 15, 23, tzinfo=current_timezone)

    with patch("generalresearch.models.thl.survey.model.datetime") as mock_datetime:
        mock_datetime.now.return_value = current_time

        assert interval.is_currently_active_by_worker_tz(worker_timezone) is True
        mock_datetime.now.assert_called_once_with(tz=current_timezone)


@pytest.mark.parametrize(
    ("off_start", "off_end", "active_start", "active_end"),
    [
        (time(1), time(5), time(5), time(1)),
        (time(22), time(6), time(6), time(22)),
    ],
)
def test_from_off_hours_returns_complementary_active_hours(
    off_start, off_end, active_start, active_end
):
    off_hours = TimeInterval(
        start=off_start,
        end=off_end,
        timezone="America/New_York",
    )

    active_hours = Survey.active_hours_from_off_hours(off_hours)

    assert active_hours == [
        TimeInterval(
            start=active_start,
            end=active_end,
            timezone="America/New_York",
        )
    ]


def test_from_off_hours_result_can_be_assigned_to_survey():
    off_hours = TimeInterval(start=time(1), end=time(5), timezone=None)

    survey = Survey(
        source=Source.TESTING,
        survey_id="off-hours-test",
        active_hours=Survey.active_hours_from_off_hours(off_hours),
    )

    assert survey.active_hours == [
        TimeInterval(start=time(5), end=time(1), timezone=None)
    ]
