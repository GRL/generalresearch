from datetime import timezone, datetime

import pandas as pd
import pytest
from pydantic import ValidationError


class TestReportRequest:
    def test_base(self, utc_60days_ago):
        from generalresearch.models.admin.request import (
            ReportRequest,
            ReportType,
        )

        rr = ReportRequest()

        assert isinstance(rr.start, datetime), "rr.start incorrect type"
        assert isinstance(rr.start_floor, datetime), "rr.start_floor incorrect type"

        assert rr.report_type == ReportType.POP_SESSION
        assert rr.start != rr.start_floor, "rr.start != rr.start_floor"
        assert rr.start_floor.tzinfo == timezone.utc, "rr.start_floor.tzinfo not utc"

        rr1 = ReportRequest.model_validate(
            {
                "start": datetime(
                    year=datetime.now().year,
                    month=1,
                    day=1,
                    hour=0,
                    minute=30,
                    second=25,
                    microsecond=35,
                    tzinfo=timezone.utc,
                ),
                "interval": "1h",
            }
        )

        assert isinstance(rr1.start, datetime), "rr1.start incorrect type"
        assert isinstance(rr1.start_floor, datetime), "rr1.start_floor incorrect type"

        rr2 = ReportRequest.model_validate(
            {
                "start": datetime(
                    year=datetime.now().year,
                    month=1,
                    day=1,
                    hour=6,
                    minute=30,
                    second=25,
                    microsecond=35,
                    tzinfo=timezone.utc,
                ),
                "interval": "1d",
            }
        )

        assert isinstance(rr2.start, datetime), "rr2.start incorrect type"
        assert isinstance(rr2.start_floor, datetime), "rr2.start_floor incorrect type"

        assert rr1.start != rr2.start, "rr1.start != rr2.start"
        assert rr1.start_floor == rr2.start_floor, "rr1.start_floor == rr2.start_floor"

        # datetime.datetime(2025, 7, 9, 0, 0, tzinfo=datetime.timezone.utc)
        # datetime.datetime(2025, 7, 9, 0, 0, tzinfo=datetime.timezone.utc)

        #  datetime.datetime(2025, 7, 9, 0, 0, tzinfo=datetime.timezone.utc) =
        #  ReportRequest(report_type=<ReportType.POP_SESSION: 'pop_session'>,
        #  index0='started', index1='product_id',
        #  start=datetime.datetime(2025, 7, 9, 0, 46, 23, 145756, tzinfo=datetime.timezone.utc),
        #  end=datetime.datetime(2025, 9, 7, 0, 46, 23, 149195, tzinfo=datetime.timezone.utc),
        #  interval='1h', include_open_bucket=True,
        #  start_floor=datetime.datetime(2025, 7, 9, 0, 0, tzinfo=datetime.timezone.utc)).start_floor

        #  datetime.datetime(2025, 7, 9, 0, 0, tzinfo=datetime.timezone.utc) =
        #  ReportRequest(report_type=<ReportType.POP_SESSION: 'pop_session'>,
        #  index0='started', index1='product_id',
        #  start=datetime.datetime(2025, 7, 9, 0, 46, 23, 145756, tzinfo=datetime.timezone.utc),
        #  end=datetime.datetime(2025, 9, 7, 0, 46, 23, 149267, tzinfo=datetime.timezone.utc),
        #  interval='1d', include_open_bucket=True,
        #  start_floor=datetime.datetime(2025, 7, 9, 0, 0, tzinfo=datetime.timezone.utc)).start_floor

    def test_start_end_range(self, utc_90days_ago, utc_30days_ago):
        from generalresearch.models.admin.request import ReportRequest

        with pytest.raises(expected_exception=ValidationError) as cm:
            ReportRequest.model_validate(
                {"start": utc_30days_ago, "end": utc_90days_ago}
            )

        with pytest.raises(expected_exception=ValidationError) as cm:
            ReportRequest.model_validate(
                {
                    "start": datetime(year=1990, month=1, day=1),
                    "end": datetime(year=1950, month=1, day=1),
                }
            )

    def test_start_end_range_tz(self):
        from generalresearch.models.admin.request import ReportRequest
        from zoneinfo import ZoneInfo

        pacific_tz = ZoneInfo("America/Los_Angeles")

        with pytest.raises(expected_exception=ValidationError) as cm:
            ReportRequest.model_validate(
                {
                    "start": datetime(year=2000, month=1, day=1, tzinfo=pacific_tz),
                    "end": datetime(year=2000, month=6, day=1, tzinfo=pacific_tz),
                }
            )

    def test_start_floor_naive(self):
        from generalresearch.models.admin.request import ReportRequest

        rr = ReportRequest()

        assert rr.start_floor_naive.tzinfo is None

    def test_end_naive(self):
        from generalresearch.models.admin.request import ReportRequest

        rr = ReportRequest()

        assert rr.end_naive.tzinfo is None

    def test_pd_interval(self):
        from generalresearch.models.admin.request import ReportRequest

        rr = ReportRequest()

        assert isinstance(rr.pd_interval, pd.Interval)

    def test_interval_timedelta(self):
        from generalresearch.models.admin.request import ReportRequest

        rr = ReportRequest()

        assert isinstance(rr.interval_timedelta, pd.Timedelta)

    def test_buckets(self):
        from generalresearch.models.admin.request import ReportRequest

        rr = ReportRequest()

        assert isinstance(rr.buckets(), pd.DatetimeIndex)

    def test_bucket_ranges(self):
        from generalresearch.models.admin.request import ReportRequest

        rr = ReportRequest()
        assert isinstance(rr.bucket_ranges(), list)

        rr = ReportRequest.model_validate(
            {
                "interval": "1d",
                "start": datetime(year=2000, month=1, day=1, tzinfo=timezone.utc),
                "end": datetime(year=2000, month=1, day=10, tzinfo=timezone.utc),
            }
        )

        assert len(rr.bucket_ranges()) == 10
