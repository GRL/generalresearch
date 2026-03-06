import pandas as pd
from datetime import datetime, timezone, timedelta


class TestIntervalIndex:

    def test_init(self):
        start = datetime(year=2000, month=1, day=1)
        end = datetime(year=2000, month=1, day=10)

        iv_r: pd.IntervalIndex = pd.interval_range(
            start=start, end=end, freq="1d", closed="left"
        )
        assert isinstance(iv_r, pd.IntervalIndex)
        assert len(iv_r.to_list()) == 9

        # If the offset is longer than the end - start it will not
        #   error. It will simply have 0 rows.
        iv_r: pd.IntervalIndex = pd.interval_range(
            start=start, end=end, freq="30d", closed="left"
        )
        assert isinstance(iv_r, pd.IntervalIndex)
        assert len(iv_r.to_list()) == 0
