from datetime import datetime, timezone, timedelta
from random import sample
from typing import List

import numpy as np
import pandas as pd
import pytest

from generalresearch.incite.schemas import empty_dataframe_from_schema
from generalresearch.incite.schemas.admin_responses import (
    AdminPOPSchema,
    SIX_HOUR_SECONDS,
)
from generalresearch.locales import Localelator


class TestAdminPOPSchema:
    schema_df = empty_dataframe_from_schema(AdminPOPSchema)
    countries = list(Localelator().get_all_countries())[:5]
    dates = [datetime(year=2024, month=1, day=i, tzinfo=None) for i in range(1, 10)]

    @classmethod
    def assign_valid_vals(cls, df: pd.DataFrame) -> pd.DataFrame:
        for c in df.columns:
            check_attrs: dict = AdminPOPSchema.columns[c].checks[0].statistics
            df[c] = np.random.randint(
                check_attrs["min_value"], check_attrs["max_value"], df.shape[0]
            )

        return df

    def test_empty(self):
        with pytest.raises(Exception):
            AdminPOPSchema.validate(pd.DataFrame())

    def test_new_empty_df(self):
        df = empty_dataframe_from_schema(AdminPOPSchema)

        assert isinstance(df, pd.DataFrame)
        assert isinstance(df.index, pd.MultiIndex)
        assert df.columns.size == len(AdminPOPSchema.columns)

    def test_valid(self):
        # (1) Works with raw naive datetime
        dates = [
            datetime(year=2024, month=1, day=i, tzinfo=None).isoformat()
            for i in range(1, 10)
        ]
        df = pd.DataFrame(
            index=pd.MultiIndex.from_product(
                iterables=[dates, self.countries], names=["index0", "index1"]
            ),
            columns=self.schema_df.columns,
        )
        df = self.assign_valid_vals(df)

        df = AdminPOPSchema.validate(df)
        assert isinstance(df, pd.DataFrame)

        # (2) Works with isoformat naive datetime
        dates = [datetime(year=2024, month=1, day=i, tzinfo=None) for i in range(1, 10)]
        df = pd.DataFrame(
            index=pd.MultiIndex.from_product(
                iterables=[dates, self.countries], names=["index0", "index1"]
            ),
            columns=self.schema_df.columns,
        )
        df = self.assign_valid_vals(df)

        df = AdminPOPSchema.validate(df)
        assert isinstance(df, pd.DataFrame)

    def test_index_tz_parser(self):
        tz_dates = [
            datetime(year=2024, month=1, day=i, tzinfo=timezone.utc)
            for i in range(1, 10)
        ]

        df = pd.DataFrame(
            index=pd.MultiIndex.from_product(
                iterables=[tz_dates, self.countries], names=["index0", "index1"]
            ),
            columns=self.schema_df.columns,
        )
        df = self.assign_valid_vals(df)

        # Initially, they're all set with a timezone
        timestmaps: List[pd.Timestamp] = [i for i in df.index.get_level_values(0)]
        assert all([ts.tz == timezone.utc for ts in timestmaps])

        # After validation, the timezone is removed
        df = AdminPOPSchema.validate(df)
        timestmaps: List[pd.Timestamp] = [i for i in df.index.get_level_values(0)]
        assert all([ts.tz is None for ts in timestmaps])

    def test_index_tz_no_future_beyond_one_year(self):
        now = datetime.now(tz=timezone.utc)
        tz_dates = [now + timedelta(days=i * 365) for i in range(1, 10)]

        df = pd.DataFrame(
            index=pd.MultiIndex.from_product(
                iterables=[tz_dates, self.countries], names=["index0", "index1"]
            ),
            columns=self.schema_df.columns,
        )
        df = self.assign_valid_vals(df)

        with pytest.raises(Exception) as cm:
            AdminPOPSchema.validate(df)

        assert (
            "Index 'index0' failed element-wise validator "
            "number 0: less_than(" in str(cm.value)
        )

    def test_index_only_str(self):
        # --- float64 to str! ---
        df = pd.DataFrame(
            index=pd.MultiIndex.from_product(
                iterables=[self.dates, np.random.rand(1, 10)[0]],
                names=["index0", "index1"],
            ),
            columns=self.schema_df.columns,
        )
        df = self.assign_valid_vals(df)

        vals = [i for i in df.index.get_level_values(1)]
        assert all([isinstance(v, float) for v in vals])

        df = AdminPOPSchema.validate(df, lazy=True)

        vals = [i for i in df.index.get_level_values(1)]
        assert all([isinstance(v, str) for v in vals])

        # --- int to str ---

        df = pd.DataFrame(
            index=pd.MultiIndex.from_product(
                iterables=[self.dates, sample(range(100), 20)],
                names=["index0", "index1"],
            ),
            columns=self.schema_df.columns,
        )
        df = self.assign_valid_vals(df)

        vals = [i for i in df.index.get_level_values(1)]
        assert all([isinstance(v, int) for v in vals])

        df = AdminPOPSchema.validate(df, lazy=True)

        vals = [i for i in df.index.get_level_values(1)]
        assert all([isinstance(v, str) for v in vals])

        # a = 1
        assert isinstance(df, pd.DataFrame)

    def test_invalid_parsing(self):
        # (1) Timezones AND as strings will still parse correctly
        tz_str_dates = [
            datetime(
                year=2024, month=1, day=1, minute=i, tzinfo=timezone.utc
            ).isoformat()
            for i in range(1, 10)
        ]
        df = pd.DataFrame(
            index=pd.MultiIndex.from_product(
                iterables=[tz_str_dates, self.countries],
                names=["index0", "index1"],
            ),
            columns=self.schema_df.columns,
        )
        df = self.assign_valid_vals(df)
        df = AdminPOPSchema.validate(df, lazy=True)

        assert isinstance(df, pd.DataFrame)
        timestmaps: List[pd.Timestamp] = [i for i in df.index.get_level_values(0)]
        assert all([ts.tz is None for ts in timestmaps])

        # (2) Timezones are removed
        dates = [
            datetime(year=2024, month=1, day=1, minute=i, tzinfo=timezone.utc)
            for i in range(1, 10)
        ]
        df = pd.DataFrame(
            index=pd.MultiIndex.from_product(
                iterables=[dates, self.countries], names=["index0", "index1"]
            ),
            columns=self.schema_df.columns,
        )
        df = self.assign_valid_vals(df)

        # Has tz before validation, and none after
        timestmaps: List[pd.Timestamp] = [i for i in df.index.get_level_values(0)]
        assert all([ts.tz is timezone.utc for ts in timestmaps])

        df = AdminPOPSchema.validate(df, lazy=True)

        timestmaps: List[pd.Timestamp] = [i for i in df.index.get_level_values(0)]
        assert all([ts.tz is None for ts in timestmaps])

    def test_clipping(self):
        df = pd.DataFrame(
            index=pd.MultiIndex.from_product(
                iterables=[self.dates, self.countries],
                names=["index0", "index1"],
            ),
            columns=self.schema_df.columns,
        )
        df = self.assign_valid_vals(df)
        df = AdminPOPSchema.validate(df)
        assert df.elapsed_avg.max() < SIX_HOUR_SECONDS

        # Now that we know it's valid, break the elapsed avg
        df["elapsed_avg"] = np.random.randint(
            SIX_HOUR_SECONDS, SIX_HOUR_SECONDS + 10_000, df.shape[0]
        )
        assert df.elapsed_avg.max() > SIX_HOUR_SECONDS

        # Confirm it doesn't fail if the values are greater, and that
        #   all the values are clipped to the max
        df = AdminPOPSchema.validate(df)
        assert df.elapsed_avg.eq(SIX_HOUR_SECONDS).all()

    def test_rounding(self):
        df = pd.DataFrame(
            index=pd.MultiIndex.from_product(
                iterables=[self.dates, self.countries],
                names=["index0", "index1"],
            ),
            columns=self.schema_df.columns,
        )
        df = self.assign_valid_vals(df)

        df["payout_avg"] = 2.123456789900002

        assert df.payout_avg.sum() == 95.5555555455001

        df = AdminPOPSchema.validate(df)
        assert df.payout_avg.sum() == 95.40000000000003
