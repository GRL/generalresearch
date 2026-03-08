from datetime import timedelta
from typing import Set

from pandera import Check, Column, DataFrameSchema, Index

from generalresearch.incite.schemas import ARCHIVE_AFTER
from generalresearch.locales import Localelator
from generalresearch.models import Source

COUNTRY_ISOS: Set[str] = Localelator().get_all_countries()
kosovo = "xk"
COUNTRY_ISOS.add(kosovo)

"""
A single file containing, over the past year, one row per:
    date (YYYY-MM-DD), product_id (optional), buyer_id (optional), country_iso, source
with counts for this aggregation for the following:
    Status.COMPLETE, Status.FAIL, ..., StatusNULL, StatusCode1.BUYER_FAIL, ...
For e.g:
    2024-01-01, 70bXXXXXXXXXXX, NULL, 'us', 'm', 100, 234, 123,
"""

YMWallSummarySchema = DataFrameSchema(
    # index is meaningless
    index=Index(dtype=int),
    columns={
        "date": Column(
            dtype=str,
            checks=Check.str_matches("20[0-9][0-9]-[0-9]{2}-[0-9]{2}"),
            nullable=False,
        ),
        "product_id": Column(
            dtype=str,
            checks=Check.str_length(min_value=32, max_value=32),
            nullable=True,
        ),
        "buyer_id": Column(
            dtype=str,
            checks=Check.str_length(min_value=1, max_value=32),
            nullable=True,
        ),
        "country_iso": Column(
            dtype=str,
            checks=[
                Check.str_length(min_value=1, max_value=2),
                Check.isin(COUNTRY_ISOS),  # 2 letter, lowercase
            ],
            nullable=True,
        ),
        "source": Column(
            dtype=str,
            checks=[
                Check.str_length(max_value=2),
                Check.isin([e.value for e in Source]),
            ],
        ),
        "Status.COMPLETE": Column(dtype=int, checks=Check.greater_than_or_equal_to(0)),
        "Status.FAIL": Column(dtype=int, checks=Check.greater_than_or_equal_to(0)),
        "Status.ABANDON": Column(dtype=int, checks=Check.greater_than_or_equal_to(0)),
        "Status.TIMEOUT": Column(
            dtype=int,
            checks=Check.greater_than_or_equal_to(0),
            description="this includes those where the status is None",
        ),
        "StatusCode1.BUYER_FAIL": Column(
            dtype=int, checks=Check.greater_than_or_equal_to(0)
        ),
    },
    checks=[],
    coerce=True,
    strict=True,
    unique=["date", "product_id", "buyer_id", "country_iso", "source"],
    metadata={ARCHIVE_AFTER: timedelta(minutes=90)},
)
