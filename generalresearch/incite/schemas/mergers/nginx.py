# MergeType.NGINX_GRS: NGINXGRSSchema,
# MergeType.NGINX_FSB: NGINXFSBSchema,
# MergeType.NGINX_CORE: NGINXCoreSchema,

from datetime import timedelta

import pandas as pd
from pandera import Check, Column, DataFrameSchema, Index

from generalresearch.incite.schemas import ARCHIVE_AFTER, PARTITION_ON

NGINXBaseSchema = DataFrameSchema(
    columns={
        "time": Column(dtype=pd.DatetimeTZDtype(tz="UTC"), nullable=False),
        "method": Column(
            dtype=str, checks=[Check.str_length(max_value=8)], nullable=True
        ),
        "user_agent": Column(
            dtype=str, checks=[Check.str_length(max_value=3_000)], nullable=True
        ),
        "upstream_route": Column(
            dtype=str, checks=[Check.str_length(max_value=255)], nullable=True
        ),
        "host": Column(
            dtype=str, checks=[Check.str_length(max_value=255)], nullable=True
        ),
        "status": Column(
            dtype="Int32",
            checks=[Check.between(min_value=0, max_value=600)],
            nullable=False,
        ),
        "upstream_status": Column(
            dtype="Int32",
            checks=[Check.between(min_value=0, max_value=600)],
            nullable=False,
        ),
        "request_time": Column(
            dtype=float,
            checks=[Check.greater_than_or_equal_to(min_value=0)],
            nullable=False,
        ),
        "upstream_response_time": Column(
            dtype=float,
            checks=[Check.greater_than_or_equal_to(min_value=0)],
            nullable=False,
        ),
        "upstream_cache_hit": Column(dtype=bool, nullable=False),
    }
)

NGINXGRSSchema = DataFrameSchema(
    index=Index(dtype=int, checks=Check.greater_than_or_equal_to(0)),
    columns=NGINXBaseSchema.columns
    | {
        # --- GRL Custom
        "product_id": Column(
            dtype=str, checks=Check.str_length(min_value=1, max_value=32), nullable=True
        ),
        "product_user_id": Column(
            dtype=str,
            checks=Check.str_length(min_value=1, max_value=128),
            nullable=True,
        ),
        "wall_uuid": Column(
            dtype=str,
            # It's modified by some people and so this breaks..
            # checks=[Check.str_length(min_value=32, max_value=32)],
            nullable=True,
        ),
        "custom_query_params": Column(
            dtype=str, checks=[Check.str_length(max_value=3_000)], nullable=True
        ),
    },
    checks=[],
    coerce=True,
    metadata={PARTITION_ON: ["product_id"], ARCHIVE_AFTER: timedelta(minutes=1)},
)

NGINXCoreSchema = DataFrameSchema(
    index=Index(dtype=int, checks=Check.greater_than_or_equal_to(0)),
    columns=NGINXBaseSchema.columns
    | {
        # --- GRL Custom
        "request_path": Column(
            dtype=str,
            checks=Check.str_length(min_value=1, max_value=3_000),
            nullable=False,
        ),
        "referer": Column(
            dtype=str,
            checks=Check.str_length(min_value=1, max_value=128),
            nullable=True,
        ),
        "session_id": Column(
            dtype=str, checks=Check.str_length(max_value=3_000), nullable=True
        ),
        "request_id": Column(
            dtype=str, checks=Check.str_length(max_value=3_000), nullable=True
        ),
        "nudge_id": Column(
            dtype=str, checks=Check.str_length(max_value=3_000), nullable=True
        ),
        "request_custom_query_params": Column(
            dtype=str, checks=[Check.str_length(max_value=3_000)], nullable=True
        ),
    },
    checks=[],
    coerce=True,
    metadata={PARTITION_ON: None, ARCHIVE_AFTER: timedelta(minutes=1)},
)

NGINXFSBSchema = DataFrameSchema(
    index=Index(dtype=int, checks=Check.greater_than_or_equal_to(0)),
    columns=NGINXBaseSchema.columns
    | {
        # --- GRL Custom
        "product_id": Column(
            dtype=str, checks=Check.str_length(min_value=1, max_value=32), nullable=True
        ),
        "product_user_id": Column(
            dtype=str,
            checks=Check.str_length(min_value=1, max_value=128),
            nullable=True,
        ),
        "n_bins": Column(
            dtype="Int32",
            checks=Check.greater_than_or_equal_to(min_value=0),
            nullable=True,
        ),
        "is_offerwall": Column(dtype=bool, nullable=False),
        "offerwall": Column(dtype=bool, nullable=False),
        "is_report": Column(dtype=bool, nullable=False),
        "custom_query_params": Column(
            dtype=str, checks=[Check.str_length(max_value=3_000)], nullable=True
        ),
    },
    checks=[],
    coerce=True,
    metadata={PARTITION_ON: ["product_id"], ARCHIVE_AFTER: timedelta(minutes=1)},
)
