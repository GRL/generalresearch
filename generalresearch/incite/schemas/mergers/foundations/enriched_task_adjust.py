from typing import Set

import pandas as pd
from pandera import Check, Column, DataFrameSchema, Index

from generalresearch.incite.schemas import ARCHIVE_AFTER, ORDER_KEY
from generalresearch.incite.schemas.thl_web import THLTaskAdjustmentSchema
from generalresearch.locales import Localelator
from generalresearch.models import DeviceType, Source
from generalresearch.models.thl.definitions import (
    WallAdjustedStatus,
)

thl_task_adj_columns = THLTaskAdjustmentSchema.columns.copy()

COUNTRY_ISOS: Set[str] = Localelator().get_all_countries()
kosovo = "xk"
COUNTRY_ISOS.add(kosovo)
BIGINT = 9223372036854775807

EnrichedTaskAdjustSchema = DataFrameSchema(
    index=Index(dtype=int, checks=Check.greater_than_or_equal_to(0)),
    columns={
        "wall_uuid": Column(
            dtype=str,
            checks=[
                Check.str_length(min_value=32, max_value=32),
            ],
        ),
        "user_id": Column(
            dtype="Int32",
            checks=Check.between(min_value=0, max_value=BIGINT),
            nullable=False,
        ),
        "source": Column(
            dtype=str,
            checks=[
                Check.str_length(max_value=2),
                Check.isin([e.value for e in Source]),
            ],
            nullable=False,
        ),
        "survey_id": Column(
            dtype=str, checks=[Check.str_length(max_value=32)], nullable=False
        ),
        "amount": Column(dtype=float),
        "adjusted_status": Column(
            dtype=str,
            checks=[
                Check.str_length(min_value=2, max_value=2),
                Check.isin([e.value for e in WallAdjustedStatus]),
            ],
        ),
        "adjusted_status_last": Column(
            dtype=str,
            checks=[
                Check.str_length(min_value=2, max_value=2),
                Check.isin([e.value for e in WallAdjustedStatus]),
            ],
        ),
        "alerted": Column(dtype=pd.DatetimeTZDtype(tz="UTC")),
        "alerted_last": Column(dtype=pd.DatetimeTZDtype(tz="UTC")),
        "started": Column(dtype=pd.DatetimeTZDtype(tz="UTC")),
        "buyer_id": Column(
            dtype=str, checks=Check.str_length(max_value=32), nullable=True
        ),
        "country_iso": Column(
            dtype=str,
            checks=[
                Check.str_length(min_value=1, max_value=2),
                Check.isin(COUNTRY_ISOS),  # 2 letter, lowercase
            ],
            nullable=True,
        ),
        "device_type": Column(
            dtype="Int32",
            checks=Check.isin([e.value for e in DeviceType]),
            nullable=True,
        ),
        "adjustments": Column(
            dtype="Int32",
            checks=Check.between(min_value=0, max_value=BIGINT),
            nullable=False,
        ),
        "product_id": Column(
            dtype=str,
            checks=Check.str_length(min_value=32, max_value=32),
            nullable=False,
        ),
        "team_id": Column(
            dtype=str,
            checks=Check.str_length(min_value=32, max_value=32),
            nullable=True,
        ),
    },
    checks=[],
    coerce=True,
    metadata={ORDER_KEY: "alerted", ARCHIVE_AFTER: None},
)
