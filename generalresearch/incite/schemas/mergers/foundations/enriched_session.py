from datetime import timedelta

from pandera import DataFrameSchema, Column, Check

from generalresearch.incite.schemas import ARCHIVE_AFTER, ORDER_KEY, PARTITION_ON
from generalresearch.incite.schemas.thl_web import THLSessionSchema

thl_session_columns = THLSessionSchema.columns.copy()

EnrichedSessionSchema = DataFrameSchema(
    index=THLSessionSchema.index,
    columns=thl_session_columns
    | {
        # --- From thl_user MySQL-RR
        "product_id": Column(
            dtype=str,
            checks=Check.str_length(min_value=32, max_value=32),
            nullable=False,
        ),
        # -- nullable until it can be back-filled
        "team_id": Column(
            dtype=str,
            checks=Check.str_length(min_value=32, max_value=32),
            nullable=True,
        ),
        # --- Calculated from WallCollection ---
        "attempt_count": Column(dtype="Int64", nullable=False),
    },
    checks=[],
    coerce=True,
    metadata={
        ORDER_KEY: "started",
        ARCHIVE_AFTER: timedelta(minutes=90),
        PARTITION_ON: None,
    },
)
