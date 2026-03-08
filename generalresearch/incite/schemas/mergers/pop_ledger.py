from datetime import timedelta

import pandas as pd
from more_itertools import flatten
from pandera import Check, Column, DataFrameSchema, Index

from generalresearch.incite.schemas import ARCHIVE_AFTER, ORDER_KEY, PARTITION_ON
from generalresearch.incite.schemas.thl_web import TxSchema
from generalresearch.models.thl.ledger import Direction, TransactionType

"""
- In reality, a multi-index would be appropriate here, but dask does not support this, so we're keeping it flat.
    As such, the index in this schema is simply an autoindex and has no meaning.

- The "virtual" index (conceptually) is (time, account_id), and the columns are all combinations of 
    '{TransactionType}.{Direction}'.

- We want both credit + debit amounts so we know, for e.g., an account got $+10 of positive recons 
    and $-20 of negative recons.
"""

# If an amount is "very" large, something is def wrong. Defining "very" somewhat arbitrarily here.
SUSPICIOUSLY_LARGE_NUMBER = (2**32 / 2) - 1  # 2147483647

NonNegativeAmount = Column(
    dtype="Int32",
    nullable=True,
    checks=Check.between(
        min_value=0, max_value=SUSPICIOUSLY_LARGE_NUMBER, include_min=True
    ),
)

numerical_col_names = list(
    flatten(
        [
            [
                e.value + "." + Direction.CREDIT.name,
                e.value + "." + Direction.DEBIT.name,
            ]
            for e in TransactionType
        ]
    )
)
numerical_cols = {k: NonNegativeAmount for k in numerical_col_names}

PopLedgerSchema = DataFrameSchema(
    index=Index(name="id", dtype=int, checks=Check.greater_than_or_equal_to(0)),
    columns=numerical_cols
    | {
        "time_idx": Column(
            dtype=pd.DatetimeTZDtype(tz="UTC"),
            checks=Check(lambda x: (x.dt.second == 0) & (x.dt.microsecond == 0)),
            nullable=False,
        ),
        "account_id": TxSchema.columns["account_id"],
    },
    checks=[],
    coerce=True,
    metadata={
        ORDER_KEY: None,
        ARCHIVE_AFTER: timedelta(minutes=90),
        PARTITION_ON: None,
    },
)
