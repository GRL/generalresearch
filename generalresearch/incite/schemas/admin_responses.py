from datetime import datetime

from pandera import (
    Check,
    Column,
    DataFrameSchema,
    Index,
    MultiIndex,
    Parser,
    Timestamp,
)

BIG_INT32 = 2_147_483_647
SIX_HOUR_SECONDS = 6 * 60 * 6
ROUNDING = 2

AdminPOPSchema = DataFrameSchema(
    # Generic: used for Session or Wall
    index=MultiIndex(
        indexes=[
            # It seems to be impossible to create a list of optional names,
            #   and given that we allow the index1 to be different depending
            #   on the split_by, let's just use generic names for now. However,
            #   we know the first is also an iso string for now (19 chars)
            Index(
                name="index0",
                dtype=Timestamp,
                parsers=[Parser(lambda i: i.dt.tz_localize(None))],
                checks=[
                    Check.less_than(
                        max_value=datetime(year=datetime.now().year + 1, month=1, day=1)
                    )
                ],
            ),
            Index(
                name="index1",
                dtype=str,
                checks=[Check.str_length(max_value=255)],
            ),
        ],
        coerce=True,
    ),
    columns={
        "elapsed_avg": Column(
            dtype=float,
            parsers=[
                Parser(lambda s: s.fillna(value=0.00)),
                Parser(lambda s: s.clip(lower=0, upper=SIX_HOUR_SECONDS)),
                Parser(lambda s: s.round(decimals=ROUNDING)),
            ],
            checks=Check.between(min_value=0, max_value=SIX_HOUR_SECONDS),
        ),
        "elapsed_total": Column(
            dtype=int,
            parsers=[
                Parser(lambda s: s.fillna(value=0)),
            ],
            checks=Check.between(min_value=0, max_value=BIG_INT32),
        ),
        "payout_avg": Column(
            dtype=float,
            parsers=[
                Parser(lambda s: s.fillna(value=0.00)),
                Parser(lambda s: s.round(decimals=ROUNDING)),
            ],
            checks=Check.between(min_value=0, max_value=100),
        ),
        "payout_total": Column(
            dtype=float,
            parsers=[
                Parser(lambda s: s.fillna(value=0.00)),
                Parser(lambda s: s.round(decimals=ROUNDING)),
            ],
            checks=Check.between(min_value=0, max_value=BIG_INT32),
        ),
        "entrances": Column(
            dtype=int,
            parsers=[
                Parser(lambda s: s.fillna(value=0)),
            ],
            checks=Check.between(min_value=0, max_value=BIG_INT32),
        ),
        "completes": Column(
            dtype=int,
            parsers=[
                Parser(lambda s: s.fillna(value=0)),
            ],
            checks=Check.between(min_value=0, max_value=BIG_INT32),
        ),
        "users": Column(
            dtype=int,
            parsers=[
                Parser(lambda s: s.fillna(value=0)),
            ],
            checks=Check.between(min_value=0, max_value=BIG_INT32),
        ),
        "conversion": Column(
            dtype=float,
            parsers=[
                Parser(lambda s: s.fillna(value=0.00)),
                Parser(lambda s: s.round(decimals=ROUNDING)),
            ],
            checks=Check.between(min_value=0.00, max_value=1.00),
        ),
        "epc": Column(
            dtype=float,
            parsers=[
                Parser(lambda s: s.fillna(value=0.00)),
                Parser(lambda s: s.round(decimals=ROUNDING)),
            ],
            checks=Check.between(min_value=0, max_value=100),
        ),
        "eph": Column(
            dtype=float,
            parsers=[
                Parser(lambda s: s.fillna(value=0.00)),
                Parser(lambda s: s.round(decimals=ROUNDING)),
            ],
            checks=Check.between(min_value=0, max_value=BIG_INT32),
        ),
        "cpc": Column(
            dtype=float,
            parsers=[
                Parser(lambda s: s.fillna(value=0.00)),
                Parser(lambda s: s.round(decimals=ROUNDING)),
            ],
            checks=Check.between(min_value=0, max_value=250),
        ),
    },
    coerce=True,
)

admin_pop_index = AdminPOPSchema.index
admin_pop_columns = AdminPOPSchema.columns.copy()

AdminPOPWallSchema = DataFrameSchema(
    index=admin_pop_index,
    columns=admin_pop_columns
    | {
        "buyers": Column(
            dtype=int,
            parsers=[
                Parser(lambda s: s.fillna(value=0)),
            ],
            checks=Check.between(min_value=0, max_value=BIG_INT32),
        ),
        "surveys": Column(
            dtype=int,
            parsers=[
                Parser(lambda s: s.fillna(value=0)),
            ],
            checks=Check.between(min_value=0, max_value=BIG_INT32),
        ),
        "sessions": Column(
            dtype=int,
            parsers=[
                Parser(lambda s: s.fillna(value=0)),
            ],
            checks=Check.between(min_value=0, max_value=BIG_INT32),
        ),
    },
    coerce=True,
)

AdminPOPSessionSchema = DataFrameSchema(
    index=admin_pop_index,
    columns=admin_pop_columns
    | {
        "attempts_avg": Column(
            dtype=float,
            parsers=[
                Parser(lambda s: s.fillna(value=0.00)),
                Parser(lambda s: s.round(decimals=ROUNDING)),
            ],
            checks=Check.between(min_value=0, max_value=25),
        ),
        "attempts_total": Column(
            dtype=int,
            parsers=[
                Parser(lambda s: s.fillna(value=0)),
            ],
            checks=Check.between(min_value=0, max_value=BIG_INT32),
        ),
    },
    coerce=True,
)
