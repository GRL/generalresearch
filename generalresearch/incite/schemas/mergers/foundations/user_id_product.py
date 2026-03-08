from datetime import timedelta

from pandera import Category, Check, Column, DataFrameSchema, Index

from generalresearch.incite.schemas import ARCHIVE_AFTER

BIGINT = 9223372036854775807

UserIdIndex = Index(
    name="id",
    dtype=int,
    checks=Check.between(min_value=0, max_value=BIGINT),
    unique=True,
)

"""
Simply stores a mapping between user ID and product ID. product_id is a category
which is much smaller."""
UserIdProductSchema = DataFrameSchema(
    index=UserIdIndex,
    columns={
        "product_id": Column(dtype=Category, nullable=False),
    },
    checks=[],
    coerce=False,
    metadata={ARCHIVE_AFTER: timedelta(minutes=0)},
)
