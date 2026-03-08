from datetime import timedelta

from pandera import Check, Column, DataFrameSchema, Index

from generalresearch.incite.schemas import ARCHIVE_AFTER, ORDER_KEY
from generalresearch.incite.schemas.thl_web import THLSessionSchema, THLWallSchema

thl_wall_columns = THLWallSchema.columns.copy()

thl_wall_columns = {
    k: v
    for k, v in thl_wall_columns.items()
    if k
    in {
        "source",
        "buyer_id",
        "started",
        "session_id",
        "survey_id",
        "cpi",
        "status",
        "status_code_1",
        "status_code_2",
        "ext_status_code_1",
        "ext_status_code_2",
        "ext_status_code_3",
        "report_value",
    }
}
thl_session_columns = THLSessionSchema.columns.copy()
thl_session_columns = {
    k: v
    for k, v in thl_session_columns.items()
    if k in {"user_id", "country_iso", "device_type"}
}

"""
This is used by YM-survey-predict and train. It is mostly THLWall with:
 - Adjusted columns removed, (YM will get this info from the 
    TaskAdjustment collection)

 - Fields from the session joined in (user_id, country_iso, device_type, 
    session's uuid)

 - Product_id and blocked (from User). Blocked means blocked NOW (latest), 
    not when the session was attempted.
"""

YMSurveyWallSchema = DataFrameSchema(
    # index is the wall's uuid
    index=Index(
        name="uuid", dtype=str, checks=Check.str_length(min_value=32, max_value=32)
    ),
    columns=thl_wall_columns
    | thl_session_columns
    | {
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
        "elapsed": Column(dtype="Int64", nullable=True),
        "in_progress": Column(
            dtype=bool,
            required=False,
            description="This is time-sensitive, so will not be included in archived files. True if"
            "the entrance started less than 90 min ago and has not yet returned.",
        ),
        "pass_ps": Column(
            dtype=bool,
            required=False,
            description="Did this entrance pass the pre-screener and actually enter the client?"
            "Note: we mark abandonments as True."
            "Note: there is no 'in-progress' determination here. A user who 'just' entered"
            "and hasn't come back yet is also marked as True",
        ),
        "quality_fail": Column(
            dtype=bool,
            required=False,
            description="Did the user fail for quality reasons? We generally want to exclude these for"
            "yield-management.",
        ),
        "abandon": Column(
            dtype=bool,
            required=False,
            description="In-progress is not considered. A user who is in-progress and hasn't come back"
            "is still marked as abandon.",
        ),
    },
    checks=[],
    coerce=True,
    strict=True,
    unique=["session_id", "source", "survey_id"],
    metadata={ORDER_KEY: "started", ARCHIVE_AFTER: timedelta(minutes=90)},
)
