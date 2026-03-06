from datetime import timedelta

import pandas as pd
from pandera import DataFrameSchema, Column, Check, Index

from generalresearch.incite.schemas import PARTITION_ON, ARCHIVE_AFTER
from generalresearch.locales import Localelator
from generalresearch.models import DeviceType, Source
from generalresearch.models.thl.definitions import (
    Status,
    StatusCode1,
    ReportValue,
    WallStatusCode2,
)

IP_REGEX_PATTERN = (
    r"^((([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\.){3}([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4]["
    r"0-9]|25[0-5])$|^(([a-fA-F]|[a-fA-F][a-fA-F0-9\-]*[a-fA-F0-9])\.)*([A-Fa-f]|[A-Fa-f]["
    r"A-Fa-f0-9\-]*[A-Fa-f0-9])$|^(?:(?:(?:(?:(?:(?:(?:[0-9a-fA-F]{1,4})):){6})(?:(?:(?:(?:(?:["
    r"0-9a-fA-F]{1,4})):(?:(?:[0-9a-fA-F]{1,4})))|(?:(?:(?:(?:(?:25[0-5]|(?:[1-9]|1[0-9]|2[0-4])?["
    r"0-9]))\.){3}(?:(?:25[0-5]|(?:[1-9]|1[0-9]|2[0-4])?[0-9])))))))|(?:(?:::(?:(?:(?:[0-9a-fA-F]{1,"
    r"4})):){5})(?:(?:(?:(?:(?:[0-9a-fA-F]{1,4})):(?:(?:[0-9a-fA-F]{1,4})))|(?:(?:(?:(?:(?:25[0-5]|("
    r"?:[1-9]|1[0-9]|2[0-4])?[0-9]))\.){3}(?:(?:25[0-5]|(?:[1-9]|1[0-9]|2[0-4])?[0-9])))))))|(?:(?:("
    r"?:(?:(?:[0-9a-fA-F]{1,4})))?::(?:(?:(?:[0-9a-fA-F]{1,4})):){4})(?:(?:(?:(?:(?:[0-9a-fA-F]{1,"
    r"4})):(?:(?:[0-9a-fA-F]{1,4})))|(?:(?:(?:(?:(?:25[0-5]|(?:[1-9]|1[0-9]|2[0-4])?[0-9]))\.){3}(?:("
    r"?:25[0-5]|(?:[1-9]|1[0-9]|2[0-4])?[0-9])))))))|(?:(?:(?:(?:(?:(?:[0-9a-fA-F]{1,4})):){0,"
    r"1}(?:(?:[0-9a-fA-F]{1,4})))?::(?:(?:(?:[0-9a-fA-F]{1,4})):){3})(?:(?:(?:(?:(?:[0-9a-fA-F]{1,"
    r"4})):(?:(?:[0-9a-fA-F]{1,4})))|(?:(?:(?:(?:(?:25[0-5]|(?:[1-9]|1[0-9]|2[0-4])?[0-9]))\.){3}(?:("
    r"?:25[0-5]|(?:[1-9]|1[0-9]|2[0-4])?[0-9])))))))|(?:(?:(?:(?:(?:(?:[0-9a-fA-F]{1,4})):){0,"
    r"2}(?:(?:[0-9a-fA-F]{1,4})))?::(?:(?:(?:[0-9a-fA-F]{1,4})):){2})(?:(?:(?:(?:(?:[0-9a-fA-F]{1,"
    r"4})):(?:(?:[0-9a-fA-F]{1,4})))|(?:(?:(?:(?:(?:25[0-5]|(?:[1-9]|1[0-9]|2[0-4])?[0-9]))\.){3}(?:("
    r"?:25[0-5]|(?:[1-9]|1[0-9]|2[0-4])?[0-9])))))))|(?:(?:(?:(?:(?:(?:[0-9a-fA-F]{1,4})):){0,"
    r"3}(?:(?:[0-9a-fA-F]{1,4})))?::(?:(?:[0-9a-fA-F]{1,4})):)(?:(?:(?:(?:(?:[0-9a-fA-F]{1,"
    r"4})):(?:(?:[0-9a-fA-F]{1,4})))|(?:(?:(?:(?:(?:25[0-5]|(?:[1-9]|1[0-9]|2[0-4])?[0-9]))\.){3}(?:("
    r"?:25[0-5]|(?:[1-9]|1[0-9]|2[0-4])?[0-9])))))))|(?:(?:(?:(?:(?:(?:[0-9a-fA-F]{1,4})):){0,"
    r"4}(?:(?:[0-9a-fA-F]{1,4})))?::)(?:(?:(?:(?:(?:[0-9a-fA-F]{1,4})):(?:(?:[0-9a-fA-F]{1,"
    r"4})))|(?:(?:(?:(?:(?:25[0-5]|(?:[1-9]|1[0-9]|2[0-4])?[0-9]))\.){3}(?:(?:25[0-5]|(?:[1-9]|1["
    r"0-9]|2[0-4])?[0-9])))))))|(?:(?:(?:(?:(?:(?:[0-9a-fA-F]{1,4})):){0,5}(?:(?:[0-9a-fA-F]{1,"
    r"4})))?::)(?:(?:[0-9a-fA-F]{1,4})))|(?:(?:(?:(?:(?:(?:[0-9a-fA-F]{1,4})):){0,"
    r"6}(?:(?:[0-9a-fA-F]{1,4})))?::)))))$"
)
BIGINT = 9223372036854775807

COUNTRY_ISOS = Localelator().get_all_countries()
kosovo = "xk"
COUNTRY_ISOS.add(kosovo)

EnrichedWallSchema = DataFrameSchema(
    index=Index(
        name="uuid",  # this is the wall event's uuid
        dtype=str,
        checks=Check.str_length(min_value=32, max_value=32),
    ),
    columns={
        # --- Wall based ---
        "source": Column(
            dtype=str,
            checks=[
                Check.str_length(max_value=2),
                Check.isin([e.value for e in Source]),
            ],
            nullable=False,
        ),
        "buyer_id": Column(
            dtype=str, checks=Check.str_length(max_value=32), nullable=True
        ),
        "survey_id": Column(
            dtype=str, checks=[Check.str_length(max_value=32)], nullable=False
        ),
        "started": Column(dtype=pd.DatetimeTZDtype(tz="UTC"), nullable=False),
        "finished": Column(dtype=pd.DatetimeTZDtype(tz="UTC"), nullable=True),
        "status": Column(
            dtype=str,
            checks=[
                Check.str_length(min_value=1, max_value=1),
                Check.isin([e.value for e in Status]),
            ],
            nullable=True,
        ),
        "status_code_1": Column(
            dtype="Int32",
            checks=[Check.isin([e.value for e in StatusCode1])],
            nullable=True,
        ),
        "status_code_2": Column(
            dtype="Int32",
            checks=[Check.isin([e.value for e in WallStatusCode2])],
            nullable=True,
        ),
        "cpi": Column(
            dtype=float,
            checks=Check.between(min_value=0, max_value=1_000),
            nullable=False,
        ),
        "ext_status_code_1": Column(
            dtype=str, checks=Check.str_length(max_value=32), nullable=True
        ),
        "ext_status_code_2": Column(
            dtype=str, checks=Check.str_length(max_value=32), nullable=True
        ),
        "ext_status_code_3": Column(
            dtype=str, checks=Check.str_length(max_value=32), nullable=True
        ),
        "report_value": Column(
            dtype="Int64",
            checks=Check.isin([e.value for e in ReportValue]),
            nullable=True,
        ),
        # --- Session based ---
        "session_id": Column(dtype=int, checks=Check.greater_than(0), nullable=False),
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
        "payout": Column(
            dtype=float,
            checks=Check.between(min_value=0, max_value=1_000),
            nullable=True,
        ),
        # --- User based ---
        "user_id": Column(
            dtype="Int32",
            checks=Check.between(min_value=0, max_value=BIGINT),
            nullable=False,
        ),
        "product_id": Column(
            dtype=str,
            checks=Check.str_length(min_value=32, max_value=32),
            nullable=False,
        ),
    },
    checks=[],
    coerce=True,
    metadata={PARTITION_ON: None, ARCHIVE_AFTER: timedelta(minutes=90)},
)
