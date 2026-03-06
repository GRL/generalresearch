from datetime import timezone, datetime, timedelta

import pandas as pd
from pandera import DataFrameSchema, Column, Check, Index, MultiIndex

from generalresearch.incite.schemas import ORDER_KEY, ARCHIVE_AFTER
from generalresearch.locales import Localelator
from generalresearch.models import DeviceType, Source
from generalresearch.models.thl.definitions import (
    StatusCode1,
    WallStatusCode2,
    ReportValue,
    WallAdjustedStatus,
    Status,
    SessionStatusCode2,
    SessionAdjustedStatus,
)
from generalresearch.models.thl.ledger import TransactionMetadataColumns
from generalresearch.models.thl.maxmind.definitions import UserType

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

THLUserSchema = DataFrameSchema(
    index=Index(
        name="id", dtype=int, checks=Check.between(min_value=0, max_value=BIGINT)
    ),
    columns={
        "uuid": Column(
            dtype=str,
            checks=Check.str_length(min_value=32, max_value=32),
            nullable=False,
        ),
        "product_id": Column(
            dtype=str,
            checks=Check.str_length(min_value=32, max_value=32),
            nullable=False,
        ),
        "product_user_id": Column(
            dtype=str,
            checks=Check.str_length(min_value=3, max_value=128),
            nullable=False,
        ),
        "created": Column(dtype=pd.DatetimeTZDtype(tz="UTC"), nullable=True),
    },
    checks=[],
    coerce=True,
    # This may be an issue with how we handle updates... and reading from
    # last_seen as multiple of the same user could be in a dataframe, and we
    # only want the latest record.
    # unique=["product_id", "product_user_id"],
    metadata={
        ORDER_KEY: "created",
        ARCHIVE_AFTER: timedelta(minutes=1),
    },
)

THLWallSchema = DataFrameSchema(
    index=Index(
        name="uuid", dtype=str, checks=Check.str_length(min_value=32, max_value=32)
    ),
    columns={
        "source": Column(
            dtype=str,
            checks=[
                Check.str_length(max_value=2),
                Check.isin([e.value for e in Source]),
            ],
        ),
        "buyer_id": Column(
            dtype=str, checks=Check.str_length(max_value=32), nullable=True
        ),
        "req_survey_id": Column(dtype=str, checks=Check.str_length(max_value=32)),
        "req_cpi": Column(
            dtype=float, checks=Check.between(min_value=0, max_value=1_000)
        ),
        "started": Column(
            dtype=pd.DatetimeTZDtype(tz="UTC"),
            checks=[Check(lambda x: x < datetime.now(tz=timezone.utc))],
            nullable=False,
        ),
        "session_id": Column(
            dtype="Int32", checks=Check.between(min_value=0, max_value=BIGINT)
        ),
        "survey_id": Column(
            dtype=str, checks=Check.str_length(max_value=32), nullable=True
        ),
        "cpi": Column(
            dtype=float,
            checks=Check.between(min_value=0, max_value=1_000),
            nullable=True,
        ),
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
            dtype="Int64",
            checks=Check.isin([e.value for e in StatusCode1]),
            nullable=True,
        ),
        "status_code_2": Column(
            dtype="Int64",
            checks=Check.isin([e.value for e in WallStatusCode2]),
            nullable=True,
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
        "report_notes": Column(
            dtype=str, checks=Check.str_length(max_value=255), nullable=True
        ),
        "adjusted_status": Column(
            dtype=str,
            checks=[
                Check.str_length(min_value=2, max_value=2),
                Check.isin([e.value for e in WallAdjustedStatus]),
            ],
            nullable=True,
        ),
        "adjusted_cpi": Column(
            dtype=float,
            checks=Check.between(min_value=0, max_value=1_000),
            nullable=True,
        ),
        "adjusted_timestamp": Column(dtype=pd.DatetimeTZDtype(tz="UTC"), nullable=True),
    },
    checks=[
        # Lets require more than a few Sources
        # Check(check_fn=lambda df: df.source.unique().size > 3,
        #       error="Issue with the distribution of Sources")
        # Check(check_fn=lambda df: df['started'] <= df['finished'],
        #       element_wise=True,
        #       ignore_na=True,
        #       error='"Finished" must be greater than "started"'),
        # If adjusted, ensure all adjusted_* fields are set
        # If status !=e, sure finished is set
    ],
    coerce=True,
    unique=["session_id", "source", "survey_id"],
    metadata={
        ORDER_KEY: "started",
        ARCHIVE_AFTER: timedelta(minutes=90),
    },
)

THLSessionSchema = DataFrameSchema(
    index=Index(name="id", dtype=int, checks=Check.greater_than(0)),
    columns={
        "uuid": Column(
            dtype=str,
            checks=Check.str_length(min_value=32, max_value=32),
            nullable=False,
            unique=True,
        ),
        "user_id": Column(
            dtype="Int32",
            checks=Check.between(min_value=0, max_value=BIGINT),
            nullable=False,
        ),
        "started": Column(
            dtype=pd.DatetimeTZDtype(tz="UTC"),
            checks=[Check(lambda x: x < datetime.now(tz=timezone.utc))],
            nullable=True,
        ),
        "finished": Column(
            dtype=pd.DatetimeTZDtype(tz="UTC"),
            checks=[Check(lambda x: x < datetime.now(tz=timezone.utc))],
            nullable=True,
        ),
        "loi_min": Column(dtype="Int64", nullable=True),
        "loi_max": Column(dtype="Int64", nullable=True),
        "user_payout_min": Column(dtype=float, nullable=True),
        "user_payout_max": Column(dtype=float, nullable=True),
        "country_iso": Column(
            dtype=str,
            checks=[
                Check.str_length(min_value=1, max_value=2),
                Check.isin(COUNTRY_ISOS),  # 2 letter, lowercase
            ],
            nullable=True,
        ),
        "device_type": Column(
            dtype="Int64",
            checks=Check.isin([e.value for e in DeviceType]),
            nullable=True,
        ),
        "ip": Column(
            dtype=str,
            checks=[
                Check.str_length(min_value=7),
                Check.str_matches(pattern=IP_REGEX_PATTERN),
            ],
            nullable=True,
        ),
        "status": Column(
            dtype=str,
            checks=[
                Check.str_length(min_value=1, max_value=1),
                Check.isin([e.value for e in Status]),
            ],
            nullable=True,
        ),
        "status_code_1": Column(
            dtype="Int64",
            checks=Check.isin([e.value for e in StatusCode1]),
            nullable=True,
        ),
        "status_code_2": Column(
            dtype="Int64",
            checks=Check.isin([e.value for e in SessionStatusCode2]),
            nullable=True,
        ),
        "payout": Column(
            dtype=float,
            checks=Check.between(min_value=0, max_value=1_000),
            nullable=True,
        ),
        "user_payout": Column(
            dtype=float,
            checks=Check.between(min_value=0, max_value=1_000),
            nullable=True,
        ),
        "adjusted_status": Column(
            dtype=str,
            checks=[
                Check.str_length(min_value=2, max_value=2),
                Check.isin([e.value for e in SessionAdjustedStatus]),
            ],
            nullable=True,
        ),
        "adjusted_payout": Column(
            dtype=float,
            checks=Check.between(min_value=0, max_value=1_000),
            nullable=True,
        ),
        "adjusted_user_payout": Column(
            dtype=float,
            checks=Check.between(min_value=0, max_value=1_000),
            nullable=True,
        ),
        "adjusted_timestamp": Column(dtype=pd.DatetimeTZDtype(tz="UTC"), nullable=True),
        "url_metadata": Column(dtype=str, nullable=True),
    },
    checks=[
        # Check(lambda df: df['started'] <= df['finished'],
        #       element_wise=True,
        #       ignore_na=True,
        #       error='"Finished" should be greater than "started"'),
        # Check(check_fn=lambda df: df.source.unique().size > 3,
        #       error="Issue with the distribution of Sources")
    ],
    coerce=True,
    metadata={ORDER_KEY: "started", ARCHIVE_AFTER: timedelta(minutes=90)},
)

THLIPInfoSchema = DataFrameSchema(
    index=Index(
        name="ip",
        dtype=str,
        checks=[
            Check.str_length(min_value=7),
            Check.str_matches(pattern=IP_REGEX_PATTERN),
        ],
    ),
    columns={
        "geoname_id": Column(
            dtype=str,
            checks=[
                Check.str_length(min_value=5, max_value=8),
            ],
        ),
        "country_iso": Column(
            dtype=str,
            checks=[
                Check.str_length(min_value=2, max_value=2),
            ],
            nullable=False,
        ),
        "registered_country_iso": Column(
            dtype=str,
            checks=[
                Check.str_length(min_value=2, max_value=2),
            ],
            nullable=True,
        ),
        "is_anonymous": Column(dtype=bool),
        "is_anonymous_vpn": Column(dtype=bool),
        "is_hosting_provider": Column(dtype=bool),
        "is_public_proxy": Column(dtype=bool),
        "is_tor_exit_node": Column(dtype=bool),
        "is_residential_proxy": Column(dtype=bool),
        "autonomous_system_number": Column(
            dtype="Int64",
            checks=[
                Check.greater_than(min_value=0),
            ],
            nullable=True,
        ),
        "autonomous_system_organization": Column(
            dtype=str,
            checks=[
                Check.str_length(min_value=2, max_value=255),
            ],
            nullable=True,
        ),
        "domain": Column(
            dtype=str,
            checks=[
                Check.str_length(min_value=3, max_value=255),
            ],
            nullable=True,
        ),
        "isp": Column(
            dtype=str,
            checks=[
                Check.str_length(min_value=2, max_value=255),
            ],
            nullable=True,
        ),
        # Don't know what this is..
        "mobile_country_code": Column(dtype=str, nullable=True),
        # Don't know what this is..
        "mobile_network_code": Column(dtype=str, nullable=True),
        "network": Column(
            dtype=str,
            checks=[
                Check.str_length(min_value=7, max_value=255),
            ],
            nullable=True,
        ),
        "organization": Column(
            dtype=str,
            checks=[
                Check.str_length(min_value=2, max_value=255),
            ],
            nullable=True,
        ),
        "static_ip_score": Column(
            dtype=float,
            checks=[
                Check.greater_than(min_value=0),
            ],
            nullable=True,
        ),
        "user_type": Column(
            dtype=str,
            checks=[
                Check.str_length(min_value=3, max_value=255),
                Check.isin([e.value for e in UserType]),
            ],
            nullable=True,
        ),
        "postal_code": Column(
            dtype=str,
            checks=[
                Check.str_length(min_value=2, max_value=9),
            ],
            nullable=True,
        ),
        "latitude": Column(dtype=float, nullable=True),
        "longitude": Column(dtype=float, nullable=True),
        "accuracy_radius": Column(
            dtype="Int64",
            checks=[
                # Checked on 2024-02-24 Max
                Check.between(min_value=0, max_value=1_000),
            ],
            nullable=True,
        ),
        "updated": Column(dtype=pd.DatetimeTZDtype(tz="UTC")),
    },
    checks=[],
    coerce=True,
    metadata={
        ORDER_KEY: "updated",
        ARCHIVE_AFTER: timedelta(minutes=1),
    },
)

THLTaskAdjustmentSchema = DataFrameSchema(
    index=Index(
        name="uuid", dtype=str, checks=Check.str_length(min_value=32, max_value=32)
    ),
    columns={
        "adjusted_status": Column(
            dtype=str,
            checks=[
                Check.str_length(min_value=2, max_value=2),
                Check.isin([e.value for e in WallAdjustedStatus]),
            ],
        ),
        "ext_status_code": Column(dtype=str, checks=[], nullable=True),
        "amount": Column(dtype=float),
        "alerted": Column(dtype=pd.DatetimeTZDtype(tz="UTC")),
        "created": Column(dtype=pd.DatetimeTZDtype(tz="UTC")),
        "user_id": Column(
            dtype="Int32", checks=Check.between(min_value=0, max_value=BIGINT)
        ),
        "wall_uuid": Column(
            dtype=str,
            checks=[
                Check.str_length(min_value=32, max_value=32),
            ],
        ),
        "started": Column(
            dtype=pd.DatetimeTZDtype(tz="UTC"),
            checks=[Check(lambda x: x < datetime.now(tz=timezone.utc))],
        ),
        "source": Column(
            dtype=str,
            checks=[
                Check.str_length(max_value=2),
                Check.isin([e.value for e in Source]),
            ],
        ),
        "survey_id": Column(
            dtype=str,
            checks=Check.str_length(max_value=32),
        ),
    },
    checks=[
        # started < created
    ],
    coerce=True,
    metadata={
        ORDER_KEY: "created",
        ARCHIVE_AFTER: timedelta(minutes=1),
    },
)

UserHealthAuditLogSchema = DataFrameSchema(
    index=Index(
        name="id",
        dtype=int,
        checks=[
            Check.between(min_value=1, max_value=BIGINT),
        ],
    ),
    columns={
        "user_id": Column(
            dtype="Int32",
            checks=[
                Check.between(min_value=1, max_value=BIGINT),
            ],
            nullable=False,
        ),
        "created": Column(dtype=pd.DatetimeTZDtype(tz="UTC"), nullable=False),
        "level": Column(
            dtype="Int32",
            checks=[
                Check.between(min_value=0, max_value=32767),
            ],
            nullable=False,
        ),
        "event_type": Column(
            dtype=str,
            checks=[
                Check.str_length(min_value=1, max_value=64),
            ],
            nullable=False,
        ),
        "event_msg": Column(
            dtype=str,
            checks=[
                Check.str_length(min_value=1, max_value=256),
            ],
            nullable=True,
        ),
        "event_value": Column(dtype=float, nullable=True),
    },
    checks=[],
    coerce=True,
    metadata={
        ORDER_KEY: "created",
        ARCHIVE_AFTER: timedelta(minutes=1),
    },
)

UserHealthIPHistorySchema = DataFrameSchema(
    index=Index(
        name="id",
        dtype=int,
        checks=[
            Check.between(min_value=1, max_value=BIGINT),
        ],
    ),
    columns={
        "user_id": Column(
            dtype="Int32",
            checks=[
                Check.between(min_value=1, max_value=BIGINT),
            ],
            nullable=False,
        ),
        "ip": Column(
            dtype=str,
            checks=[
                Check.str_length(min_value=7),
                Check.str_matches(pattern=IP_REGEX_PATTERN),
            ],
            nullable=False,
        ),
        "created": Column(dtype=pd.DatetimeTZDtype(tz="UTC"), nullable=False),
        "forwarded_ip1": Column(
            dtype=str,
            checks=[
                Check.str_length(min_value=7),
                Check.str_matches(pattern=IP_REGEX_PATTERN),
            ],
            nullable=True,
        ),
        "forwarded_ip2": Column(
            dtype=str,
            checks=[
                Check.str_length(min_value=7),
                Check.str_matches(pattern=IP_REGEX_PATTERN),
            ],
            nullable=True,
        ),
        "forwarded_ip3": Column(
            dtype=str,
            checks=[
                Check.str_length(min_value=7),
                Check.str_matches(pattern=IP_REGEX_PATTERN),
            ],
            nullable=True,
        ),
        "forwarded_ip4": Column(
            dtype=str,
            checks=[
                Check.str_length(min_value=7),
                Check.str_matches(pattern=IP_REGEX_PATTERN),
            ],
            nullable=True,
        ),
        "forwarded_ip5": Column(
            dtype=str,
            checks=[
                Check.str_length(min_value=7),
                Check.str_matches(pattern=IP_REGEX_PATTERN),
            ],
            nullable=True,
        ),
        "forwarded_ip6": Column(
            dtype=str,
            checks=[
                Check.str_length(min_value=7),
                Check.str_matches(pattern=IP_REGEX_PATTERN),
            ],
            nullable=True,
        ),
    },
    checks=[],
    coerce=True,
    metadata={ORDER_KEY: "created", ARCHIVE_AFTER: timedelta(minutes=1)},
)

UserHealthIPHistoryWSSchema = DataFrameSchema(
    index=Index(
        name="id",
        dtype=int,
        checks=[
            Check.between(min_value=1, max_value=BIGINT),
        ],
    ),
    columns={
        "user_id": Column(
            dtype="Int32",
            checks=[
                Check.between(min_value=1, max_value=BIGINT),
            ],
            nullable=False,
        ),
        "ip": Column(
            dtype=str,
            checks=[
                Check.str_length(min_value=7),
                Check.str_matches(pattern=IP_REGEX_PATTERN),
            ],
            nullable=False,
        ),
        "created": Column(dtype=pd.DatetimeTZDtype(tz="UTC"), nullable=False),
        "last_seen": Column(dtype=pd.DatetimeTZDtype(tz="UTC"), nullable=False),
    },
    checks=[],
    coerce=True,
    metadata={ORDER_KEY: "last_seen", ARCHIVE_AFTER: timedelta(minutes=1)},
)

TxSchema = DataFrameSchema(
    index=Index(
        name="entry_id",
        dtype=int,
        checks=[
            Check.between(min_value=1, max_value=BIGINT),
        ],
    ),
    columns={
        # -----------------
        # ledger_transaction
        # -----------------
        "tx_id": Column(
            dtype=int,
            checks=[
                Check.between(min_value=1, max_value=BIGINT),
            ],
        ),
        "created": Column(dtype=pd.DatetimeTZDtype(tz="UTC"), nullable=False),
        "ext_description": Column(
            dtype=str,
            checks=[Check.str_length(min_value=1, max_value=255)],
            nullable=True,
        ),
        "tag": Column(
            dtype=str,
            checks=[Check.str_length(min_value=1, max_value=255)],
            nullable=True,
        ),
        # -----------------
        # ledger_entry
        # -----------------
        "direction": Column(
            dtype="Int32",
            checks=[
                Check.isin([-1, 1]),
            ],
            nullable=False,
        ),
        "amount": Column(
            dtype="Int32",
            checks=[Check.between(min_value=1, max_value=BIGINT)],
            nullable=False,
        ),
        "account_id": Column(
            dtype=str,
            checks=[
                Check.str_length(min_value=32, max_value=32),
            ],
            nullable=False,
        ),
        # -----------------
        # ledger_account
        # -----------------
        "display_name": Column(
            dtype=str,
            checks=[
                Check.str_length(min_value=1, max_value=64),
            ],
            nullable=False,
        ),
        "qualified_name": Column(
            dtype=str,
            checks=[
                Check.str_length(min_value=1, max_value=255),
            ],
            nullable=False,
            # I don't think this can be unique in Pandera bc of the MultiIndex makes
            # it show up twice...
            unique=False,
        ),
        "account_type": Column(
            dtype=str,
            checks=[
                Check.str_length(min_value=1, max_value=30),
            ],
            nullable=True,
        ),
        "normal_balance": Column(
            dtype="Int32",
            checks=[
                Check.isin([-1, 1]),
            ],
            nullable=False,
        ),
        "reference_type": Column(
            dtype=str,
            checks=[
                Check.str_length(min_value=1, max_value=30),
            ],
            nullable=True,
        ),
        "reference_uuid": Column(
            dtype=str,
            checks=[
                Check.str_length(min_value=1, max_value=32),
            ],
            nullable=True,
        ),
        "currency": Column(
            dtype=str,
            checks=[
                Check.str_length(min_value=1, max_value=32),
            ],
            nullable=False,
        ),
    },
    checks=[],
    coerce=True,
    metadata={ORDER_KEY: "created", ARCHIVE_AFTER: timedelta(minutes=1)},
)

TxMetaSchema = DataFrameSchema(
    index=MultiIndex(
        indexes=[
            Index(
                name="tx_id",
                dtype=int,
                checks=[
                    Check.between(min_value=1, max_value=BIGINT),
                ],
            ),
            Index(
                name="tx_metadata_id",
                dtype=int,
                checks=[
                    Check.between(min_value=1, max_value=BIGINT),
                ],
            ),
        ]
    ),
    columns={
        "key": Column(
            dtype=str,
            checks=[
                Check.str_length(min_value=1, max_value=30),
                Check.isin([e.value for e in TransactionMetadataColumns]),
            ],
            nullable=False,
        ),
        "value": Column(
            dtype=str,
            checks=[Check.str_length(min_value=1, max_value=255)],
            nullable=False,
        ),
    },
    checks=[],
    coerce=True,
    metadata={ARCHIVE_AFTER: timedelta(minutes=1)},
)

meta_obj = {}
for e in TransactionMetadataColumns:
    meta_obj[e.value] = Column(
        dtype=str, checks=[Check.str_length(min_value=1, max_value=255)], nullable=True
    )

# The weird hybrid DF that actually gets saved out
LedgerSchema = DataFrameSchema(
    index=TxSchema.index,
    columns=TxSchema.columns | meta_obj,
    checks=[],
    coerce=True,
    metadata={
        ARCHIVE_AFTER: timedelta(minutes=1),
        ORDER_KEY: "created",
    },
)
