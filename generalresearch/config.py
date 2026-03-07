from datetime import datetime, timezone
from typing import Optional
from pathlib import Path

from pydantic import RedisDsn, Field, MariaDBDsn, DirectoryPath, PostgresDsn
from pydantic_settings import BaseSettings

from generalresearch.models.custom_types import DaskDsn, SentryDsn, MySQLOrMariaDsn


def is_debug() -> bool:
    import os

    is_developer: bool = os.getenv("USER") in {"nanis", "gstupp"}
    is_pytest1: bool = bool(os.getenv("PYTEST_TEST", False))
    is_pytest2: bool = bool(os.getenv("PYTEST_CURRENT_TEST", False))
    is_pytest3: bool = bool(os.getenv("PYTEST_VERSION", False))
    is_debugging1: bool = os.getenv("DEBUG", "").lower() in ("1", "true", "yes")
    is_debugging2: bool = os.getenv("PYTHON_DEBUG", "").lower() in ("1", "true", "yes")
    is_jenkins: bool = bool(os.getenv("JENKINS_HOME")) or bool(os.getenv("JENKINS_URL"))
    is_vscode: bool = (
        os.getenv("DEBUGPY_RUNNING") == "true" or os.getenv("TERM_PROGRAM") == "vscode"
    )

    return (
        is_developer
        or is_pytest1
        or is_pytest2
        or is_pytest3
        or is_debugging1
        or is_debugging2
        or is_jenkins
        or is_vscode
    )


class GRLBaseSettings(BaseSettings):
    debug: bool = Field(default=True)

    redis: Optional[RedisDsn] = Field(default=None)
    redis_timeout: float = Field(default=0.10)

    thl_redis: Optional[RedisDsn] = Field(default=None)

    dask: Optional[DaskDsn] = Field(default=None, description="")

    sentry: Optional[SentryDsn] = Field(
        default=None, description="The sentry.io DSN for connecting to a project"
    )

    thl_mkpl_rw_db: Optional[MariaDBDsn] = Field(default=None)
    thl_mkpl_rr_db: Optional[MariaDBDsn] = Field(default=None)

    # Primary DB, SELECT permissions
    thl_web_ro_db: Optional[PostgresDsn] = Field(default=None)
    # Primary DB, SELECT, INSERT, UPDATE permissions
    thl_web_rw_db: Optional[PostgresDsn] = Field(default=None)
    # Primary DB, SELECT, INSERT, UPDATE, DELETE permissions
    thl_web_rwd_db: Optional[PostgresDsn] = Field(default=None)
    # Slave/secondary/read-replica SELECT permission only
    thl_web_rr_db: Optional[PostgresDsn] = Field(default=None)

    tmp_dir: DirectoryPath = Field(default=Path("/tmp"))

    spectrum_rw_db: Optional[MariaDBDsn] = Field(default=None)
    spectrum_rr_db: Optional[MariaDBDsn] = Field(default=None)

    precision_rw_db: Optional[MariaDBDsn] = Field(default=None)
    precision_rr_db: Optional[MariaDBDsn] = Field(default=None)

    # --- GR ----
    gr_db: Optional[PostgresDsn] = Field(default=None)
    gr_redis: Optional[RedisDsn] = Field(default=None)

    # --- GRL IQ ---
    grliq_db: Optional[PostgresDsn] = Field(default=None)
    mnt_grliq_archive_dir: Optional[str] = Field(
        default=None,
        description="Where gr-api can pull GRL-IQ Forensic archive items like"
        "the captured screenshots.",
    )

    mnt_gr_api_dir: Optional[str] = Field(
        default=None,
        description="Where gr-api can pull parquet files from.",
    )

    # --- TangoCard Configuration ---
    tango_platform_name: Optional[str] = Field(default=None)
    tango_platform_key: Optional[str] = Field(default=None)
    tango_account_id: Optional[str] = Field(default=None)
    tango_customer_id: Optional[str] = Field(default=None)

    # --- Keeping this here as we use these ids regardless of the AMT account
    amt_bonus_cashout_method_id: Optional[str] = Field(default=None)
    amt_assignment_cashout_method_id: Optional[str] = Field(default=None)

    # --- Maxmind Configuration ---
    maxmind_account_id: Optional[str] = Field(default=None)
    maxmind_license_key: Optional[str] = Field(default=None)


EXAMPLE_PRODUCT_ID = "1108d053e4fa47c5b0dbdcd03a7981e7"

# AMT accounting was changed many times and txs before this date
# are either missing AMT bonuses, or not accounting for hit rewards.
JAMES_BILLINGS_BPID = "888dbc589987425fa846d6e2a8daed04"
JAMES_BILLINGS_TX_CUTOFF = datetime(2026, 1, 1, tzinfo=timezone.utc)
