from __future__ import annotations

import os
from datetime import UTC, datetime
from pathlib import Path

from pydantic import DirectoryPath, Field, MariaDBDsn, PostgresDsn, RedisDsn
from pydantic_settings import BaseSettings, SettingsConfigDict

from generalresearch.models.custom_types import DaskDsn, InternalHostname, SentryDsn

os.environ["DISABLE_PANDERA_IMPORT_WARNING"] = "True"


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
    model_config = SettingsConfigDict(
        env_file=(".env.test", ".env.testing", ".env.staging", ".env.prod"),
        env_file_encoding="utf-8",
        extra="allow",
    )

    debug: bool = Field(default=True)

    # --- Pytest ---

    testing_postgres: InternalHostname | None = Field(default=None)
    testing_postgres_user: str | None = Field(default=None)
    testing_postgres_pass: str | None = Field(default=None)

    git_creds: str | None = Field(default=None)

    # ---

    redis: RedisDsn | None = Field(default=None)
    redis_timeout: float = Field(default=0.10)

    thl_redis: RedisDsn | None = Field(default=None)

    dask: DaskDsn | None = Field(default=None, description="")

    sentry: SentryDsn | None = Field(
        default=None, description="The sentry.io DSN for connecting to a project"
    )

    thl_mkpl_rw_db: MariaDBDsn | None = Field(default=None)
    thl_mkpl_rr_db: MariaDBDsn | None = Field(default=None)

    # Primary DB, SELECT permissions
    thl_web_ro_db: PostgresDsn | None = Field(default=None)
    # Primary DB, SELECT, INSERT, UPDATE permissions
    thl_web_rw_db: PostgresDsn | None = Field(default=None)
    # Primary DB, SELECT, INSERT, UPDATE, DELETE permissions
    thl_web_rwd_db: PostgresDsn | None = Field(default=None)
    # Slave/secondary/read-replica SELECT permission only
    thl_web_rr_db: PostgresDsn | None = Field(default=None)

    tmp_dir: DirectoryPath = Field(default=Path("/tmp"))

    spectrum_rw_db: MariaDBDsn | None = Field(default=None)
    spectrum_rr_db: MariaDBDsn | None = Field(default=None)

    precision_rw_db: MariaDBDsn | None = Field(default=None)
    precision_rr_db: MariaDBDsn | None = Field(default=None)

    # --- GR ----
    gr_db: PostgresDsn | None = Field(default=None)
    gr_redis: RedisDsn | None = Field(default=None)

    # --- GRL IQ ---
    grliq_db: PostgresDsn | None = Field(default=None)
    mnt_grliq_archive_dir: str | None = Field(
        default=None,
        description="Where gr-api can pull GRL-IQ Forensic archive items like"
        "the captured screenshots.",
    )

    mnt_gr_api_dir: str | None = Field(
        default=None,
        description="Where gr-api can pull parquet files from.",
    )

    # --- TangoCard Configuration ---
    tango_platform_name: str | None = Field(default=None)
    tango_platform_key: str | None = Field(default=None)
    tango_account_id: str | None = Field(default=None)
    tango_customer_id: str | None = Field(default=None)

    # --- Keeping this here as we use these ids regardless of the AMT account
    amt_bonus_cashout_method_id: str | None = Field(default=None)
    amt_assignment_cashout_method_id: str | None = Field(default=None)

    # --- Maxmind Configuration ---
    maxmind_account_id: str | None = Field(default=None)
    maxmind_license_key: str | None = Field(default=None)


EXAMPLE_PRODUCT_ID = "1108d053e4fa47c5b0dbdcd03a7981e7"

# AMT accounting was changed many times and txs before this date
# are either missing AMT bonuses, or not accounting for hit rewards.
JAMES_BILLINGS_BPID = "888dbc589987425fa846d6e2a8daed04"
JAMES_BILLINGS_TX_CUTOFF = datetime(2026, 1, 1, tzinfo=UTC)
