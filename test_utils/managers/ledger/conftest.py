from __future__ import annotations

import pytest

from generalresearch.managers.base import Permission
from generalresearch.managers.thl.ledger_manager.ledger import (
    LedgerAccountManager,
    LedgerManager,
    LedgerTransactionManager,
)
from generalresearch.managers.thl.ledger_manager.thl_ledger import (
    ThlLedgerManager,
)
from generalresearch.pg_helper import PostgresConfig
from generalresearch.redis_helper import RedisConfig

# --- Ledger ---


@pytest.fixture(scope="session")
def ledger_manager(
    thl_web_rw: PostgresConfig, thl_redis_config: RedisConfig
) -> LedgerManager:

    return LedgerManager(
        pg_config=thl_web_rw,
        permissions=[
            Permission.CREATE,
            Permission.READ,
            Permission.UPDATE,
            Permission.DELETE,
        ],
        testing=True,
        redis_config=thl_redis_config,
    )


@pytest.fixture(scope="session")
def ledger_tx_manager(
    thl_web_rw: PostgresConfig, thl_redis_config: RedisConfig
) -> LedgerTransactionManager:
    assert thl_web_rw.dsn.path
    assert "/unittest-" in thl_web_rw.dsn.path

    from generalresearch.managers.thl.ledger_manager.ledger import (
        LedgerTransactionManager,
    )

    return LedgerTransactionManager(
        pg_config=thl_web_rw,
        permissions=[Permission.CREATE, Permission.READ],
        testing=True,
        redis_config=thl_redis_config,
    )


@pytest.fixture(scope="session")
def ledger_account_manager(
    thl_web_rw: PostgresConfig, thl_redis_config: RedisConfig
) -> LedgerAccountManager:
    assert thl_web_rw.dsn.path
    assert "/unittest-" in thl_web_rw.dsn.path

    from generalresearch.managers.thl.ledger_manager.ledger import (
        LedgerAccountManager,
    )

    return LedgerAccountManager(
        pg_config=thl_web_rw,
        permissions=[Permission.CREATE, Permission.READ],
        testing=True,
        redis_config=thl_redis_config,
    )


# --- THL Ledger ---


@pytest.fixture(scope="session")
def thl_ledger_manager(
    thl_web_rw: PostgresConfig, thl_redis_config: RedisConfig
) -> ThlLedgerManager:

    return ThlLedgerManager(
        pg_config=thl_web_rw,
        permissions=[
            Permission.CREATE,
            Permission.READ,
            Permission.UPDATE,
            Permission.DELETE,
        ],
        testing=True,
        redis_config=thl_redis_config,
    )
