from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from generalresearch.managers.base import Permission
from generalresearch.managers.thl.contest_manager import ContestManager

if TYPE_CHECKING:
    from generalresearch.pg_helper import PostgresConfig


@pytest.fixture(scope="session")
def contest_manager(thl_web_rw: PostgresConfig) -> ContestManager:
    assert thl_web_rw.dsn
    assert thl_web_rw.dsn.path
    assert "/unittest-" in thl_web_rw.dsn.path

    return ContestManager(
        pg_config=thl_web_rw,
        permissions=[
            Permission.CREATE,
            Permission.READ,
            Permission.UPDATE,
            Permission.DELETE,
        ],
    )
