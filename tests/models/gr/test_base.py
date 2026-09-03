from __future__ import annotations

import subprocess
from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from pydantic import PostgresDsn

if TYPE_CHECKING:
    from generalresearch.pg_helper import PostgresConfig


class TestGRPostgresDjangoCreation:

    def test_git(self, gr_repo: Callable[..., Path]):
        repo_path = gr_repo()

        try:
            # Run the git command inside the target directory
            result = subprocess.run(
                ["git", "rev-parse", "--is-inside-work-tree"],
                cwd=repo_path,
                capture_output=True,
                text=True,
                check=True,
            )
            # Check if the output string is exactly "true"
            assert result.stdout.strip() == "true"

        except (subprocess.CalledProcessError, FileNotFoundError) as e:
            pytest.fail(f"Directory is not a Git repo or Git is not installed: {e}")

    def test_django_creation(
        self,
        django_db_factory: Callable[..., None],
    ):

        dsn = django_db_factory("gr.common")
        assert isinstance(dsn, PostgresDsn)

    def test_django_tables(self, gr_db: PostgresConfig):
        """
        WARNING: This will always be the thl_django tables in addition
        to the GR tables due to the way our fixtures are loaded.
        """

        res = gr_db.execute_sql_query(query="""
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema = 'public';
        """)
        assert len(res) == 1
        assert res[0]["count"] == 65
