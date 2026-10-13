from collections.abc import Callable

from pydantic import PostgresDsn


class TestGRPostgresDjangoCreation:
    def test_django_creation(
        self,
        django_db_factory: Callable[..., PostgresDsn],
    ):
        dsn = django_db_factory("generalresearch.gr_django")
        assert isinstance(dsn, PostgresDsn)
