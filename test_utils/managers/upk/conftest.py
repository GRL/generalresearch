from typing import Callable, Generator

import pytest

from generalresearch.managers.thl.profiling.question import (
    QuestionManager,
)
from generalresearch.managers.thl.profiling.schema import (
    UpkSchemaManager,
)
from generalresearch.managers.thl.profiling.uqa import UQAManager
from generalresearch.managers.thl.profiling.user_upk import (
    UserUpkManager,
)
from generalresearch.models.thl.user import User
from generalresearch.pg_helper import PostgresConfig
from generalresearch.redis_helper import RedisConfig


@pytest.fixture(scope="session")
def upk_schema_manager(thl_web_rw: PostgresConfig) -> UpkSchemaManager:
    return UpkSchemaManager(pg_config=thl_web_rw)


@pytest.fixture(scope="session")
def user_upk_manager(
    thl_web_rw: PostgresConfig, thl_redis_config: RedisConfig
) -> UserUpkManager:

    return UserUpkManager(pg_config=thl_web_rw, redis_config=thl_redis_config)


@pytest.fixture(scope="session")
def question_manager(
    thl_web_rw: PostgresConfig,
) -> QuestionManager:
    return QuestionManager(pg_config=thl_web_rw)


@pytest.fixture(scope="session")
def uqa_manager(
    thl_web_rw: PostgresConfig, thl_redis_config: RedisConfig
) -> UQAManager:

    return UQAManager(redis_config=thl_redis_config, pg_config=thl_web_rw)


@pytest.fixture(scope="function")
def uqa_manager_clear_cache_factory(
    uqa_manager: UQAManager,
) -> Callable[..., Generator[None]]:

    def _inner(user: User) -> Generator[None]:
        # On successive py-test/jenkins runs, the cache may contain
        #   the previous run's info (keyed under the same user_id)
        uqa_manager.clear_cache(user)

        yield

        uqa_manager.clear_cache(user)

    return _inner


@pytest.fixture(scope="function")
def uqa_manager_clear_cache(
    uqa_manager_clear_cache_factory: Callable[..., None], user: User
):
    uqa_manager_clear_cache_factory(user=user)
