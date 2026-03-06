from enum import Enum
from typing import Collection, Optional

from generalresearch.pg_helper import PostgresConfig
from generalresearch.redis_helper import RedisConfig
from generalresearch.sql_helper import SqlHelper


class Permission(int, Enum):
    READ = 1
    UPDATE = 2
    CREATE = 3
    DELETE = 4


class Manager:
    pass


class SqlManager(Manager):
    def __init__(
        self,
        sql_helper: SqlHelper,
        permissions: Optional[Collection[Permission]] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.sql_helper = sql_helper
        self.permissions = set(permissions) if permissions else set()
        # This is susceptible to sql injection, so don't ever pass arbitrary input into it
        #   (https://stackoverflow.com/a/64412951/1991066)
        self.db_name = self.sql_helper.db_name


class PostgresManager(Manager):
    def __init__(
        self,
        pg_config: PostgresConfig,
        permissions: Collection[Permission] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.pg_config = pg_config
        self.permissions = set(permissions) if permissions else set()


class RedisManager(Manager):
    CACHE_PREFIX = None

    def __init__(
        self,
        redis_config: RedisConfig,
        cache_prefix: Optional[str] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.redis_config = redis_config
        self.cache_prefix = cache_prefix or self.CACHE_PREFIX or ""
        self.redis_client = self.redis_config.create_redis_client()


class SqlManagerWithRedis(SqlManager, RedisManager):
    def __init__(
        self,
        sql_helper: SqlHelper,
        redis_config: RedisConfig,
        permissions: Collection[Permission] = None,
        cache_prefix: Optional[str] = None,
    ):
        super().__init__(
            sql_helper=sql_helper,
            redis_config=redis_config,
            permissions=permissions,
            cache_prefix=cache_prefix,
        )


class PostgresManagerWithRedis(PostgresManager, RedisManager):
    def __init__(
        self,
        pg_config: PostgresConfig,
        redis_config: RedisConfig,
        permissions: Collection[Permission] = None,
        cache_prefix: Optional[str] = None,
    ):
        super().__init__(
            pg_config=pg_config,
            permissions=permissions,
            redis_config=redis_config,
            cache_prefix=cache_prefix,
        )
