import logging
from datetime import datetime
from functools import lru_cache
from typing import Collection, List, Optional
from uuid import uuid4

from pydantic import RedisDsn

from generalresearch.managers.base import Permission
from generalresearch.managers.thl.product import ProductManager
from generalresearch.managers.thl.user_manager import UserDoesntExistError
from generalresearch.managers.thl.user_manager.mysql_user_manager import (
    MysqlUserManager,
)
from generalresearch.managers.thl.user_manager.rate_limit import (
    UserManagerLimiter,
)
from generalresearch.managers.thl.user_manager.redis_user_manager import (
    RedisUserManager,
)
from generalresearch.models.custom_types import UUIDStr
from generalresearch.models.thl.product import Product
from generalresearch.models.thl.user import User
from generalresearch.pg_helper import PostgresConfig
from generalresearch.utils.copying_cache import deepcopy_return

logging.basicConfig()
logger = logging.getLogger()
auditlog = logging.getLogger("auditlog")


class UserManager:
    def __init__(
        self,
        redis: Optional[RedisDsn] = None,
        pg_config: Optional[PostgresConfig] = None,
        pg_config_rr: Optional[PostgresConfig] = None,
        sql_permissions: Optional[Collection[Permission]] = None,
        cache_prefix: Optional[str] = None,
        redis_timeout: Optional[float] = None,
    ):

        if sql_permissions is None:
            sql_permissions = []

        if pg_config is not None:
            assert (
                pg_config_rr is not None
            ), "you should pass RR credentials also for fast lookups"

        assert Permission.DELETE not in sql_permissions, "delete not allowed"
        if Permission.UPDATE in sql_permissions or Permission.CREATE in sql_permissions:
            assert pg_config is not None, "must pass pg_config"

        self.sql_permissions = set(sql_permissions) if sql_permissions else set()
        self.mysql_user_manager = None
        if pg_config:
            self.mysql_user_manager = MysqlUserManager(pg_config, is_read_replica=False)

        self.mysql_user_manager_rr = None
        if pg_config_rr:
            self.mysql_user_manager_rr = MysqlUserManager(
                pg_config_rr, is_read_replica=True
            )

        self.user_manager_limiter = None
        self.redis_user_manager = None
        if redis:
            # Assuming we have full write access to redis if clients exist
            self.user_manager_limiter = UserManagerLimiter(redis=redis)
            self.redis_user_manager = RedisUserManager(
                redis_dsn=redis,
                cache_prefix=cache_prefix,
                redis_timeout=redis_timeout,
            )

        self.product_manager = ProductManager(
            pg_config=pg_config, permissions=[Permission.READ]
        )

    def set_last_seen(self, user: User) -> None:
        assert Permission.UPDATE in self.sql_permissions, "permission error"
        return self.mysql_user_manager._set_last_seen(user)

    def audit_log(
        self,
        user: User,
        level: int,
        event_type: str,
        event_msg: Optional[str] = None,
        event_value: Optional[float] = None,
    ) -> None:
        from generalresearch.managers.thl.userhealth import AuditLogManager
        from generalresearch.models.thl.userhealth import AuditLogLevel

        alm = AuditLogManager(pg_config=self.mysql_user_manager.pg_config)
        alm.create(
            user_id=user.user_id,
            level=AuditLogLevel(level),
            event_type=event_type,
            event_msg=event_msg,
            event_value=event_value,
        )

        return None

    def cache_clear(self):
        # Generally this is used in testing. This clears the .get_user's lru_cache.
        # There is no way of clearing only a specific key from the cache.
        # It does not clear any redis caches; that has to be done separately.
        self.get_user.__wrapped__.cache_clear()

    @deepcopy_return
    @lru_cache(maxsize=10000)
    def get_user(
        self,
        *,
        product_id: Optional[str] = None,
        product_user_id: Optional[str] = None,
        user_id: Optional[int] = None,
        user_uuid: Optional[UUIDStr] = None,
    ) -> User:
        """
        Retrieve User from (product_id & product_user_id) or (user_id), or (uuid).
        Looks up in lru_cache, then (redis, memcached), then mysql.
        Raises UserDoesntExistError if user is not found.
        (the * makes all arguments keyword-only arguments)
        """
        assert (
            (product_id and product_user_id) or user_id or user_uuid
        ), "Must pass either (product_id, product_user_id), or user_id, or uuid"
        if product_id or product_user_id:
            assert (
                product_id and product_user_id
            ), "Must pass both product_id and product_user_id"
        assert (
            sum(map(bool, [product_id or product_id, user_id, user_uuid])) == 1
        ), "Must pass only 1 of (product_id, product_user_id), or user_id, or uuid"
        user = self.get_user_inmemory_cache(
            product_id=product_id,
            product_user_id=product_user_id,
            user_id=user_id,
            user_uuid=user_uuid,
        )

        if user:
            return user

        # We can use the read-replica here b/c when we create a user we'll
        #   put it in the in-memory cache
        mysql_user_manager = self.mysql_user_manager_rr or self.mysql_user_manager
        user = mysql_user_manager.get_user_from_mysql(
            product_id=product_id,
            product_user_id=product_user_id,
            user_id=user_id,
            user_uuid=user_uuid,
            can_use_read_replica=True,
        )

        # Note: Do not return None for a user that doesn't exist. If the user
        #   doesn't exist in mysql, this function will return None until the
        #   cache expires. Throw exception instead.
        if user is None:
            raise UserDoesntExistError(
                f"user doesn't exist: {product_id}, {product_user_id}, {user_id}, {user_uuid}"
            )

        # Set the redis/memcached caches to we don't end up hitting mysql again!
        self.set_user_inmemory_cache(user)
        return user

    def get_user_if_exists(
        self,
        product_id: Optional[str] = None,
        product_user_id: Optional[str] = None,
    ) -> Optional[User]:
        """
        Look up User from (product_id & product_user_id). Returns
        None if user does not exist.
        """
        try:
            return self.get_user(product_id=product_id, product_user_id=product_user_id)
        except UserDoesntExistError:
            return None

    def get_user_inmemory_cache(
        self,
        *,
        product_id: Optional[str] = None,
        product_user_id: Optional[str] = None,
        user_id: Optional[int] = None,
        user_uuid: Optional[UUIDStr] = None,
    ) -> Optional[User]:

        input_str = f"{product_id}, {product_user_id}, {user_id}, {user_uuid}"
        if self.redis_user_manager:
            import redis

            logger.info(f"get_user from redis: {input_str}")
            try:
                user = self.redis_user_manager.get_user(
                    product_id=product_id,
                    product_user_id=product_user_id,
                    user_id=user_id,
                    user_uuid=user_uuid,
                )
            except (
                redis.exceptions.TimeoutError,
                redis.exceptions.ConnectionError,
            ) as e:
                logger.info(f"get_user from redis failed: {input_str}, {e}")
            else:
                return user

        return None

    def set_user_inmemory_cache(self, user: User) -> None:
        if self.redis_user_manager:
            import redis

            try:
                self.redis_user_manager.set_user(user)
            except (
                redis.exceptions.TimeoutError,
                redis.exceptions.ConnectionError,
            ) as e:
                logger.info(f"redis.set_user failed: {user}, {e}")

        return None

    def clear_user_inmemory_cache(self, user: User) -> None:
        if self.redis_user_manager:
            # this should only be used by tests
            self.redis_user_manager.clear_user(user)

        return None

    def get_or_create_user(self, product_user_id: str, product_id: str) -> User:
        """
        Given a bp_user_id and a product_id, get or create a User
        """
        assert Permission.CREATE in self.sql_permissions
        assert self.mysql_user_manager is not None
        assert (
            self.redis_user_manager is not None
        ), "need at least redis to synchronize user creation"

        assert (
            self.user_manager_limiter is not None
        ), "Need user_manager_limiter to get_or_create_user"
        # Attempt to create common_struct solely for validation purposes
        if not User.is_valid_ubp(
            product_id=product_id, product_user_id=product_user_id
        ):
            # Hopefully FSB checks this before it gets here and returns a helpful error message
            raise ValueError("invalid product_id/product_user_id")

        u = self.get_user_if_exists(
            product_id=product_id, product_user_id=product_user_id
        )
        if u is not None:
            return u

        return self.create_user(product_id=product_id, product_user_id=product_user_id)

    def create_user(
        self,
        product_user_id: str,
        product_id: Optional[UUIDStr] = None,
        product: Optional[Product] = None,
        created: Optional[datetime] = None,
    ) -> User:

        assert (
            self.user_manager_limiter is not None
        ), "Need user_manager_limiter to create_user"
        assert product_id or product, "Needs a product_id or a Product instance"

        if product is None:
            product = self.product_manager.get_by_uuid(product_uuid=product_id)

        # This will raise a UserCreateNotAllowedError Exception if the
        # product_id is over the limit

        # TODO: DB source for enable/disable user creation rate limit
        # if product.id not in {}:
        #     self.user_manager_limiter.raise_allow_user_create(product=product)

        user = self.mysql_user_manager.create_user(
            product_user_id=product_user_id,
            product_id=product.id,
            created=created,
        )

        self.set_user_inmemory_cache(user=user)

        return user

    def create_dummy(
        self,
        # --- Create dummy "optional" --- #
        product_user_id: Optional[str] = None,
        # --- Optional --- #
        product_id: Optional[UUIDStr] = None,
        product: Optional[Product] = None,
        created: Optional[datetime] = None,
    ) -> User:

        product_user_id = product_user_id or uuid4().hex

        return self.create_user(
            product_user_id=product_user_id,
            product_id=product_id,
            product=product,
            created=created,
        )

    def product_id_exists(self, product_id: str) -> bool:
        mysql_user_manager = self.mysql_user_manager_rr or self.mysql_user_manager
        return mysql_user_manager.product_id_exists(product_id)

    def block_user(self, user: User) -> bool:
        """
        Block this user "permanently".
        Writes to `300large`.`thl_user`.blocked.
        :param user: User
        :return: if the user has been blocked (i.e. False if they are already blocked)
        """
        if user.blocked:
            logger.info(f"User {user} is already blocked")
            return False
        if self.is_whitelisted(user):
            logger.info(f"User {user} is whitelisted")
            return False

        self.mysql_user_manager._block_user(user=user)
        user.blocked = True

        # If we change something about a user, we should update the in-memory caches
        self.set_user_inmemory_cache(user)
        # There is no way to clear a single key from the lru_cache...
        # https://bugs.python.org/issue28178
        self.cache_clear()
        return True

    def is_whitelisted(self, user: User) -> bool:
        """
        We have a user whitelist/blocklist system, which protects a user against a hard block

        Currently, this sets a key in the userprofile_userstat table.
        TODO: this should be a property of the user?
        """
        return self.mysql_user_manager.is_whitelisted(user=user)

    def fetch_by_bpuids(
        self,
        *,
        product_id: str,
        product_user_ids: Collection[str],
    ) -> List[User]:
        assert product_id, "must pass product_id"
        assert len(product_user_ids) > 0, "must pass 1 or more product_user_ids"
        return self.mysql_user_manager_rr.fetch_by_bpuids(
            product_id=product_id, product_user_ids=product_user_ids
        )

    def fetch(
        self,
        *,
        user_ids: Collection[int] = None,
        user_uuids: Collection[str] = None,
    ) -> List[User]:
        assert (user_ids or user_uuids) and not (
            user_ids and user_uuids
        ), "Must pass ONE of user_ids, user_uuids"
        return self.mysql_user_manager_rr.fetch(
            user_ids=user_ids, user_uuids=user_uuids
        )
