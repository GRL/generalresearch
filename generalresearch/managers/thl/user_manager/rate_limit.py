import logging

from limits import RateLimitItem, RateLimitItemPerHour, storage, strategies
from limits.limits import TIME_TYPES, safe_string
from pydantic import RedisDsn

from generalresearch.managers.thl.user_manager import (
    UserCreateNotAllowedError,
    get_bp_user_create_limit_hourly,
)
from generalresearch.models.thl.product import Product

logger = logging.getLogger()


class RateLimitItemPerHourConstantKey(RateLimitItem):
    """
    Per hour rate limit, where the key is specified manually
    """

    GRANULARITY = TIME_TYPES["hour"]

    def key_for(self, *identifiers: str) -> str:
        """
        By default, the key includes the rate limit values. e.g.
        `LIMITER/thl-grpc/allow_user_create/f1eb616ae68e488ab5b1f6839cb06f6a/61/1/hour`
        This changes so that the key does not include the `61/1/hour` part, so that if the
            actual limit changes (the limit of 61 hits per hour in this example), the
            cache item key doesn't change
        """
        remainder = "/".join([safe_string(k) for k in identifiers])
        return f"{self.namespace}/{remainder}"


class UserManagerLimiter:
    def __init__(self, redis: RedisDsn):
        self.redis = redis

        # memcache supported: connect_timeout=1, timeout=1), what about redis?
        self.storage = storage.RedisStorage(uri=str(redis))

        self.window = strategies.FixedWindowRateLimiter(storage=self.storage)
        # self.window = strategies.MovingWindowRateLimiter(storage=self.storage)

    def raise_allow_user_create(self, product: Product) -> None:
        """
        Checks if this product_id is allowed to create a new user now.
        Sends only 1 sentry event per product_id per hour (if the product_id has exceeded the limit)
        :raises UserCreateNotAllowedError
        """
        allowed = self.user_create_allowed(product=product)
        if not allowed:
            err_msg = f"product_id {product.id} exceeded user creation limit"

            # -- Don't spam Sentry.io
            sentry_rl = RateLimitItemPerHour(1)
            if self.window.hit(
                sentry_rl,
                "thl-grpc",
                "allow_user_create",
                "sentry",
                product.id,
                cost=1,
            ):
                logger.exception(err_msg)

            raise UserCreateNotAllowedError(err_msg)

    def user_create_allowed(self, product: Product) -> bool:
        """
        :returns if this product_id is not allowed to create a user
        """
        rl_value = get_bp_user_create_limit_hourly(product)
        rl = RateLimitItemPerHourConstantKey(rl_value)

        return self.window.hit(rl, "thl-grpc", "allow_user_create", product.id, cost=1)
