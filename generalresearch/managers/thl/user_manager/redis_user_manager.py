from typing import Optional

import redis
from pydantic import RedisDsn

from generalresearch.models.thl.user import User


class RedisUserManager:
    def __init__(
        self,
        redis_dsn: RedisDsn,
        cache_prefix: Optional[str] = None,
        redis_timeout: Optional[float] = None,
    ):
        self.redis = redis_dsn
        self.redis_timeout = redis_timeout if redis_timeout else 0.10
        self.client = self.create_client()
        self.cache_prefix = cache_prefix if cache_prefix else "user-lookup"

    def create_client(self) -> redis.Redis:
        # Clients are thread safe. We can just create one upon init
        redis_config_dict = {
            "url": str(self.redis),
            "decode_responses": True,
            "socket_timeout": self.redis_timeout,
            "socket_connect_timeout": self.redis_timeout,
        }

        return redis.Redis.from_url(**redis_config_dict)

    def get_user(
        self,
        *,
        product_id: Optional[str] = None,
        product_user_id: Optional[str] = None,
        user_id: Optional[int] = None,
        user_uuid: Optional[str] = None,
    ) -> Optional[User]:
        # assume we did input validation in user_manager.get_user() function
        if user_uuid:
            d = self.client.get(f"{self.cache_prefix}:uuid:{user_uuid}")

        elif user_id:
            d = self.client.get(f"{self.cache_prefix}:user_id:{user_id}")

        else:
            d = self.client.get(
                f"{self.cache_prefix}:ubp:{product_id}:{product_user_id}"
            )

        if d:
            return User.model_validate_json(d)

        return None

    def set_user(self, user: User) -> None:
        d = user.to_json()
        with self.client.pipeline(transaction=False) as p:
            p.set(
                name=f"{self.cache_prefix}:uuid:{user.uuid}",
                value=d,
                ex=60 * 60 * 24,
            )
            p.set(
                name=f"{self.cache_prefix}:user_id:{user.user_id}",
                value=d,
                ex=60 * 60 * 24,
            )
            p.set(
                name=f"{self.cache_prefix}:ubp:{user.product_id}:{user.product_user_id}",
                value=d,
                ex=60 * 60 * 24,
            )

            p.execute()

        return None

    def clear_user(self, user: User) -> None:
        # this should only be used by tests
        with self.client.pipeline(transaction=False) as p:
            p.delete(f"{self.cache_prefix}:uuid:{user.uuid}")
            p.delete(f"{self.cache_prefix}:user_id:{user.user_id}")
            p.delete(
                f"{self.cache_prefix}:ubp:{user.product_id}:{user.product_user_id}"
            )
            p.execute()

        return None
