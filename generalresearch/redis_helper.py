import redis
from pydantic import RedisDsn


class RedisConfig:
    def __init__(
        self,
        dsn: RedisDsn,
        decode_responses: bool = True,
        socket_timeout: float = 0.1,
        socket_connect_timeout: float = 0.1,
    ):
        """
        Holds configuration for creating redis clients.
        """
        self.dsn = dsn
        self.decode_responses = decode_responses
        self.socket_timeout = socket_timeout
        self.socket_connect_timeout = socket_connect_timeout

    @property
    def db(self):
        return self.dsn.path[1:]

    def create_redis_client(self) -> redis.Redis:
        # Clients are thread safe. We can just create one upon init
        redis_config_dict = {
            "url": str(self.dsn),
            "decode_responses": self.decode_responses,
            "socket_timeout": self.socket_timeout,
            "socket_connect_timeout": self.socket_connect_timeout,
        }
        return redis.Redis.from_url(**redis_config_dict)
