# from typing import List, Optional
#
# import pylibmc
#
# from generalresearch.models.thl.user import User
#
#
# class MemcachedUserManager:
#     def __init__(self, servers: List[str], cache_prefix: Optional[str] = None):
#         self.servers = servers
#         self.cache_prefix = cache_prefix if cache_prefix else "user-lookup"
#
#     def create_client(self):
#         # Clients are NOT thread safe. Make a new one each time
#
#         # There's a receive_timeout and send_timeout also, but the documentation is incomprehensible,
#         #   and they don't seem to do anything??? (I tested setting them at 1ms and I can't
#         #   get it to fail)
#         # https://sendapatch.se/projects/pylibmc/behaviors.html
#         mc_client = pylibmc.Client(servers=self.servers, binary=True,
#                                    behaviors={'connect_timeout': 100})
#         return mc_client
#
#     def get_user(self, *, product_id: str = None, product_user_id: str = None, user_id: int = None,
#                  user_uuid: UUIDStr = None) -> User:
#         # assume we did input validation in user_manager.get_user() function
#         mc_client = self.create_client()
#         if user_uuid:
#             d = mc_client.get(f"{self.cache_prefix}:uuid:{user_uuid}")
#         elif user_id:
#             d = mc_client.get(f"{self.cache_prefix}:user_id:{user_id}")
#         else:
#             d = mc_client.get(f"{self.cache_prefix}:ubp:{product_id}:{product_user_id}")
#         if d:
#             return User.model_validate_json(d)
#
#     def set_user(self, user: User):
#         d = user.to_json()
#         mc_client = self.create_client()
#         mc_client.set(f"{self.cache_prefix}:uuid:{user.uuid}", d, time=60 * 60 * 24)
#         mc_client.set(f"{self.cache_prefix}:user_id:{user.user_id}", d, time=60 * 60 * 24)
#         mc_client.set(f"{self.cache_prefix}:ubp:{user.product_id}:{user.product_user_id}", d, time=60 * 60 * 24)
#
#     def clear_user(self, user: User):
#         # this should only be used by tests
#         mc_client = self.create_client()
#         mc_client.delete(f"{self.cache_prefix}:uuid:{user.uuid}")
#         mc_client.delete(f"{self.cache_prefix}:user_id:{user.user_id}")
#         mc_client.delete(f"{self.cache_prefix}:ubp:{user.product_id}:{user.product_user_id}")
