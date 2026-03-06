from generalresearch.managers.marketplace.user_pid import UserPidManager
from generalresearch.models import Source


class PollfishUserPidManager(UserPidManager):
    TABLE_NAME = "pollfish_userpid"
    SOURCE = Source.POLLFISH
