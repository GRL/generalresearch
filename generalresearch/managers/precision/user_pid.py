from generalresearch.managers.marketplace.user_pid import UserPidManager
from generalresearch.models.definitions import Source


class PrecisionUserPidManager(UserPidManager):
    TABLE_NAME = "precision_userpid"
    SOURCE = Source.PRECISION
