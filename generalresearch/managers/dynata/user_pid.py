from generalresearch.managers.marketplace.user_pid import UserPidManager
from generalresearch.models import Source


class DynataUserPidManager(UserPidManager):
    TABLE_NAME = "dynata_userpid"
    SOURCE = Source.DYNATA
