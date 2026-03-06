from generalresearch.managers.marketplace.user_pid import UserPidManager
from generalresearch.models import Source


class CintUserPidManager(UserPidManager):
    TABLE_NAME = "cint_userpid"
    SOURCE = Source.CINT
