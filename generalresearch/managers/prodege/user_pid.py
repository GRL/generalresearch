from generalresearch.managers.marketplace.user_pid import UserPidManager
from generalresearch.models import Source


class ProdegeUserPidManager(UserPidManager):
    TABLE_NAME = "prodege_userpid"
    SOURCE = Source.PRODEGE
