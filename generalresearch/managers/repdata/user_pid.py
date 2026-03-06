from generalresearch.managers.marketplace.user_pid import UserPidManager
from generalresearch.models import Source


class RepdataUserPidManager(UserPidManager):
    TABLE_NAME = "repdata_userpid"
    SOURCE = Source.REPDATA
