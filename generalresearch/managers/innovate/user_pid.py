from generalresearch.managers.marketplace.user_pid import UserPidManager
from generalresearch.models import Source


class InnovateUserPidManager(UserPidManager):
    TABLE_NAME = "innovate_userpid"
    SOURCE = Source.INNOVATE
