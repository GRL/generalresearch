from generalresearch.managers.marketplace.user_pid import UserPidManager
from generalresearch.models import Source


class MorningUserPidManager(UserPidManager):
    TABLE_NAME = "morning_userpid"
    SOURCE = Source.MORNING_CONSULT
