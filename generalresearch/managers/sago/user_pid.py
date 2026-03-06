from generalresearch.managers.marketplace.user_pid import UserPidManager
from generalresearch.models import Source


class SagoUserPidManager(UserPidManager):
    TABLE_NAME = "sago_userpid"
    SOURCE = Source.SAGO
