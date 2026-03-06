from generalresearch.managers.marketplace.user_pid import UserPidManager
from generalresearch.models import Source


class SpectrumUserPidManager(UserPidManager):
    TABLE_NAME = "spectrum_userpid"
    SOURCE = Source.SPECTRUM
