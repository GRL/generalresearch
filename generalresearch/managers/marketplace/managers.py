from generalresearch.managers.cint.user_pid import CintUserPidManager
from generalresearch.managers.dynata.user_pid import DynataUserPidManager
from generalresearch.managers.innovate.user_pid import InnovateUserPidManager
from generalresearch.managers.morning.user_pid import MorningUserPidManager
from generalresearch.managers.precision.user_pid import PrecisionUserPidManager
from generalresearch.managers.prodege.user_pid import ProdegeUserPidManager
from generalresearch.managers.repdata.user_pid import RepdataUserPidManager
from generalresearch.managers.sago.user_pid import SagoUserPidManager
from generalresearch.managers.spectrum.user_pid import SpectrumUserPidManager

_managers = [
    CintUserPidManager,
    DynataUserPidManager,
    InnovateUserPidManager,
    MorningUserPidManager,
    PrecisionUserPidManager,
    ProdegeUserPidManager,
    RepdataUserPidManager,
    SagoUserPidManager,
    SpectrumUserPidManager,
]

USER_PID_MANAGERS = {x.SOURCE: x for x in _managers}
