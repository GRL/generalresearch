from __future__ import annotations

from enum import Enum

from generalresearch.utils.enum import ReprEnumMeta


class Source(str, Enum, metaclass=ReprEnumMeta):
    # The external marketplace, or the source of the survey / work.
    #   Max length of the value is 2.
    GRS = "g"
    CINT = "c"
    DALIA = "a"  # deprecated
    DYNATA = "d"
    ETX = "et"
    FULL_CIRCLE = "f"
    INNOVATE = "i"
    LUCID = "l"
    MORNING_CONSULT = "m"
    OPEN_LABS = "n"
    POLLFISH = "o"
    PRECISION = "e"
    PRODEGE_USER = "r"  # deprecated
    PRODEGE = "pr"  # using 'r' for vendor_wall
    PULLEY = "p"  # deprecated
    REPDATA = "rd"  # using 'q' for vendor_wall
    SAGO = "h"
    SPECTRUM = "s"
    TESTING = "t"  # Used internally for testing
    TESTING2 = "u"  # Used internally for testing
    WXET = "w"


class DebitKey(int, Enum, metaclass=ReprEnumMeta):
    # The debit key for marketplaces
    CINT = 8
    DALIA = 9
    DYNATA = 6
    # ETX = None
    FULL_CIRCLE = 15
    INNOVATE = 7
    LUCID = 0
    MORNING_CONSULT = 12
    # OPEN_LABS = None
    POLLFISH = 13
    PRECISION = 14
    PRODEGE = 11
    SAGO = 10
    SPECTRUM = 5
    # WXET = None


class DeviceType(int, Enum, metaclass=ReprEnumMeta):
    UNKNOWN = 0
    MOBILE = 1
    DESKTOP = 2
    TABLET = 3


class LogicalOperator(str, Enum, metaclass=ReprEnumMeta):
    OR = "OR"
    AND = "AND"
    # There is currently no use case for NOT. See MarketplaceCondition.explain_not
    NOT = "NOT"


class TaskStatus(str, Enum, metaclass=ReprEnumMeta):
    # A survey is live if it is open and, given all conditions are met, is
    # possible to send in traffic. All other statuses are just variants of
    # NOT Live (not accepting traffic)
    LIVE = "LIVE"

    # This is a generic NOT Live status. A marketplace may use other more
    # specific statuses but in practice they don't matter because all we care
    # about is if the task is LIVE.
    NOT_LIVE = "NOT_LIVE"

    # We need a status to mark if a survey we thought was live does not come
    # back from the API, we'll mark it as NOT_FOUND.
    NOT_FOUND = "NOT_FOUND"


class TaskCalculationType(str, Enum):
    COMPLETES = "COMPLETES"
    STARTS = "STARTS"

    @classmethod
    def from_api(cls, v: str) -> "TaskCalculationType":
        return {
            "complete": cls.COMPLETES,
            "completes": cls.COMPLETES,
            "survey start": cls.STARTS,
            "survey starts": cls.STARTS,
            "start": cls.STARTS,
            "prescreens": cls.STARTS,
            "prescreen": cls.STARTS,
        }[v.lower()]

    @classmethod
    def prodege_from_api(cls, v: int) -> "TaskCalculationType":
        return {1: cls.COMPLETES, 2: cls.STARTS}[v]

    @classmethod
    def innovate_from_api(cls, v: int) -> "TaskCalculationType":
        return {0: cls.COMPLETES, 1: cls.STARTS}[v]


class URLQueryKey(str, Enum, metaclass=ReprEnumMeta):
    PRODUCT_ID = "39057c8b"
    PRODUCT_USER_ID = "c184efc0"
    SESSION_ID = "0bb50182"


MAX_INT32 = 2**31
