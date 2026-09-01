from enum import Enum, StrEnum

from generalresearch.utils.enum import ReprEnumMeta


class TransferMethod(Enum, metaclass=ReprEnumMeta):
    ACH = 0
    WIRE = 1


class BusinessType(StrEnum, metaclass=ReprEnumMeta):
    INDIVIDUAL = "i"
    COMPANY = "c"
