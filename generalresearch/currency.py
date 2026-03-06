import warnings
from decimal import Decimal
from enum import Enum
from typing import Any

from pydantic import GetCoreSchemaHandler, NonNegativeInt
from pydantic_core import CoreSchema, core_schema

from generalresearch.utils.enum import ReprEnumMeta


class LedgerCurrency(str, Enum, metaclass=ReprEnumMeta):
    USD = "USD"
    USDCent = "USDCent"
    USDMill = "USDMill"
    TEST = "test"


def format_usd_cent(usd_cent: int) -> str:
    """USDCent can't be negative. However, we want some helper properties
    so show the value even if it's negative
    """
    v = USDCent(abs(usd_cent)).to_usd_str()
    return f"-{v}" if usd_cent < 0 else v


class USDCent(int):
    def __new__(cls, value, *args, **kwargs):

        if isinstance(value, float):
            warnings.warn(
                "USDCent init with a float. Rounding behavior may " "be unexpected"
            )

        if isinstance(value, Decimal):
            warnings.warn(
                "USDCent init with a Decimal. Rounding behavior may " "be unexpected"
            )

        if value < 0:
            raise ValueError("USDCent not be less than zero")

        return super(cls, cls).__new__(cls, value)

    def __add__(self, other):
        assert isinstance(other, USDCent)
        res = super(USDCent, self).__add__(other)
        return self.__class__(res)

    def __sub__(self, other):
        assert isinstance(other, USDCent)
        res = super(USDCent, self).__sub__(other)
        return self.__class__(res)

    def __mul__(self, other):
        assert isinstance(other, USDCent)
        res = super(USDCent, self).__mul__(other)
        return self.__class__(res)

    def __abs__(self):
        res = super(USDCent, self).__abs__()
        return self.__class__(res)

    def __truediv__(self, other):
        raise ValueError("Division not allowed for USDCent")

    def __str__(self):
        return "%d" % int(self)

    def __repr__(self):
        return "USDCent(%d)" % int(self)

    @classmethod
    def __get_pydantic_core_schema__(
        cls, source_type: Any, handler: GetCoreSchemaHandler
    ) -> CoreSchema:
        """
        https://docs.pydantic.dev/latest/concepts/types/#customizing-validation-with-__get_pydantic_core_schema__
        """
        return core_schema.no_info_after_validator_function(
            cls, handler(NonNegativeInt)
        )

    def to_usd(self) -> Decimal:
        return Decimal(int(self) / 100).quantize(Decimal(".01"))

    def to_usd_str(self) -> str:
        return "${:,.2f}".format(float(self.to_usd()))


class USDMill(int):
    """
    This represents 1/1000 of a US dollar, or 1/10th of a USD cent.
    """

    def __new__(cls, value, *args, **kwargs):

        if isinstance(value, float):
            warnings.warn(
                "USDMill init with a float. Rounding behavior " "may be unexpected"
            )

        if isinstance(value, Decimal):
            warnings.warn(
                "USDMill init with a Decimal. Rounding behavior " "may be unexpected"
            )

        if value < 0:
            raise ValueError("USDMill not be less than zero")

        return super(cls, cls).__new__(cls, value)

    def __add__(self, other):
        assert isinstance(other, USDMill)
        res = super(USDMill, self).__add__(other)
        return self.__class__(res)

    def __sub__(self, other):
        assert isinstance(other, USDMill)
        res = super(USDMill, self).__sub__(other)
        return self.__class__(res)

    def __mul__(self, other):
        assert isinstance(other, USDMill)
        res = super(USDMill, self).__mul__(other)
        return self.__class__(res)

    def __abs__(self):
        res = super(USDMill, self).__abs__()
        return self.__class__(res)

    def __truediv__(self, other):
        raise ValueError("Division not allowed for USDMill")

    def __str__(self):
        return "%d" % int(self)

    def __repr__(self):
        return "USDMill(%d)" % int(self)

    @classmethod
    def __get_pydantic_core_schema__(
        cls, source_type: Any, handler: GetCoreSchemaHandler
    ) -> CoreSchema:
        """
        https://docs.pydantic.dev/latest/concepts/types/#customizing-validation-with-__get_pydantic_core_schema__
        """
        return core_schema.no_info_after_validator_function(
            cls, handler(NonNegativeInt)
        )

    def to_usd(self) -> Decimal:
        return Decimal(int(self) / 1_000).quantize(Decimal(".001"))

    def to_usd_str(self) -> str:
        return "${:,.3f}".format(float(self.to_usd()))
