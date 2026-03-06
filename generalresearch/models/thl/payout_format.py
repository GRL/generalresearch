import decimal
import re

from pydantic import AfterValidator, Field
from typing_extensions import Annotated

# Matches only digits, parenthesis, + , -, *, / and the string payout.
xform_format_re = re.compile(pattern=r"^[\d()+\-*/.]*payout[\d()+\-*/.]*$")


def validate_payout_format(payout_format: str) -> str:
    # We validate the payout format by just trying to use it with 4 example
    #   numbers. Each step is checked along the way. This is not really any
    #   slower to do for real.
    assert len(payout_format) < 40, "invalid format"
    for payout_int in [0, 1, 200, 2245]:
        format_payout_format(payout_format, payout_int)
    return payout_format


def format_payout_format(payout_format: str, payout_int: int) -> str:
    """
    Generate a str representation of a payout. Typically, this would be displayed to a user.
    :param payout_format: see BPC_DEFAULTS.payout_format
    :param payout_int: The actual value in integer usd cents.
    """
    assert isinstance(payout_int, int), "payout_int must be an integer"
    try:
        lidx = payout_format.index("{")
    except ValueError:
        raise ValueError("Must wrap payout transformation in {}")

    try:
        ridx = payout_format.index("}")
    except ValueError:
        raise ValueError("Must wrap payout transformation in {}")

    prefix = payout_format[:lidx]
    suffix = payout_format[ridx + 1 :]
    inside = payout_format[lidx + 1 : ridx]

    try:
        xform, formatstr = inside.split(":")
    except ValueError as e:
        raise ValueError(
            "Payout format string must contain ':' to distinguish between transformations and formatting."
        )

    assert xform_format_re.match(xform) is not None, "Invalid transformation"

    try:
        # if we only cared about strings, could do: set(xform) <= allowed
        # ()+-*/, "payout"

        # x = re.search("(payout)+[\d\(\)\+\-\*\/\ ]*", xform)
        # print("xform:", xform, bool(x))

        payout = decimal.Decimal(eval(xform, {"payout": payout_int}))

    except NameError as e:
        raise ValueError("Payout format string must contain 'payout' variable.")

    except ZeroDivisionError as e:
        raise ValueError("Cannot divide by zero.")

    except TypeError as e:
        # "{payout()*1:}" - TypeError: 'int' object is not callable
        raise ValueError("Invalid type reference.")
    except Exception as e:
        raise ValueError(f"Invalid payout transformation")

    formatstr = f"{{:{formatstr}}}"

    try:
        payout_str = prefix + formatstr.format(payout) + suffix
    except ValueError:
        raise ValueError("Invalid format string.")
    return payout_str


description = """
The format describing the str representation of a payout. Typically, this would be displayed to a user.
The payout_format is similar to python format string with a subset of functionality supported.
Only float with a precision are supported along with an optional comma for a thousands separator.
In addition, a mathematical operator can be applied, such as dividing by 100. 
Examples are shown assuming payout = 100 (one dollar).
- "{payout*10:,.0f} Points" -> "1,000 Points"
- "${payout/100:.2f}" -> "$1.00"
"""
examples = ["{payout*10:,.0f} Points", "${payout/100:.2f}", "{payout:.0f}"]

PayoutFormatField = Field(description=description, examples=examples)
PayoutFormatOptionalField = Field(
    default=None, description=description, examples=examples
)
PayoutFormatType = Annotated[str, AfterValidator(validate_payout_format)]
