import re
from typing import Annotated

from pydantic import (
    AfterValidator,
    StringConstraints,
)

BPUID_ALLOWED = r"0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ!#$%&()*+,-.:;<=>?@[\]^_{|}~"


def validate_product_user_id(v: str) -> str:
    if " " in v:
        raise ValueError("String cannot contain spaces")
    if "\\" in v:
        raise ValueError("String cannot contain backslash")
    if "/" in v:
        raise ValueError("String cannot contain slash")
    # I think the * on the regex messes up value matches that are
    # the same length as the
    rex = re.fullmatch("[" + BPUID_ALLOWED + "]*", v)
    if not bool(rex):
        raise ValueError("String is not valid regex")
    return v


# Used in other places where the bpuid is part of a model that's used in
# the API (separate from a User)
BPUIDStr = Annotated[
    str,
    StringConstraints(min_length=3, max_length=128),
    AfterValidator(validate_product_user_id),
]
