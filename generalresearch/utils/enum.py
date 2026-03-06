from __future__ import annotations

import inspect
import re
from enum import EnumMeta
from typing import Dict


class ReprEnumMeta(EnumMeta):
    def as_openapi(self) -> str:
        return "\n".join([f" - `{e.value}` = {e.name}" for e in self])

    def as_openapi_with_value_descriptions(self) -> str:
        descriptions = get_enum_comments(self)

        # This doesn't work in Python 3.12, so check if None
        val = self.__doc__
        if val:
            return f"{val.strip()}\n\nAllowed values: \n" + "\n".join(
                [f" - __{e.value}__ *({e.name})*: {descriptions[e.name]}" for e in self]
            )
        else:
            return f"\nAllowed values: \n" + "\n".join(
                [f" - __{e.value}__ *({e.name})*: {descriptions[e.name]}" for e in self]
            )

    def as_openapi_with_value_descriptions_name(self) -> str:
        # For use when the allowed values are the enum's NAME (like in the
        #   task status's status_code_1)
        descriptions = get_enum_comments(self)

        # This doesn't work in Python 3.12, so check if None
        val = self.__doc__
        if val:
            return f"{val.strip()}\n\nAllowed values: \n" + "\n".join(
                [f" - __{e.name}__: {descriptions[e.name]}" for e in self]
            )
        else:
            return f"\nAllowed values: \n" + "\n".join(
                [f" - __{e.name}__: {descriptions[e.name]}" for e in self]
            )


def get_enum_comments(enum_class) -> Dict:
    source = inspect.getsource(enum_class)
    # Regular expression to match multi-line comments and enum values
    pattern = re.compile(r"((?:\s*#.*?\n)+)\s*(\w+)\s*=")
    matches = pattern.findall(source)
    comments_dict = {}
    for match in matches:
        comment = []
        for line in match[0].strip().split("\n")[::-1]:
            if line == "":
                # Don't match empty lines in between comments
                break
            comment.append(line)
        comment = "\n".join(comment[::-1])
        comment = comment.replace("\n", " ").replace("#", "").strip()
        comment = re.sub(r"\s+", " ", comment)
        comments_dict[match[1]] = comment
    return comments_dict
