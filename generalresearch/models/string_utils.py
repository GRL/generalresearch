import unicodedata
from typing import Optional


def remove_nbsp(s: Optional[str]) -> Optional[str]:
    # Some text comes back from the API with lots of (copied from excel or
    # something), and random unicode...
    if s:
        s = s.replace("\u00a0", " ").strip()
        s = unicodedata.normalize("NFKD", s)

    return s
