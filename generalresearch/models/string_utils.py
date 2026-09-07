import unicodedata


def remove_nbsp(s: str | None) -> str | None:
    # Some text comes back from the API with lots of (copied from excel or
    # something), and random unicode...
    if s:
        s = s.replace("\u00a0", " ").strip()
        s = unicodedata.normalize("NFKD", s)

    return s
