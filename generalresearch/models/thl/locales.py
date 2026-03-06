from typing import Annotated, Set

from pydantic import AfterValidator

from generalresearch.locales import Localelator
from generalresearch.models.custom_types import (
    to_comma_sep_str,
    from_comma_sep_str,
)

locale_helper = Localelator()
COUNTRY_ISOS: Set[str] = locale_helper.get_all_countries()
LANGUAGE_ISOS: Set[str] = locale_helper.get_all_languages()


def is_valid_country_iso(v: str) -> str:
    assert v in COUNTRY_ISOS, f"invalid country_iso: {v}"
    return v


def is_valid_language_iso(v: str) -> str:
    assert v in LANGUAGE_ISOS, f"invalid language_iso: {v}"
    return v


# ISO 3166-1 alpha-2 (two-letter codes, lowercase)
CountryISO = Annotated[str, AfterValidator(is_valid_country_iso)]
# 3-char ISO 639-2/B, lowercase
LanguageISO = Annotated[str, AfterValidator(is_valid_language_iso)]

CountryISOs = Annotated[Set[CountryISO], to_comma_sep_str, from_comma_sep_str]
LanguageISOs = Annotated[Set[LanguageISO], to_comma_sep_str, from_comma_sep_str]
