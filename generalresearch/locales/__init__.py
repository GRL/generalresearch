"""
THL/GR is using:

country codes: ISO 3166-1 alpha-2 (two-letter codes)
https://en.wikipedia.org/wiki/ISO_3166-1_alpha-2
https://en.wikipedia.org/wiki/List_of_ISO_3166_country_codes

language codes: ISO 639-2/B (three-letter codes)
https://en.wikipedia.org/wiki/ISO_639-2
https://en.wikipedia.org/wiki/List_of_ISO_639-1_codes
"""

import json
import pkgutil
from typing import Set


class Localelator:
    """
    EVERYTHING IS LOWERCASE!!! (except this comment)
    """

    lang_alpha2_to_alpha3b = dict()
    lang_alpha3_to_alpha3b = dict()
    languages = set()

    def __init__(self):
        d = json.loads(pkgutil.get_data(__name__, "iso639-3.json"))
        self.lang_alpha2_to_alpha3b = {x["alpha_2"]: x["alpha_3b"] for x in d}
        self.lang_alpha3_to_alpha3b = {x["alpha_3"]: x["alpha_3b"] for x in d}
        self.languages = (
            set(self.lang_alpha2_to_alpha3b.keys())
            | set(self.lang_alpha2_to_alpha3b.values())
            | set(self.lang_alpha3_to_alpha3b.keys())
        )
        d = json.loads(pkgutil.get_data(__name__, "iso3166-1.json"))
        self.country_alpha3_to_alpha2 = {x["alpha_3"]: x["alpha_2"] for x in d}
        self.countries = set(self.country_alpha3_to_alpha2.keys()) | set(
            self.country_alpha3_to_alpha2.values()
        )

        self.country_default_lang = json.loads(
            pkgutil.get_data(__name__, "country_default_lang.json")
        )

    def get_all_languages(self) -> Set[str]:
        # returns only the ISO 639-2/B (three-letter codes)
        return set(self.lang_alpha2_to_alpha3b.values())

    def get_all_countries(self) -> Set[str]:
        # returns only the ISO 3166-1 alpha-2 (two-letter codes)
        return set(self.country_alpha3_to_alpha2.values())

    def get_language_iso(self, input_iso: str) -> str:
        # input_iso is a 2 (ISO 639-1) or 3 (ISO 639-2/T) char language ISO
        # output is a 3 char ISO 639-2/B
        assert len(input_iso) in {
            2,
            3,
        }, f"input_iso must be len 2 or 3, got: {input_iso}"
        assert input_iso.lower() == input_iso, "input_iso must be lowercase"
        assert (
            input_iso in self.languages
        ), f"language input_iso: {input_iso} not recognized"

        return (
            self.lang_alpha2_to_alpha3b.get(input_iso)
            or self.lang_alpha3_to_alpha3b.get(input_iso)
            or input_iso
        )

    def get_country_iso(self, input_iso: str) -> str:
        # input_iso is a 2 (ISO 3166-1 alpha-2) or 3 (ISO 3166-1 alpha-3) char country ISO
        # output is a 2 char ISO 3166-1 alpha-2
        assert len(input_iso) in {
            2,
            3,
        }, f"input_iso must be len 2 or 3, got: {input_iso}"
        assert input_iso.lower() == input_iso, "input_iso must be lowercase"
        assert (
            input_iso in self.countries
        ), f"country input_iso: {input_iso} not recognized"
        return self.country_alpha3_to_alpha2.get(input_iso) or input_iso

    def get_default_lang_from_country(self, input_iso):
        country_iso = self.get_country_iso(input_iso)
        return self.country_default_lang.get(country_iso)

    def run_tests(self):
        assert self.get_language_iso("de") == "ger"
        assert self.get_language_iso("deu") == "ger"
        assert self.get_language_iso("ger") == "ger"
        assert self.get_country_iso("deu") == "de"
        assert self.get_country_iso("de") == "de"
        assert self.get_default_lang_from_country("deu") == "ger"
        assert self.get_default_lang_from_country("de") == "ger"
