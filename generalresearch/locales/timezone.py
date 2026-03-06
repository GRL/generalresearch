from typing import Optional

from pytz import country_timezones


def get_default_timezone(country_iso: str) -> Optional[str]:
    # to list all:
    # from pytz import country_names, country_timezones
    # [country_timezones.get(country) for country in country_names]

    # country_iso can be upper or lower, doesn't matter
    return country_timezones.get(country_iso, [None])[0]


# There is no official list for this ....
country_default_locale = {
    "af": "fa-AF",
    "al": "sq-AL",
    "dz": "ar-DZ",
    "ar": "es-AR",
    "au": "en-AU",
    "at": "de-AT",
    "br": "pt-BR",
    "ca": "en-CA",
    "cn": "zh-CN",
    "eg": "ar-EG",
    "fr": "fr-FR",
    "de": "de-DE",
    "in": "hi-IN",
    "jp": "ja-JP",
    "ke": "sw-KE",
    "mx": "es-MX",
    "ru": "ru-RU",
    "kr": "ko-KR",
    "gb": "en-GB",
    "us": "en-US",
    "lt": "lt-LT",
    "lu": "lb-LU",
    "mg": "mg-MG",
    "my": "ms-MY",
    "mv": "dv-MV",
    "ml": "fr-ML",
    "mt": "mt-MT",
    "mn": "mn-MN",
    "ma": "ar-MA",
    "np": "ne-NP",
    "nl": "nl-NL",
    "nz": "en-NZ",
    "ng": "en-NG",
    "no": "no-NO",
    "pk": "ur-PK",
    "pa": "es-PA",
    "pe": "es-PE",
    "ph": "tl-PH",
    "pl": "pl-PL",
    "pt": "pt-PT",
    "qa": "ar-QA",
    "ro": "ro-RO",
    "sa": "ar-SA",
    "sg": "en-SG",
    "za": "en-ZA",
    "es": "es-ES",
    "lk": "si-LK",
    "se": "sv-SE",
    "ch": "de-CH",
    "th": "th-TH",
    "tr": "tr-TR",
    "ua": "uk-UA",
    "ae": "ar-AE",
    "vn": "vi-VN",
    "zw": "en-ZW",
}


def get_default_locale(country_iso: str) -> Optional[str]:
    # todo: "https://cdn.simplelocalize.io/public/v1/locales"   to fill in the rest?
    return country_default_locale.get(country_iso, None)
