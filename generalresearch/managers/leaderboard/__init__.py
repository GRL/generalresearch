from typing import Dict
from zoneinfo import ZoneInfo

import pytz
from cachetools import cached, LRUCache


@cached(cache=LRUCache(maxsize=1))
def country_timezone() -> Dict[str, ZoneInfo]:
    """
    Most countries only have 1 tz. I am picking the most populous for the rest.
    A timezone is unique for a country, as in America/New_York and America/Toronto
    both have the same UTC offset, but one is for US only and one is for CA only,
    Each timezone is unique per country.
    """
    ct = dict(pytz.country_timezones)
    ct = {k: v[0] for k, v in ct.items()}
    ct["US"] = "America/New_York"
    ct["AR"] = "America/Argentina/Buenos_Aires"
    ct["AU"] = "Australia/Sydney"
    ct["BR"] = "America/Sao_Paulo"
    ct["CA"] = "America/Toronto"
    ct["CL"] = "America/Santiago"
    ct["CN"] = "Asia/Shanghai"
    ct["DE"] = "Europe/Berlin"
    ct["EC"] = "America/Guayaquil"
    ct["ES"] = "Europe/Madrid"
    ct["ID"] = "Asia/Jakarta"
    ct["KZ"] = "Asia/Almaty"
    ct["MX"] = "America/Mexico_City"
    ct["MY"] = "Asia/Kuala_Lumpur"
    ct["NZ"] = "Pacific/Auckland"
    ct["PT"] = "Europe/Lisbon"
    ct["RU"] = "Europe/Moscow"
    ct["UA"] = "Europe/Kiev"
    ct = {k.lower(): ZoneInfo(v) for k, v in ct.items()}
    return ct
