from enum import Enum

from generalresearch.utils.enum import ReprEnumMeta


class UserType(Enum, metaclass=ReprEnumMeta):
    # https://support.maxmind.com/hc/en-us/articles/4408430082971-IP-Trait-Risk-Data#h_01FN6V8JMQMWZGWNPPAW77ZPY4
    BUSINESS = "business"
    CAFE = "cafe"
    CELLULAR = "cellular"
    COLLEGE = "college"
    CDN = "content_delivery_network"
    CPN = "consumer_privacy_network"
    GOVERNMENT = "government"
    HOSTING = "hosting"
    LIBRARY = "library"
    MILITARY = "military"
    RESIDENTIAL = "residential"
    ROUTER = "router"
    SCHOOL = "school"
    SEARCH_ENGINE = "search_engine_spider"
    TRAVELER = "traveler"
