from user_agents import parse as parse_ua

from generalresearch.models import DeviceType


def parse_device_from_useragent(user_agent: str) -> DeviceType:
    ua = parse_ua(user_agent)
    if ua.is_mobile:
        return DeviceType.MOBILE
    elif ua.is_tablet:
        return DeviceType.TABLET
    elif ua.is_pc:
        return DeviceType.DESKTOP
    else:
        return DeviceType.UNKNOWN
