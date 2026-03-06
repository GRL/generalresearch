import pytest

iphone_ua_string = (
    "Mozilla/5.0 (iPhone; CPU iPhone OS 5_1 like Mac OS X) AppleWebKit/534.46 (KHTML, like Gecko) "
    "Version/5.1 Mobile/9B179 Safari/7534.48.3"
)
ipad_ua_string = (
    "Mozilla/5.0(iPad; U; CPU iPhone OS 3_2 like Mac OS X; en-us) AppleWebKit/531.21.10 (KHTML, "
    "like Gecko) Version/4.0.4 Mobile/7B314 Safari/531.21.10"
)
windows_ie_ua_string = "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0)"
chromebook_ua_string = (
    "Mozilla/5.0 (X11; CrOS i686 0.12.433) AppleWebKit/534.30 (KHTML, like Gecko) "
    "Chrome/12.0.742.77 Safari/534.30"
)


class TestDeviceUA:
    def test_device_ua(self):
        from generalresearch.models import DeviceType
        from generalresearch.models.device import parse_device_from_useragent

        assert parse_device_from_useragent(iphone_ua_string) == DeviceType.MOBILE
        assert parse_device_from_useragent(ipad_ua_string) == DeviceType.TABLET
        assert parse_device_from_useragent(windows_ie_ua_string) == DeviceType.DESKTOP
        assert parse_device_from_useragent(chromebook_ua_string) == DeviceType.DESKTOP
        assert parse_device_from_useragent("greg bot") == DeviceType.UNKNOWN
