import hashlib
from enum import Enum
from typing import Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator
from typing_extensions import Self
from user_agents import parse as ua_parse
from user_agents.parsers import UserAgent


class BrowserFamily(str, Enum):
    CHROME_MOBILE = "Chrome Mobile"
    CHROME = "Chrome"
    CHROME_MOBILE_WEBVIEW = "Chrome Mobile WebView"
    MOBILE_SAFARI_UI_WKWEBVIEW = "Mobile Safari UI/WKWebView"
    MOBILE_SAFARI = "Mobile Safari"
    EDGE = "Edge"
    FIREFOX = "Firefox"
    SAMSUNG_INTERNET = "Samsung Internet"
    SAFARI = "Safari"
    CHROME_MOBILE_IOS = "Chrome Mobile iOS"
    OPERA = "Opera"
    EDGE_MOBILE = "Edge Mobile"
    MIUI_BROWSER = "MiuiBrowser"
    OPERA_MOBILE = "Opera Mobile"
    FIREFOX_MOBILE = "Firefox Mobile"
    GOOGLE = "Google"
    AMAZON_SILK = "Amazon Silk"
    FIREFOX_IOS = "Firefox iOS"
    YANDEX_BROWSER = "Yandex Browser"
    OTHER = "Other"


class OSFamily(str, Enum):
    ANDROID = "Android"
    WINDOWS = "Windows"
    IOS = "iOS"
    MAC_OSX = "Mac OS X"
    LINUX = "Linux"
    CHROME_OS = "Chrome OS"
    UBUNTU = "Ubuntu"
    OTHER = "Other"


class DeviceBrand(str, Enum):
    GENERIC_ANDROID = "Generic_Android"
    NONE = "None"
    APPLE = "Apple"
    SAMSUNG = "Samsung"
    OPPO = "Oppo"
    MOTOROLA = "Motorola"
    GENERIC_ANDROID_TABLET = "Generic_Android_Tablet"
    VIVO = "vivo"
    HUAWEI = "Huawei"
    XIAOMI = "XiaoMi"
    INFINIX = "Infinix"
    GENERIC = "Generic"
    NOKIA = "Nokia"
    GOOGLE = "Google"
    TECNO = "Tecno"
    AMAZON = "Amazon"
    ONE_PLUS = "OnePlus"
    LENOVO = "Lenovo"
    OTHER = "Other"


class DeviceModelFamily(str, Enum):
    NONE = "None"
    OTHER = "Other"
    K = "K"
    IPHONE = "iPhone"
    MAC = "Mac"
    IPAD = "iPad"


firefox_families = {
    BrowserFamily.FIREFOX,
    BrowserFamily.FIREFOX_MOBILE,
    BrowserFamily.FIREFOX_IOS,
}

safari_families = {
    BrowserFamily.SAFARI,
    BrowserFamily.MOBILE_SAFARI,
    BrowserFamily.MOBILE_SAFARI_UI_WKWEBVIEW,
}

chrome_families = {
    BrowserFamily.CHROME,
    BrowserFamily.CHROME_MOBILE,
    BrowserFamily.CHROME_MOBILE_IOS,
    BrowserFamily.CHROME_MOBILE_WEBVIEW,
}

mobile_families = {
    BrowserFamily.CHROME_MOBILE,
    BrowserFamily.CHROME_MOBILE_IOS,
    BrowserFamily.CHROME_MOBILE_WEBVIEW,
    BrowserFamily.MOBILE_SAFARI,
    BrowserFamily.MOBILE_SAFARI_UI_WKWEBVIEW,
    BrowserFamily.FIREFOX_MOBILE,
    BrowserFamily.FIREFOX_IOS,
}


class OSInfo(BaseModel):
    family: OSFamily = Field()
    version_string: Optional[str] = Field()

    @field_validator("family", mode="before")
    @classmethod
    def enum_or_other(cls, value: str) -> OSFamily:
        try:
            return OSFamily(value)
        except ValueError:
            return OSFamily.OTHER


class BrowserInfo(BaseModel):
    family: BrowserFamily = Field()
    version_string: Optional[str] = Field()

    @field_validator("family", mode="before")
    @classmethod
    def enum_or_other(cls, value: str) -> BrowserFamily:
        try:
            return BrowserFamily(value)
        except ValueError:
            return BrowserFamily.OTHER


class DeviceInfo(BaseModel):
    family: DeviceModelFamily = Field()
    brand: DeviceBrand = Field()
    model: DeviceModelFamily = Field()

    @field_validator("family", "model", mode="before")
    @classmethod
    def enum_or_other(cls, value: str) -> DeviceModelFamily:
        try:
            return (
                DeviceModelFamily(value)
                if value is not None
                else DeviceModelFamily.NONE
            )
        except ValueError:
            return DeviceModelFamily.OTHER

    @field_validator("brand", mode="before")
    @classmethod
    def enum_or_other2(cls, value: str) -> DeviceBrand:
        try:
            return DeviceBrand(value) if value is not None else DeviceBrand.NONE
        except ValueError:
            return DeviceBrand.OTHER


class GrlUserAgent(BaseModel):
    """
    The UserAgent library parses useragents, but does not enumerate possible
    values for anything. We go a step further here and have Enums for
    things like OS families, browsers, etc.
    """

    model_config = ConfigDict(
        extra="forbid", validate_assignment=True, arbitrary_types_allowed=True
    )

    ua_string: str = Field(description="The actual useragent string")
    ua_parsed: UserAgent = Field(
        description="UserAgent object. Internally is a bunch of namedtuples with strings."
    )

    os: OSInfo = Field()
    browser: BrowserInfo = Field()
    device: DeviceInfo = Field()

    # These 4 are determined by the UserAgent library, not be me.
    is_mobile: bool = Field()
    is_tablet: bool = Field()
    is_pc: bool = Field()
    is_bot: bool = Field()

    @property
    def ua_string_values(self) -> Dict[str, str]:
        # Returns the raw parsed string values for each of these. To be used
        #   for db filtering, identifying trends, etc.
        d = dict()
        d["ua_browser_family"] = self.ua_parsed.browser.family
        d["ua_browser_version"] = self.ua_parsed.browser.version_string
        d["ua_os_family"] = self.ua_parsed.os.family
        d["ua_os_version"] = self.ua_parsed.os.version_string
        d["ua_device_family"] = self.ua_parsed.device.family
        d["ua_device_brand"] = self.ua_parsed.device.brand
        d["ua_device_model"] = self.ua_parsed.device.model
        s = "|".join([str(x[1]) for x in sorted(d.items(), key=lambda x: x[0])])
        d["ua_hash"] = hashlib.md5(s.encode("utf-8")).hexdigest()
        return d

    @classmethod
    def from_ua_str(cls, user_agent: str) -> Self:
        ua = ua_parse(user_agent)
        return cls.model_validate(
            {
                "ua_string": user_agent,
                "ua_parsed": ua,
                "os": {
                    "family": ua.os.family,
                    "version_string": ua.os.version_string,
                },
                "browser": {
                    "family": ua.browser.family,
                    "version_string": ua.browser.version_string,
                },
                "device": {
                    "family": ua.device.family,
                    "brand": ua.device.brand,
                    "model": ua.device.model,
                },
                "is_mobile": ua.is_mobile,
                "is_tablet": ua.is_tablet,
                "is_pc": ua.is_pc,
                "is_bot": ua.is_bot,
            }
        )


class UserAgentHints(BaseModel):
    """The forensic post also includes output from the useragent hints API

    https://developer.mozilla.org/en-US/docs/Web/API/User-Agent_Client_Hints_API
    https://developer.chrome.com/docs/privacy-security/user-agent-client-hints
    """

    model_config = ConfigDict(
        extra="forbid", validate_assignment=True, populate_by_name=True
    )

    brands: Optional[List[Dict]] = Field(validation_alias="b", default=None)
    brands_full: Optional[List[Dict]] = Field(validation_alias="fv", default=None)
    mobile: bool = Field(validation_alias="m", default=False)
    model: Optional[str] = Field(validation_alias="md", default=None)
    platform: Optional[str] = Field(validation_alias="o", default=None)
    platform_version: Optional[str] = Field(validation_alias="ov", default=None)
    architecture: Optional[str] = Field(validation_alias="a", default=None)
    bitness: Optional[str] = Field(validation_alias="bt", default=None)
