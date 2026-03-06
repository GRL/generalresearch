import hashlib
import re
from collections import Counter
from datetime import datetime, timezone, timedelta
from enum import Enum
from functools import cached_property
from typing import Literal, Optional, Dict, List, Set, Any
from uuid import uuid4

import pycountry
from faker import Faker
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    StringConstraints,
    AfterValidator,
    AwareDatetime,
    NonNegativeInt,
)
from pydantic.json_schema import SkipJsonSchema
from pydantic_extra_types.timezone_name import TimeZoneName
from typing_extensions import Self, Annotated

from generalresearch.grliq.models import (
    AUDIO_CODEC_NAMES,
    VIDEO_CODEC_NAMES,
    SUPPORTED_FONTS,
)
from generalresearch.grliq.models.events import (
    PointerMove,
    TimingData,
    MouseEvent,
    KeyboardEvent,
)
from generalresearch.grliq.models.forensic_result import (
    GrlIqForensicCategoryResult,
    GrlIqCheckerResults,
)
from generalresearch.grliq.models.forensic_result import Phase
from generalresearch.grliq.models.useragents import (
    GrlUserAgent,
    OSFamily,
    UserAgentHints,
)
from generalresearch.models.custom_types import (
    UUIDStr,
    IPvAnyAddressStr,
    BigAutoInteger,
    AwareDatetimeISO,
    CountryISOLike,
)
from generalresearch.models.thl.ipinfo import GeoIPInformation
from generalresearch.models.thl.session import Session

fake = Faker()


class Platform(str, Enum):
    MAC_INTEL = "MacIntel"
    ARM = "ARM"
    IPAD = "iPad"
    IPHONE = "iPhone"
    WIN32 = "Win32"
    WIN64 = "Win64"
    LINUX_X86_64 = "Linux x86_64"
    LINUX_ARMV81 = "Linux armv81"
    LINUX_ARMV8l = "Linux armv8l"
    LINUX_ARMV7l = "Linux armv7l"
    LINUX_AARCH64 = "Linux aarch64"
    OTHER = "Other"


class PassFailError(str, Enum):
    PASS = "pass"
    FAIL = "fail"
    ERROR = "error"

    @classmethod
    def from_int_1_0_1(cls, v: str | int) -> Self:
        lookup = {1: cls.PASS, 0: cls.FAIL, -1: cls.ERROR}
        try:
            return cls(v)
        except ValueError:
            return lookup.get(int(v), cls.ERROR)

    @classmethod
    def from_int_2_1_0(cls, v: str | int) -> Self:
        try:
            return cls(v)
        except ValueError:
            return {2: cls.PASS, 1: cls.FAIL, 0: cls.ERROR, -1: cls.ERROR}[int(v)]


class SupportLevel(str, Enum):
    # Used for checking if certain features are available in the browser
    FULL = "full"
    PARTIAL = "partial"
    NONE = "none"

    @classmethod
    def from_int(cls, v: str | int) -> Self:
        try:
            return cls(v)
        except ValueError:
            return {2: cls.FULL, 1: cls.PARTIAL, 0: cls.NONE}[int(v)]


def check_valid_hex(v: str) -> str:
    if not all(char in "0123456789abcdefABCDEF" for char in v):
        raise ValueError("The hash128 must only contain hexadecimal characters.")
    return v


Hash128 = Annotated[
    str,
    StringConstraints(min_length=32, max_length=32),
    AfterValidator(check_valid_hex),
]


class GrlIqData(BaseModel):
    """Forensics JS POSTs ~200 pipe-separated fields to the fake SSO view. We
    parse that (along with a couple other fields) into this object.
    """

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    # --- Attributes on the db table directly ---

    id: Optional[BigAutoInteger] = Field(default=None, exclude=True)
    uuid: UUIDStr = Field(
        default_factory=lambda: uuid4().hex,
        description="A unique identifier for this data object",
        examples=[uuid4().hex],
    )

    mid: Optional[UUIDStr] = Field(
        description="The mid the of the User's attempt (thl-session) that "
        "is associated with this data",
        examples=[uuid4().hex],
    )
    phase: Optional[Phase] = Field(
        description="The phase of a thl-session in which this data was collected",
        default=Phase.OFFERWALL_ENTER,
    )
    product_id: Optional[UUIDStr] = Field(
        default=None,
        description="The Brokerage Product ID (BPID)",
        examples=[uuid4().hex],
    )
    product_user_id: Optional[str] = Field(
        default=None,
        description="The Brokerage Product User ID (BPUID).",
        examples=["test-user-2dbeaaf4"],
    )

    country_iso: CountryISOLike = Field(
        examples=["us"],
        description="This is the country that the offerwall was requested "
        "for. Looked up in the thl_session table, via the mid.",
    )

    client_ip: IPvAnyAddressStr = Field(
        description="This comes from the actual web request's headers",
        examples=["72.39.217.116"],
    )
    client_ip_detail: Optional[GeoIPInformation] = Field(default=None)

    created_at: AwareDatetimeISO = Field(
        description="When we actually received this data. The timestamp field "
        "below comes from the post body and could be manipulated "
        "by a baddie."
    )

    request_headers: Dict = Field(
        description="The full request headers from the actual HTTP call that was made."
    )

    # data: Dict = Field()
    # result_data: Dict = Field()
    # fraud_score: int = Field()
    # is_attempt_allowed: Optional[bool] = Field(default=None)
    results: Optional[GrlIqCheckerResults] = Field(default=None)
    category_result: Optional[GrlIqForensicCategoryResult] = Field(
        default=None, description="Saved in the database as a jsonb"
    )

    # --- Attributes in the json dict data field ---

    # Note: request_headers should contain origin (the webpage that made the request)
    #   and referer (URL of the previous page).

    # ---- Below here are parsed from the post body ----

    timezone_success: PassFailError = Field(description="Should always be 1")
    timezone: TimeZoneName = Field(examples=["America/Mexico_City"])
    timezone_offset: int = Field(
        description="timezone offset from utc in minutes", examples=[360]
    )
    calendar: str = Field(examples=["gregory"])
    numbering_system: str = Field(examples=["latn"])

    timestamp: AwareDatetime = Field(
        examples=["Fri Jan 10 2025 16:52:17 GMT-0600 (Central Standard Time)"]
    )

    user_agent_str: str = Field(
        examples=[
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
        ]
    )
    user_agent_str_2: Optional[str] = Field(
        description="This will only be set if different than user_agent_str"
    )
    user_agent_hints: Optional[UserAgentHints] = Field(
        description="Comes from the User-Agent Client Hints API", default=None
    )

    platform: Platform = Field(description="navigator.platform")
    platform_2: Optional[Platform] = Field(description="navigator.platform")
    platform_3: Optional[Platform] = Field()
    language: str = Field(examples=["en-US"])
    language_2: str = Field(examples=["en-US"])
    language_3: Optional[str] = Field()
    calender_locale: str = Field(examples=["en-US"])

    screen_width: NonNegativeInt = Field()
    screen_height: NonNegativeInt = Field()
    screen_avail_width: NonNegativeInt = Field()
    screen_avail_height: NonNegativeInt = Field()
    inner_width: NonNegativeInt = Field()
    inner_height: NonNegativeInt = Field()
    outer_width: NonNegativeInt = Field()
    outer_height: NonNegativeInt = Field()
    device_pixel_ratio: float = Field()
    color_depth_pixel_depth: str = Field()

    app_name: Literal["Netscape"] = Field(
        description="Navigator.appName. Always 'Netscape'"
    )
    product_sub: Optional[Literal["20030107", "20100101"]] = Field(
        description="Navigator.productSub"
    )
    vendor: Optional[Literal["Apple Computer, Inc.", "Google Inc.", "NAVER Corp."]] = (
        Field(description="Navigator.vendor.")
    )

    history_length: int = Field(description="window.history.length. Current tab only")

    webrtc_is_supported: PassFailError = Field()
    webrtc_error: bool = Field()
    webrtc_local_ip: str = Field()
    webrtc_ip: Optional[IPvAnyAddressStr] = Field(examples=[fake.ipv4_public()])
    webrtc_ip_detail: Optional[GeoIPInformation] = Field(default=None)

    hardware_concurrency: Optional[int] = Field(
        description="Sometimes this is an empty str"
    )
    hardware_concurrency_2: Optional[int] = Field()
    hardware_concurrency_3: Optional[int] = Field()

    # Browser/session properties
    navigator_java_enabled: bool = Field()
    do_not_track_enabled: str = Field(description="unspecified or '' ? ")
    mime_types_length: int = Field()
    # todo: some report actual values, some are (always) faked by the os/browser
    #  as anti-fingerprint 10737418240 (10gb) typical on firefox windows,
    #  2147483648 (20gb) chrome, iPhones typically only 8 different values
    #  android seems to show real, user-specific values.
    storage_estimate_quota: int = Field()
    navigator_cookieEnabled: bool = Field()

    # Browser properties
    rendering_engine: str = Field()
    graphics_api: str = Field()
    graphics_renderer: str = Field()

    eval_to_string_length: int = Field(
        description="eval.toString().length. Different in different browsers"
    )
    navigator_keys_len: int = Field(
        description="Object.keys(Object.getPrototypeOf(navigator)).length"
    )
    mozilla_web_app_exists: bool = Field(
        description="checking for presence of https://developer.mozilla.org/en-US/docs/Web/Progressive_web_apps"
    )
    microsoft_credentials_exists: bool = Field(
        description="checking for presence of microsoft credential manager"
    )
    window_external_exists: bool = Field(
        description="checking if window.external exists"
    )
    window_client_information: bool = Field(
        description="checking if window.clientInformation exists"
    )
    window_opera: bool = Field(description="checking if window.opera exists")
    window_chrome: bool = Field(description="checking if window.chrome exists")
    navigator_brave: bool = Field(description="checking if navigator.brave exists")
    window_active_x_object: bool = Field(
        description="checking if ActiveXObject in window"
    )
    no_edge_pdf_plugin: bool = Field(
        description="checking if we don't have a microsoft edge pdf plugin"
    )
    request_fs_exists: bool = Field(
        description="checking if window.requestFileSystem or webkitRequestFileSystem exists."
    )
    indexedDbData_available: bool = Field(
        description="indexedDbData available and functional."
    )
    localStorage_available: bool = Field(
        description="localStorage available and functional."
    )
    web_sql_exists: bool = Field(description="whether window.openDatabase exists")
    webgl_flag: bool = Field()
    webgl_check_1: bool = Field()
    window_installTrigger_exists: bool = Field()
    error_message: str = Field(
        description="Its checking what the browser's error message is"
    )
    math_result_1: str = Field()
    math_result_2: str = Field()
    # todo: validate per device
    speech_synthesis_voices_count: int = Field(description="221 for iphones")
    speech_synthesis_voice_1: str = Field()
    browser_by_properties: str = Field(
        description="looking for the presence of certain objects, to try to "
        "detect the browser. 'c' means chrome. Look in code for "
        "the list (function ee(n))"
    )
    indexedDbData_blob: bool = Field(
        description="whether blobs are supported in IndexedDB T/F"
    )
    plugins_hash: str = Field(
        description="pipe-sep hash|count of installed plugins. typically either "
        "'de355917bf33e0789539450797b843f9|5' (windows, iphone, mac) or '|0' (typically android). "
    )
    chrome_extensions: str = Field(description="comma sep str of chrome extensions")
    audio_codecs: Optional[str] = Field(
        examples=["1,1,1,1,1,3,1,3,1,3,3,1,1,3,3,3,3,1,3,3,3,2,1,1"],
        description="canPlayType: {'3': probably, '2': maybe, '1': no, 0: error}",
        min_length=47,
        max_length=47,
    )
    video_codecs: Optional[str] = Field(
        examples=["1,3,3,3,3,3,3,3,3,3,1,1,1,1,1,1,3,1,1,1,3,3,1"],
        description="canPlayType: {'3': probably, '2': maybe, '1': no, 0: error}",
        min_length=45,
        max_length=45,
    )

    # Browser Functionality Check
    session_storage_check: PassFailError = Field(
        description="If this doesn't work, something is probably manipulated"
    )
    cookie_check: str = Field(description="idk")
    audio_context_flag: PassFailError = Field(
        description="checking some audio buffer thing idk"
    )
    canvas_pixel_check: bool = Field()

    # Browser Automation Checks
    navigator_webdriver: bool = Field()
    webdriver_detected: bool = Field()
    webdriver_detected_msg: str = Field()
    non_native_function: bool = Field(description='checking for "[native code]"')
    non_native_function_flag: str = Field(
        description="comma-sep str", examples=["6,9,16"]
    )
    error_message_stack_access_count: int = Field()
    error_message_stack_access_count_worker: int = Field(
        description="I think should equal error_message_stack_access_count?"
    )

    # Device Properties
    ontouchstart: bool = Field()
    # todo: confirm this is 5 for an iphone
    max_touch_points: int = Field()
    navigator_deviceMemory: Optional[float] = Field()
    memory_jsHeapSizeLimit: int = Field()
    navigator_mediaDevices_len: int = Field()
    unmasked_vendor_webgl: str = Field()
    unmasked_renderer_webgl: str = Field()
    keyboard_detected: bool = Field()
    keyboard_layout_size: Optional[int] = Field(description="mobile safari None?")
    window_orientation: int = Field(description="0 or 1. idk which is which")

    # Session properties
    # todo: we should check this across POSTs for a user, b/c it should be
    #  *different* each (to make sure they aren't reusing posts)
    execution_time_ms: float = Field()
    performance_loop_time: float = Field()
    connection_rtt: Optional[int] = Field()
    connection_downlink: Optional[float] = Field()
    connection_type: str = Field()
    connection_effectiveType: str = Field()

    # fingerprint stuff
    canvas_support_level: SupportLevel = Field()
    canvas_hash: Optional[Hash128] = Field(
        description="dfiq's canvas image fingerprint"
    )
    canvas_hash_2: Optional[Hash128] = Field(
        description="simpler canvas image fingerprint stolen from amiunique.org",
        default=None,
    )
    webgl_hash: Optional[Hash128] = Field(
        description="DFIQ's version of webgl hash. It has stuff included in the hash: anisotropy, supported "
        "extensions, etc."
    )
    webgl_context: Optional[
        Literal[
            "webgl2",
            "webgl",
            "experimental-webgl2",
            "experimental-webgl",
            "webkit-3d",
            "moz-webgl",
        ]
    ] = Field(default=None)
    webgl_max_anisotropy: Optional[int] = Field(default=None, examples=[16])
    webgl_shading_language_version: Optional[str] = Field(
        default=None,
        examples=["WebGL GLSL ES 3.00 (OpenGL ES GLSL ES 3.0 Chromium)"],
    )
    webgl_hash_2: Optional[Hash128] = Field(
        description="hash128 of the canvas image, without additional stuff concatenated to it",
        default=None,
    )
    webgl_extensions: Optional[str] = Field(
        description="pipe-separated list of webgl extensions",
        default=None,
        examples=[
            "EXT_clip_control;EXT_color_buffer_float;EXT_color_buffer_half_float"
        ],
    )

    audio_context_hash: Optional[Hash128] = Field()
    audio_intensity_fingerprint: Optional[float] = Field()
    audio_compressor_reduction: Optional[float] = Field()
    speech_synthesis_voice_hash: Optional[Hash128] = Field()
    speech_synthesis_avail_voices_count: int = Field()
    path_fingerprint: int = Field(
        description="maybe is consistent? sum of pixel value of some path."
    )
    text_2d_fingerprint: Optional[Hash128] = Field()
    canvas_fingerprint: int = Field()

    # User Preferences
    color_gamut: Optional[Literal["1", "2", "3", "0"]] = Field(
        description="{'1': 'rec2020', '2':'p3', '3':'srgb', '0': none}  # p3 typically used in macbooks n stuff"
    )
    prefers_contrast: Optional[Literal["0", "1", "2", "3", "4", "5", "9"]] = Field(
        description="{'no-preference': 0, 'high': 1, 'more': 2, 'low': 3, 'less': 4, 'forced': 5, None: 9}"
    )
    prefers_reduced_motion: bool = Field(description="reduce (1) vs no-preference (0)")
    dynamic_range: bool = Field(description="high (1) vs standard (0)")
    inverted_colors: bool = Field(description="{'inverted': 1, 'none': 0}")
    forced_colors: bool = Field(description="{'active': 1, 'none': 0}")
    prefers_color_scheme: bool = Field(description="{'dark': 1, '?': 0}")

    # Battery Info
    battery_charging: Optional[bool] = Field(default=None)
    battery_charging_time: Optional[float] = Field(default=None)
    battery_discharging_time: Optional[float] = Field(default=None)
    battery_level: Optional[float] = Field(default=None, ge=0, le=1)

    supported_fonts_str: Optional[str] = Field(
        default=None,
        description="Bit-packed string for font support. Each element is 32 bits, with each bit representing T/F for "
        "font support.",
        examples=[
            "72|768|262144|1073741824|0|0|540672|73728|7340032|1342177280|117446656|256|16|0|543|4290797636"
            "|1677723648|4168998400|0|1048576|262144|268500994|1342177280|262144|125829376|37888000|0|435363842|0"
            "|2147483648|109543424|1880099872|268435471"
        ],
    )

    # Time it took for the client to download the logo.jpg
    logo_download_ms: Optional[float] = Field(default=None, gt=0)

    # --- Not from post body ----

    prefetched: SkipJsonSchema[bool] = Field(
        default=False, description="Has prefetch been run?"
    )

    # Can optionally be loaded from the grliq_forensicevents table
    events: Optional[List[Dict]] = Field(default=None)
    pointer_move_events: Optional[List[PointerMove]] = Field(default=None)
    mouse_events: Optional[List[MouseEvent]] = Field(default=None)
    keyboard_events: Optional[List[KeyboardEvent]] = Field(default=None)

    timing_data: Optional[TimingData] = Field(default=None)

    @property
    def session_uuid(self) -> Optional[UUIDStr]:
        return self.mid

    @cached_property
    def useragent(self) -> GrlUserAgent:
        return GrlUserAgent.from_ua_str(self.user_agent_str)

    @cached_property
    def fingerprint_keys(self) -> List[str]:
        fp_cols = [
            "country_iso",
            "canvas_hash",
            "canvas_hash_2",
            # This is removed in favor of webgl_hash_2, which is just the image hash
            # "webgl_hash",
            "webgl_hash_2",
            "audio_context_hash",
            "audio_intensity_rounded",
            "audio_compressor_reduction",
            "speech_synthesis_voice_hash",
            "speech_synthesis_avail_voices_count",
            "path_fingerprint",
            "text_2d_fingerprint",
            "canvas_fingerprint",
            "device_pixel_ratio",
            "storage_estimate_quota",
            "audio_codecs",
            "video_codecs",
            "color_gamut",
            "prefers_contrast",
            "prefers_reduced_motion",
            "dynamic_range",
            "inverted_colors",
            "forced_colors",
            "prefers_color_scheme",
        ]
        if self.useragent.os.family in {OSFamily.IOS, OSFamily.MAC_OSX}:
            fp_cols += ["screen_width", "screen_height"]
        return fp_cols

    @cached_property
    def fingerprint(self) -> str:
        s = "|".join(map(str, [getattr(self, k) for k in self.fingerprint_keys]))
        return hashlib.md5(s.encode()).hexdigest()

    @cached_property
    def audio_codecs_named(self) -> Dict:
        return dict(
            zip(
                AUDIO_CODEC_NAMES,
                [True if x == "3" else False for x in self.audio_codecs.split(",")],
            )
        )

    @cached_property
    def video_codecs_named(self) -> Dict:
        return dict(
            zip(
                VIDEO_CODEC_NAMES,
                [True if x == "3" else False for x in self.video_codecs.split(",")],
            )
        )

    @cached_property
    def supported_fonts_binary(self) -> str:
        return "".join(
            [
                format(int(packed_int), "032b")
                for packed_int in self.supported_fonts_str.split("|")
            ]
        )[-len(SUPPORTED_FONTS) :]

    @cached_property
    def supported_fonts(self) -> Set[str]:
        return {
            f for x, f in zip(self.supported_fonts_binary, SUPPORTED_FONTS) if x == "1"
        }

    @cached_property
    def audio_intensity_rounded(self) -> Optional[float]:
        # The audio intensity fingerprint seems to be purposely manipulated
        # to add randomness, but the level of randomness if very low, past
        # the 6th decimal point.
        return (
            round(self.audio_intensity_fingerprint, 6)
            if self.audio_intensity_fingerprint
            else None
        )

    @cached_property
    def event_type_count(self) -> Counter:
        return Counter([x["type"] for x in self.events])

    # @field_validator(
    #     "hardware_concurrency",
    #     mode="before",
    # )
    # @classmethod
    # def str_to_int(cls, value: str) -> int:
    #     return int(value)

    @field_validator(
        "hardware_concurrency",
        "hardware_concurrency_2",
        "hardware_concurrency_3",
        "language_3",
        "connection_rtt",
        "keyboard_layout_size",
        mode="before",
    )
    @classmethod
    def str_to_int_or_null(cls, value: str) -> Optional[int]:
        return int(value) if value not in {None, ""} else None

    @field_validator(
        "connection_downlink",
        "audio_intensity_fingerprint",
        "audio_compressor_reduction",
        "navigator_deviceMemory",
        mode="before",
    )
    @classmethod
    def str_to_float_or_null(cls, value: str) -> Optional[int]:
        return float(value) if value not in {None, ""} else None

    @field_validator(
        "vendor",
        "product_sub",
        "speech_synthesis_voice_hash",
        "webgl_hash",
        "canvas_hash",
        "audio_context_hash",
        "text_2d_fingerprint",
        "audio_codecs",
        "video_codecs",
        mode="before",
    )
    @classmethod
    def str_or_null(cls, value: str) -> Optional[str]:
        return value or None

    @field_validator(
        "webrtc_error",
        "no_edge_pdf_plugin",
        "indexedDbData_blob",
        "keyboard_detected",
        "non_native_function",
        "navigator_brave",
        "window_chrome",
        "navigator_webdriver",
        "webgl_check_1",
        "window_installTrigger_exists",
        "navigator_cookieEnabled",
        "window_active_x_object",
        "window_opera",
        "window_external_exists",
        "microsoft_credentials_exists",
        "mozilla_web_app_exists",
        "webdriver_detected",
        "canvas_pixel_check",
        "prefers_reduced_motion",
        "dynamic_range",
        "inverted_colors",
        "forced_colors",
        "prefers_color_scheme",
        mode="before",
    )
    @classmethod
    def str_to_bool(cls, value: str) -> bool:
        return bool(int(value))

    @field_validator(
        "request_fs_exists",
        "indexedDbData_available",
        "localStorage_available",
        "webgl_flag",
        mode="before",
    )
    @classmethod
    def str_to_bool_2_1(cls, value: str | bool) -> bool:
        # 2 is True, 1 is False !!!! (why?)
        if isinstance(value, str):
            return bool(int(value) - 1)
        else:
            return value

    @field_validator("timezone_success", "webrtc_is_supported", mode="before")
    @classmethod
    def pass_fail_error(cls, value: int) -> PassFailError:
        if not isinstance(value, PassFailError):
            return PassFailError.from_int_1_0_1(value)

    @field_validator("session_storage_check", "audio_context_flag", mode="before")
    @classmethod
    def pass_fail_error_210(cls, value: int) -> PassFailError:
        if not isinstance(value, PassFailError):
            return PassFailError.from_int_2_1_0(value)

    @field_validator("canvas_support_level", mode="before")
    @classmethod
    def support_level(cls, value: int) -> SupportLevel:
        if not isinstance(value, SupportLevel):
            return SupportLevel.from_int(value)

    @field_validator("country_iso")
    @classmethod
    def validate_country_iso(cls, value: str) -> str:
        if not pycountry.countries.get(alpha_2=value.lower()):
            raise ValueError(f"{value} is not a valid ISO 3166-1 alpha-2 country code.")
        return value.lower()

    @field_validator("platform", "platform_2", "platform_3", mode="before")
    @classmethod
    def platform_enum_or_other(cls, value: Optional[str]) -> Optional[Platform]:
        if value is None or value == "":
            return None
        try:
            return Platform(value)
        except ValueError:
            return Platform.OTHER

    @field_validator("webrtc_ip", mode="before")
    @classmethod
    def preprocess_ip(cls, ip: str) -> Optional[str]:
        # Strip square brackets if present
        return re.sub(r"^\[|\]$", "", ip) if ip else None

    # Doesn't work. "vi" is a valid value (en-US)
    # @field_validator("language")
    # @classmethod
    # def validate_language_country(cls, value: str) -> str:
    #     language_code, country_code = value.split("-")
    #     try:
    #         pycountry.languages.get(alpha_2=language_code)
    #     except KeyError:
    #         raise ValueError(f"Invalid language code '{language_code}'.")
    #     try:
    #         pycountry.countries.get(alpha_2=country_code.upper())
    #     except KeyError:
    #         raise ValueError(f"Invalid country code '{country_code.upper()}'.")
    #     return value

    @field_validator("timestamp", mode="before")
    @classmethod
    def parse_timestamp(cls, value: str) -> str | datetime:
        if isinstance(value, datetime):
            return value
        if "GMT" not in value:
            return value
        try:
            value = value.split("(")[0].strip()
            return datetime.strptime(value, "%a %b %d %Y %H:%M:%S GMT%z")
        except ValueError:
            raise ValueError(f"Invalid date format: {value}")

    def validate_with_session(self, session: Session) -> None:
        # product_id and product_user_id are parsed from the post body. make sure
        #   they match the session whose mid was specified
        assert self.product_id == session.user.product_id, "product_id mismatch"
        assert (
            self.product_user_id == session.user.product_user_id
        ), "product_user_id mismatch"

        # validate the Session's mid is "recent"
        assert (datetime.now(tz=timezone.utc) - session.started) < timedelta(
            minutes=90
        ), "expired session"

        return None

    def model_dump_sql(self, **kwargs) -> Dict[str, Any]:
        d = dict()
        d["uuid"] = self.uuid
        d["session_uuid"] = self.mid
        d["created_at"] = self.created_at
        d.update(self.useragent.ua_string_values)
        d["data"] = self.model_dump_json(**kwargs)
        keys = [
            "client_ip",
            "country_iso",
            "product_id",
            "product_user_id",
            "phase",
        ]

        for k in keys:
            d[k] = getattr(self, k)

        return d

    @classmethod
    def from_db(cls, d: Dict) -> Self:
        res = GrlIqData.model_validate(d["data"])

        if d.get("category_result"):
            res.category_result = GrlIqForensicCategoryResult.model_validate(
                d["category_result"]
            )

        if d.get("result_data"):
            res.results = GrlIqCheckerResults.model_validate(d["result_data"])

        return res
