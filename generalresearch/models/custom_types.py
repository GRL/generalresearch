import json
from datetime import datetime, timedelta, timezone
from typing import Any, Literal, Optional, Set
from uuid import UUID

from pydantic import (
    AnyUrl,
    AwareDatetime,
    Field,
    HttpUrl,
    IPvAnyAddress,
    StringConstraints,
    TypeAdapter,
)
from pydantic.functional_serializers import PlainSerializer
from pydantic.functional_validators import AfterValidator, BeforeValidator
from pydantic.networks import UrlConstraints, IPvAnyNetwork
from pydantic_core import Url
from typing_extensions import Annotated

from generalresearch.models import DeviceType, Source

# if TYPE_CHECKING:
#     from generalresearch.models import DeviceType


def convert_datetime_to_iso_8601_with_z_suffix(dt: datetime) -> str:
    # By default, datetimes are serialized with the %f optional. We don't
    # want that because then the deserialization fails if the datetime
    # didn't have microseconds.
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def convert_str_dt(v: Any) -> Optional[AwareDatetime]:
    # By default, pydantic is unable to handle tz-aware isoformat str. Attempt
    # to parse a str that was dumped using the iso8601 format with Z suffix.
    if v is not None and type(v) is str:
        assert v.endswith("Z") and "T" in v, "invalid format"
        return datetime.strptime(v, "%Y-%m-%dT%H:%M:%S.%fZ").replace(
            tzinfo=timezone.utc
        )
    return v


def assert_utc(v: AwareDatetime) -> AwareDatetime:
    if isinstance(v, datetime):
        # We need utcoffset b/c FastAPI parses datetimes using FixedTimezone
        assert v.tzinfo == timezone.utc or v.tzinfo.utcoffset(v) == timedelta(
            0
        ), "Timezone is not UTC"
        v = v.astimezone(timezone.utc)
    return v


InclExcl = Literal["exclude", "include"]

# Our custom AwareDatetime that correctly serializes and deserializes
#   to an ISO8601 str with timezone
AwareDatetimeISO = Annotated[
    AwareDatetime,
    BeforeValidator(convert_str_dt),
    AfterValidator(assert_utc),
    PlainSerializer(
        lambda x: x.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        when_used="json-unless-none",
    ),
]

# ISO 3166-1 alpha-2 (two-letter codes, lowercase)
# "Like" b/c it matches the format, but we're not explicitly checking
#   it is one of our supported values. See models.thl.locales for that.
CountryISOLike = Annotated[
    str, StringConstraints(max_length=2, min_length=2, pattern=r"^[a-z]{2}$")
]
# 3-char ISO 639-2/B, lowercase
LanguageISOLike = Annotated[
    str, StringConstraints(max_length=3, min_length=3, pattern=r"^[a-z]{3}$")
]


def check_valid_uuid(v: str) -> str:
    try:
        assert UUID(v).hex == v
    except Exception:
        raise ValueError("Invalid UUID")
    return v


def is_valid_uuid(v: str) -> bool:
    try:
        assert UUID(v).hex == v
    except Exception:
        return False
    return True


# Our custom field that stores a UUID4 as the .hex string representation
UUIDStr = Annotated[
    str,
    StringConstraints(min_length=32, max_length=32),
    AfterValidator(check_valid_uuid),
]
# Accepts the non-hex representation and coerces
UUIDStrCoerce = Annotated[
    str,
    StringConstraints(min_length=32, max_length=32),
    BeforeValidator(lambda value: TypeAdapter(UUID).validate_python(value).hex),
    AfterValidator(check_valid_uuid),
]

# Same thing as UUIDStr with HttpUrl field. It is confusing that this
# is not a str https://github.com/pydantic/pydantic/discussions/6395
HttpUrlStr = Annotated[
    str,
    BeforeValidator(lambda value: str(TypeAdapter(HttpUrl).validate_python(value))),
]

HttpsUrl = Annotated[Url, UrlConstraints(max_length=2083, allowed_schemes=["https"])]
HttpsUrlStr = Annotated[
    str,
    BeforeValidator(lambda value: str(TypeAdapter(HttpsUrl).validate_python(value))),
]

# Same thing as UUIDStr with IPvAnyAddress field
IPvAnyAddressStr = Annotated[
    str,
    BeforeValidator(
        lambda value: str(TypeAdapter(IPvAnyAddress).validate_python(value).exploded)
    ),
]
IPvAnyNetworkStr = Annotated[
    str,
    BeforeValidator(
        lambda value: str(TypeAdapter(IPvAnyNetwork).validate_python(value))
    ),
]


def coerce_int_to_str(data: Any) -> Any:
    """Transform input int to str, return other types as is"""
    if isinstance(data, int):
        return str(data)
    return data


# This is a string field, but accepts integers that can be coerced into strings.
CoercedStr = Annotated[str, BeforeValidator(coerce_int_to_str)]

# Serializers that can transform a collection of str into a comma separated
# str bidirectionally
to_comma_sep_str = PlainSerializer(lambda x: ",".join(sorted(list(x))), return_type=str)
enum_to_comma_sep_str = PlainSerializer(
    lambda x: ",".join(sorted([str(y.value) for y in x])), return_type=str
)
from_comma_sep_str = BeforeValidator(
    lambda x: set(x.split(",") if x != "" else []) if isinstance(x, str) else x
)

# This is a set of DeviceType, that serializes and de-serializes into a
# (sorted) comma-separated str
DeviceTypes = Annotated[Set[DeviceType], enum_to_comma_sep_str, from_comma_sep_str]

# This is a set of alphanumeric strings, that serializes and de-serializes
# into a (sorted) comma-separated str
AlphaNumStr = Annotated[str, StringConstraints(max_length=32, min_length=1)]

# a string like an IP address, but we don't need to validate that it is
# actually an IP address.
IPLikeStr = Annotated[str, StringConstraints(max_length=39, min_length=2)]


def assert_dask_auth(v: Url) -> Url:
    # Even if we're using tls and a SSL cert, Dask doesn't have the concept
    # of user authentication
    assert [v.username, v.password] == [
        None,
        None,
    ], "User & Password are not supported"
    return v


def assert_sentry_auth(v: Url) -> Url:
    assert v.username, "Sentry URL requires a user key"
    assert len(v.username) > 10, "Sentry user key seems bad"
    assert v.password is None, "Sentry password is not supported"
    assert int(v.path[1:]), "Sentry project id needs to be a number (I think)"
    assert v.port == 443, "https required"
    assert v.fragment is None
    return v


SentryDsn = Annotated[
    Url,
    UrlConstraints(
        allowed_schemes=["https"],
        default_host="ingest.us.sentry.io",
        default_port=443,
    ),
    AfterValidator(assert_sentry_auth),
]

MySQLOrMariaDsn = Annotated[
    AnyUrl,
    UrlConstraints(allowed_schemes=["mysql", "mariadb"]),
]

DaskDsn = Annotated[
    Url,
    UrlConstraints(
        allowed_schemes=["tcp", "tls"],
        default_host="127.0.0.1",
        default_port=8786,
    ),
    AfterValidator(assert_dask_auth),
]

InfluxDsn = Annotated[
    Url,
    UrlConstraints(
        allowed_schemes=["influxdb"],
        default_host="127.0.0.1",
        default_port=8086,
    ),
]

AlphaNumStrSet = Annotated[Set[AlphaNumStr], to_comma_sep_str, from_comma_sep_str]
IPLikeStrSet = Annotated[Set[IPLikeStr], to_comma_sep_str, from_comma_sep_str]
UUIDStrSet = Annotated[Set[UUIDStr], to_comma_sep_str, from_comma_sep_str]

list_models_to_json_str = PlainSerializer(
    lambda x: json.dumps([y.model_dump(mode="json") for y in x]),
    return_type=str,
    when_used="json",
)
json_str_to_model = BeforeValidator(
    lambda x: json.loads(x) if isinstance(x, str) else x
)
json_str_to_set = BeforeValidator(
    lambda x: set(json.loads(x)) if isinstance(x, str) else x
)

EnumNameSerializer = PlainSerializer(
    lambda e: e.name, return_type="str", when_used="unless-none"
)

# These are used to make it explicit which attributes are pk/fk values.
BigAutoInteger = Annotated[int, Field(strict=True, gt=0, lt=9223372036854775807)]


def validate_survey_key(v: str) -> str:
    """
    Variously called a Survey.natural_key or in Web3.0 language a CURIE
    """
    # Must contain exactly one colon
    if v.count(":") != 1:
        raise ValueError("survey_key must be '<source>:<survey_id>'")

    source, survey_id = v.split(":", 1)

    try:
        Source(source)
    except ValueError:
        raise ValueError(f"invalid source '{source}'")

    if not (1 <= len(survey_id) <= 32):
        raise ValueError("survey_id must be 1–32 characters")

    return v


SurveyKey = Annotated[
    str,
    StringConstraints(
        min_length=3,  # 1-char source: "c:x"
        max_length=35,  # 2-char source: "tt:" + 32
    ),
    AfterValidator(validate_survey_key),
]

PropertyCode = Annotated[
    str,
    StringConstraints(
        min_length=3,  # 1-char source: "c:x"
        max_length=64,  # DB max field length
        pattern=r"^[a-z]{1,2}\:.*",
    ),
]


def now_utc_factory():
    return datetime.now(tz=timezone.utc)
