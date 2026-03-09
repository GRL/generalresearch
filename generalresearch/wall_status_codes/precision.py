"""
https://integrations.precisionsample.com/api.html#survey%20status%20ID's

Possible statuses: {'s', 't', 'q', 'r', 'f'}
s - complete, t - failed ot terminated, q - quota full, r - rejected,
f - client approved the Preliminary complete as Final Complete
"""

from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple

from generalresearch.models.thl.definitions import Status, StatusCode1

status_codes_precision: Dict[str, str] = {
    "10": "Complete",
    "20": "Client Terminate",
    "21": "PS Terminate",
    "22": "PS Terminate - Device Fail",
    "23": "PS Terminate - Survey Closed",
    "24": "PS Terminate - Recaptcha Fail",
    "25": "PS Terminate - Exclusion Click",
    "30": "Client Over Quota",
    "31": "PS Over Quota",
    "32": "PS Over Quota - Allocation Full",
    "40": "Quality Sentinel Fail - Publisher Dupe",
    "41": "Quality Sentinel Fail - Survey Dupe",
    "42": "Quality Sentinel Fail - Back Button",
    "43": "Quality Sentinel Fail - Verity",
    "44": "Quality Sentinel Fail - Research Defender Dupe",
    "45": "Quality Sentinel Fail - Research Defender Fraud",
    "46": "Quality Sentinel Fail - Geo-Validation Fail",
    "47": "Quality Sentinel Fail - Fraud Member",
    "48": "Quality Sentinel Fail-Postal Code Country Mismatche",
    "50": "Ghost Complete",
    "51": "Ghost Complete - Math Validation Failed",
    "52": "Ghost Complete - Encryption Fail",
    "53": "Ghost Complete - Prescreen Skip",
    "54": "Ghost Complete - Default Complete Lin",
    "55": "Ghost Complete - Old URL",
    "56": "Ghost Complete - Wrong Guid",
    "70": "Quality Sentinel Fail - Sentry Tech Fail",
    "71": "Quality Sentinel Fail - Sentry Behavioral Fail",
    # These are secondary statuses and shouldn't be in the wall, but
    # just in case precision doesn't validate
    "60": "Client Reject",
    "80": "Final Complete",
}
status_map = defaultdict(lambda: Status.FAIL, **{"s": Status.COMPLETE})
status_codes_ext_map: Dict[StatusCode1, List[str]] = {
    StatusCode1.COMPLETE: ["10"],
    StatusCode1.BUYER_FAIL: ["20", "30"],
    StatusCode1.BUYER_QUALITY_FAIL: ["60"],
    StatusCode1.PS_BLOCKED: ["44"],
    StatusCode1.PS_QUALITY: [
        "24",
        "43",
        "45",
        "46",
        "47",
        "48",
        "50",
        "51",
        "52",
        "53",
        "54",
        "55",
        "56",
        "60",
        "70",
        "71",
    ],
    StatusCode1.PS_DUPLICATE: ["40", "41", "42", "25"],
    StatusCode1.PS_FAIL: ["21", "22"],
    StatusCode1.PS_OVERQUOTA: ["31", "32", "23"],
}
ext_status_code_map = dict()
for k, v in status_codes_ext_map.items():
    k: StatusCode1
    v: List[str]

    for vv in v:
        vv: str
        ext_status_code_map[status_codes_ext_map.get(vv, vv)] = k


def annotate_status_code(
    ext_status_code_1: str,
    ext_status_code_2: Optional[str] = None,
    ext_status_code_3: Optional[str] = None,
) -> Tuple[Status, StatusCode1, Optional[Any]]:
    """
    :params ext_status_code_1: from callback url params: status
    :params ext_status_code_2: from callback url params: code
    :params ext_status_code_3: not used

    returns: (status, status_code_1, status_code_2)
    """
    status = status_map[ext_status_code_1]
    status_code = ext_status_code_map.get(ext_status_code_2, StatusCode1.UNKNOWN)
    return status, status_code, None
