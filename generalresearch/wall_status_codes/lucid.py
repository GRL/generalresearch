"""
https://support.lucidhq.com/s/article/Lucid-Marketplace-Response-Codes
https://support.lucidhq.com/s/article/Client-Response-Codes
https://support.lucidhq.com/s/article/Collecting-Data-From-Redirects
"""

from collections import defaultdict
from typing import Optional, Tuple

from generalresearch.models.thl.definitions import Status, StatusCode1

mp_codes = {
    "-6": "Pre-Client Intermediary Page Drop Off",
    "-5": "Failure in the Post Answer Behavior",
    "-1": "Failure to Load the Lucid Marketplace",
    "1": "Currently in Screener or Drop",
    "3": "Respondent Sent to the Client Survey",
    "21": "Industry Lockout",
    "23": "Standard Qualification",
    "24": "Custom Qualification",
    "120": "Pre-Client Survey Opt Out",
    "122": "Return to Marketplace Opt Out",
    "123": "Max Client Survey Entries",
    "124": "Max Time in Router",
    "125": "Max Time in Router Warning Opt Out",
    "126": "Max Answer Limit",
    "30": "Unique IP",
    "31": "RelevantID Duplicate",
    "32": "Invalid Traffic",
    "35": "Supplier PID Duplicate",
    "36": "Cookie Duplicate",
    "37": "GEO IP Mismatch",
    "38": "RelevantID** Fraud Profile",
    "131": "Supplier Encryption Failure",
    "132": "Blocked PID",
    "133": "Blocked IP",
    "134": "Max Completes per Day Terminate",
    "138": "Survey Group Cookie Duplicate",
    "139": "Survey Group Supplier PID Duplicate",
    "230": "Survey Group Unique IP",
    "234": "Blocked Country IP",
    "236": "No Privacy Consent",
    "237": "Minimum Age",
    "238": "Found on Deny List",
    "240": "Invalid Browser",
    "241": "Respondent Threshold Limit",
    "242": "Respondent Quality Score",
    "243": "Marketplace Signature Check",
    "40": "Quota Full",
    "41": "Supplier Allocation",
    "42": "Survey Closed for Entry",
    "50": "CPI Below Supplier’s Rate Card",
    "98": "End of Router",
}

# todo: finish, there's a bunch more
client_status_map = {
    "30": StatusCode1.BUYER_QUALITY_FAIL,
    "33": StatusCode1.BUYER_QUALITY_FAIL,
    "34": StatusCode1.BUYER_QUALITY_FAIL,
    "35": StatusCode1.BUYER_QUALITY_FAIL,
}

status_map = defaultdict(lambda: Status.FAIL, **{"s": Status.COMPLETE})
status_codes_ext_map = {
    StatusCode1.COMPLETE: [],
    StatusCode1.BUYER_FAIL: ["3"],
    StatusCode1.BUYER_QUALITY_FAIL: [],
    StatusCode1.PS_BLOCKED: ["32", "132", "133", "234", "236", "237", "238", "242"],
    StatusCode1.PS_QUALITY: [
        "37",
        "38",
        "131",
        "132",
        "133",
        "234",
        "237",
        "238",
        "240",
        "243",
    ],
    StatusCode1.PS_DUPLICATE: ["21", "35", "36", "30", "31", "138", "139", "230"],
    StatusCode1.PS_FAIL: [
        "-6",
        "-5",
        "-1",
        "1",
        "120",
        "122",
        "125",
        "134",
        "23",
        "24",
        "123",
        "124",
        "236",
        "241",
        "50",
        "98",
        "126",
    ],
    StatusCode1.PS_OVERQUOTA: ["40", "41", "42"],
}
ext_status_code_map = dict()
for k, v in status_codes_ext_map.items():
    for vv in v:
        ext_status_code_map[status_codes_ext_map.get(vv, vv)] = k


def annotate_status_code(
    ext_status_code_1: str,
    ext_status_code_2: Optional[str] = None,
    ext_status_code_3: Optional[str] = None,
) -> Tuple:
    """
    :params ext_status_code_1: this indicates which callback url was hit. possible values {'s', *anything else*}
    :params ext_status_code_2: this is from the callback url params: InitialStatus
    :params ext_status_code_3: this is from the callback url params: ClientStatus
    returns: (status, status_code_1, status_code_2)
    """
    status = status_map[ext_status_code_1]
    if ext_status_code_2 == "3":
        status_code = client_status_map.get(ext_status_code_3, StatusCode1.BUYER_FAIL)
    else:
        status_code = ext_status_code_map.get(ext_status_code_2, StatusCode1.UNKNOWN)
    if status == Status.COMPLETE:
        status_code = StatusCode1.COMPLETE
    return status, status_code, None
