"""
https://developer-beta.market-cube.com/api-details#api=definition-api&operation=get-api-v1-definition-return-status-list
"""

from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple

from generalresearch.models.thl.definitions import Status, StatusCode1

status_codes_schlesinger: Dict[str, str] = {
    "1": "Complete",
    "2": "Buyer Fail",
    "3": "Buyer Fail",
    "4": "Buyer Fail",
    "6": "PS Quality Term",  # includes Duplicate
    "8": "PS Term",
    "10": "PS Over Quota",
    "15": "Buyer Fail",  # not in documentation
    "0": "Abandon",  # really it is "PS Abandon"
    "11": "Abandon",  # really it is "Buyer Abandon"
}

status_reason_name: Dict[str, str] = {
    "1": "Not a Unique Sample Cube User",
    "4": "GeoIP - wrong country",
    "7": "Duplicate - not a unique IP",
    "9": "Security Terminate - supplier not allocated to the survey",
    "16": "Client redirect - SHA-1 mismatch",
    "18": "Supplier entry - encryption failure",
    "22": "Trap question failure",
    "26": "Terminate - Min LOI logic",
    "27": "Terminate - Max LOI logic",
    "29": "Security Terminate - Survey closed",
    "30": "Recontact - wrong PID user",
    "31": "Security Terminate - financial termination",
    "33": "Security Terminate - Unique Link error",
    "34": "Partial Complete - SOI",
    "35": "Terminate - survey does not allow desktop devices",
    "36": "Terminate - survey does not allow mobile devices",
    "37": "Recontact - terminate",
    "38": "Terminate - quality check on multi-punch",
    "39": "Reconciled - terminate",
    "40": "Reconciled - complete",
    "41": "Already a Complete",
    "42": "RelevantID - duplicate attempt",
    "43": "RelevantID - fraud profile score too high",
    "44": "RelevantID - wrong country",
    "45": "RelevantID - call failed",
    "46": "Sample Cube - overquota",
    "47": "Terminate - demographic qualifications",
    "48": "Complete - not reconciled",
    "49": "Client - overquota",
    "50": "Terminate - client side logic",
    "51": "Drop Out on landing page",
    "52": "Security terminate - client side logic",
    "54": "Client redirect - drop out / in progress",
    "55": "MaxMind - IP blacklisted",
    "56": "Terminate - survey does not allow tablet devices",
    "57": "GeoIP - State check, US only",
    "58": "Drop Out on qualifications",
    "60": "Duplicate - survey group unique IP",
    "61": "Duplicate - survey group unique SID",
    "62": "Duplicate - survey group RelevantID dupe",
    "63": "Security Terminate - exceeded supplier allocation",
    "64": "Terminate - custom demographic qualifications",
    "65": "Client - entry encryption error",
    "66": "RelevantID - internal failure",
    "67": "Bot detected",
    "68": "Client redirect - secret value missing",
    "69": "Complete - supplier reservation",
    "70": "Client - authentication error",
    "71": "Terminate - blocked IE browser version",
    "72": "Duplicate - device user ID check",
    "73": "Duplicate - survey group Device User ID dupe",
    "74": "AgeDemoTerminateInconsistency",
    "75": "Terminate - inconsistent gender",
    "76": "Terminate - inconsistent zip",
    "77": "Drop Out before client entry",
    "78": "Not live survey completion",
    "79": "LinkedIn - failed to login",
    "80": "Linkedin - drop out on login",
    "81": "Client redirect - S2S not fired",
    "82": "Bad User",
    "84": "Security Terminate - Speeder",
    "85": "Sample Chain - fraudster",
    "86": "Sample Chain - wrong country",
    "87": "Sample Chain - duplicate",
    "88": "Terminate - demo terminate on advanced logic",
    "89": "Sample Chain - survey group duplicate",
    "90": "Duplicate - client logic",
    "91": "Security Terminate - Unique Link time-out",
    "92": "Security Terminate - Unique Link internal server error",
    "93": "Security Terminate - Unique Link exceeded expected CPI",
    "94": "Sample Chain - cross panel deduplication",
    "95": "First time entry exception failure",
    "96": "Prescreener start init",
    "97": "Prescreener start exception",
    "98": "Client - qualification error",
    "99": "Block User",
    "100": "MBDGoogleStart",
    "101": "Reconciled to Complete - Late",
    "102": "Reconciled to Terminate - Late",
    "103": "No Matching Unique ID",
    "104": "Matching Unique ID Already Attempted",
    "105": "Already a Terminate",
    "106": "Unique link Already Attempted",
    "107": "Internal Quality Score",
    "108": "Bad IP - VPN",
    "109": "Bad IP - Proxy",
    "110": "Client - No Surveys",
    "111": "Fraud S2S",
    "112": "Bad IPQS Fraud Score",
    "113": "Invalid Redirect Url",
    "114": "Client Eligibility Logic",
    "115": "Unique Link Blank Response",
    "116": "Unique Link Client Failure",
    "117": "Sample Chain-Terminate on OE Quality",
}

status_map = defaultdict(
    lambda: Status.FAIL, **{"1": Status.COMPLETE, "0": Status.ABANDON}
)

status_codes_ext_map: Dict[StatusCode1, List[str]] = {
    StatusCode1.COMPLETE: ["48"],
    StatusCode1.BUYER_FAIL: ["16", "29", "49", "50", "78", "114", "110", "114"],
    StatusCode1.BUYER_QUALITY_FAIL: ["26", "52", "68", "81", "84"],
    StatusCode1.PS_BLOCKED: ["99"],
    StatusCode1.PS_QUALITY: [
        "1",
        "7",
        "42",
        "43",
        "44",
        "58",
        "60",
        "61",
        "62",
        "72",
        "73",
        "74",
        "75",
        "76",
        "85",
        "86",
        "87",
        "89",
        "90",
        "99",
        "112",
        "120",
    ],
    StatusCode1.PS_DUPLICATE: [
        "1",
        "7",
        "8",
        "42",
        "60",
        "61",
        "62",
        "72",
        "73",
        "87",
        "89",
        "90",
    ],
    StatusCode1.PS_FAIL: ["7", "29", "36", "47", "56", "58", "64"],
    StatusCode1.PS_OVERQUOTA: ["29", "46", "33", "31"],
}
ext_status_code_map: Dict[str, StatusCode1] = dict()
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
    :params ext_status_code_1: from callback url params: scstatus
    :params ext_status_code_2: from callback url params: scsecuritystatus
    :params ext_status_code_3: not used

    returns: (status, status_code_1, status_code_2)
    """
    status = status_map[ext_status_code_1]
    status_code = ext_status_code_map.get(ext_status_code_2, StatusCode1.UNKNOWN)
    # According to personal communication, scsecuritystatus may not always
    #   come back for completes. Going to ignore it if the status is complete

    if status == Status.COMPLETE:
        status_code = StatusCode1.COMPLETE

    return status, status_code, None
