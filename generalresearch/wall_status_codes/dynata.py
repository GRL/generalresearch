"""
https://developers.dynata.com/docs/rex/branches/main/dispositions
checked by Greg 2023-10-10
"""

from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple

from generalresearch.models.thl.definitions import Status, StatusCode1

status_codes_name: Dict[str, str] = {
    "0.0": "Unknown",
    "0.1": "Missing Language",
    "0.2": "Missing Respondent ID",
    "0.3": "Declined Consent",
    "0.4": "Underage",
    "0.5": "Invalid Locale",
    "0.6": "Invalid Country",
    "0.7": "Invalid Language",
    "0.8": "Inactive Respondent",
    "0.9": "Respondent Not Found",
    "1.0": "Complete",
    "1.1": "Partial Complete",
    "2.1": "Dynata Disqualification",
    "2.2": "Client Disqualification",
    "2.3": "Incompatible Country",
    "2.4": "Incompatible Language",
    "2.5": "Incompatible Device",
    "2.6": "Filter Disqualification",
    "2.7": "Quota Disqualification",
    "2.8": "Undisclosed Filter",  # this is important for ym, should penalize
    "2.10": "Partner Disqualification",
    "3.0": "Default Over Quota",
    "3.1": "Dynata Over Quota",
    "3.2": "Client Over Quota",
    "3.3": "Dynata Closed Quota",
    "3.10": "Quota Group Not Open",
    "3.11": "Quota Group Field Schedule",
    "3.12": "Quota Group Click Balance",
    "3.20": "Quota Cell Not Open",
    "3.21": "Quota Cell Field Schedule",
    "3.22": "Quota Cell Click Balance",
    "4.0": "Duplicate",
    "4.1": "Duplicate Respondent",
    "4.2": "Category Exclusion",
    "5.0": "General Quality",
    "5.1": "Answer Quality",
    "5.2": "Speeding",
    "5.3": "Suspended",  # aka blocked
    "5.4": "Predicted Reconciliation",
    "5.10": "Daily Limit",
}

status_map: Dict[str, Status] = defaultdict(
    lambda: Status.FAIL, **{"1.0": Status.COMPLETE, "1.1": Status.COMPLETE}
)
status_codes_ext_map: Dict[StatusCode1, List[str]] = {
    StatusCode1.COMPLETE: ["1.0", "1.1"],
    StatusCode1.BUYER_FAIL: ["2.2", "3.2"],
    StatusCode1.BUYER_QUALITY_FAIL: ["5.1", "5.2"],
    StatusCode1.PS_BLOCKED: ["5.3"],
    StatusCode1.PS_QUALITY: [
        "0.0",
        "0.1",
        "0.2",
        "0.3",
        "0.4",
        "0.5",
        "0.6",
        "0.7",
        "0.8",
        "0.9",
        "5.0",
        "5.4",
    ],
    StatusCode1.PS_DUPLICATE: ["4.0", "4.1", "4.2"],
    StatusCode1.PS_FAIL: ["2.1", "2.3", "2.4", "2.5", "2.6", "2.7", "2.8", "2.10"],
    StatusCode1.PS_OVERQUOTA: [
        "3.0",
        "3.1",
        "3.3",
        "3.10",
        "3.11",
        "3.12",
        "3.20",
        "3.21",
        "3.22",
        "5.10",
    ],
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
    :params ext_status_code_1: this is from the callback url params:
        disposition and status, '.'-joined
    :params ext_status_code_2: not used
    :params ext_status_code_3: not used

    returns: (status, status_code_1, status_code_2)
    """
    status = status_map[ext_status_code_1]
    status_code = ext_status_code_map.get(ext_status_code_1, StatusCode1.UNKNOWN)

    return status, status_code, None


def stop_marketplace_session(status_code_1: StatusCode1, ext_status_code_1) -> bool:
    if ext_status_code_1.startswith("5"):
        # '5.10' is the user hit a Daily Limit, so they should not be sent in again today
        return True

    if status_code_1 in {
        StatusCode1.PS_QUALITY,
        StatusCode1.BUYER_QUALITY_FAIL,
        StatusCode1.PS_BLOCKED,
    }:
        return True

    return False
