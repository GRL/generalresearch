"""
Status codes are in a xlsx file. See thl-repdata readme
"""

from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple

from generalresearch.models.thl.definitions import Status, StatusCode1

status_codes_name: Dict[str, str] = {
    "2": "Search Failed",
    "3": "Activity Failed",
    "4": "Review Failed",
    "1000": "Complete",
    "2000": "Client Side Term",
    "3000": "Survey Quality Term (Client Side)",
    "4000": "General Overquota (Client Side)",
    "5001": "Stream Closed (Research Desk)",
    "5002": "Speeder Term (<30sec or <20% of LOI)",
    "5003": "Qualification mismatch",
    "5004": "Device compatibility mismatch",
    "5101": "Attempted bypass of Client Survey",
    "5102": "Encryption Failure",
    "6001": "Overall quota achieved (Research Desk)",
    "6002": "Sub-Quota achieved (Research Desk)",
    "6003": "In-Survey maximum exceeded (Research Desk)",
}
# See: 02, and 13 are de-dupes
rd_threat_name: Dict[str, str] = {
    "02": "Duplicate entrant into survey",
    "03": "Emulator Usage",
    "04": "VPN usage detected",
    "05": "TOR network detected",
    "06": "Public proxy server detected",
    "07": "Web proxy service used",
    "08": "Web crawler usage detected",
    "09": "Internet fraudster detected",
    "10": "Retail and ad-tech fraudster detected",
    "11": "Subnet detected",
    "12": "Recent Abuse detected",
    "13": "Duplicate Survey Group detected",
    "14": "Navigator Webdriver detected",
    "15": "Developer Tool detected",
    "16": "Web RTC Detected",
    "17": "Proxy Detected",
    "18": "MaxMind Failure",
}

status_map = defaultdict(lambda: Status.FAIL, **{"complete": Status.COMPLETE})
status_code_map: Dict[StatusCode1, List[str]] = {
    StatusCode1.COMPLETE: ["1000"],
    StatusCode1.BUYER_FAIL: ["2000", "4000"],
    StatusCode1.BUYER_QUALITY_FAIL: ["3000"],
    StatusCode1.PS_BLOCKED: [],
    StatusCode1.PS_QUALITY: ["2", "3", "4", "5002", "5101", "5102"],
    StatusCode1.PS_DUPLICATE: [],
    StatusCode1.PS_FAIL: ["5003", "5004"],
    StatusCode1.PS_OVERQUOTA: ["5001", "6001", "6002", "6003"],
}

status_class = dict()
for k, v in status_code_map.items():
    k: StatusCode1
    v: List[str]

    for vv in v:
        vv: str
        status_class[status_code_map.get(vv, vv)] = k


def annotate_status_code(
    ext_status_code_1: str,
    ext_status_code_2: Optional[str] = None,
    ext_status_code_3: Optional[str] = None,
) -> Tuple[Status, StatusCode1, Optional[Any]]:
    """
    :params ext_status_code_1: the redirect urls category (as defined in url param 549f3710b)
        {'term', 'overquota', 'fraud', 'complete'}
    :params ext_status_code_2: the "isc" (inbound_sub_code)
    :params ext_status_code_3: "rdThreat". only used when isc=2
    returns: (status, status_code_1, status_code_2)
    """
    status = status_map[ext_status_code_1]
    status_code = status_class.get(ext_status_code_2, StatusCode1.UNKNOWN)

    if ext_status_code_3 in {"02", "13"}:
        status_code = StatusCode1.PS_DUPLICATE
    if status == Status.COMPLETE:
        assert (
            status_code == StatusCode1.COMPLETE
        ), "inconsistent status codes for complete"

    return status, status_code, None
