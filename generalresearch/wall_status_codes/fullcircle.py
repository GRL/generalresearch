"""
fullcircle doesn't really have status codes. there is no way to distinguish
between pre-screen and client terminations. We're going to call them
buyer fails for the wall and reporting, but for yield management purposes
we'll try to infer based on the time spent in survey.
"""

from collections import defaultdict
from datetime import timedelta
from typing import Any, Dict, List, Optional, Tuple

from generalresearch.models.thl.definitions import Status, StatusCode1

status_codes_map: Dict[str, str] = {
    "1": "Complete",
    "2": "Terminate",
    "3": "Over-quota",
    "4": "Quality terminate",
}

status_map = defaultdict(lambda: Status.FAIL, **{"1": Status.COMPLETE})
status_codes_ext_map: Dict[StatusCode1, List[str]] = {
    StatusCode1.COMPLETE: ["1"],
    StatusCode1.BUYER_FAIL: ["2", "3"],
    StatusCode1.BUYER_QUALITY_FAIL: ["4"],
    StatusCode1.PS_BLOCKED: [],
    StatusCode1.PS_QUALITY: [],
    StatusCode1.PS_DUPLICATE: [],
    StatusCode1.PS_FAIL: [],
    StatusCode1.PS_OVERQUOTA: [],
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
    :params ext_status_code_1: this is from the callback url param 's'
    :params ext_status_code_2: not used
    :params ext_status_code_3: not used

    returns: (status, status_code_1, status_code_2)
    """
    status = status_map[ext_status_code_1]
    status_code = ext_status_code_map.get(ext_status_code_1, StatusCode1.UNKNOWN)
    return status, status_code, None


def is_soft_fail(elapsed: timedelta) -> bool:
    # Full circle has no status codes differentiating client vs PS failure. We need to make a
    #   determination based on the elapsed time.
    return elapsed.total_seconds() < 60
