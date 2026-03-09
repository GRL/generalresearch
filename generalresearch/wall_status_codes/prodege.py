"""
https://developer.prodege.com/surveys-feed/term-reasons
"""

from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple

from generalresearch.models.thl.definitions import Status, StatusCode1

status_map = defaultdict(lambda: Status.FAIL, **{"1": Status.COMPLETE})
status_code_map: Dict[StatusCode1, List[str]] = {
    StatusCode1.COMPLETE: [],
    StatusCode1.BUYER_FAIL: ["1", "2"],
    StatusCode1.BUYER_QUALITY_FAIL: ["10", "12"],
    StatusCode1.PS_BLOCKED: ["33"],
    StatusCode1.PS_QUALITY: [
        "3",
        "5",
        "15",
        "16",
        "23",
        "27",
        "34",
        "35",
        "36",
        "37",
        "39",
    ],
    StatusCode1.PS_DUPLICATE: ["4", "17", "19", "20", "24", "32"],
    StatusCode1.PS_FAIL: ["8", "21", "22"],
    StatusCode1.PS_OVERQUOTA: ["13", "28", "29", "30", "31", "38"],
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
    :params ext_status_code_1: status from redirect url
    :params ext_status_code_2: termreason from redirect url
    :params ext_status_code_3: dqquestionid, not used.
    returns: (status, status_code_1, status_code_2)
    """
    status = status_map[ext_status_code_1]
    status_code = status_class.get(ext_status_code_2, StatusCode1.UNKNOWN)
    if status == Status.COMPLETE:
        assert ext_status_code_2 is None
        status_code = StatusCode1.COMPLETE
    return status, status_code, None
