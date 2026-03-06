from collections import defaultdict
from typing import Optional, Dict, Tuple

from generalresearch.models.thl.definitions import StatusCode1, Status
from generalresearch.wxet.models.definitions import (
    WXETStatus,
    WXETStatusCode1,
    WXETStatusCode2,
)

status_map: Dict[WXETStatus, Status] = defaultdict(
    lambda: Status.FAIL, **{WXETStatus.COMPLETE: Status.COMPLETE}
)
status_codes_ext_map = {
    StatusCode1.COMPLETE: [WXETStatusCode1.COMPLETE],
    StatusCode1.BUYER_FAIL: [
        WXETStatusCode1.BUYER_DUPLICATE,
        WXETStatusCode1.BUYER_FAIL,
        WXETStatusCode1.BUYER_OVER_QUOTA,
        WXETStatusCode1.BUYER_TASK_NOT_AVAILABLE,
    ],
    StatusCode1.BUYER_QUALITY_FAIL: [WXETStatusCode1.BUYER_QUALITY_FAIL],
    StatusCode1.BUYER_ABANDON: [WXETStatusCode1.BUYER_ABANDON],
    StatusCode1.PS_BLOCKED: [],
    StatusCode1.PS_QUALITY: [],
    StatusCode1.PS_DUPLICATE: [],
    StatusCode1.PS_ABANDON: [WXETStatusCode1.WXET_ABANDON],
    StatusCode1.PS_FAIL: [WXETStatusCode1.WXET_FAIL],
    StatusCode1.PS_OVERQUOTA: [],
    StatusCode1.UNKNOWN: [],
    StatusCode1.MARKETPLACE_FAIL: [WXETStatusCode1.BUYER_POSTBACK_NOT_RECEIVED],
}
ext_status_code_map = dict()
for k, v in status_codes_ext_map.items():
    for vv in v:
        ext_status_code_map[vv] = k

status_code2_map = {
    StatusCode1.PS_QUALITY: [],
    StatusCode1.PS_DUPLICATE: [
        WXETStatusCode2.WORKER_INELIGIBLE,
        WXETStatusCode2.WORKER_EXCLUDED,
        WXETStatusCode2.RE_ENTRY,
    ],
    StatusCode1.PS_OVERQUOTA: [
        WXETStatusCode2.SUPPLY_CONFIG_RESTRICTED,
        WXETStatusCode2.WORKER_RATE_LIMITED,
        WXETStatusCode2.TASK_RATE_LIMITED,
        WXETStatusCode2.TASK_NOT_FOUND,
        WXETStatusCode2.TASK_NOT_AVAILABLE,
        WXETStatusCode2.TASK_NOT_FUNDED,
        WXETStatusCode2.TASK_NO_FINISHES_AVAILABLE,
        WXETStatusCode2.TASK_CONNECTOR_NO_FINISHES_AVAILABLE,
        WXETStatusCode2.INVALID_ALLOCATION_SELECTION,
        WXETStatusCode2.TASK_VERSION_MISMATCH,
    ],
}
ext_status_code2_map = dict()
for k, v in status_code2_map.items():
    for vv in v:
        ext_status_code2_map[vv] = k


def annotate_status_code(
    ext_status_code_1: str,
    ext_status_code_2: Optional[str] = None,
    ext_status_code_3: Optional[str] = None,
) -> Tuple:
    """
    :params ext_status_code_1: WXETStatus
    :params ext_status_code_2: WXETStatusCode1
    :params ext_status_code_3: WXETStatusCode2
    returns: (status, status_code_1, status_code_2)
    """
    ext_status_code_1 = WXETStatus(ext_status_code_1)
    ext_status_code_2 = WXETStatusCode1(ext_status_code_2)
    ext_status_code_3 = (
        WXETStatusCode2(ext_status_code_3) if ext_status_code_3 else None
    )
    status = status_map[ext_status_code_1]
    status_code_1 = ext_status_code_map.get(ext_status_code_2, StatusCode1.UNKNOWN)
    status_code_2 = None

    if ext_status_code_2 == WXETStatusCode1.WXET_FAIL:
        status_code_2 = ext_status_code2_map.get(ext_status_code_3)

    return status, status_code_1, status_code_2
