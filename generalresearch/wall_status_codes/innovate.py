"""
https://innovatemr.stoplight.io/docs/supplier-api/ZG9jOjEzNzYxMTg2-statuses-term-reasons-and-categories
Term Reasons xls

This is super confusing. We get a "status" (1 through 8), a term reason, (and a
category?). The status we can't use directly, because they call a quality
term (8) due to both dedupes and due to actual quality issues. So, some we
can map directly, and some we have to look at the category.
"""

from collections import defaultdict
from typing import Optional, Tuple

from generalresearch.models.thl.definitions import Status, StatusCode1

status_codes_innovate = {
    "1": "Complete",
    "2": "Buyer Fail",
    "3": "Buyer Over Quota",
    "4": "Buyer Quality Term",
    "5": "PS Termination",
    # We can't use this directly because the sub reasons are not all the same
    "7": "PS Over Quota",  # same as above. They think Quota full is a PS Term
    "8": "PS Quality Term",  # same as above. they think dupe is a quality term
    "0": "PS Abandon",
    "6": "Buyer Abandon",  # really it is "Buyer Abandon"
}
status_map = defaultdict(
    lambda: Status.FAIL,
    **{"1": Status.COMPLETE, "0": Status.ABANDON, "6": Status.ABANDON},
)
status_codes_ext_map = {
    StatusCode1.BUYER_FAIL: ["2", "3"],
    StatusCode1.BUYER_QUALITY_FAIL: ["4"],
    StatusCode1.PS_BLOCKED: [],
    StatusCode1.PS_QUALITY: ["8"],
    StatusCode1.PS_DUPLICATE: [],
    StatusCode1.PS_FAIL: ["5"],
    StatusCode1.PS_OVERQUOTA: ["7"],
}
ext_status_code_map = dict()
for k, v in status_codes_ext_map.items():
    for vv in v:
        ext_status_code_map[status_codes_ext_map.get(vv, vv)] = k

category_innovate = {
    "Selected threat potential score at joblevel not allow the survey": StatusCode1.PS_QUALITY,
    "OE Validation": StatusCode1.PS_QUALITY,
    "Unique IP": StatusCode1.PS_DUPLICATE,
    "Unique PID": StatusCode1.PS_DUPLICATE,
    # 'Duplicated to token {token} and Group {groupID}': StatusCode1.PS_DUPLICATE,
    # 'Duplicate Due to Multi Groups: Token {token} and Group {groupID}': StatusCode1.PS_DUPLICATE,
    # todo: we should not send them into this marketplace for a day?
    "User has attended {count} survey in 5 range": StatusCode1.PS_FAIL,
    "PII_OPT": StatusCode1.PS_QUALITY,
    "Recaptcha": StatusCode1.PS_QUALITY,
    "URL Manipulation - Multiple Tries": StatusCode1.PS_QUALITY,
    "URL Manipulation": StatusCode1.PS_QUALITY,
    "Quota closed": StatusCode1.PS_OVERQUOTA,
    "OpinionRoute Timeout Error": StatusCode1.PS_FAIL,
    "OpinionRoute Error": StatusCode1.PS_FAIL,
    "Invalid opinionRoute Token": StatusCode1.PS_FAIL,
    "GEOIP": StatusCode1.PS_QUALITY,
    "Speeder": StatusCode1.PS_QUALITY,
    "Error respondent risk is too high": StatusCode1.PS_QUALITY,
    "Group NA": StatusCode1.PS_OVERQUOTA,
    "Job NA": StatusCode1.PS_OVERQUOTA,
    "Supplier NA": StatusCode1.PS_OVERQUOTA,
    "This survey Country mismatch": StatusCode1.PS_QUALITY,
    "DeviceType": StatusCode1.PS_FAIL,
    "Off hours": StatusCode1.PS_FAIL,
    "Panel Duplicate": StatusCode1.PS_DUPLICATE,
    "Not Eligible(sameSurveyElimination)": StatusCode1.PS_FAIL,
    "ClientQualTerm": StatusCode1.BUYER_QUALITY_FAIL,
    "BlockedRespondent": StatusCode1.PS_BLOCKED,
}


def annotate_status_code(
    ext_status_code_1: str,
    ext_status_code_2: Optional[str] = None,
    ext_status_code_3: Optional[str] = None,
) -> Tuple:
    """
    Only quality terminate (4 and 8), and PS term (5) return a term_reason (af=).

    :params ext_status_code_1: this is from the callback url param '&ac='
    :params ext_status_code_2: callback url param '&af='
    :params ext_status_code_3: not used

    returns: (status, status_code_1, status_code_2) status_code_2 is always None
    """
    status = status_map[ext_status_code_1]
    if status == Status.COMPLETE:
        return status, StatusCode1.COMPLETE, None
    # First use the 1 through 8 status code. Then, using the reason (af=), if available,
    #   try to maybe reclassify it.
    if ext_status_code_1 not in ext_status_code_map:
        return status, StatusCode1.UNKNOWN, None
    status_code = ext_status_code_map.get(ext_status_code_1, StatusCode1.UNKNOWN)
    if ext_status_code_2 in category_innovate:
        status_code = category_innovate[ext_status_code_2]
    if ext_status_code_2:
        # Some of these have ids in them... so we have to pattern match it
        if (
            "Duplicated to token " in ext_status_code_2
            or "Duplicate Due to Multi Groups" in ext_status_code_2
        ):
            # innovate calls this 8 (quality fail). I think its a PS Dupe ...
            status_code = StatusCode1.PS_DUPLICATE
        elif "User has attended " in ext_status_code_2:
            status_code = StatusCode1.PS_FAIL
        elif "RED_HERRING_" in ext_status_code_2:
            # innovate calls this 5 (PS fail), I think its a quality fail
            status_code = StatusCode1.PS_QUALITY

    return status, status_code, None
