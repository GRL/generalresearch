from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple

from generalresearch.models.thl.definitions import Status, StatusCode1

status_codes_map: Dict[str, str] = {
    "quo_ful": "quota_full",
    "sur_clo": "survey_closed",
    "profilin": "profiling",
    "screenou": "screenout",
    "duplicat": "duplicate",
    "security": "security",
    "geomissm": "geomissmatch",
    "quality": "quality",
    "has_ans": "hasty_answers",
    "gibberis": "gibberish",
    "captcha": "captcha",
    "tp_term": "third_party_termination",
    "tp_fraud": "third_party_termination_fraud",
    "tp_qual": "third_party_termination_quality",
    "use_rej": "user_rejection",
    "vpn": "vpn",
    "sur_exp": "survey_expired",
    "und_pro": "underage_profiling",
    "ban_phr": "banned_phrase",
    "dis_rul": "disqualification_rule",
    "str_lin": "straight_lining",
    "su_al_ta": "survey_already_taken",
    "complete": "complete",
}
status_map = defaultdict(lambda: Status.FAIL, **{"complete": Status.COMPLETE})
status_codes_ext_map: Dict[StatusCode1, List[str]] = {
    StatusCode1.COMPLETE: ["complete"],
    StatusCode1.BUYER_FAIL: ["third_party_termination", "screenout"],
    StatusCode1.BUYER_QUALITY_FAIL: [
        "third_party_termination_fraud",
        "third_party_termination_quality",
        "disqualification_rule",
        "hasty_answers",
        "gibberish",
        "banned_phrase",
        "straight_lining",
    ],
    StatusCode1.PS_BLOCKED: [],
    StatusCode1.PS_QUALITY: [
        "security",
        "geomissmatch",
        "quality",
        "captcha",
        "vpn",
    ],
    StatusCode1.PS_DUPLICATE: ["duplicate", "survey_already_taken"],
    StatusCode1.PS_FAIL: [
        "profiling",
        "underage_profiling",
        "user_rejection",
        "underage_profiling",
    ],
    StatusCode1.PS_OVERQUOTA: ["quota_full", "survey_closed", "survey_expired"],
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
    :params ext_status_code_1: from callback url params: &sti={{status_id}}
    :params ext_status_code_2: from callback url params: &sdi={{status_detail_id}}
    :params ext_status_code_3: not used

    returns: (status, status_code_1, status_code_2)
    """
    status = status_map[ext_status_code_1]
    status_code = ext_status_code_map.get(ext_status_code_2, StatusCode1.UNKNOWN)
    return status, status_code, None
