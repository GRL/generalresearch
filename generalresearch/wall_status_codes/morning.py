from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple

from generalresearch.models.thl.definitions import Status, StatusCode1

"""
Status IDs.
We don't use these except for "complete". We use the "id" instead which is 
the detailed status 

complete: The survey was completed successfully.
failure: The respondent was rejected for an unknown reason. These will be investigated.
over_quota: The respondent qualified for a quota, but there were no open completes available.
quality_termination: The respondent was rejected for quality reasons.
screenout: The respondent did not qualify for the survey or quota.
timeout: The respondent completed the survey after the timeout period had expired.
in_progress: The respondent interview session is still in progress, such as in the prescreener or survey.
"""

short_code_to_status_codes_morning: Dict[str, str] = {
    "att_che": "attention_check",
    "banned": "banned",
    "bid_clo": "bid_closed",
    "bi_no_fo": "bid_not_found",
    "bid_pau": "bid_paused",
    "complete": "complete",
    "co_in_fb": "country_invalid_for_bid",
    "deduplic": "deduplicated",
    "excluded": "excluded",
    "fa_pr_ca": "failed_prescreener_captcha",
    "failure": "failure",
    "inactive": "inactive",
    "in_ad_qu": "in_additional_questions",
    "ineligib": "ineligible",
    "in_pre": "in_prescreener",
    "in_sur": "in_survey",
    "in_su_fa": "in_survey_failure",
    "in_su_oq": "in_survey_over_quota",
    "in_su_sc": "in_survey_screenout",
    "in_en_pa": "invalid_entry_parameters",
    "in_en_si": "invalid_entry_signature",
    "la_in_fb": "language_invalid_for_bid",
    "no_co_av": "no_completes_available",
    "no_co_re": "no_completes_required",
    "pr_co_er": "prescreener_completion_error",
    "pre_tim": "prescreener_timeout",
    "qu_te_ot": "quality_termination_other",
    "qu_in_fb": "quota_invalid_for_bid",
    "reentry": "reentry",
    "speeding": "speeding",
    "straight": "straightlining",
    "sur_tim": "survey_timeout",
    "tem_ban": "temporarily_banned",
}
status_map = defaultdict(lambda: Status.FAIL, **{"complete": Status.COMPLETE})

status_codes_ext_map: Dict[StatusCode1, List[str]] = {
    StatusCode1.COMPLETE: ["complete"],
    StatusCode1.BUYER_FAIL: [
        "in_survey_failure",
        "in_survey_over_quota",
        "in_survey_screenout",
        "survey_timeout",
    ],
    StatusCode1.BUYER_QUALITY_FAIL: [
        "quality_termination_other",
        "open_ended_response",
        "speeding",
        "attention_check",
        "straightlining",
        "suspect_response_pattern",
    ],
    StatusCode1.PS_BLOCKED: ["banned", "temporarily_banned"],
    StatusCode1.PS_QUALITY: [
        "prescreener_attention_check",
        "country_invalid_for_bid",
        "failed_prescreener_captcha",
        "inactive",
        "invalid_entry_signature",
        "invalid_entry_parameters",
    ],
    StatusCode1.PS_DUPLICATE: ["reentry", "deduplicated", "excluded"],
    StatusCode1.PS_FAIL: [
        "failure",
        "language_invalid_for_bid",
        "minimum_cost_per_interview",
        "prescreener_completion_error",
        "prescreener_timeout",
        "ineligible",
    ],
    StatusCode1.PS_OVERQUOTA: [
        "bid_not_found",
        "bid_closed",
        "bid_paused",
        "no_completes_available",
        "no_completes_required",
        "quota_invalid_for_bid",
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
    :params ext_status_code_1: from callback url params: &sti={{status_id}}
    :params ext_status_code_2: from callback url params: &sdi={{status_detail_id}}
    :params ext_status_code_3: not used

    returns: (status, status_code_1, status_code_2)
    """
    # We pretty much do not use the status_id because it is Morning's status category, which
    #   lumps de-dupes into a different category, and doesn't differentiate between in-client
    #   and not terms.
    status = status_map[ext_status_code_1]
    status_code = ext_status_code_map.get(ext_status_code_2, StatusCode1.UNKNOWN)
    return status, status_code, None
