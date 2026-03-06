"""
https://purespectrum.atlassian.net/wiki/spaces/PA/pages/33613201/Minimizing+Clickwaste+with+ps+rstatus
"""

from collections import defaultdict
from typing import Optional, Tuple

from generalresearch.models.thl.definitions import Status, StatusCode1

status_codes_spectrum = {
    "11": "PS Drop",
    "12": "PS Quota Full Core",
    "13": "PS Termination Core",
    "14": "PS Side In Progress",
    "15": "PS Quality",
    "16": "Buyer In Progress",
    "17": "Buyer Quota Full",
    "18": "Buyer Termination",
    "19": "Buyer Drop",
    "20": "Buyer Quality Termination",
    "21": "Complete",
    "22": "PS Survey Closed Termination",
    "23": "PS Survey Paused Termination",
    "24": "PS Unopened Quota Term",
    "25": "PS Supplier Allocation Full",
    "26": "PS Past Participation Fail",
    "27": "PS Supplier Quota Allocation Full",
    "28": "PS Invalid Survey",
    "29": "PS LOI Threshold Failure",
    "30": "Buyer Security (De-Dupe)",
    "31": "Buyer Hash Failure",
    "32": "PS Grouping Termination",
    "33": "Buyer Reconcilliation Reject",
    "35": "PS No matched quotas",
    "36": "PS Max IP Throttling Termination",
    "37": "PS Quota Throttling Termination",
    "38": "PS PSID Geo Termination",
    "40": "PS GeoIP Fail",
    "41": "PS Bot Fail",
    "42": "PS BlackList Fail",
    "43": "PS Anonymous Fail",
    "44": "PS Include Fail",
    "45": "PS Termination Extended",
    "46": "PS Termination Custom",
    "47": "PS Quota Full Extended",
    "48": "PS Quota Full Custom",
    "49": "PS Include Fail",
    "50": "PS Exclude Fail",
    "51": "Invalid Supplier",
    "52": "PSID Service Fail",
    "55": "PS Unique Link Termination",
    "56": "Unauthorized Augment",
    "57": "PS Supplier Quota Full",
    "58": "PS Supplier Quota Throttling Termination",
    "59": "Buyer Config Error",
    "60": "PS_Js_Fail",
    "62": "Ps_NoPureScore",
    "63": "PS_Blacklist_Data_Quality",
    "64": "PS_Blacklist_Data_Quality_2",
    "67": "PS_SC_Fraudster_Fail",
    "68": "PS_SC_Threat_Fail",
    "69": "PS_TC_Termination",
    "70": "PS_DF_DUPE",
    "71": "ScHashFail",
    "73": "PS_Transaction_Fraud",
    "74": "PS_Respondent_Redirect_Fail",  # this apparently means dedupe
    "75": "PS_Blacklist_Data_Quality_4",
    "76": "PS_DQ_Screener_Invalid",
    "77": "PS_Supply_Inbound_Hash_Security",
    "78": "PS_DQ_Honeypot_Fail",
    "79": "PS_PureText_Dedupe_Fail",
    "80": "PS_AI_Text_Fail",
    "81": "PS_Puretext_Language_Fail",
    "82": "PS_Survey_Signature_Fail",
    "83": "PS_Browser_Manipulation_Fail",
    "84": "Buyer_PS_API_Fail",  # no idea what this means
    "85": "PS_RD_Predupe",
    "86": "PS_DF_Dupe_Grouping",
    "87": "PS_Supplier_Invalid_Bbsec",
    "88": "PS_Supplier_Allocation_Throttle",
}
status_map = defaultdict(lambda: Status.FAIL, **{"21": Status.COMPLETE})
status_codes_ext_map = {
    StatusCode1.COMPLETE: ["21"],
    StatusCode1.BUYER_FAIL: ["16", "17", "18", "19", "30", "59", "84"],
    StatusCode1.BUYER_QUALITY_FAIL: ["20", "31"],
    StatusCode1.PS_BLOCKED: ["42", "75"],
    StatusCode1.PS_QUALITY: [
        "15",
        "29",
        "38",
        "40",
        "41",
        "43",
        "60",
        "62",
        "63",
        "64",
        "65",
        "67",
        "68",
        "69",
        "71",
        "73",
        "76",
        "77",
        "78",
        "79",
        "81",
        "82",
        "83",
        "87",
    ],
    StatusCode1.PS_DUPLICATE: ["26", "32", "69", "70", "74", "85", "86"],
    StatusCode1.PS_FAIL: [
        "11",
        "13",
        "35",
        "45",
        "46",
        "49",
        "50",
    ],
    StatusCode1.PS_OVERQUOTA: [
        "12",
        "22",
        "23",
        "24",
        "25",
        "36",
        "37",
        "47",
        "48",
        "57",
        "58",
        "58",
        "88",
    ],
    StatusCode1.UNKNOWN: [
        "72",
    ],
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
    :params ext_status_code_1: from url params: ps_rstatus
    https://purespectrum.atlassian.net/wiki/spaces/PA/pages/33613201/Minimizing+Clickwaste+with+ps+rstatus
    :params ext_status_code_2: not used
    :params ext_status_code_3: not used

    returns: (status, status_code_1, status_code_2)
    """
    status = status_map[ext_status_code_1]
    status_code = ext_status_code_map.get(ext_status_code_1, StatusCode1.UNKNOWN)

    return status, status_code, None
