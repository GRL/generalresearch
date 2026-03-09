from typing import Optional
from urllib.parse import urlencode


def generate_offerwall_entry_url(
    base_url: str,
    obj_id: str,
    bp_user_id: str,
    request_id: Optional[str] = None,
    nudge_id: Optional[str] = None,
) -> str:
    # For an offerwall entry link, we need the clicked bucket_id and the
    #   request hash (so we know which GetOfferwall cache to get
    query_dict = {"i": obj_id, "b": bp_user_id}
    if request_id:
        query_dict["66482fb"] = request_id
    if nudge_id:
        query_dict["5e0e0323"] = nudge_id
    enter_url = base_url + urlencode(query_dict)
    return enter_url
