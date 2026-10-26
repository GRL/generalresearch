from typing import Any

from generalresearch.managers.thl.profiling.question import QuestionManager
from generalresearch.managers.thl.profiling.uqa import UQAManager
from generalresearch.managers.thl.profiling.user_upk import UserUpkManager
from generalresearch.managers.thl.userhealth import UserIpHistoryManager
from generalresearch.models.thl.profiling.upk_property import PropertyType
from generalresearch.models.thl.profiling.user_info import (
    MarketProfileKnowledge,
    UserProfileKnowledge,
    UserProfilingData,
)
from generalresearch.models.thl.profiling.user_question_answer import (
    MarketplaceResearchProfileQuestion,
)
from generalresearch.models.thl.user import User
from generalresearch.models.thl.user_ref import UserRef


def get_user_mrpq(
    user: User,
    uqa_manager: UQAManager,
    question_manager: QuestionManager,
    country_iso: str | None = None,
) -> list[MarketplaceResearchProfileQuestion]:
    res = uqa_manager.get(user=user)
    if country_iso:
        res = [x for x in res if x.country_iso == country_iso]
    mrpqs = []
    for x in res:
        mrpqs.extend(x.get_mrpqs(question_manager=question_manager))
    return mrpqs


def get_user_upk_formatted(
    user_id: int, user_upk_manager: UserUpkManager, country_iso: str | None = None
):
    res = user_upk_manager.get_user_upk(user_id=user_id)
    if country_iso:
        res = [x for x in res if x.country_iso == country_iso]
    new_res = []
    for x in res:
        d: dict[str, Any] = {
            "property_id": x.property_id,
            "property_label": x.property_label,
            "created": x.created,
            "translation": "",
        }
        if x.prop_type in {PropertyType.UPK_NUMERICAL, PropertyType.UPK_TEXT}:
            d["answer"] = [{"value": str(x.value)}]
        else:
            d["answer"] = [{"id": x.item_id, "label": x.item_label, "translation": ""}]
        new_res.append(d)
    d = {}
    for x in new_res:
        if x["property_id"] in d:
            d[x["property_id"]]["answer"].extend(x["answer"])
        else:
            d[x["property_id"]] = x
    return list(d.values())


def get_user_profiling_data(
    user_upk_manager: UserUpkManager,
    uqa_manager: UQAManager,
    question_manager: QuestionManager,
    user: User,
    country_iso: str | None = None,
    user_ip_history_manager: UserIpHistoryManager | None = None,
) -> UserProfilingData:
    """
    Get user's mrpq and upk data, for API response
    """
    assert user.user_id is not None
    user = user if isinstance(user, UserRef) else user.to_user_ref()

    if country_iso is None and user_ip_history_manager is not None:
        country_iso = user_ip_history_manager.get_user_latest_country(user)

    mrpq = get_user_mrpq(
        user=user,
        uqa_manager=uqa_manager,
        question_manager=question_manager,
        country_iso=country_iso,
    )
    mrpq = [
        MarketProfileKnowledge.from_MarketplaceResearchProfileQuestion(x) for x in mrpq
    ]

    upk = get_user_upk_formatted(
        user_id=user.user_id,
        user_upk_manager=user_upk_manager,
        country_iso=country_iso,
    )
    upk = [UserProfileKnowledge.model_validate(x) for x in upk]

    d = {
        "user_profile_knowledge": upk,
        "marketplace_profile_knowledge": mrpq,
        "user": user,
    }
    return UserProfilingData.model_validate(d)
