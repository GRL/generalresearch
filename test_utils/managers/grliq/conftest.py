from __future__ import annotations

from datetime import datetime, timezone
from typing import Callable
from uuid import uuid4

import pytest

from generalresearch.grliq.managers import DUMMY_GRLIQ_DATA
from generalresearch.grliq.managers.forensic_data import GrlIqDataManager
from generalresearch.grliq.models.forensic_data import GrlIqData


@pytest.fixture
def grliq_data_factory(grliq_dm: GrlIqDataManager) -> Callable[..., GrlIqData]:

    def _inner(
        is_attempt_allowed: bool = True,
        product_id: str | None = None,
        product_user_id: str | None = None,
        uuid: str | None = None,
        mid: str | None = None,
        created_at: datetime | None = None,
    ) -> GrlIqData:
        """
        Creates a dummy record in the db with a GrlIqData (data), GrlIqCheckerResults (result_data),
            and GrlIqForensicCategoryResult (category_results)
        :param is_attempt_allowed: Whether the attempt is allowed.
        :param product_id: product_id of user
        :param product_user_id:  product_user_id of user
        :param uuid: uuid for the grliq data record
        :param mid: the thl_session:uuid / mid for the attempt.
        :return:
        """
        import copy

        res: GrlIqData = copy.deepcopy(DUMMY_GRLIQ_DATA[int(is_attempt_allowed)])

        product_id = product_id or uuid4().hex
        product_user_id = product_user_id or uuid4().hex
        uuid = uuid or uuid4().hex
        mid = mid or uuid4().hex
        created_at = created_at or datetime.now(tz=timezone.utc)

        res["data"].product_id = product_id
        res["data"].product_user_id = product_user_id
        res["data"].uuid = uuid
        res["data"].mid = mid
        res["data"].created_at = created_at
        res["result_data"].uuid = uuid
        res["category_result"].uuid = uuid

        return grliq_dm.create(
            iq_data=res["data"],
            result_data=res["result_data"],
            category_result=res["category_result"],
            fraud_score=res["category_result"].fraud_score,
            is_attempt_allowed=res["category_result"].is_attempt_allowed(),
        )

    return _inner
