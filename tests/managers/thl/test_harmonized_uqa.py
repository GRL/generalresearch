from datetime import datetime, timezone

import pytest

from generalresearch.managers.thl.profiling.uqa import UQAManager
from generalresearch.models.thl.profiling.user_question_answer import (
    UserQuestionAnswer,
    DUMMY_UQA,
)
from generalresearch.models.thl.user import User


@pytest.mark.usefixtures("uqa_db_index", "upk_data", "uqa_manager_clear_cache")
class TestUQAManager:

    def test_init(self, uqa_manager: UQAManager, user: User):
        uqas = uqa_manager.get(user)
        assert len(uqas) == 0

    def test_create(self, uqa_manager: UQAManager, user: User):
        now = datetime.now(tz=timezone.utc)
        uqas = [
            UserQuestionAnswer(
                user_id=user.user_id,
                question_id="fd5bd491b75a491aa7251159680bf1f1",
                country_iso="us",
                language_iso="eng",
                answer=("2",),
                timestamp=now,
                property_code="m:job_role",
                calc_answers={"m:job_role": ("2",)},
            )
        ]
        uqa_manager.create(user, uqas)

        res = uqa_manager.get(user=user)
        assert len(res) == 1
        assert res[0] == uqas[0]

        # Same question, so this gets updated
        now = datetime.now(tz=timezone.utc)
        uqas_update = [
            UserQuestionAnswer(
                user_id=user.user_id,
                question_id="fd5bd491b75a491aa7251159680bf1f1",
                country_iso="us",
                language_iso="eng",
                answer=("3",),
                timestamp=now,
                property_code="m:job_role",
                calc_answers={"m:job_role": ("3",)},
            )
        ]
        uqa_manager.create(user, uqas_update)
        res = uqa_manager.get(user=user)
        assert len(res) == 1
        assert res[0] == uqas_update[0]

        # Add a new answer
        now = datetime.now(tz=timezone.utc)
        uqas_new = [
            UserQuestionAnswer(
                user_id=user.user_id,
                question_id="3b65220db85f442ca16bb0f1c0e3a456",
                country_iso="us",
                language_iso="eng",
                answer=("3",),
                timestamp=now,
                property_code="gr:children_age_gender",
                calc_answers={"gr:children_age_gender": ("3",)},
            )
        ]
        uqa_manager.create(user, uqas_new)
        res = uqa_manager.get(user=user)
        assert len(res) == 2
        assert res[1] == uqas_update[0]
        assert res[0] == uqas_new[0]


@pytest.mark.usefixtures("uqa_db_index", "upk_data", "uqa_manager_clear_cache")
class TestUQAManagerCache:

    def test_get_uqa_empty(self, uqa_manager: UQAManager, user: User, caplog):
        res = uqa_manager.get(user=user)
        assert len(res) == 0

        res = uqa_manager.get_from_db(user=user)
        assert len(res) == 0

        # Checking that the cache has only the dummy_uqa in it
        res = uqa_manager.get_from_cache(user=user)
        assert res == [DUMMY_UQA]

        with caplog.at_level("INFO"):
            res = uqa_manager.get(user=user)
            assert f"thl-grpc:uqa-cache-v2:{user.user_id} exists" in caplog.text
        assert len(res) == 0

    def test_get_uqa(self, uqa_manager: UQAManager, user: User, caplog):

        # Now the user sends an answer
        uqas = [
            UserQuestionAnswer(
                question_id="5d6d9f3c03bb40bf9d0a24f306387d7c",
                answer=("1",),
                timestamp=datetime.now(tz=timezone.utc),
                country_iso="us",
                language_iso="eng",
                property_code="gr:gender",
                user_id=user.user_id,
                calc_answers={},
            )
        ]
        uqa_manager.update_cache(user=user, uqas=uqas)
        res = uqa_manager.get_from_cache(user=user)
        assert res == uqas
