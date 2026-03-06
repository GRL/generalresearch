import json
from decimal import Decimal
from uuid import uuid4

import pytest


class TestUserQuestionAnswers:
    """This is for the GRS POST submission that may contain multiple
    Question+Answer(s) combinations for a single GRS Survey. It is
    responsible for making sure the same question isn't submitted
    more than once per submission, and other "list validation"
    checks that aren't possible on an individual level.
    """

    def test_json_init(
        self,
        product_manager,
        user_manager,
        session_manager,
        wall_manager,
        user_factory,
        product,
        session_factory,
        utc_hour_ago,
    ):
        from generalresearch.models.thl.session import Session, Wall
        from generalresearch.models.thl.user import User
        from generalresearch.models import Source
        from generalresearch.models.legacy.questions import (
            UserQuestionAnswers,
        )

        u: User = user_factory(product=product)

        s1: Session = session_factory(
            user=u,
            wall_count=1,
            started=utc_hour_ago,
            wall_req_cpi=Decimal("0.00"),
            wall_source=Source.GRS,
        )
        assert isinstance(s1, Session)
        w1 = s1.wall_events[0]
        assert isinstance(w1, Wall)

        instance = UserQuestionAnswers.model_validate_json(
            json.dumps(
                {
                    "product_id": product.uuid,
                    "product_user_id": u.product_user_id,
                    "session_id": w1.uuid,
                    "answers": [
                        {"question_id": uuid4().hex, "answer": ["a", "b"]},
                        {"question_id": uuid4().hex, "answer": ["a", "b"]},
                    ],
                }
            )
        )
        assert isinstance(instance, UserQuestionAnswers)

    def test_simple_validation_errors(
        self, product_manager, user_manager, session_manager, wall_manager
    ):
        from generalresearch.models.legacy.questions import (
            UserQuestionAnswers,
        )

        with pytest.raises(ValueError):
            UserQuestionAnswers.model_validate(
                {
                    "product_user_id": f"test-user-{uuid4().hex[:6]}",
                    "session_id": uuid4().hex,
                    "answers": [{"question_id": uuid4().hex, "answer": ["a", "b"]}],
                }
            )

        with pytest.raises(ValueError):
            UserQuestionAnswers.model_validate(
                {
                    "product_id": uuid4().hex,
                    "session_id": uuid4().hex,
                    "answers": [{"question_id": uuid4().hex, "answer": ["a", "b"]}],
                }
            )

        # user is validated only if a session_id is passed
        UserQuestionAnswers.model_validate(
            {
                "product_id": uuid4().hex,
                "product_user_id": f"test-user-{uuid4().hex[:6]}",
                "answers": [{"question_id": uuid4().hex, "answer": ["a", "b"]}],
            }
        )

        with pytest.raises(ValueError):
            UserQuestionAnswers.model_validate(
                {
                    "product_id": uuid4().hex,
                    "product_user_id": f"test-user-{uuid4().hex[:6]}",
                    "session_id": uuid4().hex,
                }
            )

        with pytest.raises(ValueError):
            UserQuestionAnswers.model_validate(
                {
                    "product_id": uuid4().hex,
                    "product_user_id": f"test-user-{uuid4().hex[:6]}",
                    "session_id": uuid4().hex,
                    "answers": [],
                }
            )

        with pytest.raises(ValueError):
            answers = [
                {"question_id": uuid4().hex, "answer": ["a"]} for i in range(101)
            ]
            UserQuestionAnswers.model_validate(
                {
                    "product_id": uuid4().hex,
                    "product_user_id": f"test-user-{uuid4().hex[:6]}",
                    "session_id": uuid4().hex,
                    "answers": answers,
                }
            )

        with pytest.raises(ValueError):
            UserQuestionAnswers.model_validate(
                {
                    "product_id": uuid4().hex,
                    "product_user_id": f"test-user-{uuid4().hex[:6]}",
                    "session_id": uuid4().hex,
                    "answers": "aaa",
                }
            )

    def test_no_duplicate_questions(self):
        # TODO: depending on if or how many of these types of errors actually
        #   occur, we could get fancy and just drop one of them. I don't
        #   think this is worth exploring yet unless we see if it's a problem.
        from generalresearch.models.legacy.questions import (
            UserQuestionAnswers,
        )

        consistent_qid = uuid4().hex
        with pytest.raises(ValueError) as cm:
            UserQuestionAnswers.model_validate(
                {
                    "product_id": uuid4().hex,
                    "product_user_id": f"test-user-{uuid4().hex[:6]}",
                    "session_id": uuid4().hex,
                    "answers": [
                        {"question_id": consistent_qid, "answer": ["aaa"]},
                        {"question_id": consistent_qid, "answer": ["bbb"]},
                    ],
                }
            )

        assert "Don't provide answers to duplicate questions" in str(cm.value)

    def test_allow_answer_failures_silent(
        self,
        product_manager,
        user_manager,
        session_manager,
        wall_manager,
        product,
        user_factory,
        utc_hour_ago,
        session_factory,
    ):
        """
        There are many instances where suppliers may be submitting answers
        manually, and they're just totally broken. We want to silently remove
        that one QuestionAnswerIn without "loosing" any of the other
        QuestionAnswerIn items that they provided.
        """
        from generalresearch.models.thl.session import Session, Wall
        from generalresearch.models.thl.user import User
        from generalresearch.models.legacy.questions import (
            UserQuestionAnswers,
        )

        u: User = user_factory(product=product)

        s1: Session = session_factory(user=u, wall_count=1, started=utc_hour_ago)
        assert isinstance(s1, Session)
        w1 = s1.wall_events[0]
        assert isinstance(w1, Wall)

        data = {
            "product_id": product.uuid,
            "product_user_id": u.product_user_id,
            "session_id": w1.uuid,
            "answers": [
                {"question_id": uuid4().hex, "answer": ["aaa"]},
                {"question_id": f"broken-{uuid4().hex[:6]}", "answer": ["bbb"]},
            ],
        }
        # load via .model_validate()
        instance = UserQuestionAnswers.model_validate(data)
        assert isinstance(instance, UserQuestionAnswers)

        # One of the QuestionAnswerIn items was invalid, so it was dropped
        assert 1 == len(instance.answers)

        # Confirm that this also works via model_validate_json
        json_data = json.dumps(data)
        instance = UserQuestionAnswers.model_validate_json(json_data)
        assert isinstance(instance, UserQuestionAnswers)

        # One of the QuestionAnswerIn items was invalid, so it was dropped
        assert 1 == len(instance.answers)

        assert instance.user is None
        instance.prefetch_user(um=user_manager)
        assert isinstance(instance.user, User)


class TestUserQuestionAnswerIn:
    """This is for the individual Question+Answer(s) that may come back from
    a GRS POST.
    """

    def test_simple_validation_errors(self):
        from generalresearch.models.legacy.questions import (
            UserQuestionAnswerIn,
        )

        with pytest.raises(ValueError):
            UserQuestionAnswerIn.model_validate(
                {"question_id": f"test-{uuid4().hex[:6]}", "answer": ["123"]}
            )

        with pytest.raises(ValueError):
            UserQuestionAnswerIn.model_validate({"answer": ["123"]})

        with pytest.raises(ValueError):
            UserQuestionAnswerIn.model_validate(
                {"question_id": uuid4().hex, "answer": [123]}
            )

        with pytest.raises(ValueError):
            UserQuestionAnswerIn.model_validate(
                {"question_id": uuid4().hex, "answer": [""]}
            )

        with pytest.raises(ValueError):
            UserQuestionAnswerIn.model_validate(
                {"question_id": uuid4().hex, "answer": ["  "]}
            )

        with pytest.raises(ValueError):
            UserQuestionAnswerIn.model_validate(
                {"question_id": uuid4().hex, "answer": ["a" * 5_001]}
            )

        with pytest.raises(ValueError):
            UserQuestionAnswerIn.model_validate(
                {"question_id": uuid4().hex, "answer": []}
            )

    def test_only_single_answers(self):
        from generalresearch.models.legacy.questions import (
            UserQuestionAnswerIn,
        )

        for qid in {
            "2fbedb2b9f7647b09ff5e52fa119cc5e",
            "4030c52371b04e80b64e058d9c5b82e9",
            "a91cb1dea814480dba12d9b7b48696dd",
            "1d1e2e8380ac474b87fb4e4c569b48df",
        }:
            # This is the UserAgent question which only allows a single answer
            with pytest.raises(ValueError) as cm:
                UserQuestionAnswerIn.model_validate(
                    {"question_id": qid, "answer": ["a", "b"]}
                )

            assert "Too many answer values provided" in str(cm.value)

    def test_answer_item_limit(self):
        from generalresearch.models.legacy.questions import (
            UserQuestionAnswerIn,
        )

        answer = [uuid4().hex[:6] for i in range(11)]
        with pytest.raises(ValueError) as cm:
            UserQuestionAnswerIn.model_validate(
                {"question_id": uuid4().hex, "answer": answer}
            )
        assert "List should have at most 10 items after validation" in str(cm.value)

    def test_disallow_duplicate_answer_values(self):
        from generalresearch.models.legacy.questions import (
            UserQuestionAnswerIn,
        )

        answer = ["aaa" for i in range(5)]
        with pytest.raises(ValueError) as cm:
            UserQuestionAnswerIn.model_validate(
                {"question_id": uuid4().hex, "answer": answer}
            )
