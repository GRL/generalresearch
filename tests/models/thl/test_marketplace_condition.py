import pytest
from pydantic import ValidationError


class TestMarketplaceCondition:

    def test_list_or(self):
        from generalresearch.models import LogicalOperator
        from generalresearch.models.thl.survey.condition import (
            MarketplaceCondition,
            ConditionValueType,
        )

        user_qas = {"q1": {"a2"}}
        c = MarketplaceCondition(
            question_id="q1",
            negate=False,
            value_type=ConditionValueType.LIST,
            values=["a1", "a2", "a3"],
            logical_operator=LogicalOperator.OR,
        )
        assert c.evaluate_criterion(user_qas)
        c = MarketplaceCondition(
            question_id="q1",
            negate=False,
            value_type=ConditionValueType.LIST,
            values=["a2"],
            logical_operator=LogicalOperator.OR,
        )
        assert c.evaluate_criterion(user_qas)
        c = MarketplaceCondition(
            question_id="q1",
            negate=False,
            value_type=ConditionValueType.LIST,
            values=["a1", "a3"],
            logical_operator=LogicalOperator.OR,
        )
        assert not c.evaluate_criterion(user_qas)
        c = MarketplaceCondition(
            question_id="q2",
            negate=False,
            value_type=ConditionValueType.LIST,
            values=["a1", "a2", "a3"],
            logical_operator=LogicalOperator.OR,
        )
        assert c.evaluate_criterion(user_qas) is None

    def test_list_or_negate(self):
        from generalresearch.models import LogicalOperator
        from generalresearch.models.thl.survey.condition import (
            MarketplaceCondition,
            ConditionValueType,
        )

        user_qas = {"q1": {"a2"}}
        c = MarketplaceCondition(
            question_id="q1",
            negate=True,
            value_type=ConditionValueType.LIST,
            values=["a1", "a2", "a3"],
            logical_operator=LogicalOperator.OR,
        )
        assert not c.evaluate_criterion(user_qas)
        c = MarketplaceCondition(
            question_id="q1",
            negate=True,
            value_type=ConditionValueType.LIST,
            values=["a2"],
            logical_operator=LogicalOperator.OR,
        )
        assert not c.evaluate_criterion(user_qas)
        c = MarketplaceCondition(
            question_id="q1",
            negate=True,
            value_type=ConditionValueType.LIST,
            values=["a1", "a3"],
            logical_operator=LogicalOperator.OR,
        )
        assert c.evaluate_criterion(user_qas)
        c = MarketplaceCondition(
            question_id="q2",
            negate=True,
            value_type=ConditionValueType.LIST,
            values=["a1", "a2", "a3"],
            logical_operator=LogicalOperator.OR,
        )
        assert c.evaluate_criterion(user_qas) is None

    def test_list_and(self):
        from generalresearch.models import LogicalOperator
        from generalresearch.models.thl.survey.condition import (
            MarketplaceCondition,
            ConditionValueType,
        )

        user_qas = {"q1": {"a1", "a2"}}
        c = MarketplaceCondition(
            question_id="q1",
            negate=False,
            value_type=ConditionValueType.LIST,
            values=["a1", "a2", "a3"],
            logical_operator=LogicalOperator.AND,
        )
        assert not c.evaluate_criterion(user_qas)
        c = MarketplaceCondition(
            question_id="q1",
            negate=False,
            value_type=ConditionValueType.LIST,
            values=["a2"],
            logical_operator=LogicalOperator.AND,
        )
        assert c.evaluate_criterion(user_qas)
        user_qas = {"q1": {"a1"}}
        c = MarketplaceCondition(
            question_id="q1",
            negate=False,
            value_type=ConditionValueType.LIST,
            values=["a1", "a2"],
            logical_operator=LogicalOperator.AND,
        )
        assert not c.evaluate_criterion(user_qas)
        c = MarketplaceCondition(
            question_id="q1",
            negate=False,
            value_type=ConditionValueType.LIST,
            values=["a1", "a3"],
            logical_operator=LogicalOperator.AND,
        )
        assert not c.evaluate_criterion(user_qas)
        c = MarketplaceCondition(
            question_id="q2",
            negate=False,
            value_type=ConditionValueType.LIST,
            values=["a1", "a2", "a3"],
            logical_operator=LogicalOperator.AND,
        )
        assert c.evaluate_criterion(user_qas) is None

    def test_list_and_negate(self):
        from generalresearch.models import LogicalOperator
        from generalresearch.models.thl.survey.condition import (
            MarketplaceCondition,
            ConditionValueType,
        )

        user_qas = {"q1": {"a1", "a2"}}
        c = MarketplaceCondition(
            question_id="q1",
            negate=True,
            value_type=ConditionValueType.LIST,
            values=["a1", "a2", "a3"],
            logical_operator=LogicalOperator.AND,
        )
        assert c.evaluate_criterion(user_qas)
        c = MarketplaceCondition(
            question_id="q1",
            negate=True,
            value_type=ConditionValueType.LIST,
            values=["a2"],
            logical_operator=LogicalOperator.AND,
        )
        assert not c.evaluate_criterion(user_qas)
        c = MarketplaceCondition(
            question_id="q1",
            negate=True,
            value_type=ConditionValueType.LIST,
            values=["a1", "a3"],
            logical_operator=LogicalOperator.AND,
        )
        assert c.evaluate_criterion(user_qas)
        c = MarketplaceCondition(
            question_id="q2",
            negate=True,
            value_type=ConditionValueType.LIST,
            values=["a1", "a2", "a3"],
            logical_operator=LogicalOperator.AND,
        )
        assert c.evaluate_criterion(user_qas) is None

    def test_ranges(self):
        from generalresearch.models import LogicalOperator
        from generalresearch.models.thl.survey.condition import (
            MarketplaceCondition,
            ConditionValueType,
        )

        user_qas = {"q1": {"2", "50"}}
        c = MarketplaceCondition(
            question_id="q1",
            negate=False,
            value_type=ConditionValueType.RANGE,
            values=["1-4", "10-20"],
            logical_operator=LogicalOperator.OR,
        )
        assert c.evaluate_criterion(user_qas)
        c = MarketplaceCondition(
            question_id="q1",
            negate=False,
            value_type=ConditionValueType.RANGE,
            values=["1-4"],
            logical_operator=LogicalOperator.OR,
        )
        assert c.evaluate_criterion(user_qas)
        c = MarketplaceCondition(
            question_id="q1",
            negate=False,
            value_type=ConditionValueType.RANGE,
            values=["10-20"],
            logical_operator=LogicalOperator.OR,
        )
        assert not c.evaluate_criterion(user_qas)
        c = MarketplaceCondition(
            question_id="q2",
            negate=False,
            value_type=ConditionValueType.RANGE,
            values=["1-4", "10-20"],
            logical_operator=LogicalOperator.OR,
        )
        assert c.evaluate_criterion(user_qas) is None
        # --- negate
        c = MarketplaceCondition(
            question_id="q1",
            negate=True,
            value_type=ConditionValueType.RANGE,
            values=["1-4", "10-20"],
            logical_operator=LogicalOperator.OR,
        )
        assert not c.evaluate_criterion(user_qas)
        c = MarketplaceCondition(
            question_id="q1",
            negate=True,
            value_type=ConditionValueType.RANGE,
            values=["10-20"],
            logical_operator=LogicalOperator.OR,
        )
        assert c.evaluate_criterion(user_qas)
        # --- AND
        with pytest.raises(expected_exception=ValidationError):
            c = MarketplaceCondition(
                question_id="q1",
                negate=False,
                value_type=ConditionValueType.RANGE,
                values=["1-4", "10-20"],
                logical_operator=LogicalOperator.AND,
            )

    def test_ranges_to_list(self):
        from generalresearch.models import LogicalOperator
        from generalresearch.models.thl.survey.condition import (
            MarketplaceCondition,
            ConditionValueType,
        )

        user_qas = {"q1": {"2", "50"}}
        MarketplaceCondition._CONVERT_LIST_TO_RANGE = ["q1"]
        c = MarketplaceCondition(
            question_id="q1",
            negate=False,
            value_type=ConditionValueType.RANGE,
            values=["1-4", "10-12", "3-5"],
            logical_operator=LogicalOperator.OR,
        )
        assert c.evaluate_criterion(user_qas)
        assert ConditionValueType.LIST == c.value_type
        assert ["1", "10", "11", "12", "2", "3", "4", "5"] == c.values

    def test_ranges_infinity(self):
        from generalresearch.models import LogicalOperator
        from generalresearch.models.thl.survey.condition import (
            MarketplaceCondition,
            ConditionValueType,
        )

        user_qas = {"q1": {"2", "50"}}
        c = MarketplaceCondition(
            question_id="q1",
            negate=False,
            value_type=ConditionValueType.RANGE,
            values=["1-4", "10-inf"],
            logical_operator=LogicalOperator.OR,
        )
        assert c.evaluate_criterion(user_qas)
        user_qas = {"q1": {"5", "50"}}
        c = MarketplaceCondition(
            question_id="q1",
            negate=False,
            value_type=ConditionValueType.RANGE,
            values=["1-4", "60-inf"],
            logical_operator=LogicalOperator.OR,
        )
        assert not c.evaluate_criterion(user_qas)

        # need to test negative infinity!
        c = MarketplaceCondition(
            question_id="q1",
            negate=False,
            value_type=ConditionValueType.RANGE,
            values=["inf-40"],
            logical_operator=LogicalOperator.OR,
        )
        assert c.evaluate_criterion({"q1": {"5", "50"}})
        c = MarketplaceCondition(
            question_id="q1",
            negate=False,
            value_type=ConditionValueType.RANGE,
            values=["inf-40"],
            logical_operator=LogicalOperator.OR,
        )
        assert not c.evaluate_criterion({"q1": {"50"}})

    def test_answered(self):
        from generalresearch.models.thl.survey.condition import (
            MarketplaceCondition,
            ConditionValueType,
        )

        user_qas = {"q1": {"a2"}}
        c = MarketplaceCondition(
            question_id="q1",
            negate=False,
            value_type=ConditionValueType.ANSWERED,
            values=[],
        )
        assert c.evaluate_criterion(user_qas)
        c = MarketplaceCondition(
            question_id="q2",
            negate=False,
            value_type=ConditionValueType.ANSWERED,
            values=[],
        )
        assert not c.evaluate_criterion(user_qas)
        c = MarketplaceCondition(
            question_id="q1",
            negate=True,
            value_type=ConditionValueType.ANSWERED,
            values=[],
        )
        assert not c.evaluate_criterion(user_qas)
        c = MarketplaceCondition(
            question_id="q2",
            negate=True,
            value_type=ConditionValueType.ANSWERED,
            values=[],
        )
        assert c.evaluate_criterion(user_qas)

    def test_invite(self):
        from generalresearch.models.thl.survey.condition import (
            MarketplaceCondition,
            ConditionValueType,
        )

        user_groups = {"g1", "g2", "g3"}
        c = MarketplaceCondition(
            question_id=None,
            negate=False,
            value_type=ConditionValueType.RECONTACT,
            values=["g1", "g4"],
        )
        assert c.evaluate_criterion(user_qas=dict(), user_groups=user_groups)
        c = MarketplaceCondition(
            question_id=None,
            negate=False,
            value_type=ConditionValueType.RECONTACT,
            values=["g4"],
        )
        assert not c.evaluate_criterion(user_qas=dict(), user_groups=user_groups)

        c = MarketplaceCondition(
            question_id=None,
            negate=True,
            value_type=ConditionValueType.RECONTACT,
            values=["g1", "g4"],
        )
        assert not c.evaluate_criterion(user_qas=dict(), user_groups=user_groups)
        c = MarketplaceCondition(
            question_id=None,
            negate=True,
            value_type=ConditionValueType.RECONTACT,
            values=["g4"],
        )
        assert c.evaluate_criterion(user_qas=dict(), user_groups=user_groups)
