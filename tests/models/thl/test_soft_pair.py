from generalresearch.models import Source
from generalresearch.models.thl.soft_pair import SoftPairResult, SoftPairResultType


def test_model():
    from generalresearch.models.dynata.survey import (
        DynataCondition,
        ConditionValueType,
    )

    c1 = DynataCondition(
        question_id="1", value_type=ConditionValueType.LIST, values=["a", "b"]
    )
    c2 = DynataCondition(
        question_id="2", value_type=ConditionValueType.LIST, values=["c", "d"]
    )
    sr = SoftPairResult(
        pair_type=SoftPairResultType.CONDITIONAL,
        source=Source.DYNATA,
        survey_id="xxx",
        conditions={c1, c2},
    )
    assert sr.grpc_string == "xxx:1;2"
    assert sr.survey_sid == "d:xxx"
