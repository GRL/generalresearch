from dataclasses import dataclass
from enum import Enum
from typing import Optional, Set

from generalresearch.models import Source
from generalresearch.models.thl.survey.condition import (
    MarketplaceCondition,
)


class SoftPairResultType(int, Enum):
    # type=1 - Eligible unconditionally
    UNCONDITIONAL = 1
    # type=3 - Eligible conditionally. Must include question_ids.
    CONDITIONAL = 3
    # type=2 - Eligible conditionally, includes the option_ids that would make
    # the pairing eligible. This is unused in practice because it is often
    # impossible to describe the relationship
    UNUSED = 2
    # This isn't used in practice b/c the survey just wouldn't be returned.
    # This is just for testing/validation.
    INELIGIBLE = 4


@dataclass
class SoftPairResult:
    # We use this within the Marketplace's get_opportunities_soft_pairing
    # "hot path". We instantiate a SoftPairResult for each survey-result.
    # There is a lot of overhead in pydantic and that causes the call to be
    # kind of slow, especially for spectrum. There isn't a lot of validation
    # needed here, so I think it is a reasonable tradeoff to make this a
    # dataclass instead.
    pair_type: SoftPairResultType
    source: Source
    survey_id: str
    conditions: Optional[Set[MarketplaceCondition]] = None

    @property
    def survey_sid(self) -> str:
        return self.source + ":" + self.survey_id

    @property
    def grpc_string(self) -> Optional[str]:
        # This is what is expected by thl-grpc in a mp_pb2.MPOpportunityIDListSoftPairing response (grpc)
        if self.pair_type == SoftPairResultType.UNCONDITIONAL:
            return self.survey_id
        elif self.pair_type == SoftPairResultType.CONDITIONAL:
            return (
                self.survey_id
                + ":"
                + ";".join(sorted(set([c.question_id for c in self.conditions])))
            )
        else:
            return None


@dataclass
class SoftPairResultOut:
    # This is used by the thl-grpc to parse the grpc message
    pair_type: SoftPairResultType
    source: Source
    survey_id: str
    question_ids: Optional[Set[str]] = None

    @property
    def survey_sid(self) -> str:
        return self.source + ":" + self.survey_id
