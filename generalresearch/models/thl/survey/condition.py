import hashlib
from abc import ABC
from enum import Enum
from functools import cached_property
from typing import Any, Dict, List, Optional, Set, Tuple

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    PrivateAttr,
    StringConstraints,
    computed_field,
    field_validator,
    model_validator,
)
from typing_extensions import Annotated, Self

from generalresearch.models import LogicalOperator

MarketplaceConditionHash = Annotated[
    str, StringConstraints(min_length=7, max_length=7, pattern=r"^[a-f0-9]+$")
]


class ConditionValueType(int, Enum):
    # The values are a list of strings that are matched entirely.
    #   e.g. ['a', 'b', 'c']
    LIST = 1

    # The values are a list of ranges. e.g. ["19-25", "35-40"],
    RANGE = 2

    # The values should be empty, we only care that the user has an answer
    #   for question_id
    ANSWERED = 3

    # The condition cannot be defined in any way that can be understood by us.
    #   The question_id may not even be one that is exposed to us. This is
    #   solely to indicate there is additional profiling on a survey.
    #   `values` is ignored.
    INEFFABLE = 4

    # The condition is checking a user's membership / group IDs; this is
    #   analogous to a recontact, where specific users are targeted. The
    #   question_id here should be null. In dynata this is called an invite
    #   collection.
    RECONTACT = 5


class MarketplaceCondition(BaseModel, ABC):
    """This represents a targeting condition that can be attached to a
    qualification or quota
    """

    model_config = ConfigDict(populate_by_name=True)

    logical_operator: LogicalOperator = Field(default=LogicalOperator.OR)
    value_type: ConditionValueType = Field()
    negate: bool = Field(default=False)

    # ---- These fields should be overridden in the implementor ---
    question_id: Optional[str] = Field(frozen=True)
    values: List[str] = Field()

    # These question_ids get converted to list value types
    _CONVERT_LIST_TO_RANGE: List[str] = PrivateAttr(default_factory=list)

    @field_validator("values", mode="after")
    def sort_values(cls, values: List[str]):
        return sorted(values)

    @field_validator("values", mode="after")
    def check_values_lower(cls, values: List[str]):
        assert values == [s.lower() for s in values], "values must be lowercase"
        return values

    @field_validator("logical_operator", mode="after")
    def explain_not(cls, logical_operator: LogicalOperator):
        assert logical_operator != LogicalOperator.NOT, (
            "Use LogicalOperator.OR/AND and negate=True in place of LogicalOperator.NOT. Otherwise the meaning"
            "is ambiguous. Do you want people who don't have a (CAT and DOG), or either don't have a CAT or"
            "don't have a DOG?"
        )
        return logical_operator

    @model_validator(mode="after")
    def type_values_default(self) -> Self:
        if self.value_type in {
            ConditionValueType.ANSWERED,
            ConditionValueType.INEFFABLE,
        }:
            assert not self.values, "values must be empty"
        return self

    @model_validator(mode="after")
    def check_type_question_id_agreement(self) -> Self:
        if self.value_type in {ConditionValueType.RECONTACT}:
            assert (
                self.question_id is None
            ), "question_id should be NULL for ConditionValueType.RECONTACT"
        else:
            assert self.question_id is not None, "question_id must be set"
        return self

    @model_validator(mode="after")
    def check_type_values_agreement(self) -> Self:
        if self.value_type in {
            ConditionValueType.LIST,
            ConditionValueType.RANGE,
        }:
            assert len(self.values) > 0, "values must not be empty"
        if self.value_type == ConditionValueType.RANGE:
            assert (
                self.logical_operator == LogicalOperator.OR
            ), "Only OR is allowed with ranges"
            assert all(
                s.count("-") == 1 for s in self.values
            ), "range values must have one hyphen"
            for v in self.values:
                assert all(
                    self.is_numeric_including_inf(x) for x in v.split("-")
                ), f"invalid range: {v}"
        elif self.value_type in {
            ConditionValueType.ANSWERED,
            ConditionValueType.INEFFABLE,
        }:
            assert len(self.values) == 0, "values must be empty"
        return self

    @model_validator(mode="after")
    def change_ranges_to_list(self) -> Self:
        """
        Decide to do this per marketplace.
        Some use ranges for ages. Ranges take longer to evaluate b/c they have to be converted
            into ints and then require multiple evaluations. Just convert into a list of values
            which only requires one easy match.
        e.g. convert age values from '20-22|20-21|25-26' to '|20|21|22|25|26|'
        """
        if (
            self.question_id in self._CONVERT_LIST_TO_RANGE
            and self.value_type == ConditionValueType.RANGE
        ):
            try:
                values = [tuple(map(int, v.split("-"))) for v in self.values]
                assert all(len(x) == 2 for x in values)
            except (ValueError, AssertionError):
                return self
            self.values = sorted(
                {str(val) for tupl in values for val in range(tupl[0], tupl[1] + 1)}
            )
            self.value_type = ConditionValueType.LIST
        return self

    @computed_field
    @cached_property
    def criterion_hash(self) -> MarketplaceConditionHash:
        # This model is frozen, so the criterion string can/will never change.
        return self._hash_string(self._criterion_str)

    @property
    def hash(self) -> MarketplaceConditionHash:
        return self.criterion_hash

    @cached_property
    def values_str(self) -> str:
        return f"|{'|'.join(self.values)}|".lower() if self.values else ""

    @cached_property
    def _criterion_str(self) -> str:
        # e.g. '42;OR;False;1;|18|19|20|21|'
        return ";".join(
            [
                str(self.question_id),
                self.logical_operator,
                str(self.negate),
                str(self.value_type.value),
                self.values_str,
            ]
        )

    @cached_property
    def values_minified(self) -> str:
        if len(self.values) > 6:
            v = self.values[:3] + ["…"] + self.values[-3:]
        else:
            v = self.values
        return f"|{'|'.join(v)}|"

    @cached_property
    def minified(self):
        return ";".join(
            [
                str(self.question_id),
                self.logical_operator,
                str(self.negate),
                str(self.value_type.value),
                self.values_minified,
            ]
        )

    @computed_field
    @property
    def value_len(self) -> int:
        return len(self.values)

    @computed_field
    @property
    def sizeof(self) -> int:
        return sum(len(v) for v in self.values)

    @cached_property
    def values_ranges(self) -> List[Tuple[float, float]]:
        assert (
            self.value_type == ConditionValueType.RANGE
        ), "only call this method when value_type is RANGE"
        values = [tuple(map(float, v.split("-"))) for v in self.values]
        # Treat 'inf' as negative infinity if it is a lower bound.
        values = [
            (float("-inf") if start == float("inf") else start, end)
            for start, end in values
        ]
        return values

    @classmethod
    def _hash_string(cls, s: str) -> str:
        return hashlib.md5(s.encode()).hexdigest()[:7]

    @classmethod
    def from_mysql(cls, d: Dict[str, Any]) -> Self:
        d["values"] = d["values"][1:-1].split("|") if d["values"][1:-1] else []
        return cls.model_validate(d)

    def to_mysql(self) -> Dict[str, str]:
        # This is what is stored in the xxx_criterion table
        d = self.model_dump(
            mode="json",
            include={
                "question_id",
                "criterion_hash",
                "value_type",
                "logical_operator",
                "negate",
            },
        )
        d["values"] = self.values_str
        return d

    @staticmethod
    def is_numeric_including_inf(s) -> bool:
        try:
            float(s)
            return True
        except ValueError:
            return False

    def __hash__(self) -> int:
        # this is so it can be put into a set / dictionary key
        return hash(self.criterion_hash)

    def __repr__(self) -> str:
        # Fancy repr that only shows the first and last 3 values if there are more than 6.
        repr_args = list(self.__repr_args__())
        for n, (k, v) in enumerate(repr_args):
            if k == "values":
                if v and len(v) > 6:
                    v = v[:3] + ["…"] + v[-3:]
                    repr_args[n] = ("values", v)
        join_str = ", "
        repr_str = join_str.join(
            repr(v) if a is None else f"{a}={v!r}" for a, v in repr_args
        )
        return f"{self.__repr_name__()}({repr_str})"

    def evaluate_criterion(
        self,
        user_qas: Dict[str, Set[str]],
        user_groups: Optional[Set[str]] = None,
    ) -> Optional[bool]:
        """Given this user's MRPQs, do they "pass" this criterion?

        :param user_qas: user's quals. Looks like {'qid1': {'ans1', 'ans2'}}
        :param user_groups: a list of "groups" the user is associated with.
            This is only used for RECONTACT conditions.
        :return: True, False, or None (means we don't know)
        """
        if self.value_type == ConditionValueType.RECONTACT:
            assert (
                user_groups is not None
            ), "user_groups must be known for RECONTACT conditions"
            if self.logical_operator == LogicalOperator.OR:
                passes = any(x in self.values for x in user_groups)
                return not passes if self.negate else passes
            elif self.logical_operator == LogicalOperator.AND:
                passes = all(x in user_groups for x in self.values)
                return not passes if self.negate else passes

        # It is unclear what we should do with INEFFABLE conditions. We keep
        #   them b/c we want to know that they exist, but we have nothing to
        #   check, so they'll just return True always
        if self.value_type == ConditionValueType.INEFFABLE:
            return True

        answer = user_qas.get(self.question_id)
        if self.value_type == ConditionValueType.ANSWERED:
            if answer is None:
                return self.negate
            else:
                return not self.negate

        if answer is None:
            return None

        if self.value_type == ConditionValueType.LIST:
            if self.logical_operator == LogicalOperator.OR:
                passes = any(f"|{x}|" in self.values_str for x in answer)
                return not passes if self.negate else passes
            elif self.logical_operator == LogicalOperator.AND:
                passes = all(x in answer for x in self.values)
                return not passes if self.negate else passes

        if self.value_type == ConditionValueType.RANGE:
            assert (
                self.logical_operator == LogicalOperator.OR
            ), "Only OR is allowed with ranges"
            # The answer and values are assumed here to be numeric. The values
            #   are expected to be two numerics separated by a dash. e.g.
            #   "1-10". The interval is always closed (includes the endpoints,
            #   gte/lte). Unbounded ranges are also supported, indicated by
            #   "inf". e.g. 'inf-100' (meaning -Infinity to 100), or '10-inf'
            try:
                answer = list(map(float, answer))
            except ValueError:
                return None
            values = self.values_ranges
            passes = any([start <= x <= end for start, end in values for x in answer])
            return not passes if self.negate else passes
