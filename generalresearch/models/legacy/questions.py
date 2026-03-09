from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, List, Optional

from pydantic import (
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    NonNegativeInt,
    StringConstraints,
    ValidationError,
    field_validator,
    model_validator,
)
from sentry_sdk import capture_exception
from typing_extensions import Annotated, Self

from generalresearch.models.custom_types import UUIDStr
from generalresearch.models.legacy.api_status import StatusResponse
from generalresearch.models.thl.profiling.upk_question import (
    UpkQuestionOut,
)
from generalresearch.models.thl.session import Wall
from generalresearch.models.thl.user import User

if TYPE_CHECKING:
    from generalresearch.managers.thl.user_manager.user_manager import (
        UserManager,
    )
    from generalresearch.managers.thl.wall import WallManager


class UpkQuestionResponse(StatusResponse):
    questions: List[UpkQuestionOut] = Field()
    consent_questions: List[Dict[str, Any]] = Field(
        description="For internal use", default_factory=list
    )
    special_questions: List[Dict[str, Any]] = Field(
        description="For internal use", default_factory=list
    )
    count: NonNegativeInt = Field(description="The number of questions returned")


AnswerStr = Annotated[
    # TODO: What should the max_length be? TE open ended questions could
    #   mess with this...
    str,
    StringConstraints(min_length=1, max_length=5_000),
]


class UserQuestionAnswerIn(BaseModel):
    """Send the answers to one or more questions for a user. A question is
    uniquely specified by the question_id key. The answer is: the choice_id
    if the question_type is "MC" the actual entered text if the
    question_type is "TE"

    TODO: look up the question_type from the question_id to apply MC or
        TE specific validation on the answer(s)
    """

    model_config = ConfigDict(
        # This is applied to private empty strings as answers. However, it may
        # alter TE input from users in unexpected ways for security or other
        # forms of validation checks as it seems to modify the values in place.
        str_strip_whitespace=True,
        extra="forbid",
        frozen=True,
    )

    question_id: UUIDStr = Field(examples=["fb20fd4773304500b39c4f6de0012a5a"])

    answer: List[AnswerStr] = Field(
        min_length=1,
        max_length=10,
        description="The user's answers to this question. Must pass the "
        "choice_id if the question is a Multiple Choice, or the "
        "actual text if the question is Text Entry",
        examples=[["1"]],
    )

    # --- Validation ---
    @model_validator(mode="after")
    def single_answer_questions(self):
        user_agent_qid = "2fbedb2b9f7647b09ff5e52fa119cc5e"
        fingerprint_langs = "4030c52371b04e80b64e058d9c5b82e9"
        fingerprint_tz = "a91cb1dea814480dba12d9b7b48696dd"
        fingerprint_fingerprint = "1d1e2e8380ac474b87fb4e4c569b48df"

        if self.question_id in {
            user_agent_qid,
            fingerprint_langs,
            fingerprint_tz,
            fingerprint_fingerprint,
        }:
            if len(self.answer) != 1:
                raise ValueError("Too many answer values provided")

        return self

    @model_validator(mode="after")
    def user_agent_check(self) -> Self:
        # TODO: where / how do I want to pass in this Werz user_agent stuff?
        user_agent_qid = "2fbedb2b9f7647b09ff5e52fa119cc5e"

        if self.question_id == user_agent_qid:
            val = self.answer[0]
            # assert val == request.user_agent.to_header():
            pass

        return self

    @field_validator("answer", mode="after")
    @classmethod
    def no_duplicate_answer_values(cls, v: List[AnswerStr]) -> List[AnswerStr]:
        if len(v) != len(set(v)):
            raise ValueError("Don't provide duplicate answers")

        return v

    @field_validator("answer", mode="after")
    @classmethod
    def sort_answer_values(cls, v: List[AnswerStr]) -> List[AnswerStr]:
        return sorted(v)

    # --- Properties ---

    # --- Methods ---


def preflight(li):
    # https://github.com/pydantic/pydantic/discussions/7660
    new_li = []
    for x in li:
        try:
            x = UserQuestionAnswerIn.model_validate(x)
            new_li.append(x)
        except ValidationError as e:
            capture_exception(error=e)
            continue

    return new_li


class UserQuestionAnswers(BaseModel):
    model_config = ConfigDict(extra="forbid", arbitrary_types_allowed=True)

    product_id: UUIDStr = Field(examples=["4fe381fb7186416cb443a38fa66c6557"])

    product_user_id: str = Field(
        min_length=3,
        max_length=128,
        examples=["app-user-9329ebd"],
        description="A unique identifier for each user, which is set by the "
        "Supplier. It should not contain any sensitive information"
        "like email or names, and should avoid using any"
        "incrementing values.",
    )

    # Notice: There may be an issue where we could have told Suppliers that
    # POST /profiling-questions/ that they could use a randomly generated
    # session_id... I'm not sure, but it's entirely possible this will start
    # to cause issues in production.
    session_id: Optional[UUIDStr] = Field(
        default=None,
        description="The Session ID corresponds to the Wall.uuid. If profiling"
        "answers are being submitted directly, this can be None.",
    )

    # We don't apply a default_factory here because there is no valid reason
    # why a GRS submission would come valid without any answers.
    answers: Annotated[List[UserQuestionAnswerIn], BeforeValidator(preflight)] = Field(
        min_length=1,
        max_length=100,
        description="The list of questions and their answers that are being"
        "submitted by the user (if via GRS), or by the Supplier "
        "(if via FSB).",
    )

    user: Optional[User] = Field(default=None)
    wall: Optional[Wall] = Field(default=None)

    # --- Validation ---

    # A user that doesn't yet exist can submit profiling questions,
    #   since there is no explicit "Create User" call. If session_id
    #   is passed, then the user should exist.
    # @model_validator(mode="after")
    # def user_exists(self):
    #     if self.user is None:
    #         raise ValueError("Invalid user")
    #     return self

    @model_validator(mode="after")
    def valid_wall_event(self):
        # session_id is Optional, so break early if we can't proceed.
        if self.session_id is None:
            return self
        return self

    # I have this commented out for now because there is an argument to be made
    # that a blocked user can or should be able to submit profiling data, or
    # at least init a MarketplaceUserQuestionAnswer.
    # @model_validator(mode="after")
    # def grs_allowed_user(self):
    #     assert not self.user.blocked, "blocked user can't submit profiling "
    #     return self

    @field_validator("answers", mode="after")
    @classmethod
    def no_duplicate_questions(cls, v: List[UserQuestionAnswerIn]):
        answer_qids = [qa.question_id for qa in v]
        if len(answer_qids) != len(set(answer_qids)):
            raise ValueError("Don't provide answers to duplicate questions")

        return v

    # --- Prefetch ---
    def prefetch_user(self, um: "UserManager") -> None:
        from generalresearch.models.thl.user import User

        res: Optional[User] = um.get_user_if_exists(
            product_id=self.product_id, product_user_id=self.product_user_id
        )

        if res is None:
            raise ValidationError("Invalid user")

        self.user = res
        return None

    def prefetch_wall(self, wm: "WallManager") -> None:
        from generalresearch.models import Source
        from generalresearch.models.thl.session import Wall

        res: Optional[Wall] = wm.get_from_uuid_if_exists(wall_uuid=self.session_id)

        if res is None:
            raise ValueError("Invalid Event for session_id")

        if res.source != Source.GRS:
            raise ValueError("Not a valid GRS event")

        if res.user_id != self.product_user_id:
            raise ValueError("Not a valid GRS event for this user")

        # I think it's fair to say a UserQuestionAnswers instance can / should
        # only be initialized for a Wall event that exists, but hasn't been
        # finished yet. Therefor this is safe to do for legit users for now
        if res.finished is not None:
            raise ValueError("Not a valid GRS event status")

        self.wall = res
        return None
