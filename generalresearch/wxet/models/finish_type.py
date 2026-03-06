from enum import Enum
from typing import Set, Optional

from generalresearch.utils.enum import ReprEnumMeta
from generalresearch.wxet.models.definitions import WXETStatus, WXETStatusCode1


class FinishType(str, Enum, metaclass=ReprEnumMeta):
    """A Task can be classified as "finished" based on different outcomes.
    <br/>
    This controls how the `Task.required_finish_count` value
    is consumed, which informs Suppliers how many "remaining spots" are
    available for this Task. <br/>
    <b>Note</b>: This has nothing to do with deciding when a worker will be paid, it is
    only for counting attempts towards the required_finish_count.
    """

    # Worker made it to the client Task (eg: Pay a Worker to go to a website,
    # view content, or otherwise enter a Task that may not redirect them)
    # An entrance only "counts" if the user was redirected into the client's
    # task. This is indicated by WXETStatusCode1 >= 10.
    # This is also commonly referred to as "clicks".
    ENTRANCE = "entrance"

    # Buyer reports the attempt status as Complete
    COMPLETE = "complete"

    # Buyer reports the attempt status as Fail or Complete. This is
    # used when Abandons are not wanted to be counted.
    FAIL_OR_COMPLETE = "fail_or_complete"

    # Buyer reports the attempt status as Fail
    FAIL = "fail"

    @property
    def finish_statuses(self) -> Set[Optional[WXETStatus]]:
        """For this particular FinishType, what are the different WXETStatus
        values that are consider
        """

        match self:
            case FinishType.ENTRANCE:
                # When an attempt occurs and the user has not yet returned, the status will be None.
                # This has to count towards the finish count if the FinishType is ENTRANCE.
                return {
                    None,
                    WXETStatus.ABANDON,
                    WXETStatus.COMPLETE,
                    WXETStatus.FAIL,
                    WXETStatus.TIMEOUT,
                }

            case FinishType.COMPLETE:
                return {WXETStatus.COMPLETE}

            case FinishType.FAIL:
                return {WXETStatus.FAIL}

            case FinishType.FAIL_OR_COMPLETE:
                return {WXETStatus.FAIL, WXETStatus.COMPLETE}

            case _:
                raise ValueError()


def is_a_finish(
    status: Optional[WXETStatus],
    status_code_1: Optional[WXETStatusCode1],
    finish_type: Optional[FinishType],
) -> bool:
    """Determines if a wall event should be considered a finish or not.

    :param status: The status of the wall event.
    :param status_code_1: The status_code_1 of the wall event.
    :param finish_type: The finish_type of the task.
    """

    if status:
        assert isinstance(status, WXETStatus), "Invalid status"

    if status_code_1:
        assert isinstance(status_code_1, WXETStatusCode1), "Invalid status_code_1"

    if status is None:
        assert status_code_1 is None, "Cannot provide status_code_1 without a status"

    # If the Worker never entered the Task, then it is not a Finish,
    #  regardless of the FinishType. This `is_pre_task_entry_fail` tells us
    #  if the Worker ever left WXET and actually made it into the Task
    #  experience.
    if status_code_1 is not None and status_code_1.is_pre_task_entry_fail:
        return False

    return status in finish_type.finish_statuses
