from enum import Enum
from typing import Optional, Tuple

from generalresearch.currency import USDMill
from generalresearch.utils.enum import ReprEnumMeta


class IncExcFilterType(str, Enum, metaclass=ReprEnumMeta):
    INCLUDE = "include"
    EXCLUDE = "exclude"


# Note: This is exactly the same as the py-utils:models/thl/definitions.py:Status.
# Keeping this because the comments (and as a result, the documentation)
#   is slightly different, and specific to wxet.
class WXETStatus(str, Enum, metaclass=ReprEnumMeta):
    """
    The outcome of a task attempt. If the attempt is still in progress, the status will be NULL.
    """

    # Worker completed the task.
    COMPLETE = "c"

    # Worker did not complete task. They were rejected by either WXET or buyer.
    FAIL = "f"

    # Worker abandoned the task. Only set if the Buyer informs us that the
    # user took some action to exit out of the task
    ABANDON = "a"

    # Worker either abandoned the task or was never returned. After a
    #   pre-determined amount of time (configurable), any task that does
    #   not have a status will time out.
    # todo: setup the timeout logic for wxet
    TIMEOUT = "t"


# Basically same note as for WxetStatus for WallAdjustedStatus
class WXETAdjustedStatus(str, Enum, metaclass=ReprEnumMeta):
    # Task was reconciled to complete
    ADJUSTED_TO_COMPLETE = "ac"

    # Task was reconciled to incomplete
    ADJUSTED_TO_FAIL = "af"

    # The cpi for a task was adjusted
    CPI_ADJUSTMENT = "ca"

    # The user was redirected without a Postback, but the postback was then "immediately"
    #   recieved. The supplier thinks this was a failure. This is distinct from an
    #   actual adjustment to complete.
    POSTBACK_COMPLETE = "pc"


class WXETStatusCode1(int, Enum, metaclass=ReprEnumMeta):
    """
    __High level status code for outcome of the attempt.__
    This should only be NULL if the WXETStatus is ABANDON or TIMEOUT
    """

    # This shouldn't be returned.
    UNKNOWN = 1
    # Worker failed to be sent into a task.
    WXET_FAIL = 2
    # The worker abandoned/timed out within wxet before being sent to task
    WXET_ABANDON = 3

    # This should never happen
    # Buyer is explicitly blocked by the marketplace.
    # WXET_BUYER_BLOCKED = 4

    # Open statuses
    # WXET_ = 5
    # WXET_ = 6
    # WXET_ = 7
    # WXET_ = 8
    # WXET_ = 9
    # WXET_ = 10

    # Values below here are considered buyer-rejections

    # Worker rejected by buyer for over quota
    BUYER_OVER_QUOTA = 11
    # Worker rejected by buyer for duplicate entrance
    BUYER_DUPLICATE = 16
    # Worker unable to enter task due it not being available.
    BUYER_TASK_NOT_AVAILABLE = 12
    # Worker abandoned/timed out within the task
    BUYER_ABANDON = 13
    # Worker terminated in buyer task
    BUYER_FAIL = 14
    # Worker terminated in buyer task due to quality reasons
    BUYER_QUALITY_FAIL = 15

    # Worker redirect with no callback received
    BUYER_POSTBACK_NOT_RECEIVED = 30

    # Completed the Task
    COMPLETE = 99

    @property
    def is_pre_task_entry_fail(self) -> bool:
        """This property helper indicates if the WXET Attempt made it into
        the WXET Account's (eg: the "buyer"'s) Task.
        """
        return False if self.value > 10 else True


class WXETStatusCode2(int, Enum, metaclass=ReprEnumMeta):
    """
    __Status Detail__
    These are generally only set if the StatusCode1 is WXET_FAIL,
      but don't *have* to be. Can be NULL even if StatusCode1 is WXET_FAIL
    """

    # Unexpected error
    INTERNAL_ERROR = 1

    # 2,3,4,5,6 not implemented

    # Worker does not meet supply configuration
    SUPPLY_CONFIG_RESTRICTED = 2
    # Worker is ineligible due to include/exclude rules on this task
    WORKER_INELIGIBLE = 3
    # Worker is excluded due to prior participation in another task via exclusion rules
    WORKER_EXCLUDED = 4
    # Worker is rate limited
    WORKER_RATE_LIMITED = 5
    # Task is rate limited
    TASK_RATE_LIMITED = 6

    # Worker has previously entered this task
    RE_ENTRY = 7

    # Worker was sent to a task which does not exist
    TASK_NOT_FOUND = 10

    # Task is no longer live
    TASK_NOT_AVAILABLE = 11

    # Task is not funded. (this shouldn't happen because if the task is live, it must be funded)
    TASK_NOT_FUNDED = 12

    # The task's required_finish_count was reached.
    TASK_NO_FINISHES_AVAILABLE = 18

    # The upper_limit was met within a task's connectors (e.g. within matching quotas)
    TASK_CONNECTOR_NO_FINISHES_AVAILABLE = 14

    # Worker was sent to a Task with Allocation(s) which are not valid. (quota specified
    #   is not associated with the task specified)
    INVALID_ALLOCATION_SELECTION = 15

    # Worker was sent to a previous version of Task. (not implemented)
    TASK_VERSION_MISMATCH = 16

    # Not eligible due to not passing the task's task_group_filters
    TASK_GROUP_FILTERS_FAIL = 17

    # Not eligible due to not passing the task's respondent filters
    RESPONDENT_FILTERS_FAIL = 18

    # Not eligible due to not passing the task's scheduled fielding
    SCHEDULED_FIELDING_FAIL = 19


def check_wxet_status_consistent(
    status: WXETStatus,
    status_code_1: Optional[WXETStatusCode1] = None,
    status_code_2: Optional[WXETStatusCode2] = None,
) -> bool:
    """
    Raises an AssertionError if inconsistent
    """

    if status == WXETStatus.COMPLETE:
        assert (
            status_code_1 == WXETStatusCode1.COMPLETE
        ), "Invalid StatusCode1 when Status=COMPLETE. Use WXETStatusCode1.COMPLETE"

    if status == WXETStatus.ABANDON:
        assert status_code_1 in {
            WXETStatusCode1.WXET_ABANDON,
            WXETStatusCode1.BUYER_ABANDON,
        }, "Invalid StatusCode1 when Status=ABANDON. Use WXET_ABANDON or BUYER_ABANDON"

    if status == WXETStatus.FAIL:
        # status_code_1 can be anything except complete or abandon
        assert status_code_1 not in {
            WXETStatusCode1.COMPLETE,
            WXETStatusCode1.WXET_ABANDON,
            WXETStatusCode1.BUYER_ABANDON,
        }, "Invalid StatusCode1 when Status=FAIL."

    # (Currently), status code 2 is only used if WXETStatusCode1 is wxet fail
    if status_code_2 is not None:
        assert (
            status_code_1 == WXETStatusCode1.WXET_FAIL
        ), "status_code_1 should be WXET_FAIL if a status_code_2 is set"

    return True


def check_wxet_adjusted_status_attempt_consistent(
    status: WXETStatus,
    status_code_1: Optional[WXETStatusCode1] = None,
    cpi: Optional[USDMill] = None,
    adjusted_status: Optional[WXETAdjustedStatus] = None,
    adjusted_cpi: Optional[USDMill] = None,
    new_adjusted_status: Optional[WXETAdjustedStatus] = None,
    new_adjusted_cpi: Optional[USDMill] = None,
) -> Tuple[bool, str]:
    """
    Raises an AssertionError if inconsistent.
    - status, status_code_1, adjusted_status, adjusted_cpi, cpi are the attempt's CURRENT values
    - new_adjusted_status & new_adjusted_cpi are attempting to be set
    We are checking if the adjustment is allowed, based on the attempt's current status.
    """
    try:
        _check_wxet_adjusted_status_attempt_consistent(
            status=status,
            status_code_1=status_code_1,
            cpi=cpi,
            adjusted_status=adjusted_status,
            adjusted_cpi=adjusted_cpi,
            new_adjusted_status=new_adjusted_status,
            new_adjusted_cpi=new_adjusted_cpi,
        )
    except AssertionError as e:
        return False, str(e)
    return True, ""


def _check_wxet_adjusted_status_attempt_consistent(
    status: WXETStatus,
    status_code_1: Optional[WXETStatusCode1] = None,
    cpi: Optional[USDMill] = None,
    adjusted_status: Optional[WXETAdjustedStatus] = None,
    adjusted_cpi: Optional[USDMill] = None,
    new_adjusted_status: Optional[WXETAdjustedStatus] = None,
    new_adjusted_cpi: Optional[USDMill] = None,
) -> None:
    """
    Raises an AssertionError if inconsistent.
    - status, status_code_1, adjusted_status, adjusted_cpi, cpi are the attempt's CURRENT values
    - new_adjusted_status & new_adjusted_cpi are attempting to be set
    We are checking if the adjustment is allowed, based on the attempt's current status.
    """
    # Check the original attempt actually even entered the client survey
    if status_code_1 and status_code_1.is_pre_task_entry_fail:
        raise AssertionError("pre-task entry fail, can't adjust status")

    # Check that we're actually changing something
    if adjusted_status == new_adjusted_status and adjusted_cpi == new_adjusted_cpi:
        raise AssertionError(f"attempt is already {adjusted_status=}, {adjusted_cpi=}")

    # adjusted_status/adjusted_cpi agreement
    _check_wxet_adjusted_status_consistent(
        adjusted_status=new_adjusted_status, adjusted_cpi=new_adjusted_cpi
    )
    if new_adjusted_status == WXETAdjustedStatus.ADJUSTED_TO_FAIL:
        assert new_adjusted_cpi == USDMill(
            0
        ), "adjusted_cpi should be 0 if adjusted_status is ADJUSTED_TO_FAIL"
    elif new_adjusted_status == WXETAdjustedStatus.ADJUSTED_TO_COMPLETE:
        assert (
            new_adjusted_cpi == cpi
        ), "adjusted_cpi should be equal to the original cpi if adjusted_status is ADJUSTED_TO_COMPLETE"
    elif new_adjusted_status == WXETAdjustedStatus.CPI_ADJUSTMENT:
        assert new_adjusted_cpi != cpi and new_adjusted_cpi != USDMill(
            0
        ), "adjusted_cpi should be different than the original cpi if CPI_ADJUSTMENT"
    elif adjusted_status is None:
        # It'll be None if we are going, for e.g. Complete -> Fail -> Complete
        assert new_adjusted_cpi is None, "adjusted_cpi should be None"

    # status / adjusted_status agreement
    if status == WXETStatus.COMPLETE:
        assert (
            new_adjusted_status != WXETAdjustedStatus.ADJUSTED_TO_COMPLETE
        ), "adjusted status can't be ADJUSTED_TO_COMPLETE if the status is COMPLETE"
    elif status == WXETStatus.FAIL:
        assert (
            new_adjusted_status != WXETAdjustedStatus.ADJUSTED_TO_FAIL
        ), "adjusted status can't be ADJUSTED_TO_FAIL if the status is FAIL"
    else:
        # status is None/timeout/abandon, which we treat as a fail anyway
        assert (
            new_adjusted_status != WXETAdjustedStatus.ADJUSTED_TO_FAIL
        ), "attempt is already a failure"

    # adjusted_status / new_adjusted_status agreement
    if new_adjusted_status == WXETAdjustedStatus.CPI_ADJUSTMENT:
        assert (
            new_adjusted_cpi != adjusted_cpi
        ), f"adjusted_cpi is already {adjusted_cpi}"


def _check_wxet_adjusted_status_consistent(
    adjusted_status: Optional[WXETAdjustedStatus] = None,
    adjusted_cpi: Optional[USDMill] = None,
) -> None:
    """
    Raises an AssertionError if inconsistent.
    - adjusted_status & adjusted_cpi are attempting to be set
    """
    # adjusted_status/adjusted_cpi agreement
    adjusted_cpi = adjusted_cpi if adjusted_cpi is not None else USDMill(0)
    if adjusted_status == WXETAdjustedStatus.ADJUSTED_TO_FAIL:
        assert adjusted_cpi == USDMill(
            0
        ), "adjusted_cpi should be 0 if adjusted_status is ADJUSTED_TO_FAIL"
    elif adjusted_status == WXETAdjustedStatus.ADJUSTED_TO_COMPLETE:
        assert adjusted_cpi != USDMill(
            0
        ), "adjusted_cpi should be equal to the original cpi if adjusted_status is ADJUSTED_TO_COMPLETE"
    elif adjusted_status == WXETAdjustedStatus.CPI_ADJUSTMENT:
        assert adjusted_cpi != USDMill(
            0
        ), "adjusted_cpi should be different than the original cpi if CPI_ADJUSTMENT"
    elif adjusted_status is None:
        # It'll be None if we are going, for e.g. Complete -> Fail -> Complete
        pass
