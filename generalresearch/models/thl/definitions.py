import copy
from enum import Enum

from generalresearch.utils.enum import ReprEnumMeta


class ReservedQueryParameters(str, Enum, metaclass=ReprEnumMeta):
    PRODUCT_ID = "product_id"
    PRODUCT_USER_ID = "bp_user_id"
    BPUID = "bpuid"

    COUNTRY_CODES = "country_codes"
    COUNTRY = "country"
    COUNTRY_ISO = "country_iso"
    LANGUAGE_CODES = "lang_codes"
    LANGUAGES = "languages"

    CPI = "cpi"
    CURRENCY = "currency"
    PAYOUT = "payout"

    DURATION = "duration"
    LOI = "loi"
    REQUESTED_LOI = "req_loi"

    IP_ADDRESS = "ip"
    KEYWORD_ARGS = "kwargs"

    NAME = "name"
    DESCRIPTION = "description"
    QUALITY = "quality"
    QUALITY_CATEGORY = "quality_category"

    STATUS = "status"
    TASK_URI = "task_uri"
    TASKS = "tasks"
    TASK_STATUS_ID = "tsid"

    URI = "uri"
    URL = "url"
    FORMAT = "format"
    ENTRY_LINK = "entry_link"
    BUCKET = "b"
    INDEX = "i"
    INDEX_VERBOSE = "idx"
    INFO = "info"
    ELLE = "l"
    N_BINS = "n_bins"


class THLPaths(str, Enum, metaclass=ReprEnumMeta):

    # Endpoints on thl-fsb
    TASK_ADJUSTMENT = "f4484dbdf144451ab60cda256ce14266"
    ACCESS_CONTROL = "9bf111afe03e40719c5cd0de0dc43c31"

    # Endpoints on thl-core (TO BE REMOVED)
    #   (under /api/v1/)
    GET_GRLIQ_JS = "d9e1d3fbfa934b249abfd71f0f3bd667"
    GET_GRLIQ_LOGO = "1fe9fdec9eae43fa848c930972141436"

    # Endpoints on GRL-IQ
    #   (under /api/)
    GET_GRLIQ_JS_INLINE = "d9e1d3fbfa934b249abfd71f0f3bd667"
    GET_GRLIQ_JS_ATTR = "4a2954b34cc24f93be3e8b218e323b88"


class Status(str, Enum, metaclass=ReprEnumMeta):
    """
    The outcome of a session or wall event. If the session is still in
    progress, the status will be NULL.
    """

    # User completed the job successfully and should be paid something
    COMPLETE = "c"
    # User did not successfully complete the task. They were rejected by either
    #   GRL, the marketplace, or the buyer.
    FAIL = "f"
    # User abandoned the task. This would only get set if the BP lets us know
    # the user took some action to exit out of the task
    ABANDON = "a"
    # User either abandoned the task or was never returned to us for some
    # reason. After a pre-determined amount of time (configurable on the BP
    # level), any task that does not have a status will time out.
    TIMEOUT = "t"


class WallAdjustedStatus(str, Enum, metaclass=ReprEnumMeta):
    # Task was reconciled to complete
    ADJUSTED_TO_COMPLETE = "ac"
    # Task was reconciled to incomplete
    ADJUSTED_TO_FAIL = "af"
    # The cpi for a task was changed. This applies to Wall events ONLY.
    CPI_ADJUSTMENT = "ca"
    # This is only supported for compatibility reasons, as we currently do not
    #   do anything with confirmed completes as they have historically been
    #   meaningless. They only get added to the thl_taskadjustment table, and
    #   won't get used in the Wall.adjusted_status (for now. The
    #   WallManager.adjust_status does not support doing anything with this).
    CONFIRMED_COMPLETE = "cc"


class SessionAdjustedStatus(str, Enum, metaclass=ReprEnumMeta):
    """An adjusted_status is set if a session is adjusted by the marketplace
    after the original return. A session can be adjusted multiple times.
    This is the most recent status. If a session was originally a complete,
    was adjusted to incomplete, then back to complete, the adjusted_status
    will be None, but the adjusted_timestamp will be set to the most recent
    change.
    """

    # Task was reconciled to complete
    ADJUSTED_TO_COMPLETE = "ac"
    # Task was reconciled to incomplete
    ADJUSTED_TO_FAIL = "af"
    # The payout was changed. This applies to Sessions ONLY.
    PAYOUT_ADJUSTMENT = "pa"


class StatusCode1(int, Enum, metaclass=ReprEnumMeta):
    """
    __High level status code for outcome of the session.__
    This should only be NULL if the Status is ABANDON or TIMEOUT
    """

    # Do not use 0 because grpc does not distinguish between 0 and None.

    # User terminated in buyer survey
    BUYER_FAIL = 1
    # User terminated in buyer survey for quality reasons
    BUYER_QUALITY_FAIL = 2
    # User failed in marketplace's prescreener
    PS_FAIL = 3
    # User rejected by marketplace for quality reasons
    PS_QUALITY = 4
    # User is explicitly blocked by the marketplace. Note: on some marketplaces,
    #   users can have multiple PS_QUALITY terminations and still complete
    #   surveys.
    PS_BLOCKED = 5
    # User rejected by marketplace for over quota
    PS_OVERQUOTA = 6
    # User rejected by marketplace for duplicate
    PS_DUPLICATE = 7
    # The user failed within the GRS Platform
    GRS_FAIL = 8
    # The user failed within the GRS Platform for quality reasons
    GRS_QUALITY_FAIL = 9

    # The user abandoned/timed out within the GRS Platform
    GRS_ABANDON = 10
    # The user abandoned/timed out within the marketplace's pre-screen system.
    #   Note: On most marketplaces, we have no way of distinguishing between
    #   this and BUYER_ABANDON. BUYER_ABANDON is used as the default, unless we
    #   know it is PS_ABANDON.
    PS_ABANDON = 11
    # The user abandoned/timed out within the client survey
    BUYER_ABANDON = 12

    # The status code is not documented
    UNKNOWN = 13
    # The user completed the task successfully
    COMPLETE = 14

    # Something was wrong upon the user redirecting from the marketplace, e.g. no postback received,
    #   or url hashing failures.
    MARKETPLACE_FAIL = 15

    # **** Below here should ONLY be used on a Session (not a Wall) ****

    # User failed before being sent into a marketplace
    SESSION_START_FAIL = 16
    # User failed between attempts
    SESSION_CONTINUE_FAIL = 17
    # User failed before being sent into a marketplace for "security" reasons
    SESSION_START_QUALITY_FAIL = 18
    # User failed between attempts for "security" reasons
    SESSION_CONTINUE_QUALITY_FAIL = 19


class SessionStatusCode2(int, Enum, metaclass=ReprEnumMeta):
    """
    __Status Detail__
    This should be set if the Session.status_code_1 is SESSION_XXX_FAIL
    """

    # Unable to parse either the bucket_id, request_id, or nudge_id from the url
    ENTRY_URL_MODIFICATION = 1
    # The client's IP failed maxmind lookup, or we failed to store it for some reason
    UNRECOGNIZED_IP = 2
    # User is using an anonymous IP
    USER_IS_ANONYMOUS = 3
    # User is blocked
    USER_IS_BLOCKED = 4
    # User is rate limited
    USER_IS_RATE_LIMITED = 5
    # The client's useragent was not categorized as desktop, mobile, or tablet
    UNRECOGNIZED_DEVICE = 6
    # The user clicked after 5 min
    OFFERWALL_EXPIRED = 7
    # Something unexpected happened
    INTERNAL_ERROR = 8
    # The user requested the offerwall for a different country than their IP
    # address indicates
    OFFERWALL_COUNTRY_MISMATCH = 9
    # The bucket id indicated in the url does not exist. This is likely due
    # to the user clicking on a bucket for an offerwall that has already
    # been refreshed.
    INVALID_BUCKET_ID = 10
    # Not necessarily the user's fault. We thought we had surveys, but due to
    # for e.g. the user entering on a different device than we thought, there
    # really are none. If we get a lot of these, then that might indicate
    # something is wrong.
    NO_TASKS_AVAILABLE = 11
    # The entrance attempt was flagged by GRLIQ as suspicious
    ATTEMPT_IS_SUSPICIOUS = 12
    # No GRLIQ forensics post was received
    GRLIQ_MISSING = 13


class WallStatusCode2(int, Enum, metaclass=ReprEnumMeta):
    """
    This should be set if the Wall.status_code_1 is MARKETPLACE_FAIL
    """

    # The redirect URL (coming back from the marketplace) failed hashing checks
    URL_HASHING_CHECK_FAILED = 12
    # The redirect URL was missing required query params or was unparseable
    BROKEN_REDIRECT = 16
    # The redirect URL was invalid or inconsistent in some way and as a result
    # we could not determine the outcome. This could be if a redirect received
    # did not match the user's most recent attempt.
    INVALID_REDIRECT = 17
    # The redirect indicated a complete, but no/invalid Postback was received
    # from the marketplace
    INVALID_MARKETPLACE_POSTBACK = 13
    # No/invalid Postback was received from the marketplace. Used in cases where
    # the redirect does not contain a status.
    NO_MARKETPLACE_POSTBACK = 18
    # The marketplace indicates the user completed the survey, but we don't
    # think this is valid due to speeding. Generally this cutoff is the 95th
    # percentile of our calculated CompletionTime survey stat.
    COMPLETE_TOO_FAST = 14
    # Something happened during the handling of this redirect (on our side)
    INTERNAL_ERROR = 15


WALL_ALLOWED_STATUS_STATUS_CODE = {
    Status.COMPLETE: {StatusCode1.COMPLETE},
    Status.FAIL: {
        StatusCode1.BUYER_FAIL,
        StatusCode1.BUYER_QUALITY_FAIL,
        StatusCode1.PS_FAIL,
        StatusCode1.PS_QUALITY,
        StatusCode1.PS_DUPLICATE,
        StatusCode1.PS_OVERQUOTA,
        StatusCode1.PS_BLOCKED,
        StatusCode1.GRS_FAIL,
        StatusCode1.GRS_QUALITY_FAIL,
        StatusCode1.UNKNOWN,
        StatusCode1.MARKETPLACE_FAIL,
    },
    Status.ABANDON: {
        StatusCode1.PS_ABANDON,
        StatusCode1.BUYER_ABANDON,
        StatusCode1.GRS_ABANDON,
    },
    Status.TIMEOUT: {
        StatusCode1.PS_ABANDON,
        StatusCode1.BUYER_ABANDON,
        StatusCode1.GRS_ABANDON,
    },
}
SESSION_ALLOWED_STATUS_STATUS_CODE = copy.deepcopy(WALL_ALLOWED_STATUS_STATUS_CODE)
SESSION_ALLOWED_STATUS_STATUS_CODE[Status.FAIL].update(
    {
        StatusCode1.SESSION_START_FAIL,
        StatusCode1.SESSION_START_QUALITY_FAIL,
        StatusCode1.SESSION_CONTINUE_FAIL,
        StatusCode1.SESSION_CONTINUE_QUALITY_FAIL,
    }
)

WALL_ALLOWED_STATUS_CODE_1_2 = {
    StatusCode1.MARKETPLACE_FAIL: {
        WallStatusCode2.URL_HASHING_CHECK_FAILED,
        WallStatusCode2.INVALID_MARKETPLACE_POSTBACK,
        WallStatusCode2.COMPLETE_TOO_FAST,
    }
}


class ReportValue(int, Enum, metaclass=ReprEnumMeta):
    """
    The reason a user reported a task.
    """

    # Used to indicate the user exited the task without giving feedback
    REASON_UNKNOWN = 0
    # Task is in the wrong language/country, unanswerable question, won't proceed to
    #  next question, loading forever, error message
    TECHNICAL_ERROR = 1
    # Task ended (completed or failed, and showed the user some dialog
    # indicating the task was over), but failed to redirect
    NO_REDIRECT = 2
    # Asked for full name, home address, identity on another site, cc#
    PRIVACY_INVASION = 3
    # Asked about children, employer, medical issues, drug use, STDs, etc.
    UNCOMFORTABLE_TOPICS = 4
    # Asked to install software, signup/login to external site, access webcam,
    #  promise to pay using external site, etc.
    ASKED_FOR_NOT_ALLOWED_ACTION = 5
    # Task doesn't work well on a mobile device
    BAD_ON_MOBILE = 6
    # Too long, too boring, confusing, complicated, too many
    # open-ended/free-response questions
    DIDNT_LIKE = 7


class PayoutStatus(str, Enum, metaclass=ReprEnumMeta):
    """The max size of the db field that holds this value is 20, so please
    don't add new values longer than that!
    """

    # The user has requested a payout. The money is taken from their
    #   wallet. A PENDING request can either be APPROVED, REJECTED, or
    #   CANCELLED. We can also implicitly skip the APPROVED step and go
    #   straight to COMPLETE or FAILED.
    PENDING = "PENDING"
    # The request is approved (by us or automatically). Once approved,
    #   it can be FAILED or COMPLETE.
    APPROVED = "APPROVED"
    # The request is rejected. The user loses the money.
    REJECTED = "REJECTED"
    # The user requests to cancel the request, the money goes back into their wallet.
    CANCELLED = "CANCELLED"
    # The payment was approved, but failed within external payment provider.
    #   This is an "error" state, as the money won't have moved anywhere. A
    #   FAILED payment can be tried again and be COMPLETE.
    FAILED = "FAILED"
    # The payment was sent successfully and (usually) a fee was charged
    #   to us for it.
    COMPLETE = "COMPLETE"
    # Not supported # REFUNDED: I'm not sure if this is possible or
    #   if we'd want to allow it.
