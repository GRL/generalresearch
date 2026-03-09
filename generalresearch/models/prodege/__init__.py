from enum import Enum
from typing import Literal

from pydantic import Field
from typing_extensions import Annotated

ProdegeQuestionIdType = Annotated[
    str, Field(min_length=1, max_length=16, pattern=r"^[0-9]+$")
]


class ProdegeStatus(str, Enum):
    LIVE = "LIVE"
    # We need another status to mark if a survey we thought was live does not come back
    #   from the API, we'll mark it as NOT_FOUND
    NOT_FOUND = "NOT_FOUND"
    # We need another status to mark if a survey is ineligible for entrances (b/c it doesn't have a single
    #   live quota)
    INELIGIBLE = "INELIGIBLE"


class ProdegePastParticipationType(str, Enum):
    # These come from the "participation_types" key in the survey API response
    #   which is how we filter by users' past_participation.
    CLICK = "click"
    COMPLETE = "complete"
    DQ = "dq"
    OQ = "oq"


# This is the value of the 'status' url param in the redirect
# https://developer.prodege.com/surveys-feed/redirects
# Note: there is no status for ProdegePastParticipationType.CLICK b/c
#   that would be an abandonent
# Note: there is no ProdegePastParticipationType for quality (status 4)
ProdgeRedirectStatus = Literal["1", "2", "3", "4"]
# I'm not using the ProdegePastParticipationType for the values here
#   b/c there is not a 1-to-1 mapping.
ProdgeRedirectStatusNameMap = {"1": "complete", "2": "oq", "3": "dq", "4": "quality"}
