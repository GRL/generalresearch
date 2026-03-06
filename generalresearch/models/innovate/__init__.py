from enum import Enum

from pydantic import StringConstraints
from typing_extensions import Annotated

# Note, this is called the KEY in the Question model
InnovateQuestionID = Annotated[
    str, StringConstraints(min_length=1, max_length=64, pattern=r"^[^A-Z]+$")
]


class InnovateStatus(str, Enum):
    LIVE = "LIVE"
    NOT_LIVE = "NOT_LIVE"


class InnovateQuotaStatus(str, Enum):
    OPEN = "OPEN"
    CLOSED = "CLOSED"


class InnovateDuplicateCheckLevel(str, Enum):
    # How we should check for de-dupes / survey exclusions.
    # https://innovatemr.stoplight.io/docs/supplier-api/ZG9jOjEzNzYxMTg2-statuses-term-reasons-and-categories
    # #duplicatedtoken

    JOB = "JOB"  # user cannot participate if they have participated in a survey with the same job id
    EXCLUDED_SURVEYS = "EX_SURVEYS"  # cannot participate if they've done any survey in the "excluded_surveys"
    SURVEY = "SURVEY"  # only dedupe check is on the survey itself
    NA = "NA"  # idk how this is different from SURVEY

    @classmethod
    def from_api(cls, s: str):
        return {
            "Job Level": cls.JOB,
            "Multi Surveys": cls.EXCLUDED_SURVEYS,
            "Survey Level": cls.SURVEY,
        }.get(s, cls.NA)
