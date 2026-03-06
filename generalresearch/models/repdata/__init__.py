from enum import Enum


class RepDataStatus(str, Enum):
    LIVE = "LIVE"
    DRAFT = "DRAFT"
    PAUSED = "PAUSED"
    COMPLETE = "COMPLETE"
    CANCELLED = "CANCELLED"
    # We need another status to mark if a survey we thought was live does not
    # come back from the API, we'll mark it as NOT_FOUND
    NOT_FOUND = "NOT_FOUND"
    # We need another status to mark if a survey is ineligible for entrances
    # (b/c it doesn't have a single live stream) and so we are not bothering
    # to make API calls to update it
    INELIGIBLE = "INELIGIBLE"
