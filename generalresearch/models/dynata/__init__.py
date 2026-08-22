from enum import StrEnum


class DynataStatus(StrEnum):
    OPEN = "OPEN"
    PAUSED = "PAUSED"
    CLOSED = "CLOSED"
