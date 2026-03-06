from __future__ import annotations

from enum import Enum

from generalresearch.utils.enum import ReprEnumMeta


class ContestStatus(str, Enum):
    ACTIVE = "active"
    COMPLETED = "completed"
    CANCELLED = "cancelled"


class ContestType(str, Enum, metaclass=ReprEnumMeta):
    """There are 3 contest types. They have a common base, with some unique
    configurations and behaviors for each.
    """

    # Explicit entries, winner(s) by random draw among entries. aka "random draw".
    RAFFLE = "raffle"

    # Winner(s) by rank in a leaderboard. No entries.
    LEADERBOARD = "leaderboard"

    # Reward is guaranteed for everyone who passes a threshold / meets some criteria
    MILESTONE = "milestone"


class ContestEndReason(str, Enum):
    """
    Defines why a contest ended
    """

    # Contest was cancelled. There are no winners.
    CANCELLED = "cancelled"

    # Contest reached the target entry amount.
    TARGET_ENTRY_AMOUNT = "target_entry_amount"

    # Contest reached the target end date.
    ENDS_AT = "ends_at"

    # Contest reached the max number of winners (only in a milestone contest)
    MAX_WINNERS = "max_winners"


class ContestPrizeKind(str, Enum, metaclass=ReprEnumMeta):
    # A physical prize (e.g. a iPhone, cash in the mail, dinner with Max)
    PHYSICAL = "physical"

    # A promotion is a temporary or special offer that provides extra value
    # or benefits (e.g. 20% bonus on completes for the next 7 days)
    PROMOTION = "promotion"

    # Money is deposited into user's virtual wallet
    CASH = "cash"


class ContestEntryTrigger(str, Enum):
    """
    Defines what action/event triggers a (possible) entry into the contest (automatically).
    This only is valid on milestone contests
    """

    TASK_COMPLETE = "task_complete"
    TASK_ATTEMPT = "task_attempt"
    REFERRAL = "referral"


class ContestEntryType(str, Enum, metaclass=ReprEnumMeta):
    """
    All entries into a contest must be of the same type, and match
    the entry_type of the Contest itself.
    """

    # Each entry into the contest is an integer "count". In all current use
    # cases, the value is 1, but we could change this if needed.
    # This could be for e.g. each Task Complete, task attempt, or even each
    # referral, etc.
    COUNT = "count"

    # Each entry is tracking cash in units of USDCent.
    CASH = "cash"


class LeaderboardTieBreakStrategy(str, Enum):
    """
    Strategies for resolving ties in leaderboard-based contests.
    """

    # All tied users at a rank split the total value of prizes for those ranks.
    # All prizes must be CASH
    SPLIT_PRIZE_POOL = "split_prize_pool"

    # All tied users receive the full prize for that rank (i.e., duplicate
    # prizes are issued). All prizes must be type PROMOTION
    DUPLICATE_PRIZES = "duplicate_prizes"

    # First user(s) to reach the score win in case of a tie
    # Might be used in case of physical prizes that can't be split
    EARLIEST_TO_REACH = "earliest_to_reach"
