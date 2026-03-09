from typing import TYPE_CHECKING, Dict, List

if TYPE_CHECKING:
    from generalresearch.currency import USDCent
    from generalresearch.models.thl.leaderboard import LeaderboardRow
    from generalresearch.models.thl.user import User


def censor_product_user_id(user: "User") -> str:
    s = user.product_user_id

    if len(s) >= 24:
        return f"{s[:4]}{'*' * (len(s) - 8)}{s[-4:]}"
    elif len(s) >= 6:
        return f"{s[:1]}{'*' * (len(s) - 2)}{s[-1:]}"
    else:
        return "*" * len(s)


def distribute_leaderboard_prizes(
    prizes: List["USDCent"], leaderboard_rows: List["LeaderboardRow"]
) -> Dict[str, "USDCent"]:
    """
    Distributes leaderboard prizes among tied users.
    The prizes for the tied places are pooled together and divided
        equally among all tied participants.

    :param prizes: List of cash value for prizes (in descending order).
    :param leaderboard_rows: List of LeaderboardRow, sorted by score descending / rank ascending.

    Returns:
        dict: Mapping {user: prize_amount} for all tied users.

    See also:
    https://en.wikipedia.org/wiki/Ranking#Standard_competition_ranking_(%221224%22_ranking)
    https://www.pgatour.com/fedexcup/overview

    (Points are distributed to those in tying positions using the same method
    currently used to distribute prize money when there is a tie. That is, the
    total points for each tying position will be averaged and that average will
    be distributed to each player in the tying position.)

    """
    from generalresearch.currency import USDCent

    if not prizes or not leaderboard_rows:
        return {}

    leaderboard_rows = sorted(leaderboard_rows, key=lambda x: x.rank)
    prizes = sorted(prizes, reverse=True)

    result = {}
    place = 0  # index into prizes
    rank = 1

    while place < len(prizes):
        # Get all users tied for this rank
        tie_group = [row for row in leaderboard_rows if row.rank == rank]

        # Determine which prize places this tie group occupies
        tie_prizes = prizes[place : place + len(tie_group)]
        if not tie_prizes:
            break

        # Pool prizes for all places they occupy, then split among the group
        total = sum(p for p in tie_prizes)
        split = USDCent(round(total / len(tie_group)))
        for row in tie_group:
            result[row.bpuid] = split

        # Advance prize index by number of tied users (skip the places this group occupied)
        place += len(tie_group)
        # Advance the rank
        rank += 1

    return result
