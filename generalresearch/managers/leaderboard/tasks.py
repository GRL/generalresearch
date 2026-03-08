import logging

from redis import Redis

from generalresearch.managers.leaderboard.manager import LeaderboardManager
from generalresearch.models.thl.leaderboard import (
    LeaderboardCode,
    LeaderboardFrequency,
)
from generalresearch.models.thl.session import Session

logger = logging.getLogger()


def hit_leaderboards(redis_client: Redis, session: Session):
    user = session.user
    assert user.product is not None, "prefetch user.product first"
    product_id = user.product_id
    country_iso = session.country_iso
    ts = session.started

    for freq in [
        LeaderboardFrequency.DAILY,
        LeaderboardFrequency.WEEKLY,
        LeaderboardFrequency.MONTHLY,
    ]:
        lbm = LeaderboardManager(
            redis_client=redis_client,
            board_code=LeaderboardCode.COMPLETE_COUNT,
            freq=freq,
            product_id=product_id,
            country_iso=country_iso,
            within_time=ts,
        )
        logger.info(lbm.key)
        lbm.hit(session)

        if user.product.payout_config.payout_transformation:
            lbm = LeaderboardManager(
                redis_client=redis_client,
                board_code=LeaderboardCode.SUM_PAYOUTS,
                freq=freq,
                product_id=product_id,
                country_iso=country_iso,
                within_time=ts,
            )
            logger.info(lbm.key)
            lbm.hit(session)

            lbm = LeaderboardManager(
                redis_client=redis_client,
                board_code=LeaderboardCode.LARGEST_PAYOUT,
                freq=freq,
                product_id=product_id,
                country_iso=country_iso,
                within_time=ts,
            )
            logger.info(lbm.key)
            lbm.hit(session)
