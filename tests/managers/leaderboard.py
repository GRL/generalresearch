import os
import time
import zoneinfo
from datetime import datetime, timezone
from decimal import Decimal
from uuid import uuid4

import pytest

from generalresearch.managers.leaderboard.manager import LeaderboardManager
from generalresearch.managers.leaderboard.tasks import hit_leaderboards
from generalresearch.models.thl.definitions import Status
from generalresearch.models.thl.user import User
from generalresearch.models.thl.product import Product
from generalresearch.models.thl.session import Session
from generalresearch.models.thl.leaderboard import (
    LeaderboardCode,
    LeaderboardFrequency,
    LeaderboardRow,
)
from generalresearch.models.thl.product import (
    PayoutConfig,
    PayoutTransformation,
    PayoutTransformationPercentArgs,
)

# random uuid for leaderboard tests
product_id = uuid4().hex


@pytest.fixture(autouse=True)
def set_timezone():
    os.environ["TZ"] = "UTC"
    time.tzset()
    yield
    # Optionally reset to default
    os.environ.pop("TZ", None)
    time.tzset()


@pytest.fixture
def session_factory():
    return _create_session


def _create_session(
    product_user_id="aaa", country_iso="us", user_payout=Decimal("1.00")
):
    user = User(
        product_id=product_id,
        product_user_id=product_user_id,
    )
    user.product = Product(
        id=product_id,
        name="test",
        redirect_url="https://www.example.com",
        payout_config=PayoutConfig(
            payout_transformation=PayoutTransformation(
                f="payout_transformation_percent",
                kwargs=PayoutTransformationPercentArgs(pct=0.5),
            )
        ),
    )
    session = Session(
        user=user,
        started=datetime(2025, 2, 5, 6, tzinfo=timezone.utc),
        id=1,
        country_iso=country_iso,
        status=Status.COMPLETE,
        payout=Decimal("2.00"),
        user_payout=user_payout,
    )
    return session


@pytest.fixture(scope="function")
def setup_leaderboards(thl_redis):
    complete_count = {
        "aaa": 10,
        "bbb": 6,
        "ccc": 6,
        "ddd": 6,
        "eee": 2,
        "fff": 1,
        "ggg": 1,
    }
    sum_payout = {"aaa": 345, "bbb": 100, "ccc": 100}
    max_payout = sum_payout
    country_iso = "us"
    for freq in [
        LeaderboardFrequency.DAILY,
        LeaderboardFrequency.WEEKLY,
        LeaderboardFrequency.MONTHLY,
    ]:
        m = LeaderboardManager(
            redis_client=thl_redis,
            board_code=LeaderboardCode.COMPLETE_COUNT,
            freq=freq,
            product_id=product_id,
            country_iso=country_iso,
            within_time=datetime(2025, 2, 5, 12, 12, 12),
        )
        thl_redis.delete(m.key)
        thl_redis.zadd(m.key, complete_count)
        m = LeaderboardManager(
            redis_client=thl_redis,
            board_code=LeaderboardCode.SUM_PAYOUTS,
            freq=freq,
            product_id=product_id,
            country_iso=country_iso,
            within_time=datetime(2025, 2, 5, 12, 12, 12),
        )
        thl_redis.delete(m.key)
        thl_redis.zadd(m.key, sum_payout)
        m = LeaderboardManager(
            redis_client=thl_redis,
            board_code=LeaderboardCode.LARGEST_PAYOUT,
            freq=freq,
            product_id=product_id,
            country_iso=country_iso,
            within_time=datetime(2025, 2, 5, 12, 12, 12),
        )
        thl_redis.delete(m.key)
        thl_redis.zadd(m.key, max_payout)


class TestLeaderboards:

    def test_leaderboard_manager(self, setup_leaderboards, thl_redis):
        country_iso = "us"
        board_code = LeaderboardCode.COMPLETE_COUNT
        freq = LeaderboardFrequency.DAILY
        m = LeaderboardManager(
            redis_client=thl_redis,
            board_code=board_code,
            freq=freq,
            product_id=product_id,
            country_iso=country_iso,
            within_time=datetime(2025, 2, 5, 0, 0, 0),
        )
        lb = m.get_leaderboard()
        assert lb.period_start_local == datetime(
            2025, 2, 5, 0, 0, 0, tzinfo=zoneinfo.ZoneInfo(key="America/New_York")
        )
        assert lb.period_end_local == datetime(
            2025,
            2,
            5,
            23,
            59,
            59,
            999999,
            tzinfo=zoneinfo.ZoneInfo(key="America/New_York"),
        )
        assert lb.period_start_utc == datetime(2025, 2, 5, 5, tzinfo=timezone.utc)
        assert lb.row_count == 7
        assert lb.rows == [
            LeaderboardRow(bpuid="aaa", rank=1, value=10),
            LeaderboardRow(bpuid="bbb", rank=2, value=6),
            LeaderboardRow(bpuid="ccc", rank=2, value=6),
            LeaderboardRow(bpuid="ddd", rank=2, value=6),
            LeaderboardRow(bpuid="eee", rank=5, value=2),
            LeaderboardRow(bpuid="fff", rank=6, value=1),
            LeaderboardRow(bpuid="ggg", rank=6, value=1),
        ]

    def test_leaderboard_manager_bpuid(self, setup_leaderboards, thl_redis):
        country_iso = "us"
        board_code = LeaderboardCode.COMPLETE_COUNT
        freq = LeaderboardFrequency.DAILY
        m = LeaderboardManager(
            redis_client=thl_redis,
            board_code=board_code,
            freq=freq,
            product_id=product_id,
            country_iso=country_iso,
            within_time=datetime(2025, 2, 5, 12, 12, 12),
        )
        lb = m.get_leaderboard(bp_user_id="fff", limit=1)

        # TODO: this won't work correctly if I request bpuid 'ggg', because it
        #   is ordered at the end even though it is tied, so it won't get a
        #   row after ('fff')

        assert lb.rows == [
            LeaderboardRow(bpuid="eee", rank=5, value=2),
            LeaderboardRow(bpuid="fff", rank=6, value=1),
            LeaderboardRow(bpuid="ggg", rank=6, value=1),
        ]

        lb.censor()
        assert lb.rows[0].bpuid == "ee*"

    def test_leaderboard_hit(self, setup_leaderboards, session_factory, thl_redis):
        hit_leaderboards(redis_client=thl_redis, session=session_factory())

        for freq in [
            LeaderboardFrequency.DAILY,
            LeaderboardFrequency.WEEKLY,
            LeaderboardFrequency.MONTHLY,
        ]:
            m = LeaderboardManager(
                redis_client=thl_redis,
                board_code=LeaderboardCode.COMPLETE_COUNT,
                freq=freq,
                product_id=product_id,
                country_iso="us",
                within_time=datetime(2025, 2, 5, 12, 12, 12),
            )
            lb = m.get_leaderboard(limit=1)
            assert lb.row_count == 7
            assert lb.rows == [LeaderboardRow(bpuid="aaa", rank=1, value=11)]
            m = LeaderboardManager(
                redis_client=thl_redis,
                board_code=LeaderboardCode.LARGEST_PAYOUT,
                freq=freq,
                product_id=product_id,
                country_iso="us",
                within_time=datetime(2025, 2, 5, 12, 12, 12),
            )
            lb = m.get_leaderboard(limit=1)
            assert lb.row_count == 3
            assert lb.rows == [LeaderboardRow(bpuid="aaa", rank=1, value=345)]
            m = LeaderboardManager(
                redis_client=thl_redis,
                board_code=LeaderboardCode.SUM_PAYOUTS,
                freq=freq,
                product_id=product_id,
                country_iso="us",
                within_time=datetime(2025, 2, 5, 12, 12, 12),
            )
            lb = m.get_leaderboard(limit=1)
            assert lb.row_count == 3
            assert lb.rows == [LeaderboardRow(bpuid="aaa", rank=1, value=345 + 100)]

    def test_leaderboard_hit_new_row(
        self, setup_leaderboards, session_factory, thl_redis
    ):
        session = session_factory(product_user_id="zzz")
        hit_leaderboards(redis_client=thl_redis, session=session)
        m = LeaderboardManager(
            redis_client=thl_redis,
            board_code=LeaderboardCode.COMPLETE_COUNT,
            freq=LeaderboardFrequency.DAILY,
            product_id=product_id,
            country_iso="us",
            within_time=datetime(2025, 2, 5, 12, 12, 12),
        )
        lb = m.get_leaderboard()
        assert lb.row_count == 8
        assert LeaderboardRow(bpuid="zzz", value=1, rank=6) in lb.rows

    def test_leaderboard_country(self, thl_redis):
        m = LeaderboardManager(
            redis_client=thl_redis,
            board_code=LeaderboardCode.COMPLETE_COUNT,
            freq=LeaderboardFrequency.DAILY,
            product_id=product_id,
            country_iso="jp",
            within_time=datetime(
                2025,
                2,
                1,
            ),
        )
        lb = m.get_leaderboard()
        assert lb.row_count == 0
        assert lb.period_start_local == datetime(
            2025, 2, 1, 0, 0, 0, tzinfo=zoneinfo.ZoneInfo(key="Asia/Tokyo")
        )
        assert lb.local_start_time == "2025-02-01T00:00:00+09:00"
        assert lb.local_end_time == "2025-02-01T23:59:59.999999+09:00"
        assert lb.period_start_utc == datetime(2025, 1, 31, 15, tzinfo=timezone.utc)
        print(lb.model_dump(mode="json"))
