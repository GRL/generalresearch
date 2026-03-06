import random
import time
from datetime import timedelta, datetime, timezone
from decimal import Decimal
from functools import partial
from typing import Optional
from uuid import uuid4

import math
import pytest
from math import floor

from generalresearch.managers.events import EventSubscriber
from generalresearch.models import Source
from generalresearch.models.events import (
    MessageKind,
    EventType,
    AggregateBySource,
    MaxGaugeBySource,
)
from generalresearch.models.legacy.bucket import Bucket
from generalresearch.models.thl.definitions import Status, StatusCode1
from generalresearch.models.thl.session import Session, Wall
from generalresearch.models.thl.user import User


# We don't need anything in the db, so not using the db fixtures
@pytest.fixture(scope="function")
def product_id(product_manager):
    return uuid4().hex


@pytest.fixture(scope="function")
def user_factory(product_id):
    return partial(create_dummy, product_id=product_id)


@pytest.fixture(scope="function")
def event_subscriber(thl_redis_config, product_id):
    return EventSubscriber(redis_config=thl_redis_config, product_id=product_id)


def create_dummy(
    product_id: Optional[str] = None, product_user_id: Optional[str] = None
) -> User:
    return User(
        product_id=product_id,
        product_user_id=product_user_id or uuid4().hex,
        uuid=uuid4().hex,
        created=datetime.now(tz=timezone.utc),
        user_id=random.randint(0, floor(2**32 / 2)),
    )


class TestActiveUsers:

    def test_run_empty(self, event_manager, product_id):
        res = event_manager.get_user_stats(product_id)
        assert res == {
            "active_users_last_1h": 0,
            "active_users_last_24h": 0,
            "signups_last_24h": 0,
            "in_progress_users": 0,
        }

    def test_run(self, event_manager, product_id, user_factory):
        event_manager.clear_global_user_stats()
        user1: User = user_factory()

        # No matter how many times we do this, they're only active once
        event_manager.handle_user(user1)
        event_manager.handle_user(user1)
        event_manager.handle_user(user1)

        res = event_manager.get_user_stats(product_id)
        assert res == {
            "active_users_last_1h": 1,
            "active_users_last_24h": 1,
            "signups_last_24h": 1,
            "in_progress_users": 0,
        }

        assert event_manager.get_global_user_stats() == {
            "active_users_last_1h": 1,
            "active_users_last_24h": 1,
            "signups_last_24h": 1,
            "in_progress_users": 0,
        }

        # Create a 2nd user in another product
        product_id2 = uuid4().hex
        user2: User = user_factory(product_id=product_id2)
        # Change to say user was created >24 hrs ago
        user2.created = user2.created - timedelta(hours=25)
        event_manager.handle_user(user2)

        # And now each have 1 active user
        assert event_manager.get_user_stats(product_id) == {
            "active_users_last_1h": 1,
            "active_users_last_24h": 1,
            "signups_last_24h": 1,
            "in_progress_users": 0,
        }
        # user2 was created older than 24hrs ago
        assert event_manager.get_user_stats(product_id2) == {
            "active_users_last_1h": 1,
            "active_users_last_24h": 1,
            "signups_last_24h": 0,
            "in_progress_users": 0,
        }
        # 2 globally active
        assert event_manager.get_global_user_stats() == {
            "active_users_last_1h": 2,
            "active_users_last_24h": 2,
            "signups_last_24h": 1,
            "in_progress_users": 0,
        }

    def test_inprogress(self, event_manager, product_id, user_factory):
        event_manager.clear_global_user_stats()
        user1: User = user_factory()
        user2: User = user_factory()

        # No matter how many times we do this, they're only active once
        event_manager.mark_user_inprogress(user1)
        event_manager.mark_user_inprogress(user1)
        event_manager.mark_user_inprogress(user1)
        event_manager.mark_user_inprogress(user2)

        res = event_manager.get_user_stats(product_id)
        assert res["in_progress_users"] == 2

        event_manager.unmark_user_inprogress(user1)
        res = event_manager.get_user_stats(product_id)
        assert res["in_progress_users"] == 1

        # Shouldn't do anything
        event_manager.unmark_user_inprogress(user1)
        res = event_manager.get_user_stats(product_id)
        assert res["in_progress_users"] == 1

    def test_expiry(self, event_manager, product_id, user_factory):
        event_manager.clear_global_user_stats()
        user1: User = user_factory()
        event_manager.handle_user(user1)
        event_manager.mark_user_inprogress(user1)
        sec_24hr = timedelta(hours=24).total_seconds()

        # We don't want to wait an hour to test this, so we're going to
        #   just confirm that the keys will expire
        time.sleep(1.1)
        ttl = event_manager.redis_client.httl(
            f"active_users_last_1h:{product_id}", user1.product_user_id
        )[0]
        assert 3600 - 60 <= ttl <= 3600
        ttl = event_manager.redis_client.httl(
            f"active_users_last_24h:{product_id}", user1.product_user_id
        )[0]
        assert sec_24hr - 60 <= ttl <= sec_24hr

        ttl = event_manager.redis_client.httl("signups_last_24h", user1.uuid)[0]
        assert sec_24hr - 60 <= ttl <= sec_24hr

        ttl = event_manager.redis_client.httl("in_progress_users", user1.uuid)[0]
        assert 3600 - 60 <= ttl <= 3600


class TestSessionStats:

    def test_run_empty(self, event_manager, product_id):
        res = event_manager.get_session_stats(product_id)
        assert res == {
            "session_enters_last_1h": 0,
            "session_enters_last_24h": 0,
            "session_fails_last_1h": 0,
            "session_fails_last_24h": 0,
            "session_completes_last_1h": 0,
            "session_completes_last_24h": 0,
            "sum_payouts_last_1h": 0,
            "sum_payouts_last_24h": 0,
            "sum_user_payouts_last_1h": 0,
            "sum_user_payouts_last_24h": 0,
            "session_avg_payout_last_24h": None,
            "session_avg_user_payout_last_24h": None,
            "session_complete_avg_loi_last_24h": None,
            "session_fail_avg_loi_last_24h": None,
        }

    def test_run(self, event_manager, product_id, user_factory, utc_now, utc_hour_ago):
        event_manager.clear_global_session_stats()

        user: User = user_factory()
        session = Session(
            country_iso="us",
            started=utc_hour_ago + timedelta(minutes=10),
            user=user,
        )
        event_manager.session_on_enter(session=session, user=user)
        session.update(
            finished=utc_now,
            status=Status.COMPLETE,
            status_code_1=StatusCode1.COMPLETE,
            payout=Decimal("1.00"),
            user_payout=Decimal("0.95"),
        )
        event_manager.session_on_finish(session=session, user=user)
        assert event_manager.get_session_stats(product_id) == {
            "session_enters_last_1h": 1,
            "session_enters_last_24h": 1,
            "session_fails_last_1h": 0,
            "session_fails_last_24h": 0,
            "session_completes_last_1h": 1,
            "session_completes_last_24h": 1,
            "sum_payouts_last_1h": 100,
            "sum_payouts_last_24h": 100,
            "sum_user_payouts_last_1h": 95,
            "sum_user_payouts_last_24h": 95,
            "session_avg_payout_last_24h": 100,
            "session_avg_user_payout_last_24h": 95,
            "session_complete_avg_loi_last_24h": round(session.elapsed.total_seconds()),
            "session_fail_avg_loi_last_24h": None,
        }

        # The session gets inserted into redis using the session.finished
        #   timestamp, no matter what time it is right now. So we can
        #   kind of test the expiry by setting it to 61 min ago
        session2 = Session(
            country_iso="us",
            started=utc_hour_ago - timedelta(minutes=10),
            user=user,
        )
        event_manager.session_on_enter(session=session2, user=user)
        session2.update(
            finished=utc_hour_ago - timedelta(minutes=1),
            status=Status.COMPLETE,
            status_code_1=StatusCode1.COMPLETE,
            payout=Decimal("2.00"),
            user_payout=Decimal("1.50"),
        )
        event_manager.session_on_finish(session=session2, user=user)
        avg_loi = (
            round(session.elapsed.total_seconds())
            + round(session2.elapsed.total_seconds())
        ) / 2
        assert event_manager.get_session_stats(product_id) == {
            "session_enters_last_1h": 1,
            "session_enters_last_24h": 2,
            "session_fails_last_1h": 0,
            "session_fails_last_24h": 0,
            "session_completes_last_1h": 1,
            "session_completes_last_24h": 2,
            "sum_payouts_last_1h": 100,
            "sum_payouts_last_24h": 300,
            "sum_user_payouts_last_1h": 95,
            "sum_user_payouts_last_24h": 95 + 150,
            "session_avg_payout_last_24h": math.ceil((100 + 200) / 2),
            "session_avg_user_payout_last_24h": math.ceil((95 + 150) / 2),
            "session_complete_avg_loi_last_24h": avg_loi,
            "session_fail_avg_loi_last_24h": None,
        }

        # Don't want to wait an hour, so confirm the keys will expire
        name = "session_completes_last_1h:" + product_id
        res = event_manager.redis_client.hgetall(name)
        field = (int(utc_now.timestamp()) // 60) * 60
        field_name = str(field)
        assert res == {field_name: "1"}
        assert (
            3600 - 60 < event_manager.redis_client.httl(name, field_name)[0] < 3600 + 60
        )

        # Second BP, fail
        product_id2 = uuid4().hex
        user2: User = user_factory(product_id=product_id2)
        session3 = Session(
            country_iso="us",
            started=utc_now - timedelta(minutes=1),
            user=user2,
        )
        event_manager.session_on_enter(session=session3, user=user)
        session3.update(
            finished=utc_now,
            status=Status.FAIL,
            status_code_1=StatusCode1.BUYER_FAIL,
        )
        event_manager.session_on_finish(session=session3, user=user)
        avg_loi_complete = (
            round(session.elapsed.total_seconds())
            + round(session2.elapsed.total_seconds())
        ) / 2
        assert event_manager.get_session_stats(product_id) == {
            "session_enters_last_1h": 2,
            "session_enters_last_24h": 3,
            "session_fails_last_1h": 1,
            "session_fails_last_24h": 1,
            "session_completes_last_1h": 1,
            "session_completes_last_24h": 2,
            "sum_payouts_last_1h": 100,
            "sum_payouts_last_24h": 300,
            "sum_user_payouts_last_1h": 95,
            "sum_user_payouts_last_24h": 95 + 150,
            "session_avg_payout_last_24h": math.ceil((100 + 200) / 2),
            "session_avg_user_payout_last_24h": math.ceil((95 + 150) / 2),
            "session_complete_avg_loi_last_24h": avg_loi_complete,
            "session_fail_avg_loi_last_24h": round(session3.elapsed.total_seconds()),
        }


class TestTaskStatsManager:
    def test_empty(self, event_manager):
        event_manager.clear_task_stats()
        assert event_manager.get_task_stats_raw() == {
            "live_task_count": AggregateBySource(total=0),
            "live_tasks_max_payout": MaxGaugeBySource(value=None),
            "task_created_count_last_1h": AggregateBySource(total=0),
            "task_created_count_last_24h": AggregateBySource(total=0),
        }
        assert event_manager.get_latest_task_stats() is None

        sm = event_manager.get_stats_message(product_id=uuid4().hex)
        assert sm.data.task_created_count_last_24h.total == 0
        assert sm.data.live_tasks_max_payout.value is None

    def test(self, event_manager):
        event_manager.clear_task_stats()
        event_manager.set_source_task_stats(
            source=Source.TESTING,
            live_task_count=100,
            live_tasks_max_payout=Decimal("1.00"),
        )
        assert event_manager.get_task_stats_raw() == {
            "live_task_count": AggregateBySource(
                total=100, by_source={Source.TESTING: 100}
            ),
            "live_tasks_max_payout": MaxGaugeBySource(
                value=100, by_source={Source.TESTING: 100}
            ),
            "task_created_count_last_1h": AggregateBySource(total=0),
            "task_created_count_last_24h": AggregateBySource(total=0),
        }
        event_manager.set_source_task_stats(
            source=Source.TESTING2,
            live_task_count=50,
            live_tasks_max_payout=Decimal("2.00"),
        )
        assert event_manager.get_task_stats_raw() == {
            "live_task_count": AggregateBySource(
                total=150, by_source={Source.TESTING: 100, Source.TESTING2: 50}
            ),
            "live_tasks_max_payout": MaxGaugeBySource(
                value=200, by_source={Source.TESTING: 100, Source.TESTING2: 200}
            ),
            "task_created_count_last_1h": AggregateBySource(total=0),
            "task_created_count_last_24h": AggregateBySource(total=0),
        }
        event_manager.set_source_task_stats(
            source=Source.TESTING,
            live_task_count=101,
            live_tasks_max_payout=Decimal("1.50"),
        )
        assert event_manager.get_task_stats_raw() == {
            "live_task_count": AggregateBySource(
                total=151, by_source={Source.TESTING: 101, Source.TESTING2: 50}
            ),
            "live_tasks_max_payout": MaxGaugeBySource(
                value=200, by_source={Source.TESTING: 150, Source.TESTING2: 200}
            ),
            "task_created_count_last_1h": AggregateBySource(total=0),
            "task_created_count_last_24h": AggregateBySource(total=0),
        }
        event_manager.set_source_task_stats(
            source=Source.TESTING,
            live_task_count=99,
            live_tasks_max_payout=Decimal("2.50"),
        )
        assert event_manager.get_task_stats_raw() == {
            "live_task_count": AggregateBySource(
                total=149, by_source={Source.TESTING: 99, Source.TESTING2: 50}
            ),
            "live_tasks_max_payout": MaxGaugeBySource(
                value=250, by_source={Source.TESTING: 250, Source.TESTING2: 200}
            ),
            "task_created_count_last_1h": AggregateBySource(total=0),
            "task_created_count_last_24h": AggregateBySource(total=0),
        }
        event_manager.set_source_task_stats(
            source=Source.TESTING, live_task_count=0, live_tasks_max_payout=Decimal("0")
        )
        assert event_manager.get_task_stats_raw() == {
            "live_task_count": AggregateBySource(
                total=50, by_source={Source.TESTING2: 50}
            ),
            "live_tasks_max_payout": MaxGaugeBySource(
                value=200, by_source={Source.TESTING2: 200}
            ),
            "task_created_count_last_1h": AggregateBySource(total=0),
            "task_created_count_last_24h": AggregateBySource(total=0),
        }

        event_manager.set_source_task_stats(
            source=Source.TESTING,
            live_task_count=0,
            live_tasks_max_payout=Decimal("0"),
            created_count=10,
        )
        res = event_manager.get_task_stats_raw()
        assert res["task_created_count_last_1h"] == AggregateBySource(
            total=10, by_source={Source.TESTING: 10}
        )
        assert res["task_created_count_last_24h"] == AggregateBySource(
            total=10, by_source={Source.TESTING: 10}
        )

        event_manager.set_source_task_stats(
            source=Source.TESTING,
            live_task_count=0,
            live_tasks_max_payout=Decimal("0"),
            created_count=10,
        )
        res = event_manager.get_task_stats_raw()
        assert res["task_created_count_last_1h"] == AggregateBySource(
            total=20, by_source={Source.TESTING: 20}
        )
        assert res["task_created_count_last_24h"] == AggregateBySource(
            total=20, by_source={Source.TESTING: 20}
        )

        event_manager.set_source_task_stats(
            source=Source.TESTING2,
            live_task_count=0,
            live_tasks_max_payout=Decimal("0"),
            created_count=1,
        )
        res = event_manager.get_task_stats_raw()
        assert res["task_created_count_last_1h"] == AggregateBySource(
            total=21, by_source={Source.TESTING: 20, Source.TESTING2: 1}
        )
        assert res["task_created_count_last_24h"] == AggregateBySource(
            total=21, by_source={Source.TESTING: 20, Source.TESTING2: 1}
        )

        sm = event_manager.get_stats_message(product_id=uuid4().hex)
        assert sm.data.task_created_count_last_24h.total == 21


class TestChannelsSubscriptions:
    def test_stats_worker(
        self,
        event_manager,
        event_subscriber,
        product_id,
        user_factory,
        utc_hour_ago,
        utc_now,
    ):
        event_manager.clear_stats()
        assert event_subscriber.pubsub
        # We subscribed, so manually trigger the stats worker
        #   to get all product_ids subscribed and publish a
        #   stats message into that channel
        event_manager.stats_worker_task()
        assert product_id in event_manager.get_active_subscribers()
        msg = event_subscriber.get_next_message()
        print(msg)
        assert msg.kind == MessageKind.STATS

        user = user_factory()
        session = Session(
            country_iso="us",
            started=utc_hour_ago,
            user=user,
            clicked_bucket=Bucket(user_payout_min=Decimal("0.50")),
            id=1,
        )

        event_manager.handle_session_enter(session, user)
        msg = event_subscriber.get_next_message()
        print(msg)
        assert msg.kind == MessageKind.EVENT
        assert msg.data.event_type == EventType.SESSION_ENTER

        wall = Wall(
            req_survey_id="a",
            req_cpi=Decimal("1"),
            source=Source.TESTING,
            session_id=session.id,
            user_id=user.user_id,
        )

        event_manager.handle_task_enter(wall, session, user)
        msg = event_subscriber.get_next_message()
        print(msg)
        assert msg.kind == MessageKind.EVENT
        assert msg.data.event_type == EventType.TASK_ENTER

        wall.update(
            status=Status.COMPLETE,
            status_code_1=StatusCode1.COMPLETE,
            finished=datetime.now(tz=timezone.utc),
            cpi=Decimal("1"),
        )
        event_manager.handle_task_finish(wall, session, user)
        msg = event_subscriber.get_next_message()
        print(msg)
        assert msg.kind == MessageKind.EVENT
        assert msg.data.event_type == EventType.TASK_FINISH

        session.update(
            finished=utc_now,
            status=Status.COMPLETE,
            status_code_1=StatusCode1.COMPLETE,
            payout=Decimal("1.00"),
            user_payout=Decimal("1.00"),
        )

        event_manager.handle_session_finish(session, user)
        msg = event_subscriber.get_next_message()
        print(msg)
        assert msg.kind == MessageKind.EVENT
        assert msg.data.event_type == EventType.SESSION_FINISH

        event_manager.stats_worker_task()
        assert product_id in event_manager.get_active_subscribers()
        msg = event_subscriber.get_next_message()
        print(msg)
        assert msg.kind == MessageKind.STATS
        assert msg.data.active_users_last_1h == 1
        assert msg.data.session_enters_last_24h == 1
        assert msg.data.session_completes_last_1h == 1
        assert msg.data.signups_last_24h == 1
