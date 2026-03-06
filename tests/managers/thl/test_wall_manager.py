from datetime import datetime, timezone, timedelta
from decimal import Decimal
from uuid import uuid4

import pytest

from generalresearch.models import Source
from generalresearch.models.thl.session import (
    ReportValue,
    Status,
    StatusCode1,
)
from test_utils.models.conftest import user, session


class TestWallManager:

    @pytest.mark.parametrize("wall_count", [1, 2, 5, 10, 50, 99])
    def test_get_wall_events(
        self, wall_manager, session_factory, user, wall_count, utc_hour_ago
    ):
        from generalresearch.models.thl.session import Session

        s1: Session = session_factory(
            user=user, wall_count=wall_count, started=utc_hour_ago
        )

        assert len(s1.wall_events) == wall_count
        assert len(wall_manager.get_wall_events(session_id=s1.id)) == wall_count

        db_wall_events = wall_manager.get_wall_events(session_id=s1.id)
        assert [w.uuid for w in s1.wall_events] == [w.uuid for w in db_wall_events]
        assert [w.source for w in s1.wall_events] == [w.source for w in db_wall_events]
        assert [w.buyer_id for w in s1.wall_events] == [
            w.buyer_id for w in db_wall_events
        ]
        assert [w.req_survey_id for w in s1.wall_events] == [
            w.req_survey_id for w in db_wall_events
        ]
        assert [w.started for w in s1.wall_events] == [
            w.started for w in db_wall_events
        ]

        assert sum([w.req_cpi for w in s1.wall_events]) == sum(
            [w.req_cpi for w in db_wall_events]
        )
        assert sum([w.cpi for w in s1.wall_events]) == sum(
            [w.cpi for w in db_wall_events]
        )

        assert [w.session_id for w in s1.wall_events] == [
            w.session_id for w in db_wall_events
        ]
        assert [w.user_id for w in s1.wall_events] == [
            w.user_id for w in db_wall_events
        ]
        assert [w.survey_id for w in s1.wall_events] == [
            w.survey_id for w in db_wall_events
        ]

        assert [w.finished for w in s1.wall_events] == [
            w.finished for w in db_wall_events
        ]

    def test_get_wall_events_list_input(
        self, wall_manager, session_factory, user, utc_hour_ago
    ):
        from generalresearch.models.thl.session import Session

        session_ids = []
        for idx in range(10):
            s: Session = session_factory(user=user, wall_count=5, started=utc_hour_ago)
            session_ids.append(s.id)

        session_ids.sort()
        res = wall_manager.get_wall_events(session_ids=session_ids)

        assert isinstance(res, list)
        assert len(res) == 50

        res1 = list(set([w.session_id for w in res]))
        res1.sort()

        assert session_ids == res1

    def test_create_wall(self, wall_manager, session_manager, user, session):
        w = wall_manager.create(
            session_id=session.id,
            user_id=user.user_id,
            uuid_id=uuid4().hex,
            started=datetime.now(tz=timezone.utc),
            source=Source.DYNATA,
            buyer_id="123",
            req_survey_id="456",
            req_cpi=Decimal("1"),
        )

        assert w is not None
        w2 = wall_manager.get_from_uuid(wall_uuid=w.uuid)
        assert w == w2

    def test_report_wall_abandon(
        self, wall_manager, session_manager, user, session, utc_hour_ago
    ):
        w1 = wall_manager.create(
            session_id=session.id,
            user_id=user.user_id,
            uuid_id=uuid4().hex,
            started=utc_hour_ago,
            source=Source.DYNATA,
            buyer_id="123",
            req_survey_id="456",
            req_cpi=Decimal("1"),
        )
        wall_manager.report(
            wall=w1,
            report_value=ReportValue.REASON_UNKNOWN,
            report_timestamp=utc_hour_ago + timedelta(minutes=1),
        )
        w2 = wall_manager.get_from_uuid(wall_uuid=w1.uuid)

        # I Reported a session with no status. It should be marked as an abandon with a finished ts
        assert ReportValue.REASON_UNKNOWN == w2.report_value
        assert Status.ABANDON == w2.status
        assert utc_hour_ago + timedelta(minutes=1) == w2.finished
        assert w2.report_notes is None

        # There is nothing stopping it from being un-abandoned...
        finished = w1.started + timedelta(minutes=10)
        wall_manager.finish(
            wall=w1,
            status=Status.COMPLETE,
            status_code_1=StatusCode1.COMPLETE,
            finished=finished,
        )
        w2 = wall_manager.get_from_uuid(wall_uuid=w1.uuid)
        assert ReportValue.REASON_UNKNOWN == w2.report_value
        assert w2.report_notes is None
        assert finished == w2.finished
        assert Status.COMPLETE == w2.status
        # the status and finished get updated

    def test_report_wall(
        self, wall_manager, session_manager, user, session, utc_hour_ago
    ):
        w1 = wall_manager.create(
            session_id=session.id,
            user_id=user.user_id,
            uuid_id=uuid4().hex,
            started=utc_hour_ago,
            source=Source.DYNATA,
            buyer_id="123",
            req_survey_id="456",
            req_cpi=Decimal("1"),
        )

        finish_ts = utc_hour_ago + timedelta(minutes=10)
        report_ts = utc_hour_ago + timedelta(minutes=11)
        wall_manager.finish(
            wall=w1,
            status=Status.COMPLETE,
            status_code_1=StatusCode1.COMPLETE,
            finished=finish_ts,
        )

        # I Reported a session that was already completed. Only update the report values
        wall_manager.report(
            wall=w1,
            report_value=ReportValue.TECHNICAL_ERROR,
            report_timestamp=report_ts,
            report_notes="This survey blows!",
        )
        w2 = wall_manager.get_from_uuid(wall_uuid=w1.uuid)

        assert ReportValue.TECHNICAL_ERROR == w2.report_value
        assert finish_ts == w2.finished
        assert Status.COMPLETE == w2.status
        assert "This survey blows!" == w2.report_notes

    def test_filter_wall_attempts(
        self, wall_manager, session_manager, user, session, utc_hour_ago
    ):
        res = wall_manager.filter_wall_attempts(user_id=user.user_id)
        assert len(res) == 0
        w1 = wall_manager.create(
            session_id=session.id,
            user_id=user.user_id,
            uuid_id=uuid4().hex,
            started=utc_hour_ago,
            source=Source.DYNATA,
            buyer_id="123",
            req_survey_id="456",
            req_cpi=Decimal("1"),
        )
        res = wall_manager.filter_wall_attempts(user_id=user.user_id)
        assert len(res) == 1
        w2 = wall_manager.create(
            session_id=session.id,
            user_id=user.user_id,
            uuid_id=uuid4().hex,
            started=utc_hour_ago + timedelta(minutes=1),
            source=Source.DYNATA,
            buyer_id="123",
            req_survey_id="555",
            req_cpi=Decimal("1"),
        )
        res = wall_manager.filter_wall_attempts(user_id=user.user_id)
        assert len(res) == 2


class TestWallCacheManager:

    def test_get_attempts_none(self, wall_cache_manager, user):
        attempts = wall_cache_manager.get_attempts(user.user_id)
        assert len(attempts) == 0

    def test_get_wall_events(
        self, wall_cache_manager, wall_manager, session_manager, user
    ):
        start1 = datetime.now(timezone.utc) - timedelta(hours=3)
        start2 = datetime.now(timezone.utc) - timedelta(hours=2)
        start3 = datetime.now(timezone.utc) - timedelta(hours=1)

        session = session_manager.create_dummy(started=start1, user=user)
        wall1 = wall_manager.create_dummy(
            session_id=session.id,
            user_id=session.user_id,
            started=start1,
            req_cpi=Decimal("1.23"),
            req_survey_id="11111",
            source=Source.DYNATA,
        )
        # The flag never got set, so no results!
        attempts = wall_cache_manager.get_attempts(user_id=user.user_id)
        assert len(attempts) == 0

        wall_cache_manager.set_flag(user_id=user.user_id)
        attempts = wall_cache_manager.get_attempts(user_id=user.user_id)
        assert len(attempts) == 1

        wall2 = wall_manager.create_dummy(
            session_id=session.id,
            user_id=session.user_id,
            started=start2,
            req_cpi=Decimal("1.23"),
            req_survey_id="22222",
            source=Source.DYNATA,
        )

        # We haven't set the flag, so the cache won't update!
        attempts = wall_cache_manager.get_attempts(user_id=user.user_id)
        assert len(attempts) == 1

        # Now set the flag
        wall_cache_manager.set_flag(user_id=user.user_id)
        attempts = wall_cache_manager.get_attempts(user_id=user.user_id)
        assert len(attempts) == 2
        # It is in desc order
        assert attempts[0].req_survey_id == "22222"
        assert attempts[1].req_survey_id == "11111"

        # Test the trim. Fill up the cache with 6000 events, then add another,
        # and it should be first in the list, with only 5k others
        attempts10000 = [attempts[0]] * 6000
        wall_cache_manager.update_attempts_redis_(attempts10000, user_id=user.user_id)

        session = session_manager.create_dummy(started=start3, user=user)
        wall3 = wall_manager.create_dummy(
            session_id=session.id,
            user_id=session.user_id,
            started=start3,
            req_cpi=Decimal("1.23"),
            req_survey_id="33333",
            source=Source.DYNATA,
        )
        wall_cache_manager.set_flag(user_id=user.user_id)
        attempts = wall_cache_manager.get_attempts(user_id=user.user_id)

        redis_key = wall_cache_manager.get_cache_key_(user_id=user.user_id)
        assert wall_cache_manager.redis_client.llen(redis_key) == 5000

        assert len(attempts) == 5000
        assert attempts[0].req_survey_id == "33333"
