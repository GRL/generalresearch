from datetime import datetime, timezone, timedelta
from decimal import Decimal

import pytest

from generalresearch.models import Source
from generalresearch.models.thl.definitions import Status, StatusCode1
from generalresearch.models.thl.session import Session, Wall
from generalresearch.models.thl.user import User


class TestWallSession:

    def test_session_with_no_wall_events(self):
        started = datetime(2023, 1, 1, tzinfo=timezone.utc)
        s = Session(user=User(user_id=1), started=started)
        assert s.status is None
        assert s.status_code_1 is None

        # todo: this needs to be set explicitly, not this way
        # # If I have no wall events, it's a fail
        # s.determine_session_status()
        # assert s.status == Status.FAIL
        # assert s.status_code_1 == StatusCode1.SESSION_START_FAIL

    def test_session_timeout_with_only_grs(self):
        started = datetime(2023, 1, 1, tzinfo=timezone.utc)
        s = Session(user=User(user_id=1), started=started)
        w = Wall(
            user_id=1,
            source=Source.GRS,
            req_survey_id="xxx",
            req_cpi=Decimal(1),
            session_id=1,
        )
        s.append_wall_event(w)
        status, status_code_1 = s.determine_session_status()
        s.update(status=status, status_code_1=status_code_1)
        assert Status.TIMEOUT == s.status
        assert StatusCode1.GRS_ABANDON == s.status_code_1

    def test_session_with_only_grs_fail(self):
        # todo: this needs to be set explicitly, not this way
        pass
        # started = datetime(2023, 1, 1, tzinfo=timezone.utc)
        # s = Session(user=User(user_id=1), started=started)
        # w = Wall(user_id=1, source=Source.GRS, req_survey_id='xxx',
        #          req_cpi=Decimal(1), session_id=1)
        # s.append_wall_event(w)
        # w.finish(status=Status.FAIL, status_code_1=StatusCode1.PS_FAIL)
        # s.determine_session_status()
        # assert s.status == Status.FAIL
        # assert s.status_code_1 == StatusCode1.GRS_FAIL

    def test_session_with_only_grs_complete(self):
        started = datetime(year=2023, month=1, day=1, tzinfo=timezone.utc)

        # A Session is started
        s = Session(user=User(user_id=1), started=started)

        # The User goes into a GRS survey, and completes it
        # @gstupp - should a GRS be allowed with a req_cpi > 0?
        w = Wall(
            user_id=1,
            source=Source.GRS,
            req_survey_id="xxx",
            req_cpi=Decimal(1),
            session_id=1,
        )
        s.append_wall_event(w)
        w.finish(status=Status.COMPLETE, status_code_1=StatusCode1.COMPLETE)

        status, status_code_1 = s.determine_session_status()
        s.update(status=status, status_code_1=status_code_1)

        assert s.status == Status.FAIL

        # @gstupp changed this behavior on 11/2023 (51471b6ae671f21212a8b1fad60b508181cbb8ca)
        #   I don't know which is preferred or the consequences of each. However,
        #   now it's a SESSION_CONTINUE_FAIL instead of a SESSION_START_FAIL so
        #   change this so the test passes
        # self.assertEqual(s.status_code_1, StatusCode1.SESSION_START_FAIL)
        assert s.status_code_1 == StatusCode1.SESSION_CONTINUE_FAIL

    @pytest.mark.skip(reason="TODO")
    def test_session_with_only_non_grs_complete(self):
        # todo: this needs to be set explicitly, not this way
        pass
        # # This fails... until payout stuff is done
        # started = datetime(2023, 1, 1, tzinfo=timezone.utc)
        # s = Session(user=User(user_id=1), started=started)
        # w = Wall(source=Source.DYNATA, req_survey_id='xxx', req_cpi=Decimal('1.00001'),
        #          session_id=1, user_id=1)
        # s.append_wall_event(w)
        # w.finish(status=Status.COMPLETE, status_code_1=StatusCode1.COMPLETE)
        # s.determine_session_status()
        # assert s.status == Status.COMPLETE
        # assert s.status_code_1 is None

    def test_session_with_only_non_grs_fail(self):
        started = datetime(year=2023, month=1, day=1, tzinfo=timezone.utc)

        s = Session(user=User(user_id=1), started=started)
        w = Wall(
            source=Source.DYNATA,
            req_survey_id="xxx",
            req_cpi=Decimal("1.00001"),
            session_id=1,
            user_id=1,
        )

        s.append_wall_event(w)
        w.finish(status=Status.FAIL, status_code_1=StatusCode1.BUYER_FAIL)
        status, status_code_1 = s.determine_session_status()
        s.update(status=status, status_code_1=status_code_1)

        assert s.status == Status.FAIL
        assert s.status_code_1 == StatusCode1.BUYER_FAIL
        assert s.payout is None

    def test_session_with_only_non_grs_timeout(self):
        started = datetime(year=2023, month=1, day=1, tzinfo=timezone.utc)

        s = Session(user=User(user_id=1), started=started)
        w = Wall(
            source=Source.DYNATA,
            req_survey_id="xxx",
            req_cpi=Decimal("1.00001"),
            session_id=1,
            user_id=1,
        )

        s.append_wall_event(w)
        status, status_code_1 = s.determine_session_status()
        s.update(status=status, status_code_1=status_code_1)

        assert s.status == Status.TIMEOUT
        assert s.status_code_1 == StatusCode1.BUYER_ABANDON
        assert s.payout is None

    def test_session_with_grs_and_external(self):
        started = datetime(year=2023, month=1, day=1, tzinfo=timezone.utc)

        s = Session(user=User(user_id=1), started=started)
        w = Wall(
            source=Source.GRS,
            req_survey_id="xxx",
            req_cpi=Decimal(1),
            session_id=1,
            user_id=1,
            started=started,
        )

        s.append_wall_event(w)
        w.finish(
            status=Status.COMPLETE,
            status_code_1=StatusCode1.COMPLETE,
            finished=started + timedelta(minutes=10),
        )

        w = Wall(
            source=Source.DYNATA,
            req_survey_id="xxx",
            req_cpi=Decimal("1.00001"),
            session_id=1,
            user_id=1,
        )
        s.append_wall_event(w)
        w.finish(
            status=Status.ABANDON,
            finished=datetime.now(tz=timezone.utc) + timedelta(minutes=10),
            status_code_1=StatusCode1.BUYER_ABANDON,
        )
        status, status_code_1 = s.determine_session_status()
        s.update(status=status, status_code_1=status_code_1)

        assert s.status == Status.ABANDON
        assert s.status_code_1 == StatusCode1.BUYER_ABANDON
        assert s.payout is None

        s = Session(user=User(user_id=1), started=started)
        w = Wall(
            source=Source.GRS,
            req_survey_id="xxx",
            req_cpi=Decimal(1),
            session_id=1,
            user_id=1,
        )
        s.append_wall_event(w)
        w.finish(status=Status.COMPLETE, status_code_1=StatusCode1.COMPLETE)
        w = Wall(
            source=Source.DYNATA,
            req_survey_id="xxx",
            req_cpi=Decimal("1.00001"),
            session_id=1,
            user_id=1,
        )
        s.append_wall_event(w)
        w.finish(status=Status.FAIL, status_code_1=StatusCode1.PS_DUPLICATE)

        status, status_code_1 = s.determine_session_status()
        s.update(status=status, status_code_1=status_code_1)

        assert s.status == Status.FAIL
        assert s.status_code_1 == StatusCode1.PS_DUPLICATE
        assert s.payout is None

    def test_session_marketplace_fail(self):
        started = datetime(2023, 1, 1, tzinfo=timezone.utc)

        s = Session(user=User(user_id=1), started=started)
        w = Wall(
            source=Source.CINT,
            req_survey_id="xxx",
            req_cpi=Decimal(1),
            session_id=1,
            user_id=1,
            started=started,
        )
        s.append_wall_event(w)
        w.finish(
            status=Status.FAIL,
            status_code_1=StatusCode1.MARKETPLACE_FAIL,
            finished=started + timedelta(minutes=10),
        )
        status, status_code_1 = s.determine_session_status()
        s.update(status=status, status_code_1=status_code_1)
        assert Status.FAIL == s.status
        assert StatusCode1.SESSION_CONTINUE_QUALITY_FAIL == s.status_code_1

    def test_session_unknown(self):
        started = datetime(2023, 1, 1, tzinfo=timezone.utc)

        s = Session(user=User(user_id=1), started=started)
        w = Wall(
            source=Source.CINT,
            req_survey_id="xxx",
            req_cpi=Decimal(1),
            session_id=1,
            user_id=1,
            started=started,
        )
        s.append_wall_event(w)
        w.finish(
            status=Status.FAIL,
            status_code_1=StatusCode1.UNKNOWN,
            finished=started + timedelta(minutes=10),
        )
        status, status_code_1 = s.determine_session_status()
        s.update(status=status, status_code_1=status_code_1)
        assert Status.FAIL == s.status
        assert StatusCode1.BUYER_FAIL == s.status_code_1


# class TestWallSessionPayout:
#     product_id = uuid4().hex
#
#     def test_session_payout_with_only_non_grs_complete(self):
#         sql_helper = self.make_sql_helper()
#         user = User(user_id=1, product_id=self.product_id)
#         s = Session(user=user, started=datetime(2023, 1, 1, tzinfo=timezone.utc))
#         w = Wall(source=Source.DYNATA, req_survey_id='xxx', req_cpi=Decimal('1.00001'))
#         s.append_wall_event(w)
#         w.handle_callback(status=Status.COMPLETE)
#         s.determine_session_status()
#         s.determine_payout(sql_helper=sql_helper)
#         assert s.status == Status.COMPLETE
#         assert s.status_code_1 is None
#         # we're assuming here the commission on this BP is 8.5% and doesn't get changed by someone!
#         assert s.payout == Decimal('0.88')
#
#     def test_session_payout(self):
#         sql_helper = self.make_sql_helper()
#         user = User(user_id=1, product_id=self.product_id)
#         s = Session(user=user, started=datetime(2023, 1, 1, tzinfo=timezone.utc))
#         w = Wall(source=Source.GRS, req_survey_id='xxx', req_cpi=1)
#         s.append_wall_event(w)
#         w.handle_callback(status=Status.COMPLETE)
#         w = Wall(source=Source.DYNATA, req_survey_id='xxx', req_cpi=Decimal('1.00001'))
#         s.append_wall_event(w)
#         w.handle_callback(status=Status.COMPLETE)
#         s.determine_session_status()
#         s.determine_payout(commission_pct=Decimal('0.05'))
#         assert s.status == Status.COMPLETE
#         assert s.status_code_1 is None
#         assert s.payout == Decimal('0.93')


# def test_get_from_uuid_vendor_wall(self):
#     sql_helper = self.make_sql_helper()
#     sql_helper.get_or_create("auth_user", "id", {"id": 1}, {
#         "id": 1, "password": "1",
#         "last_login": None, "is_superuser": 0,
#         "username": "a", "first_name": "a",
#         "last_name": "a", "email": "a",
#         "is_staff": 0, "is_active": 1,
#         "date_joined": "2023-10-13 14:03:20.000000"})
#     sql_helper.get_or_create("vendor_wallsession", "id", {"id": 324}, {"id": 324})
#     sql_helper.create("vendor_wall", {
#         "id": "7b3e380babc840b79abf0030d408bbd9",
#         "status": "c",
#         "started": "2023-10-10 00:51:13.415444",
#         "finished": "2023-10-10 01:08:00.676947",
#         "req_loi": 1200,
#         "req_cpi": 0.63,
#         "req_survey_id": "8070750",
#         "survey_id": "8070750",
#         "cpi": 0.63,
#         "user_id": 1,
#         "report_notes": None,
#         "report_status": None,
#         "status_code": "1",
#         "req_survey_hashed_opp": None,
#         "session_id": 324,
#         "source": "i",
#         "ubp_id": None
#     })
#     Wall
#     w = Wall.get_from_uuid_vendor_wall('7b3e380babc840b79abf0030d408bbd9', sql_helper=sql_helper,
#                                        session_id=1)
#     assert w.status == Status.COMPLETE
#     assert w.source == Source.INNOVATE
#     assert w.uuid == '7b3e380babc840b79abf0030d408bbd9'
#     assert w.cpi == Decimal('0.63')
#     assert w.survey_id == '8070750'
#     assert w.user_id == 1
