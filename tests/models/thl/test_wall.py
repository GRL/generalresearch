from datetime import datetime, timezone, timedelta
from decimal import Decimal
from uuid import uuid4

import pytest
from pydantic import ValidationError

from generalresearch.models import Source
from generalresearch.models.thl.definitions import (
    Status,
    StatusCode1,
    WallStatusCode2,
)
from generalresearch.models.thl.session import Wall


class TestWall:

    def test_wall_json(self):
        w = Wall(
            user_id=1,
            source=Source.DYNATA,
            req_survey_id="xxx",
            req_cpi=Decimal(1),
            session_id=1,
            survey_id="yyy",
            ext_status_code_1="1.0",
            status=Status.FAIL,
            status_code_1=StatusCode1.BUYER_FAIL,
            started=datetime(2023, 1, 1, 0, 0, 1, tzinfo=timezone.utc),
            finished=datetime(2023, 1, 1, 0, 10, 1, tzinfo=timezone.utc),
        )
        s = w.to_json()
        w2 = Wall.from_json(s)
        assert w == w2

    def test_status_status_code_agreement(self):
        # should not raise anything
        Wall(
            user_id=1,
            source=Source.DYNATA,
            req_survey_id="xxx",
            req_cpi=Decimal(1),
            session_id=1,
            survey_id="yyy",
            status=Status.FAIL,
            status_code_1=StatusCode1.BUYER_FAIL,
            started=datetime.now(timezone.utc),
            finished=datetime.now(timezone.utc) + timedelta(seconds=1),
        )
        Wall(
            user_id=1,
            source=Source.DYNATA,
            req_survey_id="xxx",
            req_cpi=Decimal(1),
            session_id=1,
            survey_id="yyy",
            status=Status.FAIL,
            status_code_1=StatusCode1.MARKETPLACE_FAIL,
            status_code_2=WallStatusCode2.COMPLETE_TOO_FAST,
            started=datetime.now(timezone.utc),
            finished=datetime.now(timezone.utc) + timedelta(seconds=1),
        )
        with pytest.raises(expected_exception=ValidationError) as e:
            Wall(
                user_id=1,
                source=Source.DYNATA,
                req_survey_id="xxx",
                req_cpi=Decimal(1),
                session_id=1,
                survey_id="yyy",
                status=Status.FAIL,
                status_code_1=StatusCode1.GRS_ABANDON,
                started=datetime.now(timezone.utc),
                finished=datetime.now(timezone.utc) + timedelta(seconds=1),
            )
        assert "If status is f, status_code_1 should be in" in str(e.value)

        with pytest.raises(expected_exception=ValidationError) as cm:
            Wall(
                user_id=1,
                source=Source.DYNATA,
                req_survey_id="xxx",
                req_cpi=Decimal(1),
                session_id=1,
                survey_id="yyy",
                status=Status.FAIL,
                status_code_1=StatusCode1.GRS_ABANDON,
                status_code_2=WallStatusCode2.COMPLETE_TOO_FAST,
                started=datetime.now(timezone.utc),
                finished=datetime.now(timezone.utc) + timedelta(seconds=1),
            )
        assert "If status is f, status_code_1 should be in" in str(e.value)

    def test_status_code_1_2_agreement(self):
        # should not raise anything
        Wall(
            user_id=1,
            source=Source.DYNATA,
            req_survey_id="xxx",
            req_cpi=Decimal(1),
            session_id=1,
            survey_id="yyy",
            status=Status.FAIL,
            status_code_1=StatusCode1.MARKETPLACE_FAIL,
            status_code_2=WallStatusCode2.COMPLETE_TOO_FAST,
            started=datetime.now(timezone.utc),
            finished=datetime.now(timezone.utc) + timedelta(seconds=1),
        )
        Wall(
            user_id=1,
            source=Source.DYNATA,
            req_survey_id="xxx",
            req_cpi=Decimal(1),
            session_id=1,
            survey_id="yyy",
            status=Status.FAIL,
            status_code_1=StatusCode1.BUYER_FAIL,
            status_code_2=None,
            started=datetime.now(timezone.utc),
            finished=datetime.now(timezone.utc) + timedelta(seconds=1),
        )
        Wall(
            user_id=1,
            source=Source.DYNATA,
            req_survey_id="xxx",
            req_cpi=Decimal(1),
            session_id=1,
            survey_id="yyy",
            status=Status.COMPLETE,
            status_code_1=StatusCode1.COMPLETE,
            status_code_2=None,
            started=datetime.now(timezone.utc),
            finished=datetime.now(timezone.utc) + timedelta(seconds=1),
        )

        with pytest.raises(expected_exception=ValidationError) as e:
            Wall(
                user_id=1,
                source=Source.DYNATA,
                req_survey_id="xxx",
                req_cpi=Decimal(1),
                session_id=1,
                survey_id="yyy",
                status=Status.FAIL,
                status_code_1=StatusCode1.BUYER_FAIL,
                status_code_2=WallStatusCode2.COMPLETE_TOO_FAST,
                started=datetime.now(timezone.utc),
                finished=datetime.now(timezone.utc) + timedelta(seconds=1),
            )
            assert "If status_code_1 is 1, status_code_2 should be in" in str(e.value)

    def test_annotate_status_code(self):
        w = Wall(
            user_id=1,
            source=Source.DYNATA,
            req_survey_id="xxx",
            req_cpi=Decimal(1),
            session_id=1,
            survey_id="yyy",
        )
        w.annotate_status_codes("1.0")
        assert Status.COMPLETE == w.status
        assert StatusCode1.COMPLETE == w.status_code_1
        assert w.status_code_2 is None
        assert "1.0" == w.ext_status_code_1
        assert w.ext_status_code_2 is None

    def test_buyer_too_long(self):
        buyer_id = uuid4().hex
        w = Wall(
            user_id=1,
            source=Source.DYNATA,
            req_survey_id="xxx",
            req_cpi=Decimal(1),
            session_id=1,
            survey_id="yyy",
            buyer_id=buyer_id,
        )
        assert buyer_id == w.buyer_id

        w = Wall(
            user_id=1,
            source=Source.DYNATA,
            req_survey_id="xxx",
            req_cpi=Decimal(1),
            session_id=1,
            survey_id="yyy",
            buyer_id=None,
        )
        assert w.buyer_id is None

        w = Wall(
            user_id=1,
            source=Source.DYNATA,
            req_survey_id="xxx",
            req_cpi=Decimal(1),
            session_id=1,
            survey_id="yyy",
            buyer_id=buyer_id + "abc123",
        )
        assert buyer_id == w.buyer_id

    @pytest.mark.skip(reason="TODO")
    def test_more_stuff(self):
        # todo: .update, test status logic
        pass
