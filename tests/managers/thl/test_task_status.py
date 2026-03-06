import pytest
from datetime import datetime, timezone, timedelta
from decimal import Decimal

from generalresearch.managers.thl.session import SessionManager
from generalresearch.models import Source
from generalresearch.models.thl.definitions import (
    Status,
    WallAdjustedStatus,
    StatusCode1,
)
from generalresearch.models.thl.product import (
    PayoutConfig,
    UserWalletConfig,
    PayoutTransformation,
    PayoutTransformationPercentArgs,
)
from generalresearch.models.thl.session import Session, WallOut
from generalresearch.models.thl.task_status import TaskStatusResponse
from generalresearch.models.thl.user import User


start1 = datetime(2023, 2, 1, tzinfo=timezone.utc)
finish1 = start1 + timedelta(minutes=5)
recon1 = start1 + timedelta(days=20)
start2 = datetime(2023, 2, 2, tzinfo=timezone.utc)
finish2 = start2 + timedelta(minutes=5)
start3 = datetime(2023, 2, 3, tzinfo=timezone.utc)
finish3 = start3 + timedelta(minutes=5)


@pytest.fixture(scope="session")
def bp1(product_manager):
    # user wallet disabled, payout xform NULL
    return product_manager.create_dummy(
        user_wallet_config=UserWalletConfig(enabled=False),
        payout_config=PayoutConfig(),
    )


@pytest.fixture(scope="session")
def bp2(product_manager):
    # user wallet disabled, payout xform 40%
    return product_manager.create_dummy(
        user_wallet_config=UserWalletConfig(enabled=False),
        payout_config=PayoutConfig(
            payout_transformation=PayoutTransformation(
                f="payout_transformation_percent",
                kwargs=PayoutTransformationPercentArgs(pct=0.4),
            )
        ),
    )


@pytest.fixture(scope="session")
def bp3(product_manager):
    # user wallet enabled, payout xform 50%
    return product_manager.create_dummy(
        user_wallet_config=UserWalletConfig(enabled=True),
        payout_config=PayoutConfig(
            payout_transformation=PayoutTransformation(
                f="payout_transformation_percent",
                kwargs=PayoutTransformationPercentArgs(pct=0.5),
            )
        ),
    )


class TestTaskStatus:

    def test_task_status_complete_1(
        self,
        bp1,
        user_factory,
        finished_session_factory,
        session_manager: SessionManager,
    ):
        # User Payout xform NULL
        user1: User = user_factory(product=bp1)
        s1: Session = finished_session_factory(
            user=user1, started=start1, wall_req_cpi=Decimal(1), wall_count=2
        )

        expected_tsr = TaskStatusResponse.model_validate(
            {
                "tsid": s1.uuid,
                "product_id": user1.product_id,
                "product_user_id": user1.product_user_id,
                "started": start1.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                "finished": s1.finished.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                "status": "c",
                "payout": 95,
                "user_payout": None,
                "payout_format": None,
                "user_payout_string": None,
                "status_code_1": 14,
                "payout_transformation": None,
            }
        )
        w1 = s1.wall_events[0]
        wo1 = WallOut(
            uuid=w1.uuid,
            source=Source.TESTING,
            buyer_id=None,
            req_survey_id=w1.req_survey_id,
            req_cpi=w1.req_cpi,
            started=w1.started,
            survey_id=w1.survey_id,
            cpi=w1.cpi,
            finished=w1.finished,
            status=w1.status,
            status_code_1=w1.status_code_1,
        )
        w2 = s1.wall_events[1]
        wo2 = WallOut(
            uuid=w2.uuid,
            source=Source.TESTING,
            buyer_id=None,
            req_survey_id=w2.req_survey_id,
            req_cpi=w2.req_cpi,
            started=w2.started,
            survey_id=w2.survey_id,
            cpi=w2.cpi,
            finished=w2.finished,
            status=Status.COMPLETE,
            status_code_1=StatusCode1.COMPLETE,
        )
        expected_tsr.wall_events = [wo1, wo2]
        tsr = session_manager.get_task_status_response(s1.uuid)
        assert tsr == expected_tsr

    def test_task_status_complete_2(
        self, bp2, user_factory, finished_session_factory, session_manager
    ):
        # User Payout xform 40%
        user2: User = user_factory(product=bp2)
        s2: Session = finished_session_factory(
            user=user2, started=start1, wall_req_cpi=Decimal(1), wall_count=2
        )
        payout = 95  # 1.00 - 5% commission
        user_payout = round(95 * 0.40)  # 38
        expected_tsr = TaskStatusResponse.model_validate(
            {
                "tsid": s2.uuid,
                "product_id": user2.product_id,
                "product_user_id": user2.product_user_id,
                "started": start1.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                "finished": s2.finished.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                "status": "c",
                "payout": payout,
                "user_payout": user_payout,
                "payout_format": "${payout/100:.2f}",
                "user_payout_string": "$0.38",
                "status_code_1": 14,
                "status_code_2": None,
                "payout_transformation": {
                    "f": "payout_transformation_percent",
                    "kwargs": {"pct": "0.4"},
                },
            }
        )
        w1 = s2.wall_events[0]
        wo1 = WallOut(
            uuid=w1.uuid,
            source=Source.TESTING,
            buyer_id=None,
            req_survey_id=w1.req_survey_id,
            req_cpi=w1.req_cpi,
            started=w1.started,
            survey_id=w1.survey_id,
            cpi=w1.cpi,
            finished=w1.finished,
            status=w1.status,
            status_code_1=w1.status_code_1,
            user_cpi=Decimal("0.38"),
            user_cpi_string="$0.38",
        )
        w2 = s2.wall_events[1]
        wo2 = WallOut(
            uuid=w2.uuid,
            source=Source.TESTING,
            buyer_id=None,
            req_survey_id=w2.req_survey_id,
            req_cpi=w2.req_cpi,
            started=w2.started,
            survey_id=w2.survey_id,
            cpi=w2.cpi,
            finished=w2.finished,
            status=Status.COMPLETE,
            status_code_1=StatusCode1.COMPLETE,
            user_cpi=Decimal("0.38"),
            user_cpi_string="$0.38",
        )
        expected_tsr.wall_events = [wo1, wo2]

        tsr = session_manager.get_task_status_response(s2.uuid)
        assert tsr == expected_tsr

    def test_task_status_complete_3(
        self, bp3, user_factory, finished_session_factory, session_manager
    ):
        # Wallet enabled User Payout xform 50% (the response is identical
        # to the user wallet disabled w same xform)
        user3: User = user_factory(product=bp3)
        s3: Session = finished_session_factory(
            user=user3, started=start1, wall_req_cpi=Decimal(1)
        )

        expected_tsr = TaskStatusResponse.model_validate(
            {
                "tsid": s3.uuid,
                "product_id": user3.product_id,
                "product_user_id": user3.product_user_id,
                "started": start1.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                "finished": s3.finished.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                "status": "c",
                "payout": 95,
                "user_payout": 48,
                "payout_format": "${payout/100:.2f}",
                "user_payout_string": "$0.48",
                "kwargs": {},
                "status_code_1": 14,
                "payout_transformation": {
                    "f": "payout_transformation_percent",
                    "kwargs": {"pct": "0.5"},
                },
            }
        )
        tsr = session_manager.get_task_status_response(s3.uuid)
        # Not bothering with wall events ...
        tsr.wall_events = None
        assert tsr == expected_tsr

    def test_task_status_fail(
        self, bp1, user_factory, finished_session_factory, session_manager
    ):
        # User Payout xform NULL: user payout is None always
        user1: User = user_factory(product=bp1)
        s1: Session = finished_session_factory(
            user=user1, started=start1, final_status=Status.FAIL
        )
        expected_tsr = TaskStatusResponse.model_validate(
            {
                "tsid": s1.uuid,
                "product_id": user1.product_id,
                "product_user_id": user1.product_user_id,
                "started": start1.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                "finished": s1.finished.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                "status": "f",
                "payout": 0,
                "user_payout": None,
                "payout_format": None,
                "user_payout_string": None,
                "kwargs": {},
                "status_code_1": s1.status_code_1.value,
                "status_code_2": None,
                "adjusted_status": None,
                "adjusted_timestamp": None,
                "adjusted_payout": None,
                "adjusted_user_payout": None,
                "adjusted_user_payout_string": None,
                "payout_transformation": None,
            }
        )
        tsr = session_manager.get_task_status_response(s1.uuid)
        # Not bothering with wall events ...
        tsr.wall_events = None
        assert tsr == expected_tsr

    def test_task_status_fail_xform(
        self, bp2, user_factory, finished_session_factory, session_manager
    ):
        # User Payout xform 40%: user_payout is 0 (not None)

        user: User = user_factory(product=bp2)
        s: Session = finished_session_factory(
            user=user, started=start1, final_status=Status.FAIL
        )
        expected_tsr = TaskStatusResponse.model_validate(
            {
                "tsid": s.uuid,
                "product_id": user.product_id,
                "product_user_id": user.product_user_id,
                "started": start1.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                "finished": s.finished.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                "status": "f",
                "payout": 0,
                "user_payout": 0,
                "payout_format": "${payout/100:.2f}",
                "user_payout_string": "$0.00",
                "kwargs": {},
                "status_code_1": s.status_code_1.value,
                "status_code_2": None,
                "payout_transformation": {
                    "f": "payout_transformation_percent",
                    "kwargs": {"pct": "0.4"},
                },
            }
        )
        tsr = session_manager.get_task_status_response(s.uuid)
        # Not bothering with wall events ...
        tsr.wall_events = None
        assert tsr == expected_tsr

    def test_task_status_abandon(
        self, bp1, user_factory, session_factory, session_manager
    ):
        # User Payout xform NULL: all payout fields are None
        user: User = user_factory(product=bp1)
        s = session_factory(user=user, started=start1)
        expected_tsr = TaskStatusResponse.model_validate(
            {
                "tsid": s.uuid,
                "product_id": user.product_id,
                "product_user_id": user.product_user_id,
                "started": start1.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                "finished": None,
                "status": None,
                "payout": None,
                "user_payout": None,
                "payout_format": None,
                "user_payout_string": None,
                "kwargs": {},
                "status_code_1": None,
                "status_code_2": None,
                "adjusted_status": None,
                "adjusted_timestamp": None,
                "adjusted_payout": None,
                "adjusted_user_payout": None,
                "adjusted_user_payout_string": None,
                "payout_transformation": None,
            }
        )
        tsr = session_manager.get_task_status_response(s.uuid)
        # Not bothering with wall events ...
        tsr.wall_events = None
        assert tsr == expected_tsr

    def test_task_status_abandon_xform(
        self, bp2, user_factory, session_factory, session_manager
    ):
        # User Payout xform 40%: all payout fields are None (same as when payout xform is null)
        user: User = user_factory(product=bp2)
        s = session_factory(user=user, started=start1)
        expected_tsr = TaskStatusResponse.model_validate(
            {
                "tsid": s.uuid,
                "product_id": user.product_id,
                "product_user_id": user.product_user_id,
                "started": start1.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                "finished": None,
                "status": None,
                "payout": None,
                "user_payout": None,
                "payout_format": "${payout/100:.2f}",
                "user_payout_string": None,
                "kwargs": {},
                "status_code_1": None,
                "status_code_2": None,
                "adjusted_status": None,
                "adjusted_timestamp": None,
                "adjusted_payout": None,
                "adjusted_user_payout": None,
                "adjusted_user_payout_string": None,
                "payout_transformation": {
                    "f": "payout_transformation_percent",
                    "kwargs": {"pct": "0.4"},
                },
            }
        )
        tsr = session_manager.get_task_status_response(s.uuid)
        # Not bothering with wall events ...
        tsr.wall_events = None
        assert tsr == expected_tsr

    def test_task_status_adj_fail(
        self,
        bp1,
        user_factory,
        finished_session_factory,
        wall_manager,
        session_manager,
    ):
        # Complete -> Fail
        # User Payout xform NULL: adjusted_user_* and user_* is still all None
        user: User = user_factory(product=bp1)
        s: Session = finished_session_factory(
            user=user, started=start1, wall_req_cpi=Decimal(1)
        )
        wall_manager.adjust_status(
            wall=s.wall_events[-1],
            adjusted_status=WallAdjustedStatus.ADJUSTED_TO_FAIL,
            adjusted_timestamp=recon1,
        )
        session_manager.adjust_status(s)

        expected_tsr = TaskStatusResponse.model_validate(
            {
                "tsid": s.uuid,
                "product_id": user.product_id,
                "product_user_id": user.product_user_id,
                "started": start1.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                "finished": s.finished.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                "status": "c",
                "payout": 95,
                "user_payout": None,
                "payout_format": None,
                "user_payout_string": None,
                "kwargs": {},
                "status_code_1": StatusCode1.COMPLETE.value,
                "status_code_2": None,
                "adjusted_status": WallAdjustedStatus.ADJUSTED_TO_FAIL.value,
                "adjusted_timestamp": recon1.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                "adjusted_payout": 0,
                "adjusted_user_payout": None,
                "adjusted_user_payout_string": None,
                "payout_transformation": None,
            }
        )
        tsr = session_manager.get_task_status_response(s.uuid)
        # Not bothering with wall events ...
        tsr.wall_events = None
        assert tsr == expected_tsr

    def test_task_status_adj_fail_xform(
        self,
        bp2,
        user_factory,
        finished_session_factory,
        wall_manager,
        session_manager,
    ):
        # Complete -> Fail
        # User Payout xform 40%: adjusted_user_payout is 0 (not null)
        user: User = user_factory(product=bp2)
        s: Session = finished_session_factory(
            user=user, started=start1, wall_req_cpi=Decimal(1)
        )
        wall_manager.adjust_status(
            wall=s.wall_events[-1],
            adjusted_status=WallAdjustedStatus.ADJUSTED_TO_FAIL,
            adjusted_timestamp=recon1,
        )
        session_manager.adjust_status(s)

        expected_tsr = TaskStatusResponse.model_validate(
            {
                "tsid": s.uuid,
                "product_id": user.product_id,
                "product_user_id": user.product_user_id,
                "started": start1.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                "finished": s.finished.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                "status": "c",
                "payout": 95,
                "user_payout": 38,
                "payout_format": "${payout/100:.2f}",
                "user_payout_string": "$0.38",
                "kwargs": {},
                "status_code_1": StatusCode1.COMPLETE.value,
                "status_code_2": None,
                "adjusted_status": WallAdjustedStatus.ADJUSTED_TO_FAIL.value,
                "adjusted_timestamp": recon1.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                "adjusted_payout": 0,
                "adjusted_user_payout": 0,
                "adjusted_user_payout_string": "$0.00",
                "payout_transformation": {
                    "f": "payout_transformation_percent",
                    "kwargs": {"pct": "0.4"},
                },
            }
        )
        tsr = session_manager.get_task_status_response(s.uuid)
        # Not bothering with wall events ...
        tsr.wall_events = None
        assert tsr == expected_tsr

    def test_task_status_adj_complete_from_abandon(
        self,
        bp1,
        user_factory,
        session_factory,
        wall_manager,
        session_manager,
    ):
        # User Payout xform NULL
        user: User = user_factory(product=bp1)
        s: Session = session_factory(
            user=user,
            started=start1,
            wall_req_cpi=Decimal(1),
            wall_count=2,
            final_status=Status.ABANDON,
        )
        w = s.wall_events[-1]
        wall_manager.adjust_status(
            wall=w,
            adjusted_status=WallAdjustedStatus.ADJUSTED_TO_COMPLETE,
            adjusted_cpi=w.cpi,
            adjusted_timestamp=recon1,
        )
        session_manager.adjust_status(s)

        expected_tsr = TaskStatusResponse.model_validate(
            {
                "tsid": s.uuid,
                "product_id": user.product_id,
                "product_user_id": user.product_user_id,
                "started": start1.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                "finished": None,
                "status": None,
                "payout": None,
                "user_payout": None,
                "payout_format": None,
                "user_payout_string": None,
                "kwargs": {},
                "status_code_1": None,
                "status_code_2": None,
                "adjusted_status": WallAdjustedStatus.ADJUSTED_TO_COMPLETE.value,
                "adjusted_timestamp": recon1.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                "adjusted_payout": 95,
                "adjusted_user_payout": None,
                "adjusted_user_payout_string": None,
                "payout_transformation": None,
            }
        )
        tsr = session_manager.get_task_status_response(s.uuid)
        # Not bothering with wall events ...
        tsr.wall_events = None
        assert tsr == expected_tsr

    def test_task_status_adj_complete_from_abandon_xform(
        self,
        bp2,
        user_factory,
        session_factory,
        wall_manager,
        session_manager,
    ):
        # User Payout xform 40%
        user: User = user_factory(product=bp2)
        s: Session = session_factory(
            user=user,
            started=start1,
            wall_req_cpi=Decimal(1),
            wall_count=2,
            final_status=Status.ABANDON,
        )
        w = s.wall_events[-1]
        wall_manager.adjust_status(
            wall=w,
            adjusted_status=WallAdjustedStatus.ADJUSTED_TO_COMPLETE,
            adjusted_cpi=w.cpi,
            adjusted_timestamp=recon1,
        )
        session_manager.adjust_status(s)

        expected_tsr = TaskStatusResponse.model_validate(
            {
                "tsid": s.uuid,
                "product_id": user.product_id,
                "product_user_id": user.product_user_id,
                "started": start1.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                "finished": None,
                "status": None,
                "payout": None,
                "user_payout": None,
                "payout_format": "${payout/100:.2f}",
                "user_payout_string": None,
                "kwargs": {},
                "status_code_1": None,
                "status_code_2": None,
                "adjusted_status": WallAdjustedStatus.ADJUSTED_TO_COMPLETE.value,
                "adjusted_timestamp": recon1.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                "adjusted_payout": 95,
                "adjusted_user_payout": 38,
                "adjusted_user_payout_string": "$0.38",
                "payout_transformation": {
                    "f": "payout_transformation_percent",
                    "kwargs": {"pct": "0.4"},
                },
            }
        )
        tsr = session_manager.get_task_status_response(s.uuid)
        # Not bothering with wall events ...
        tsr.wall_events = None
        assert tsr == expected_tsr

    def test_task_status_adj_complete_from_fail(
        self,
        bp1,
        user_factory,
        finished_session_factory,
        wall_manager,
        session_manager,
    ):
        # User Payout xform NULL
        user: User = user_factory(product=bp1)
        s: Session = finished_session_factory(
            user=user,
            started=start1,
            wall_req_cpi=Decimal(1),
            wall_count=2,
            final_status=Status.FAIL,
        )
        w = s.wall_events[-1]
        wall_manager.adjust_status(
            wall=w,
            adjusted_status=WallAdjustedStatus.ADJUSTED_TO_COMPLETE,
            adjusted_cpi=w.cpi,
            adjusted_timestamp=recon1,
        )
        session_manager.adjust_status(s)

        expected_tsr = TaskStatusResponse.model_validate(
            {
                "tsid": s.uuid,
                "product_id": user.product_id,
                "product_user_id": user.product_user_id,
                "started": start1.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                "finished": s.finished.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                "status": "f",
                "payout": 0,
                "user_payout": None,
                "payout_format": None,
                "user_payout_string": None,
                "kwargs": {},
                "status_code_1": s.status_code_1.value,
                "status_code_2": None,
                "adjusted_status": "ac",
                "adjusted_timestamp": recon1.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                "adjusted_payout": 95,
                "adjusted_user_payout": None,
                "adjusted_user_payout_string": None,
                "payout_transformation": None,
            }
        )
        tsr = session_manager.get_task_status_response(s.uuid)
        # Not bothering with wall events ...
        tsr.wall_events = None
        assert tsr == expected_tsr

    def test_task_status_adj_complete_from_fail_xform(
        self,
        bp2,
        user_factory,
        finished_session_factory,
        wall_manager,
        session_manager,
    ):
        # User Payout xform 40%
        user: User = user_factory(product=bp2)
        s: Session = finished_session_factory(
            user=user,
            started=start1,
            wall_req_cpi=Decimal(1),
            wall_count=2,
            final_status=Status.FAIL,
        )
        w = s.wall_events[-1]
        wall_manager.adjust_status(
            wall=w,
            adjusted_status=WallAdjustedStatus.ADJUSTED_TO_COMPLETE,
            adjusted_cpi=w.cpi,
            adjusted_timestamp=recon1,
        )
        session_manager.adjust_status(s)
        expected_tsr = TaskStatusResponse.model_validate(
            {
                "tsid": s.uuid,
                "product_id": user.product_id,
                "product_user_id": user.product_user_id,
                "started": start1.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                "finished": s.finished.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                "status": "f",
                "payout": 0,
                "user_payout": 0,
                "payout_format": "${payout/100:.2f}",
                "user_payout_string": "$0.00",
                "kwargs": {},
                "status_code_1": s.status_code_1.value,
                "status_code_2": None,
                "adjusted_status": "ac",
                "adjusted_timestamp": recon1.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                "adjusted_payout": 95,
                "adjusted_user_payout": 38,
                "adjusted_user_payout_string": "$0.38",
                "payout_transformation": {
                    "f": "payout_transformation_percent",
                    "kwargs": {"pct": "0.4"},
                },
            }
        )
        tsr = session_manager.get_task_status_response(s.uuid)
        # Not bothering with wall events ...
        tsr.wall_events = None
        assert tsr == expected_tsr
