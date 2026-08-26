from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timedelta
from decimal import Decimal
from uuid import uuid4

from faker import Faker

from generalresearch.managers.thl.session import SessionManager
from generalresearch.models import DeviceType
from generalresearch.models.gr.business import Business
from generalresearch.models.gr.team import Team
from generalresearch.models.legacy.bucket import Bucket
from generalresearch.models.thl.definitions import (
    SessionStatusCode2,
    Status,
    StatusCode1,
)
from generalresearch.models.thl.product import Product
from generalresearch.models.thl.session import Session
from generalresearch.models.thl.user import User
from generalresearch.pg_helper import PostgresConfig

fake = Faker()


class TestSessionManager:
    def test_create_session(
        self, session_manager: SessionManager, user: User, utc_hour_ago: datetime
    ):
        bucket = Bucket(
            loi_min=timedelta(seconds=60),
            loi_max=timedelta(seconds=120),
            user_payout_min=Decimal(1),
            user_payout_max=Decimal(2),
        )

        s1 = session_manager.create(
            started=utc_hour_ago,
            user=user,
            country_iso="us",
            device_type=DeviceType.MOBILE,
            ip=fake.ipv4_public(),
            bucket=bucket,
            url_metadata={"foo": "bar"},
            uuid_id=uuid4().hex,
        )

        assert s1.id is not None
        s2 = session_manager.get_from_uuid(session_uuid=s1.uuid)
        assert s1 == s2

    def test_finish_with_status(
        self, session_manager: SessionManager, user: User, utc_hour_ago: datetime
    ):
        uuid_1 = uuid4().hex
        session = session_manager.create(
            started=utc_hour_ago, user=user, uuid_id=uuid_1
        )
        session_manager.finish_with_status(
            session=session,
            status=Status.FAIL,
            status_code_1=StatusCode1.SESSION_CONTINUE_FAIL,
            status_code_2=SessionStatusCode2.USER_IS_BLOCKED,
        )

        s2 = session_manager.get_from_uuid(session_uuid=uuid_1)
        assert s2.status == Status.FAIL
        assert s2.status_code_1 == StatusCode1.SESSION_CONTINUE_FAIL
        assert s2.status_code_2 == SessionStatusCode2.USER_IS_BLOCKED


class TestSessionManagerFilter:

    def test_base(self, session_manager: SessionManager, user: User, utc_now: datetime):
        uuid_id = uuid4().hex
        session_manager.create(started=utc_now, user=user, uuid_id=uuid_id)
        res = session_manager.filter(limit=1)
        assert len(res) != 0
        assert isinstance(res, list)
        assert res[0].uuid == uuid_id

    def test_user(
        self, session_manager: SessionManager, user: User, utc_hour_ago: datetime
    ):
        session_manager.create(started=utc_hour_ago, user=user, uuid_id=uuid4().hex)
        session_manager.create(started=utc_hour_ago, user=user, uuid_id=uuid4().hex)

        res = session_manager.filter(user=user)
        assert len(res) == 2

    def test_product(
        self,
        product_factory: Callable[..., Product],
        user_factory: Callable[..., User],
        session_manager: SessionManager,
        utc_hour_ago: datetime,
    ):

        p1 = product_factory()

        for _ in range(5):
            u = user_factory(product=p1)
            session_manager.create(started=utc_hour_ago, user=u, uuid_id=uuid4().hex)

        res = session_manager.filter(
            product_uuids=[p1.uuid], started_since=utc_hour_ago
        )
        assert isinstance(res[0], Session)
        assert isinstance(res[0].user, User)
        assert len(res) == 5

    def test_team(
        self,
        product_factory: Callable[..., Product],
        user_factory: Callable[..., User],
        team: Team,
        session_manager: SessionManager,
        utc_hour_ago: datetime,
        thl_web_rr: PostgresConfig,
    ):
        p1 = product_factory(team=team)

        for _ in range(5):
            u = user_factory(product=p1)
            session_manager.create(started=utc_hour_ago, user=u, uuid_id=uuid4().hex)

        team.prefetch_products(thl_pg_config=thl_web_rr)
        assert len(team.product_uuids) == 1
        res = session_manager.filter(product_uuids=team.product_uuids)
        assert len(res) == 5

    def test_business(
        self,
        product_factory: Callable[..., Product],
        business: Business,
        user_factory: Callable[..., User],
        session_manager: SessionManager,
        utc_hour_ago: datetime,
        thl_web_rr: PostgresConfig,
    ):
        p1 = product_factory(business=business)

        for _ in range(5):
            u = user_factory(product=p1)
            session_manager.create(started=utc_hour_ago, user=u, uuid_id=uuid4().hex)

        business.prefetch_products(thl_pg_config=thl_web_rr)
        assert len(business.product_uuids) == 1
        res = session_manager.filter(product_uuids=business.product_uuids)
        assert len(res) == 5
