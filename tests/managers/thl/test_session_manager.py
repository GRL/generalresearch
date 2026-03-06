from datetime import timedelta
from decimal import Decimal
from uuid import uuid4

from faker import Faker

from generalresearch.models import DeviceType
from generalresearch.models.legacy.bucket import Bucket
from generalresearch.models.thl.definitions import (
    Status,
    StatusCode1,
    SessionStatusCode2,
)
from test_utils.models.conftest import user

fake = Faker()


class TestSessionManager:
    def test_create_session(self, session_manager, user, utc_hour_ago):
        bucket = Bucket(
            loi_min=timedelta(seconds=60),
            loi_max=timedelta(seconds=120),
            user_payout_min=Decimal("1"),
            user_payout_max=Decimal("2"),
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

    def test_finish_with_status(self, session_manager, user, utc_hour_ago):
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

    def test_base(self, session_manager, user, utc_now):
        uuid_id = uuid4().hex
        session_manager.create(started=utc_now, user=user, uuid_id=uuid_id)
        res = session_manager.filter(limit=1)
        assert len(res) != 0
        assert isinstance(res, list)
        assert res[0].uuid == uuid_id

    def test_user(self, session_manager, user, utc_hour_ago):
        session_manager.create(started=utc_hour_ago, user=user, uuid_id=uuid4().hex)
        session_manager.create(started=utc_hour_ago, user=user, uuid_id=uuid4().hex)

        res = session_manager.filter(user=user)
        assert len(res) == 2

    def test_product(
        self, product_factory, user_factory, session_manager, user, utc_hour_ago
    ):
        from generalresearch.models.thl.session import Session
        from generalresearch.models.thl.user import User

        p1 = product_factory()

        for n in range(5):
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
        product_factory,
        user_factory,
        team,
        session_manager,
        user,
        utc_hour_ago,
        thl_web_rr,
    ):
        p1 = product_factory(team=team)

        for n in range(5):
            u = user_factory(product=p1)
            session_manager.create(started=utc_hour_ago, user=u, uuid_id=uuid4().hex)

        team.prefetch_products(thl_pg_config=thl_web_rr)
        assert len(team.product_uuids) == 1
        res = session_manager.filter(product_uuids=team.product_uuids)
        assert len(res) == 5

    def test_business(
        self,
        product_factory,
        business,
        user_factory,
        session_manager,
        user,
        utc_hour_ago,
        thl_web_rr,
    ):
        p1 = product_factory(business=business)

        for n in range(5):
            u = user_factory(product=p1)
            session_manager.create(started=utc_hour_ago, user=u, uuid_id=uuid4().hex)

        business.prefetch_products(thl_pg_config=thl_web_rr)
        assert len(business.product_uuids) == 1
        res = session_manager.filter(product_uuids=business.product_uuids)
        assert len(res) == 5
