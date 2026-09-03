from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from random import choice as randchoice
from random import randint
from typing import TYPE_CHECKING
from uuid import uuid4

import pytest
from pydantic import AwareDatetime, PositiveInt
from pytest import FixtureRequest
from pytest import FixtureRequest as Request

from generalresearch.models.definitions import Source
from generalresearch.models.thl.definitions import (
    WALL_ALLOWED_STATUS_STATUS_CODE,
    Status,
)
from generalresearch.models.thl.survey.model import Buyer, Survey

if TYPE_CHECKING:
    from generalresearch.currency import USDCent
    from generalresearch.managers.thl.buyer import BuyerManager
    from generalresearch.managers.thl.ipinfo import (
        IPGeonameManager,
        IPInformationManager,
    )
    from generalresearch.managers.thl.ledger_manager.thl_ledger import ThlLedgerManager
    from generalresearch.managers.thl.payout import (
        BusinessPayoutEventManager,
    )
    from generalresearch.managers.thl.product import ProductManager
    from generalresearch.managers.thl.session import SessionManager
    from generalresearch.managers.thl.survey import SurveyManager
    from generalresearch.managers.thl.user_manager.user_manager import UserManager
    from generalresearch.managers.thl.userhealth import AuditLogManager, IPRecordManager
    from generalresearch.managers.thl.wall import WallManager
    from generalresearch.models.gr.business import (
        Business,
    )
    from generalresearch.models.gr.team import Team
    from generalresearch.models.thl.ipinfo import IPGeoname, IPInformation
    from generalresearch.models.thl.payout import (
        BrokerageProductPayoutEvent,
    )
    from generalresearch.models.thl.product import (
        PayoutConfig,
        Product,
    )
    from generalresearch.models.thl.session import Session, Wall
    from generalresearch.models.thl.user import User

# === THL ===


@pytest.fixture
def session_factory(
    session_manager: SessionManager,
    wall_manager: WallManager,
    utc_hour_ago: datetime,
    session_factory: Callable[..., Session],
    wall_factory: Callable[..., Wall],
) -> Callable[..., Session]:
    from generalresearch.models.thl.session import Source

    def _inner(
        user: User,
        # Wall details
        wall_count: int = 5,
        wall_req_cpi: Decimal = Decimal(".50"),
        wall_req_cpis: list[Decimal] | None = None,
        wall_statuses: list[Status] | None = None,
        wall_source: Source = Source.TESTING,
        # Session details
        final_status: Status = Status.COMPLETE,
        started: datetime = utc_hour_ago,
    ) -> Session:
        if wall_req_cpis:
            assert len(wall_req_cpis) == wall_count
        if wall_statuses:
            assert len(wall_statuses) == wall_count

        s = session_factory(started=started, user=user, country_iso="us")
        for idx in range(wall_count):
            if idx == 0:
                # First Wall Event in a session
                wall_started = s.started + timedelta(milliseconds=1)
            else:
                # Subsequent Wall events
                last_wall = s.wall_events[-1]
                assert last_wall.finished, "Can't add new Walls until prior finishes"
                wall_started = last_wall.started + timedelta(milliseconds=1)

            w = wall_factory(
                session_id=s.id,
                source=wall_source,
                user_id=s.user_id,
                started=wall_started,
                req_cpi=wall_req_cpis[idx] if wall_req_cpis else wall_req_cpi,
            )
            s.append_wall_event(w=w)

            # If it's the last wall in the session, respect the final_status
            #   value for the Session
            if wall_statuses:
                _final_status = wall_statuses[idx]
            else:
                _final_status = final_status if idx == wall_count - 1 else Status.FAIL

            options = list(WALL_ALLOWED_STATUS_STATUS_CODE.get(_final_status, {}))
            wall_manager.finish(
                wall=w,
                status=_final_status,
                status_code_1=randchoice(options),
                finished=w.started + timedelta(seconds=randint(a=60 * 2, b=60 * 10)),
            )

        return s

    return _inner


@pytest.fixture(scope="function")
def finished_session_factory(
    session_factory: Callable[..., Session],
    session_manager: SessionManager,
    utc_hour_ago: datetime,
) -> Callable[..., Session]:
    from generalresearch.models.thl.session import Source

    def _inner(
        user: User,
        # Wall details
        wall_count: int = 5,
        wall_req_cpi: Decimal = Decimal(".50"),
        wall_req_cpis: list[Decimal] | None = None,
        wall_statuses: list[Status] | None = None,
        wall_source: Source = Source.TESTING,
        # Session details
        final_status: Status = Status.COMPLETE,
        started: datetime = utc_hour_ago,
    ) -> Session:
        s: Session = session_factory(
            user=user,
            wall_count=wall_count,
            wall_req_cpi=wall_req_cpi,
            wall_req_cpis=wall_req_cpis,
            wall_statuses=wall_statuses,
            wall_source=wall_source,
            final_status=final_status,
            started=started,
        )
        status, status_code_1 = s.determine_session_status()
        _, _, bp_pay, user_pay = s.determine_payments()
        session_manager.finish_with_status(
            s,
            finished=s.wall_events[-1].finished,
            payout=bp_pay,
            user_payout=user_pay,
            status=status,
            status_code_1=status_code_1,
        )
        return s

    return _inner


@pytest.fixture
def session(
    user: User,
    session_manager: SessionManager,
    wall_manager: WallManager,
    session_factory: Callable[..., Session],
    wall_factory: Callable[..., Wall],
) -> Session:

    session: Session = session_factory(user=user, country_iso="us")
    wall: Wall = wall_factory(
        session_id=session.id,
        user_id=session.user_id,
        started=session.started,
    )
    session.append_wall_event(w=wall)

    return session


@pytest.fixture()
def payout_config(request: Request) -> PayoutConfig:
    from generalresearch.models.thl.product import (
        PayoutConfig,
        PayoutTransformation,
        PayoutTransformationPercentArgs,
    )

    return (
        request.param
        if hasattr(request, "payout_config")
        else PayoutConfig(
            payout_format="${payout/100:.2f}",
            payout_transformation=PayoutTransformation(
                f="payout_transformation_percent",
                kwargs=PayoutTransformationPercentArgs(pct=0.40),
            ),
        )
    )


@pytest.fixture
def product_user_wallet_yes(
    product_factory: Callable[..., Product],
    payout_config: PayoutConfig,
    product_manager: ProductManager,
) -> Product:
    from generalresearch.models.thl.product import UserWalletConfig

    return product_factory(
        payout_config=payout_config, user_wallet_config=UserWalletConfig(enabled=True)
    )


@pytest.fixture
def product_user_wallet_no(
    product_factory: Callable[..., Product], product_manager: ProductManager
) -> Product:
    from generalresearch.models.thl.product import UserWalletConfig

    return product_factory(user_wallet_config=UserWalletConfig(enabled=False))


@pytest.fixture
def product_amt_true(
    product_factory: Callable[..., Product],
    product_manager: ProductManager,
    payout_config: PayoutConfig,
) -> Product:
    from generalresearch.models.thl.product import UserWalletConfig

    return product_factory(
        user_wallet_config=UserWalletConfig(amt=True, enabled=True),
        payout_config=payout_config,
    )


@pytest.fixture(scope="session")
def buyer(buyer_manager: BuyerManager) -> Buyer:
    buyer_code = uuid4().hex
    buyer_manager.bulk_get_or_create(source=Source.TESTING, codes=[buyer_code])
    b = Buyer(
        source=Source.TESTING, code=buyer_code, label=f"test-buyer-{buyer_code[:8]}"
    )
    buyer_manager.update(b)
    return b


@pytest.fixture(scope="session")
def buyer_factory(buyer_manager: BuyerManager) -> Callable[..., Buyer]:

    def _inner() -> Buyer:
        return buyer_manager.bulk_get_or_create(
            source=Source.TESTING, codes=[uuid4().hex]
        )[0]

    return _inner


@pytest.fixture(scope="session")
def survey(survey_manager: SurveyManager, buyer: Buyer) -> Survey:
    s = Survey(source=Source.TESTING, survey_id=uuid4().hex, buyer_code=buyer.code)
    survey_manager.create_bulk([s])
    return s


@pytest.fixture(scope="session")
def survey_factory(
    survey_manager: SurveyManager, buyer_factory: Callable[..., Buyer]
) -> Callable[..., Survey]:

    def _inner(buyer: Buyer | None = None) -> Survey:
        buyer = buyer or buyer_factory()
        s = Survey(
            source=Source.TESTING,
            survey_id=uuid4().hex,
            buyer_code=buyer.code,
            buyer_id=buyer.id,
        )
        survey_manager.create_bulk([s])
        return s

    return _inner
