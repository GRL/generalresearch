from datetime import datetime, timedelta, timezone
from decimal import Decimal
from random import choice as randchoice
from random import randint
from typing import TYPE_CHECKING, Callable, Dict, List, Optional
from uuid import uuid4

import pytest
from pydantic import AwareDatetime, PositiveInt

from generalresearch.models import Source
from generalresearch.models.thl.definitions import (
    WALL_ALLOWED_STATUS_STATUS_CODE,
    Status,
)
from generalresearch.models.thl.survey.model import Buyer, Survey
from test_utils.managers.conftest import (
    business_address_manager,
    business_manager,
    gr_um,
    membership_manager,
    product_manager,
    session_manager,
    team_manager,
    user_manager,
    wall_manager,
)

if TYPE_CHECKING:
    from generalresearch.currency import USDCent
    from generalresearch.models.gr.authentication import GRToken, GRUser
    from generalresearch.models.gr.business import (
        Business,
        BusinessAddress,
        BusinessBankAccount,
    )
    from generalresearch.models.gr.team import Membership, Team
    from generalresearch.models.thl.ipinfo import IPGeoname, IPInformation
    from generalresearch.models.thl.payout import UserPayoutEvent
    from generalresearch.models.thl.product import (
        PayoutConfig,
        PayoutTransformation,
        PayoutTransformationPercentArgs,
        Product,
    )
    from generalresearch.models.thl.session import Session, Wall
    from generalresearch.models.thl.user import User
    from generalresearch.models.thl.user_iphistory import IPRecord
    from generalresearch.models.thl.userhealth import AuditLog, AuditLogLevel


# === THL ===


@pytest.fixture(scope="function")
def user(request, product_manager, user_manager, thl_web_rr) -> "User":
    product = getattr(request, "product", None)

    if product is None:
        product = product_manager.create_dummy()

    u = user_manager.create_dummy(product_id=product.id)
    u.prefetch_product(pg_config=thl_web_rr)

    return u


@pytest.fixture
def user_with_wallet(
    request, user_factory, product_user_wallet_yes: "Product"
) -> "User":
    # A user on a product with user wallet enabled, but they have no money
    return user_factory(product=product_user_wallet_yes)


@pytest.fixture
def user_with_wallet_amt(request, user_factory, product_amt_true: "Product") -> "User":
    # A user on a product with user wallet enabled, on AMT, but they have no money
    return user_factory(product=product_amt_true)


@pytest.fixture(scope="function")
def user_factory(user_manager, thl_web_rr) -> Callable:
    def _create_user(product: "Product", created: Optional[datetime] = None):
        u = user_manager.create_dummy(product=product, created=created)
        u.prefetch_product(pg_config=thl_web_rr)

        return u

    return _create_user


@pytest.fixture(scope="function")
def wall_factory(wall_manager) -> Callable:
    def _create_wall(
        session: "Session", wall_status: "Status", req_cpi: Optional[Decimal] = None
    ):

        assert session.started <= datetime.now(
            tz=timezone.utc
        ), "Session can't start in the future"

        if session.wall_events:
            # Subsequent Wall events
            wall = session.wall_events[-1]
            assert not wall.finished, "Can't add new Walls until prior finishes"
            # wall_started = last_wall.started + timedelta(milliseconds=1)
        else:
            # First Wall Event in a session
            wall_started = session.started + timedelta(milliseconds=1)

            wall = wall_manager.create_dummy(
                session_id=session.id,
                user_id=session.user_id,
                started=wall_started,
                req_cpi=req_cpi,
            )
            session.append_wall_event(w=wall)

        options = list(WALL_ALLOWED_STATUS_STATUS_CODE.get(wall_status, {}))
        wall.finish(
            finished=wall.started + timedelta(seconds=randint(a=60 * 2, b=60 * 10)),
            status=wall_status,
            status_code_1=randchoice(options),
        )

        return wall

    return _create_wall


@pytest.fixture(scope="function")
def wall(session, user, wall_manager) -> Optional["Wall"]:
    from generalresearch.models.thl.task_status import StatusCode1

    wall = wall_manager.create_dummy(session_id=session.id, user_id=user.user_id)
    # thl_session.append_wall_event(wall)
    wall.finish(
        finished=wall.started + timedelta(seconds=randint(a=60 * 2, b=60 * 10)),
        status=Status.COMPLETE,
        status_code_1=StatusCode1.COMPLETE,
    )
    return wall


@pytest.fixture(scope="function")
def session_factory(
    wall_factory, session_manager, wall_manager, utc_hour_ago
) -> Callable[..., "Session"]:
    from generalresearch.models.thl.session import Source

    def _inner(
        user: "User",
        # Wall details
        wall_count: int = 5,
        wall_req_cpi: Decimal = Decimal(".50"),
        wall_req_cpis: Optional[List[Decimal]] = None,
        wall_statuses: Optional[List[Status]] = None,
        wall_source: Source = Source.TESTING,
        # Session details
        final_status: Status = Status.COMPLETE,
        started: datetime = utc_hour_ago,
    ) -> "Session":
        if wall_req_cpis:
            assert len(wall_req_cpis) == wall_count
        if wall_statuses:
            assert len(wall_statuses) == wall_count

        s = session_manager.create_dummy(started=started, user=user, country_iso="us")
        for idx in range(wall_count):
            if idx == 0:
                # First Wall Event in a session
                wall_started = s.started + timedelta(milliseconds=1)
            else:
                # Subsequent Wall events
                last_wall = s.wall_events[-1]
                assert last_wall.finished, "Can't add new Walls until prior finishes"
                wall_started = last_wall.started + timedelta(milliseconds=1)

            w = wall_manager.create_dummy(
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
    session_factory, session_manager, utc_hour_ago
) -> Callable:
    from generalresearch.models.thl.session import Source

    def _create_finished_session(
        user: "User",
        # Wall details
        wall_count: int = 5,
        wall_req_cpi: Decimal = Decimal(".50"),
        wall_req_cpis: Optional[List[Decimal]] = None,
        wall_statuses: Optional[List[Status]] = None,
        wall_source: Source = Source.TESTING,
        # Session details
        final_status: Status = Status.COMPLETE,
        started: datetime = utc_hour_ago,
    ) -> "Session":
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
        thl_net, commission_amount, bp_pay, user_pay = s.determine_payments()
        session_manager.finish_with_status(
            s,
            finished=s.wall_events[-1].finished,
            payout=bp_pay,
            user_payout=user_pay,
            status=status,
            status_code_1=status_code_1,
        )
        return s

    return _create_finished_session


@pytest.fixture(scope="function")
def session(user, session_manager, wall_manager) -> "Session":
    from generalresearch.models.thl.session import Session, Wall

    session: Session = session_manager.create_dummy(user=user, country_iso="us")
    wall: Wall = wall_manager.create_dummy(
        session_id=session.id,
        user_id=session.user_id,
        started=session.started,
    )
    session.append_wall_event(w=wall)

    return session


@pytest.fixture
def product(request, product_manager) -> "Product":
    from generalresearch.managers.thl.product import ProductManager

    team = getattr(request, "team", None)
    business = getattr(request, "business", None)

    product_manager: ProductManager
    return product_manager.create_dummy(
        team_id=team.uuid if team else None,
        business_id=business.uuid if business else None,
    )


@pytest.fixture
def product_factory(product_manager) -> Callable:
    def _create_product(
        team: Optional["Team"] = None,
        business: Optional["Business"] = None,
        commission_pct: Decimal = Decimal("0.05"),
    ):
        return product_manager.create_dummy(
            team_id=team.uuid if team else None,
            business_id=business.uuid if business else None,
            commission_pct=commission_pct,
        )

    return _create_product


@pytest.fixture(scope="function")
def payout_config(request) -> "PayoutConfig":
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


@pytest.fixture(scope="function")
def product_user_wallet_yes(payout_config, product_manager) -> "Product":
    from generalresearch.managers.thl.product import ProductManager
    from generalresearch.models.thl.product import UserWalletConfig

    product_manager: ProductManager
    return product_manager.create_dummy(
        payout_config=payout_config, user_wallet_config=UserWalletConfig(enabled=True)
    )


@pytest.fixture(scope="function")
def product_user_wallet_no(product_manager) -> "Product":
    from generalresearch.managers.thl.product import ProductManager
    from generalresearch.models.thl.product import UserWalletConfig

    product_manager: ProductManager
    return product_manager.create_dummy(
        user_wallet_config=UserWalletConfig(enabled=False)
    )


@pytest.fixture(scope="function")
def product_amt_true(product_manager, payout_config) -> "Product":
    from generalresearch.models.thl.product import UserWalletConfig

    return product_manager.create_dummy(
        user_wallet_config=UserWalletConfig(amt=True, enabled=True),
        payout_config=payout_config,
    )


@pytest.fixture(scope="function")
def bp_payout_factory(
    thl_lm, product_manager, business_payout_event_manager
) -> Callable:
    def _create_bp_payout(
        product: Optional["Product"] = None,
        amount: Optional["USDCent"] = None,
        ext_ref_id: Optional[str] = None,
        created: Optional[AwareDatetime] = None,
        skip_wallet_balance_check: bool = False,
        skip_one_per_day_check: bool = False,
    ) -> "UserPayoutEvent":
        from generalresearch.currency import USDCent

        product = product or product_manager.create_dummy()
        amount = amount or USDCent(randint(1, 99_99))

        return business_payout_event_manager.create_bp_payout_event(
            thl_ledger_manager=thl_lm,
            product=product,
            amount=amount,
            ext_ref_id=ext_ref_id,
            created=created,
            skip_wallet_balance_check=skip_wallet_balance_check,
            skip_one_per_day_check=skip_one_per_day_check,
        )

    return _create_bp_payout


# === GR ===


@pytest.fixture(scope="function")
def business(request, business_manager) -> "Business":
    from generalresearch.managers.gr.business import BusinessManager

    business_manager: BusinessManager
    return business_manager.create_dummy()


@pytest.fixture(scope="function")
def business_address(request, business, business_address_manager) -> "BusinessAddress":
    from generalresearch.managers.gr.business import BusinessAddressManager

    business_address_manager: BusinessAddressManager
    return business_address_manager.create_dummy(business_id=business.id)


@pytest.fixture(scope="function")
def business_bank_account(
    request, business, business_bank_account_manager
) -> "BusinessBankAccount":
    from generalresearch.managers.gr.business import BusinessBankAccountManager

    business_bank_account_manager: BusinessBankAccountManager
    return business_bank_account_manager.create_dummy(business_id=business.id)


@pytest.fixture(scope="function")
def team(request, team_manager) -> "Team":
    from generalresearch.managers.gr.team import TeamManager

    team_manager: TeamManager
    return team_manager.create_dummy()


@pytest.fixture(scope="function")
def gr_user(gr_um) -> "GRUser":
    from generalresearch.managers.gr.authentication import GRUserManager

    gr_um: GRUserManager
    return gr_um.create_dummy()


@pytest.fixture(scope="function")
def gr_user_cache(gr_user, gr_db, thl_web_rr, gr_redis_config):
    gr_user.set_cache(
        pg_config=gr_db, thl_web_rr=thl_web_rr, redis_config=gr_redis_config
    )
    return gr_user


@pytest.fixture(scope="function")
def gr_user_factory(gr_um) -> Callable:
    def _create_gr_user():
        return gr_um.create_dummy()

    return _create_gr_user


@pytest.fixture()
def gr_user_token(gr_user, gr_tm, gr_db) -> "GRToken":
    gr_tm.create(user_id=gr_user.id)
    gr_user.prefetch_token(pg_config=gr_db)

    return gr_user.token


@pytest.fixture()
def gr_user_token_header(gr_user_token) -> Dict:
    return gr_user_token.auth_header


@pytest.fixture(scope="function")
def membership(request, team, gr_user, team_manager) -> "Membership":
    assert team.id, "Team must be saved"
    assert gr_user.id, "GRUser must be saved"
    return team_manager.add_user(team=team, gr_user=gr_user)


@pytest.fixture(scope="function")
def membership_factory(
    team: "Team", gr_user: "GRUser", membership_manager, team_manager, gr_um
) -> Callable:
    from generalresearch.managers.gr.team import MembershipManager

    membership_manager: MembershipManager

    def _create_membership(**kwargs):
        _team = kwargs.get("team", team_manager.create_dummy())
        _gr_user = kwargs.get("gr_user", gr_um.create_dummy())

        return membership_manager.create(team=_team, gr_user=_gr_user)

    return _create_membership


@pytest.fixture(scope="function")
def audit_log(audit_log_manager, user) -> "AuditLog":
    from generalresearch.managers.thl.userhealth import AuditLogManager

    audit_log_manager: AuditLogManager
    return audit_log_manager.create_dummy(user_id=user.user_id)


@pytest.fixture(scope="function")
def audit_log_factory(audit_log_manager) -> Callable:
    from generalresearch.managers.thl.userhealth import AuditLogManager

    audit_log_manager: AuditLogManager

    def _create_audit_log(
        user_id: PositiveInt,
        level: Optional["AuditLogLevel"] = None,
        event_type: Optional[str] = None,
        event_msg: Optional[str] = None,
        event_value: Optional[float] = None,
    ):
        return audit_log_manager.create_dummy(
            user_id=user_id,
            level=level,
            event_type=event_type,
            event_msg=event_msg,
            event_value=event_value,
        )

    return _create_audit_log


@pytest.fixture(scope="function")
def ip_geoname(ip_geoname_manager) -> "IPGeoname":
    from generalresearch.managers.thl.ipinfo import IPGeonameManager

    ip_geoname_manager: IPGeonameManager
    return ip_geoname_manager.create_dummy()


@pytest.fixture(scope="function")
def ip_information(ip_information_manager, ip_geoname) -> "IPInformation":
    from generalresearch.managers.thl.ipinfo import IPInformationManager

    ip_information_manager: IPInformationManager
    return ip_information_manager.create_dummy(
        geoname_id=ip_geoname.geoname_id, country_iso=ip_geoname.country_iso
    )


@pytest.fixture(scope="function")
def ip_information_factory(ip_information_manager) -> Callable:
    from generalresearch.managers.thl.ipinfo import IPInformationManager

    ip_information_manager: IPInformationManager

    def _create_ip_info(ip: str, geoname: "IPGeoname", **kwargs):
        return ip_information_manager.create_dummy(
            ip=ip,
            geoname_id=geoname.geoname_id,
            country_iso=geoname.country_iso,
            **kwargs,
        )

    return _create_ip_info


@pytest.fixture(scope="function")
def ip_record(ip_record_manager, ip_geoname, user) -> "IPRecord":
    from generalresearch.managers.thl.userhealth import IPRecordManager

    ip_record_manager: IPRecordManager

    return ip_record_manager.create_dummy(user_id=user.user_id)


@pytest.fixture(scope="function")
def ip_record_factory(ip_record_manager, user) -> Callable:
    from generalresearch.managers.thl.userhealth import IPRecordManager

    ip_record_manager: IPRecordManager

    def _create_ip_record(user_id: PositiveInt, ip: Optional[str] = None):
        return ip_record_manager.create_dummy(user_id=user_id, ip=ip)

    return _create_ip_record


@pytest.fixture(scope="session")
def buyer(buyer_manager) -> Buyer:
    buyer_code = uuid4().hex
    buyer_manager.bulk_get_or_create(source=Source.TESTING, codes=[buyer_code])
    b = Buyer(
        source=Source.TESTING, code=buyer_code, label=f"test-buyer-{buyer_code[:8]}"
    )
    buyer_manager.update(b)
    return b


@pytest.fixture(scope="session")
def buyer_factory(buyer_manager) -> Callable:

    def inner():
        return buyer_manager.bulk_get_or_create(
            source=Source.TESTING, codes=[uuid4().hex]
        )[0]

    return inner


@pytest.fixture(scope="session")
def survey(survey_manager, buyer) -> Survey:
    s = Survey(source=Source.TESTING, survey_id=uuid4().hex, buyer_code=buyer.code)
    survey_manager.create_bulk([s])
    return s


@pytest.fixture(scope="session")
def survey_factory(survey_manager, buyer_factory) -> Callable:

    def inner(buyer: Optional[Buyer] = None) -> Survey:
        buyer = buyer or buyer_factory()
        s = Survey(
            source=Source.TESTING,
            survey_id=uuid4().hex,
            buyer_code=buyer.code,
            buyer_id=buyer.id,
        )
        survey_manager.create_bulk([s])
        return s

    return inner
