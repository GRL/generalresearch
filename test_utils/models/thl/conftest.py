from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from decimal import ROUND_DOWN, Decimal
from random import choice as rand_choice
from random import choice as randchoice
from random import randint, random
from typing import TYPE_CHECKING, Any
from uuid import uuid4

import faker
import pytest
from grip_client.enums import AccessType
from pydantic import PositiveInt

from generalresearch.managers.thl.payout import UserPayoutEventManager
from generalresearch.models.custom_types import (
    AwareDatetimeISO,
    IPvAnyAddressStr,
    UUIDStr,
)
from generalresearch.models.thl.definitions import (
    WALL_ALLOWED_STATUS_STATUS_CODE,
    PayoutStatus,
)
from generalresearch.models.thl.payout import UserPayoutEvent
from generalresearch.models.thl.session import (
    Source,
    Status,
)
from generalresearch.models.thl.user import User
from generalresearch.models.thl.user_iphistory import IPRecord
from generalresearch.models.thl.userhealth import AuditLogLevel
from generalresearch.models.thl.wallet.definitions import PayoutType
from generalresearch.pg_helper import PostgresConfig

if TYPE_CHECKING:
    from generalresearch.currency import USDCent
    from generalresearch.managers.thl.ipinfo import (
        IPGeonameManager,
        IPInformationManager,
    )
    from generalresearch.managers.thl.ledger_manager.thl_ledger import ThlLedgerManager
    from generalresearch.managers.thl.payout import (
        BrokerageProductPayoutEventManager,
    )
    from generalresearch.managers.thl.product import ProductManager
    from generalresearch.managers.thl.session import SessionManager
    from generalresearch.managers.thl.user_manager.user_manager import UserManager
    from generalresearch.managers.thl.userhealth import AuditLogManager, IPRecordManager
    from generalresearch.managers.thl.wall import WallManager
    from generalresearch.models.custom_types import AwareDatetime
    from generalresearch.models.definitions import DeviceType
    from generalresearch.models.gr.business import Business
    from generalresearch.models.gr.team import Team
    from generalresearch.models.legacy.bucket import Bucket
    from generalresearch.models.thl.ipinfo import IPGeoname, IPInformation
    from generalresearch.models.thl.payout import BrokerageProductPayoutEvent
    from generalresearch.models.thl.product import (
        PayoutConfig,
        Product,
        ProfilingConfig,
        SessionConfig,
        SourcesConfig,
        SupplyConfig,
        UserCreateConfig,
        UserHealthConfig,
        UserWalletConfig,
    )
    from generalresearch.models.thl.session import (
        Session,
        Wall,
    )
    from generalresearch.models.thl.user_iphistory import IPRecord
    from generalresearch.models.thl.userhealth import AuditLog
    from generalresearch.models.thl.wallet.cashout_method import CashMailOrderData

fake = faker.Faker()

# --- Wall ---


# from generalresearch.models.thl.task_status import StatusCode1
# # thl_session.append_wall_event(wall)
# wall.finish(
#     finished=wall.started + timedelta(seconds=randint(a=60 * 2, b=60 * 10)),
#     status=Status.COMPLETE,
#     status_code_1=StatusCode1.COMPLETE,
# )
# return wall


@pytest.fixture
def wall_factory(
    wall_manager: WallManager,
    session_factory: Callable[..., Session],
    session_manager: SessionManager,
) -> Callable[..., Wall]:

    def _inner(
        wall_status: Status = Status.FAIL,
        save: bool = True,
        session: Session | None = None,
        session_id: PositiveInt | None = None,
        user_id: int | None = None,
        started: datetime | None = None,
        source: Source | None = None,
        req_survey_id: str | None = None,
        req_cpi: Decimal | None = None,
        buyer_id: str | None = None,
        uuid_id: str | None = None,
    ) -> Wall:
        """To be used in tests, where we don't care about certain fields"""

        if save:
            user_id = user_id or fake.random_int(min=1, max=2_147_483_648)
            _wall_started = started or fake.date_time_between(
                start_date=datetime(year=1900, month=1, day=1, tzinfo=UTC),
                end_date=datetime.now(tz=UTC),
                tzinfo=UTC,
            )

            if session:
                # If an existing Session was provided, we want to do some
                # additional validation.

                if session.wall_events:
                    # Subsequent Wall events
                    _last_wall = session.wall_events[-1]
                    assert not _last_wall.finished, (
                        "Can't add new Walls until prior finishes"
                    )
                    _wall_started = _last_wall.started + timedelta(milliseconds=1)
                else:
                    # First Wall Event in a session
                    _wall_started = session.started + timedelta(milliseconds=1)
            else:
                # If a Session was NOT provided, either (1) try to retrieve it
                # from an optionally provided session_id int, or (2) proceed
                # forward and make one
                session = (
                    session_manager.get_from_id(session_id=session_id)
                    if session_id
                    else None
                ) or session_factory(save=True, user_id=user_id)

            assert session, "Wall factory requires Session"

            source = source or rand_choice(list(Source))
            req_survey_id = req_survey_id or uuid4().hex
            req_cpi = req_cpi or Decimal(
                fake.random_int(min=1, max=150) / 100
            ).quantize(Decimal(".01"), rounding=ROUND_DOWN)

            w = wall_manager.create(
                session_id=session.id,
                user_id=session.user_id,
                started=_wall_started,
                source=source,
                req_survey_id=req_survey_id,
                req_cpi=req_cpi,
                buyer_id=buyer_id,
                uuid_id=uuid_id,
            )

            _status_code_options = list(
                WALL_ALLOWED_STATUS_STATUS_CODE.get(wall_status, {})
            )
            w.finish(
                finished=w.started + timedelta(seconds=randint(a=60 * 2, b=60 * 10)),
                status=wall_status,
                status_code_1=rand_choice(_status_code_options),
            )

            session.append_wall_event(w=w)

            return w

        else:
            raise ValueError("Unsaved Wall not yet supported")

    return _inner


@pytest.fixture
def wall(wall_factory: Callable[..., Wall]) -> Wall:
    return wall_factory(save=True)


@pytest.fixture()
def unsaved_wall(wall_factory: Callable[..., Wall]) -> Wall:
    return wall_factory(save=False)


# --- Wall: Enum(s) ---


@pytest.fixture
def wall_status() -> Status:
    return Status.COMPLETE


# --- Session ---


@pytest.fixture
def session_factory(session_manager: SessionManager, user_factory: Callable[..., User]):

    def _inner(
        save: bool = True,
        # -- Create Dummy "optional" -- #
        started: datetime | None = None,
        user: User | None = None,
        # -- Optional -- #
        country_iso: str | None = None,
        device_type: DeviceType | None = None,
        ip: str | None = None,
        bucket: Bucket | None = None,
        url_metadata: dict[str, str] | None = None,
        uuid_id: str | None = None,
    ) -> Session:

        if save:
            """To be used in tests, where we don't care about certain fields"""
            started = started or fake.date_time_between(
                start_date=datetime(year=1900, month=1, day=1, tzinfo=UTC),
                end_date=datetime(year=2000, month=1, day=1, tzinfo=UTC),
                tzinfo=UTC,
            )
            user = user or user_factory(save=True)
            assert user.user_id, "Provided User must be saved to the database"

            return session_manager.create(
                started=started,
                user=user,
                country_iso=country_iso,
                device_type=device_type,
                ip=ip,
                bucket=bucket,
                url_metadata=url_metadata,
                uuid_id=uuid_id,
            )
        else:
            # user = User(
            #     user_id=fake.random_int(min=1, max=2_147_483_648), uuid=uuid4().hex
            # )
            raise ValueError("Unsaved Session not yet supported")

    return _inner


@pytest.fixture()
def session(session_factory: Callable[..., Session]) -> Session:
    return session_factory(save=True)


@pytest.fixture
def session_w_wall(
    user: User,
    session: Session,
    wall_factory: Callable[..., Wall],
) -> Session:

    wall: Wall = wall_factory(
        session_id=session.id,
        user_id=session.user_id,
        started=session.started,
    )
    session.append_wall_event(w=wall)

    return session


@pytest.fixture()
def unsaved_session(session_factory: Callable[..., Session]) -> Session:
    return session_factory(save=False)


@pytest.fixture
def session_w_wall_factory(
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
    session_w_wall_factory: Callable[..., Session],
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
        s: Session = session_w_wall_factory(
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


# --- Product ---


@pytest.fixture()
def product_factory(product_manager: ProductManager) -> Callable[..., Product]:

    def _inner(
        save: bool = True,
        team: Team | None = None,
        team_id: UUIDStr | None = None,
        business: Business | None = None,
        business_id: UUIDStr | None = None,
        product_id: UUIDStr | None = None,
        name: str | None = None,
        redirect_url: str | None = None,
        harmonizer_domain: str | None = None,
        commission_pct: Decimal = Decimal("0.05000"),
        sources_config: SourcesConfig | SupplyConfig | None = None,
        payout_config: PayoutConfig | None = None,
        session_config: SessionConfig | None = None,
        profiling_config: ProfilingConfig | None = None,
        user_wallet_config: UserWalletConfig | None = None,
        user_create_config: UserCreateConfig | None = None,
        user_health_config: UserHealthConfig | None = None,
    ) -> Product:
        """To be used in tests, where we don't care about certain fields"""

        product_id = product_id if product_id else uuid4().hex

        team_id = (team.uuid if team else None) or team_id or uuid4().hex
        business_id = (
            (business.uuid if business else None) or business_id or uuid4().hex
        )

        name = name if name else f"name-{product_id[:12]}"
        redirect_url = redirect_url if redirect_url else "https://www.example.com/"

        if save:
            return product_manager.create(
                product_id=product_id,
                team_id=team_id,
                business_id=business_id,
                name=name,
                redirect_url=redirect_url,
                harmonizer_domain=harmonizer_domain,
                commission_pct=commission_pct,
                sources_config=sources_config,
                payout_config=payout_config,
                session_config=session_config,
                profiling_config=profiling_config,
                user_wallet_config=user_wallet_config,
                user_create_config=user_create_config,
                user_health_config=user_health_config,
            )
        else:
            raise ValueError("Unsaved Product not yet supported")

    return _inner


@pytest.fixture()
def product(product_factory: Callable[..., Product]) -> Product:
    return product_factory(save=True)


@pytest.fixture()
def unsaved_product(product_factory: Callable[..., Product]) -> Product:
    return product_factory(save=False)


# --- IP Geoname ---


@pytest.fixture
def ip_geoname_factory(
    ip_geoname_manager: IPGeonameManager,
) -> Callable[..., IPGeoname]:

    def _inner(
        save: bool,
        geoname_id: PositiveInt | None = None,
        continent_code: str | None = None,
        continent_name: str | None = None,
        country_iso: str | None = None,
        country_name: str | None = None,
        subdivision_1_iso: str | None = None,
        subdivision_1_name: str | None = None,
        subdivision_2_iso: str | None = None,
        subdivision_2_name: str | None = None,
        city_name: str | None = None,
        metro_code: int | None = None,
        time_zone: str | None = None,
        is_in_european_union: bool | None = None,
    ) -> IPGeoname:
        if save:
            return ip_geoname_manager.create(
                geoname_id=geoname_id or randint(1, 999_999_999),
                continent_code=continent_code or "na",
                continent_name=continent_name or "North America",
                country_iso=country_iso or "us",
                country_name=country_name or "United States",
                subdivision_1_iso=subdivision_1_iso or "fl",
                subdivision_1_name=subdivision_1_name or "Florida",
                subdivision_2_iso=subdivision_2_iso,
                subdivision_2_name=subdivision_2_name,
                city_name=city_name,
                metro_code=metro_code,
                time_zone=time_zone,
                is_in_european_union=is_in_european_union,
            )
        else:
            raise ValueError("Unsaved IPGeoname not yet supported")

    return _inner


@pytest.fixture()
def ip_geoname(ip_geoname_factory: Callable[..., IPGeoname]) -> IPGeoname:
    return ip_geoname_factory(save=True)


@pytest.fixture()
def unsaved_ip_geoname(ip_geoname_factory: Callable[..., IPGeoname]) -> IPGeoname:
    return ip_geoname_factory(save=True)


# --- IP Information ---


def ip_information_factory(
    ipinformation_manager: IPInformationManager,
) -> Callable[..., IPInformation]:

    def _inner(
        save: bool = True,
        ip: IPvAnyAddressStr | None = None,
        geoname_id: PositiveInt | None = None,
        country_iso: str | None = None,
        registered_country_iso: str | None = None,
        is_anonymous: bool | None = None,
        is_anonymous_vpn: bool | None = None,
        is_hosting_provider: bool | None = None,
        is_public_proxy: bool | None = None,
        is_tor_exit_node: bool | None = None,
        is_residential_proxy: bool | None = None,
        autonomous_system_number: PositiveInt | None = None,
        autonomous_system_organization: str | None = None,
        domain: str | None = None,
        isp: str | None = None,
        mobile_country_code: str | None = None,
        mobile_network_code: str | None = None,
        network: str | None = None,
        organization: str | None = None,
        static_ip_score: float | None = None,
        user_type: AccessType | None = None,
        postal_code: str | None = None,
        latitude: Decimal | None = None,
        longitude: Decimal | None = None,
        accuracy_radius: int | None = None,
    ) -> IPInformation:

        if save:
            return ipinformation_manager.create(
                ip=ip or fake.ipv4_public(),
                geoname_id=geoname_id,
                country_iso=country_iso or fake.country_code(),
                registered_country_iso=registered_country_iso,
                is_anonymous=is_anonymous,
                is_anonymous_vpn=is_anonymous_vpn,
                is_hosting_provider=is_hosting_provider,
                is_public_proxy=is_public_proxy,
                is_tor_exit_node=is_tor_exit_node,
                is_residential_proxy=is_residential_proxy,
                autonomous_system_number=autonomous_system_number,
                autonomous_system_organization=autonomous_system_organization,
                domain=domain,
                isp=isp,
                mobile_country_code=mobile_country_code,
                mobile_network_code=mobile_network_code,
                network=network,
                organization=organization,
                static_ip_score=static_ip_score,
                user_type=user_type,
                postal_code=postal_code,
                latitude=latitude,
                longitude=longitude,
                accuracy_radius=accuracy_radius,
            )
        else:
            raise ValueError("Unsaved IP Information not supported yet")

    return _inner


@pytest.fixture
def ip_information(
    ip_information_factory: Callable[..., IPInformation],
) -> IPInformation:
    return ip_information_factory(save=True)


@pytest.fixture()
def unsaved_ip_information(
    ip_information_factory: Callable[..., IPInformation],
) -> IPInformation:
    return ip_information_factory(save=False)


# --- IP Record ---


@pytest.fixture()
def ip_record_factory(ip_record_manager: IPRecordManager) -> Callable[..., IPRecord]:

    def _inner(
        user_id: PositiveInt,
        save: bool = True,
        ip: IPvAnyAddressStr | None = None,
        forwarded_ip1: IPvAnyAddressStr | None = None,
        forwarded_ip2: IPvAnyAddressStr | None = None,
        forwarded_ip3: IPvAnyAddressStr | None = None,
        forwarded_ip4: IPvAnyAddressStr | None = None,
        forwarded_ip5: IPvAnyAddressStr | None = None,
        forwarded_ip6: IPvAnyAddressStr | None = None,
    ) -> IPRecord:

        if save:
            return ip_record_manager.create(
                user_id=user_id,
                ip=ip or fake.ipv4_public(),
                forwarded_ip1=(forwarded_ip1 or fake.ipv4_public()),
                forwarded_ip2=(
                    forwarded_ip2 or fake.ipv6() if random() < 0.5 else None
                ),
                forwarded_ip3=(
                    forwarded_ip3 or fake.ipv4_public() if random() < 0.25 else None
                ),
                forwarded_ip4=forwarded_ip4,
                forwarded_ip5=forwarded_ip5,
                forwarded_ip6=forwarded_ip6,
            )
        else:
            raise ValueError("Unsaved IP Record not supported")

    return _inner


@pytest.fixture()
def ip_record(ip_record_factory: Callable[..., IPRecord]) -> IPRecord:
    return ip_record_factory(save=True)


@pytest.fixture()
def unsaved_ip_record(ip_record_factory: Callable[..., IPRecord]) -> IPRecord:
    return ip_record_factory(save=False)


# --- User ---


@pytest.fixture()
def user_factory(
    user_manager: UserManager,
    thl_web_rr: PostgresConfig,
    product_factory: Callable[..., Product],
) -> Callable[..., User]:

    def _inner(
        save: bool = True,
        # --- Create dummy "optional" --- #
        product_user_id: str | None = None,
        # --- Optional --- #
        product_id: UUIDStr | None = None,
        product: Product | None = None,
        created: datetime | None = None,
    ) -> User:
        if save:
            if product is None:
                product = product_factory()

            product_user_id = product_user_id or uuid4().hex

            u = user_manager.create_user(
                product_user_id=product_user_id,
                product_id=product_id,
                product=product,
                created=created,
            )

            u.prefetch_product(pg_config=thl_web_rr)
            return u

        else:
            raise ValueError("Unsaved User not supported")

    return _inner


@pytest.fixture()
def user(
    user_factory: Callable[..., User],
) -> User:
    return user_factory(save=True)


@pytest.fixture()
def unsaved_user(
    user_factory: Callable[..., User],
) -> User:
    return user_factory(save=False)


@pytest.fixture
def user_with_wallet(
    user_factory: Callable[..., User],
    product_user_wallet_yes: Product,
) -> User:
    # A user on a product with user wallet enabled, but they have no money
    return user_factory(save=True, product=product_user_wallet_yes)


@pytest.fixture
def user_with_wallet_amt(
    user_factory: Callable[..., User], product_amt_true: Product
) -> User:
    # A user on a product with user wallet enabled, on AMT, but they have no money
    return user_factory(save=True, product=product_amt_true)


# --- User Payout Event ---


@pytest.fixture
def user_payout_event_factory(
    user_payout_event_manager: UserPayoutEventManager,
) -> Callable[..., UserPayoutEvent]:

    def _inner(
        uuid: UUIDStr | None = None,
        debit_account_uuid: UUIDStr | None = None,
        account_reference_type: str | None = None,
        account_reference_uuid: UUIDStr | None = None,
        cashout_method_uuid: UUIDStr | None = None,
        description: str | None = None,
        created: AwareDatetimeISO | None = None,
        amount: PositiveInt | None = None,
        status: PayoutStatus | None = None,
        ext_ref_id: str | None = None,
        payout_type: PayoutType | None = None,
        request_data: dict[str, Any] | None = None,
        order_data: dict[str, Any] | CashMailOrderData | None = None,
    ) -> UserPayoutEvent:

        debit_account_uuid = debit_account_uuid or uuid4().hex
        cashout_method_uuid = cashout_method_uuid or uuid4().hex
        # account_reference_type = account_reference_type or f"acct-ref-{uuid4().hex}"
        # account_reference_uuid = account_reference_uuid or uuid4().hex
        # cashout_method_uuid = cashout_method_uuid or uuid4().hex
        amount = amount or randint(a=99, b=9_999)
        status = status or rand_choice(list(PayoutStatus))

        description = description or f"desc-{uuid4().hex[:12]}"
        # ext_ref_id = ext_ref_id or f"ext-ref-{uuid4().hex[:8]}"
        payout_type = payout_type or rand_choice(list(PayoutType))
        request_data = request_data or {}
        # order_data = order_data or None

        return user_payout_event_manager.create(
            uuid=uuid,
            debit_account_uuid=debit_account_uuid,
            account_reference_type=account_reference_type,
            account_reference_uuid=account_reference_uuid,
            cashout_method_uuid=cashout_method_uuid,
            description=description,
            created=created,
            amount=amount,
            status=status,
            ext_ref_id=ext_ref_id,
            payout_type=payout_type,
            request_data=request_data,
            order_data=order_data,
        )

    return _inner


@pytest.fixture()
def user_payout_event(
    user_payout_event_factory: Callable[..., UserPayoutEvent],
) -> UserPayoutEvent:
    return user_payout_event_factory(save=True)


@pytest.fixture()
def unsaved_user_payout_event(
    user_payout_event_factory: Callable[..., UserPayoutEvent],
) -> UserPayoutEvent:
    return user_payout_event_factory(save=True)


# -- Brokerage Product Payout Event


@pytest.fixture
def brokerage_product_payout_event_factory(
    thl_ledger_manager: ThlLedgerManager,
    brokerage_product_payout_event_manager: BrokerageProductPayoutEventManager,
    product_factory: Callable[..., Product],
) -> Callable[..., BrokerageProductPayoutEvent]:

    def _inner(
        product: Product | None = None,
        amount: USDCent | None = None,
        ext_ref_id: str | None = None,
        created: AwareDatetime | None = None,
    ) -> BrokerageProductPayoutEvent:
        from generalresearch.currency import USDCent

        product = product or product_factory()
        amount = amount or USDCent(randint(1, 99_99))

        return brokerage_product_payout_event_manager.create_bp_payout_event(
            thl_ledger_manager=thl_ledger_manager,
            product=product,
            amount=amount,
            ext_ref_id=ext_ref_id or uuid4().hex,
            created=created,
        )

    return _inner


# --- Audit Log Manager ---


@pytest.fixture()
def audit_log_factory(audit_log_manager: AuditLogManager) -> Callable[..., AuditLog]:

    def _inner(
        user_id: PositiveInt,
        level: AuditLogLevel | None = None,
        event_type: str | None = None,
        event_msg: str | None = None,
        event_value: float | None = None,
    ) -> AuditLog:

        event_types = {
            "offerwall-enter.blocked",
            "offerwall-enter.rate-limited",
            "offerwall-enter.url-modified",
        }

        return audit_log_manager.create(
            user_id=user_id,
            level=level or rand_choice(list(AuditLogLevel)),
            event_type=event_type or rand_choice(list(event_types)),
            event_msg=event_msg,
            event_value=event_value,
        )

    return _inner


@pytest.fixture()
def audit_log(auditlog_factory: Callable[..., AuditLog]) -> AuditLog:
    return auditlog_factory(save=True)


@pytest.fixture()
def unsaved_audit_log(auditlog_factory: Callable[..., AuditLog]) -> AuditLog:
    return auditlog_factory(save=False)


# --- ---


@pytest.fixture(scope="session")
def profiling_info_json() -> str:
    return (
        '[{"property_label": "hispanic", "cardinality": "*", "prop_type": "i", "country_iso": "us", '
        '"property_id": "05170ae296ab49178a075cab2a2073a6", "item_id": "7911ec1468b146ee870951f8ae9cbac1", '
        '"item_label": "panamanian", "gold_standard": 1, "options": [{"id": "c358c11e72c74fa2880358f1d4be85ab", '
        '"label": "not_hispanic"}, {"id": "b1d6c475770849bc8e0200054975dc9c", "label": "yes_hispanic"}, '
        '{"id": "bd1eb44495d84b029e107c188003c2bd", "label": "other_hispanic"}, '
        '{"id": "f290ad5e75bf4f4ea94dc847f57c1bd3", "label": "mexican"}, '
        '{"id": "49f50f2801bd415ea353063bfc02d252", "label": "puerto_rican"}, '
        '{"id": "dcbe005e522f4b10928773926601f8bf", "label": "cuban"}, '
        '{"id": "467ef8ddb7ac4edb88ba9ef817cbb7e9", "label": "salvadoran"}, '
        '{"id": "3c98e7250707403cba2f4dc7b877c963", "label": "dominican"}, '
        '{"id": "981ee77f6d6742609825ef54fea824a8", "label": "guatemalan"}, '
        '{"id": "81c8057b809245a7ae1b8a867ea6c91e", "label": "colombian"}, '
        '{"id": "513656d5f9e249fa955c3b527d483b93", "label": "honduran"}, '
        '{"id": "afc8cddd0c7b4581bea24ccd64db3446", "label": "ecuadorian"}, '
        '{"id": "61f34b36e80747a89d85e1eb17536f84", "label": "argentinian"}, '
        '{"id": "5330cfa681d44aa8ade3a6d0ea198e44", "label": "peruvian"}, '
        '{"id": "e7bceaffd76e486596205d8545019448", "label": "nicaraguan"}, '
        '{"id": "b7bbb2ebf8424714962e6c4f43275985", "label": "spanish"}, '
        '{"id": "8bf539785e7a487892a2f97e52b1932d", "label": "venezuelan"}, '
        '{"id": "7911ec1468b146ee870951f8ae9cbac1", "label": "panamanian"}], "category": [{"id": '
        '"4fd8381d5a1c4409ab007ca254ced084", "label": "Demographic", "path": "/Demographic", '
        '"adwords_vertical_id": null}]}, {"property_label": "ethnic_group", "cardinality": "*", "prop_type": '
        '"i", "country_iso": "us", "property_id": "15070958225d4132b7f6674fcfc979f6", "item_id": '
        '"64b7114cf08143949e3bcc3d00a5d8a0", "item_label": "other_ethnicity", "gold_standard": 1, "options": [{'
        '"id": "a72e97f4055e4014a22bee4632cbf573", "label": "caucasians"}, '
        '{"id": "4760353bc0654e46a928ba697b102735", "label": "black_or_african_american"}, '
        '{"id": "20ff0a2969fa4656bbda5c3e0874e63b", "label": "asian"}, '
        '{"id": "107e0a79e6b94b74926c44e70faf3793", "label": "native_hawaiian_or_other_pacific_islander"}, '
        '{"id": "900fa12691d5458c8665bf468f1c98c1", "label": "native_americans"}, '
        '{"id": "64b7114cf08143949e3bcc3d00a5d8a0", "label": "other_ethnicity"}], "category": [{"id": '
        '"4fd8381d5a1c4409ab007ca254ced084", "label": "Demographic", "path": "/Demographic", '
        '"adwords_vertical_id": null}]}, {"property_label": "educational_attainment", "cardinality": "?", '
        '"prop_type": "i", "country_iso": "us", "property_id": "2637783d4b2b4075b93e2a156e16e1d8", "item_id": '
        '"934e7b81d6744a1baa31bbc51f0965d5", "item_label": "other_education", "gold_standard": 1, "options": [{'
        '"id": "df35ef9e474b4bf9af520aa86630202d", "label": "3rd_grade_completion"}, '
        '{"id": "83763370a1064bd5ba76d1b68c4b8a23", "label": "8th_grade_completion"}, '
        '{"id": "f0c25a0670c340bc9250099dcce50957", "label": "not_high_school_graduate"}, '
        '{"id": "02ff74c872bd458983a83847e1a9f8fd", "label": "high_school_completion"}, '
        '{"id": "ba8beb807d56441f8fea9b490ed7561c", "label": "vocational_program_completion"}, '
        '{"id": "65373a5f348a410c923e079ddbb58e9b", "label": "some_college_completion"}, '
        '{"id": "2d15d96df85d4cc7b6f58911fdc8d5e2", "label": "associate_academic_degree_completion"}, '
        '{"id": "497b1fedec464151b063cd5367643ffa", "label": "bachelors_degree_completion"}, '
        '{"id": "295133068ac84424ae75e973dc9f2a78", "label": "some_graduate_completion"}, '
        '{"id": "e64f874faeff4062a5aa72ac483b4b9f", "label": "masters_degree_completion"}, '
        '{"id": "cbaec19a636d476385fb8e7842b044f5", "label": "doctorate_degree_completion"}, '
        '{"id": "934e7b81d6744a1baa31bbc51f0965d5", "label": "other_education"}], "category": [{"id": '
        '"4fd8381d5a1c4409ab007ca254ced084", "label": "Demographic", "path": "/Demographic", '
        '"adwords_vertical_id": null}]}, {"property_label": "household_spoken_language", "cardinality": "*", '
        '"prop_type": "i", "country_iso": "us", "property_id": "5a844571073d482a96853a0594859a51", "item_id": '
        '"62b39c1de141422896ad4ab3c4318209", "item_label": "dut", "gold_standard": 1, "options": [{"id": '
        '"f65cd57b79d14f0f8460761ce41ec173", "label": "ara"}, {"id": "6d49de1f8f394216821310abd29392d9", '
        '"label": "zho"}, {"id": "be6dc23c2bf34c3f81e96ddace22800d", "label": "eng"}, '
        '{"id": "ddc81f28752d47a3b1c1f3b8b01a9b07", "label": "fre"}, {"id": "2dbb67b29bd34e0eb630b1b8385542ca", '
        '"label": "ger"}, {"id": "a747f96952fc4b9d97edeeee5120091b", "label": "hat"}, '
        '{"id": "7144b04a3219433baac86273677551fa", "label": "hin"}, {"id": "e07ff3e82c7149eaab7ea2b39ee6a6dc", '
        '"label": "ita"}, {"id": "b681eff81975432ebfb9f5cc22dedaa3", "label": "jpn"}, '
        '{"id": "5cb20440a8f64c9ca62fb49c1e80cdef", "label": "kor"}, {"id": "171c4b77d4204bc6ac0c2b81e38a10ff", '
        '"label": "pan"}, {"id": "8c3ec18e6b6c4a55a00dd6052e8e84fb", "label": "pol"}, '
        '{"id": "3ce074d81d384dd5b96f1fb48f87bf01", "label": "por"}, {"id": "6138dc951990458fa88a666f6ddd907b", '
        '"label": "rus"}, {"id": "e66e5ecc07df4ebaa546e0b436f034bd", "label": "spa"}, '
        '{"id": "5a981b3d2f0d402a96dd2d0392ec2fcb", "label": "tgl"}, {"id": "b446251bd211403487806c4d0a904981", '
        '"label": "vie"}, {"id": "92fb3ee337374e2db875fb23f52eed46", "label": "xxx"}, '
        '{"id": "8b1f590f12f24cc1924d7bdcbe82081e", "label": "ind"}, {"id": "bf3f4be556a34ff4b836420149fd2037", '
        '"label": "tur"}, {"id": "87ca815c43ba4e7f98cbca98821aa508", "label": "zul"}, '
        '{"id": "0adbf915a7a64d67a87bb3ce5d39ca54", "label": "may"}, {"id": "62b39c1de141422896ad4ab3c4318209", '
        '"label": "dut"}], "category": [{"id": "4fd8381d5a1c4409ab007ca254ced084", "label": "Demographic", '
        '"path": "/Demographic", "adwords_vertical_id": null}]}, {"property_label": "gender", "cardinality": '
        '"?", "prop_type": "i", "country_iso": "us", "property_id": "73175402104741549f21de2071556cd7", '
        '"item_id": "093593e316344cd3a0ac73669fca8048", "item_label": "other_gender", "gold_standard": 1, '
        '"options": [{"id": "b9fc5ea07f3a4252a792fd4a49e7b52b", "label": "male"}, '
        '{"id": "9fdb8e5e18474a0b84a0262c21e17b56", "label": "female"}, '
        '{"id": "093593e316344cd3a0ac73669fca8048", "label": "other_gender"}], "category": [{"id": '
        '"4fd8381d5a1c4409ab007ca254ced084", "label": "Demographic", "path": "/Demographic", '
        '"adwords_vertical_id": null}]}, {"property_label": "age_in_years", "cardinality": "?", "prop_type": '
        '"n", "country_iso": "us", "property_id": "94f7379437874076b345d76642d4ce6d", "item_id": null, '
        '"item_label": null, "gold_standard": 1, "category": [{"id": "4fd8381d5a1c4409ab007ca254ced084", '
        '"label": "Demographic", "path": "/Demographic", "adwords_vertical_id": null}]}, {"property_label": '
        '"children_age_gender", "cardinality": "*", "prop_type": "i", "country_iso": "us", "property_id": '
        '"e926142fcea94b9cbbe13dc7891e1e7f", "item_id": "b7b8074e95334b008e8958ccb0a204f1", "item_label": '
        '"female_18", "gold_standard": 1, "options": [{"id": "16a6448ec24c48d4993d78ebee33f9b4", '
        '"label": "male_under_1"}, {"id": "809c04cb2e3b4a3bbd8077ab62cdc220", "label": "female_under_1"}, '
        '{"id": "295e05bb6a0843bc998890b24c99841e", "label": "no_children"}, '
        '{"id": "142cb948d98c4ae8b0ef2ef10978e023", "label": "male_0"}, '
        '{"id": "5a5c1b0e9abc48a98b3bc5f817d6e9d0", "label": "male_1"}, '
        '{"id": "286b1a9afb884bdfb676dbb855479d1e", "label": "male_2"}, '
        '{"id": "942ca3cda699453093df8cbabb890607", "label": "male_3"}, '
        '{"id": "995818d432f643ec8dd17e0809b24b56", "label": "male_4"}, '
        '{"id": "f38f8b57f25f4cdea0f270297a1e7a5c", "label": "male_5"}, '
        '{"id": "975df709e6d140d1a470db35023c432d", "label": "male_6"}, '
        '{"id": "f60bd89bbe0f4e92b90bccbc500467c2", "label": "male_7"}, '
        '{"id": "6714ceb3ed5042c0b605f00b06814207", "label": "male_8"}, '
        '{"id": "c03c2f8271d443cf9df380e84b4dea4c", "label": "male_9"}, '
        '{"id": "11690ee0f5a54cb794f7ddd010d74fa2", "label": "male_10"}, '
        '{"id": "17bef9a9d14b4197b2c5609fa94b0642", "label": "male_11"}, '
        '{"id": "e79c8338fe28454f89ccc78daf6f409a", "label": "male_12"}, '
        '{"id": "3a4f87acb3fa41f4ae08dfe2858238c1", "label": "male_13"}, '
        '{"id": "36ffb79d8b7840a7a8cb8d63bbc8df59", "label": "male_14"}, '
        '{"id": "1401a508f9664347aee927f6ec5b0a40", "label": "male_15"}, '
        '{"id": "6e0943c5ec4a4f75869eb195e3eafa50", "label": "male_16"}, '
        '{"id": "47d4b27b7b5242758a9fff13d3d324cf", "label": "male_17"}, '
        '{"id": "9ce886459dd44c9395eb77e1386ab181", "label": "female_0"}, '
        '{"id": "6499ccbf990d4be5b686aec1c7353fd8", "label": "female_1"}, '
        '{"id": "d85ceaa39f6d492abfc8da49acfd14f2", "label": "female_2"}, '
        '{"id": "18edb45c138e451d8cb428aefbb80f9c", "label": "female_3"}, '
        '{"id": "bac6f006ed9f4ccf85f48e91e99fdfd1", "label": "female_4"}, '
        '{"id": "5a6a1a8ad00c4ce8be52dcb267b034ff", "label": "female_5"}, '
        '{"id": "6bff0acbf6364c94ad89507bcd5f4f45", "label": "female_6"}, '
        '{"id": "d0d56a0a6b6f4516a366a2ce139b4411", "label": "female_7"}, '
        '{"id": "bda6028468044b659843e2bef4db2175", "label": "female_8"}, '
        '{"id": "dbb6d50325464032b456357b1a6e5e9c", "label": "female_9"}, '
        '{"id": "b87a93d7dc1348edac5e771684d63fb8", "label": "female_10"}, '
        '{"id": "11449d0d98f14e27ba47de40b18921d7", "label": "female_11"}, '
        '{"id": "16156501e97b4263962cbbb743840292", "label": "female_12"}, '
        '{"id": "04ee971c89a345cc8141a45bce96050c", "label": "female_13"}, '
        '{"id": "e818d310bfbc4faba4355e5d2ed49d4f", "label": "female_14"}, '
        '{"id": "440d25e078924ba0973163153c417ed6", "label": "female_15"}, '
        '{"id": "78ff804cc9b441c5a524bd91e3d1f8bf", "label": "female_16"}, '
        '{"id": "4b04d804d7d84786b2b1c22e4ed440f5", "label": "female_17"}, '
        '{"id": "28bc848cd3ff44c3893c76bfc9bc0c4e", "label": "male_18"}, '
        '{"id": "b7b8074e95334b008e8958ccb0a204f1", "label": "female_18"}], "category": [{"id": '
        '"e18ba6e9d51e482cbb19acf2e6f505ce", "label": "Parenting", "path": "/People & Society/Family & '
        'Relationships/Family/Parenting", "adwords_vertical_id": "58"}]}, {"property_label": "home_postal_code", '
        '"cardinality": "?", "prop_type": "x", "country_iso": "us", "property_id": '
        '"f3b32ebe78014fbeb1ed6ff77d6338bf", "item_id": null, "item_label": null, "gold_standard": 1, '
        '"category": [{"id": "4fd8381d5a1c4409ab007ca254ced084", "label": "Demographic", "path": "/Demographic", '
        '"adwords_vertical_id": null}]}, {"property_label": "household_income", "cardinality": "?", "prop_type": '
        '"n", "country_iso": "us", "property_id": "ff5b1d4501d5478f98de8c90ef996ac1", "item_id": null, '
        '"item_label": null, "gold_standard": 1, "category": [{"id": "4fd8381d5a1c4409ab007ca254ced084", '
        '"label": "Demographic", "path": "/Demographic", "adwords_vertical_id": null}]}]'
    )


@pytest.fixture(scope="session")
def profiling_user_info_json() -> str:
    return (
        '{"user_profile_knowledge": [], "marketplace_profile_knowledge": [{"source": "d", "question_id": '
        '"1", "answer": ["1"], "created": "2023-11-07T16:41:05.234096Z"}, {"source": "pr", '
        '"question_id": "3", "answer": ["1"], "created": "2023-11-07T16:41:05.234096Z"}, {"source": '
        '"h", "question_id": "60", "answer": ["58"], "created": "2023-11-07T16:41:05.234096Z"}, '
        '{"source": "c", "question_id": "43", "answer": ["1"], "created": "2023-11-07T16:41:05.234096Z"}, '
        '{"source": "s", "question_id": "211", "answer": ["111"], "created": '
        '"2023-11-07T16:41:05.234096Z"}, {"source": "s", "question_id": "1843", "answer": ["111"], '
        '"created": "2023-11-07T16:41:05.234096Z"}, {"source": "h", "question_id": "13959", "answer": ['
        '"244155"], "created": "2023-11-07T16:41:05.234096Z"}, {"source": "c", "question_id": "33092", '
        '"answer": ["1"], "created": "2023-11-07T16:41:05.234096Z"}, {"source": "c", "question_id": "gender", '
        '"answer": ["10682"], "created": "2023-11-07T16:41:05.234096Z"}, {"source": "e", "question_id": '
        '"gender", "answer": ["male"], "created": "2023-11-07T16:41:05.234096Z"}, {"source": "f", '
        '"question_id": "gender", "answer": ["male"], "created": "2023-11-07T16:41:05.234096Z"}, {"source": '
        '"i", "question_id": "gender", "answer": ["1"], "created": "2023-11-07T16:41:05.234096Z"}, '
        '{"source": "c", "question_id": "137510", "answer": ["1"], "created": "2023-11-07T16:41:05.234096Z"}, '
        '{"source": "m", "question_id": "gender", "answer": ["1"], "created": '
        '"2023-11-07T16:41:05.234096Z"}, {"source": "o", "question_id": "gender", "answer": ["male"], '
        '"created": "2023-11-07T16:41:05.234096Z"}, {"source": "c", "question_id": "gender_plus", "answer": ['
        '"7657644"], "created": "2023-11-07T16:41:05.234096Z"}, {"source": "i", "question_id": '
        '"gender_plus", "answer": ["1"], "created": "2023-11-07T16:41:05.234096Z"}, {"source": "c", '
        '"question_id": "income_level", "answer": ["9071"], "created": "2023-11-07T16:41:05.234096Z"}]}'
    )
