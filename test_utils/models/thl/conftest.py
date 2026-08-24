from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from decimal import ROUND_DOWN, Decimal
from random import choice as rand_choice
from random import choice as rchoice
from random import randint, random
from typing import Any
from uuid import uuid4

import faker
import pytest
from pydantic import PositiveInt

from generalresearch.managers.thl.ipinfo import IPGeonameManager, IPInformationManager
from generalresearch.managers.thl.payout import UserPayoutEventManager
from generalresearch.managers.thl.product import ProductManager
from generalresearch.managers.thl.session import SessionManager
from generalresearch.managers.thl.user_manager.user_manager import UserManager
from generalresearch.managers.thl.userhealth import AuditLogManager, IPRecordManager
from generalresearch.managers.thl.wall import WallManager
from generalresearch.models import DeviceType
from generalresearch.models.custom_types import (
    AwareDatetimeISO,
    IPvAnyAddressStr,
    UUIDStr,
)
from generalresearch.models.legacy.bucket import Bucket
from generalresearch.models.thl.definitions import (
    PayoutStatus,
)
from generalresearch.models.thl.ipinfo import IPGeoname, IPInformation, UserType
from generalresearch.models.thl.payout import UserPayoutEvent
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
    Source,
    Status,
    Wall,
)
from generalresearch.models.thl.user import User
from generalresearch.models.thl.user_iphistory import IPRecord
from generalresearch.models.thl.userhealth import AuditLog, AuditLogLevel
from generalresearch.models.thl.wallet import PayoutType
from generalresearch.models.thl.wallet.cashout_method import CashMailOrderData

fake = faker.Faker()


@pytest.fixture
def wall_status() -> Status:
    return Status.COMPLETE


@pytest.fixture
def user_factory(user_manager: UserManager) -> Callable[..., User]:

    def _inner(
        # --- Create dummy "optional" --- #
        product_user_id: str | None = None,
        # --- Optional --- #
        product_id: UUIDStr | None = None,
        product: Product | None = None,
        created: datetime | None = None,
    ) -> User:

        product_user_id = product_user_id or uuid4().hex

        return user_manager.create_user(
            product_user_id=product_user_id,
            product_id=product_id,
            product=product,
            created=created,
        )

    return _inner


@pytest.fixture
def wall_factory(
    wall_manager: WallManager, session_factory: Session
) -> Callable[..., Wall]:

    def _inner(
        session_id: int | None = None,
        user_id: int | None = None,
        started: datetime | None = None,
        source: Source | None = None,
        req_survey_id: str | None = None,
        req_cpi: Decimal | None = None,
        buyer_id: str | None = None,
        uuid_id: str | None = None,
    ):
        """To be used in tests, where we don't care about certain fields"""

        user_id = user_id or fake.random_int(min=1, max=2_147_483_648)
        started = started or fake.date_time_between(
            start_date=datetime(year=1900, month=1, day=1, tzinfo=UTC),
            end_date=datetime.now(tz=UTC),
            tzinfo=UTC,
        )

        if session_id is None:
            # session = SessionManager(pg_config=self.pg_config).create_dummy(
            #     started=started
            # )
            session = session_factory()
            session_id = session.id

        source = source or rchoice(list(Source))
        req_survey_id = req_survey_id or uuid4().hex
        req_cpi = req_cpi or Decimal(fake.random_int(min=1, max=150) / 100).quantize(
            Decimal(".01"), rounding=ROUND_DOWN
        )

        return wall_manager.create(
            session_id=session_id,
            user_id=user_id,
            started=started,
            source=source,
            req_survey_id=req_survey_id,
            req_cpi=req_cpi,
            buyer_id=buyer_id,
            uuid_id=uuid_id,
        )

    return _inner


@pytest.fixture
def product_factory(product_manager: ProductManager) -> Callable[..., Product]:

    def _inner(
        product_id: UUIDStr | None = None,
        team_id: UUIDStr | None = None,
        business_id: UUIDStr | None = None,
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
        team_id = team_id if team_id else uuid4().hex
        name = name if name else f"name-{product_id[:12]}"
        redirect_url = redirect_url if redirect_url else "https://www.example.com/"

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

    return _inner


@pytest.fixture
def session_factory(session_manager: SessionManager):

    def _inner(
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
        """To be used in tests, where we don't care about certain fields"""
        started = started or fake.date_time_between(
            start_date=datetime(year=1900, month=1, day=1, tzinfo=UTC),
            end_date=datetime(year=2000, month=1, day=1, tzinfo=UTC),
            tzinfo=UTC,
        )
        user = user or User(
            user_id=fake.random_int(min=1, max=2_147_483_648), uuid=uuid4().hex
        )

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

    return _inner


@pytest.fixture
def ipgeoname_factory(ipgeoname_manager: IPGeonameManager) -> Callable[..., IPGeoname]:

    def _inner(
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

        return ipgeoname_manager.create(
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

    return _inner


def ipinformation_factory(
    ipinformation_manager: IPInformationManager,
) -> Callable[..., IPInformation]:

    def _inner(
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
        user_type: UserType | None = None,
        postal_code: str | None = None,
        latitude: Decimal | None = None,
        longitude: Decimal | None = None,
        accuracy_radius: int | None = None,
    ) -> IPInformation:

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

    return _inner


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


@pytest.fixture
def iprecord_factory(iprecord_manager: IPRecordManager) -> Callable[..., IPRecord]:

    def _inner(
        user_id: PositiveInt,
        ip: IPvAnyAddressStr | None = None,
        forwarded_ip1: IPvAnyAddressStr | None = None,
        forwarded_ip2: IPvAnyAddressStr | None = None,
        forwarded_ip3: IPvAnyAddressStr | None = None,
        forwarded_ip4: IPvAnyAddressStr | None = None,
        forwarded_ip5: IPvAnyAddressStr | None = None,
        forwarded_ip6: IPvAnyAddressStr | None = None,
    ) -> IPRecord:
        return iprecord_manager.create(
            user_id=user_id,
            ip=ip or fake.ipv4_public(),
            forwarded_ip1=(forwarded_ip1 or fake.ipv4_public()),
            forwarded_ip2=(forwarded_ip2 or fake.ipv6() if random() < 0.5 else None),
            forwarded_ip3=(
                forwarded_ip3 or fake.ipv4_public() if random() < 0.25 else None
            ),
            forwarded_ip4=forwarded_ip4,
            forwarded_ip5=forwarded_ip5,
            forwarded_ip6=forwarded_ip6,
        )

    return _inner


# class AuditLogManager(PostgresManager):


@pytest.fixture
def auditlog_factory(audit_log_manager: AuditLogManager):

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
            level=level or rchoice(list(AuditLogLevel)),
            event_type=event_type or rchoice(list(event_types)),
            event_msg=event_msg,
            event_value=event_value,
        )

    return _inner
