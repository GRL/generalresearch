from __future__ import annotations

from collections.abc import Callable
from uuid import uuid4

import pytest
from pydantic import PositiveInt
from pydantic_extra_types.phone_numbers import PhoneNumber

from generalresearch.managers.gr.authentication import GRTokenManager, GRUserManager
from generalresearch.managers.gr.business import (
    BusinessAddressManager,
    BusinessBankAccountManager,
    BusinessManager,
)
from generalresearch.managers.gr.team import MembershipManager, TeamManager
from generalresearch.models.custom_types import UUIDStr
from generalresearch.models.gr.authentication import GRToken, GRUser
from generalresearch.models.gr.business import (
    Business,
    BusinessAddress,
    BusinessBankAccount,
    BusinessType,
    TransferMethod,
)
from generalresearch.models.gr.team import Membership, Team
from generalresearch.pg_helper import PostgresConfig
from generalresearch.redis_helper import RedisConfig

# --- Static ---


# --- Factory / Database ---


@pytest.fixture
def gr_user_factory(gr_user_manager: GRUserManager) -> Callable[..., GRUser]:

    def _inner(
        sub: str | None = None,
        is_superuser: bool = False,
    ) -> GRUser:
        sub = sub or f"{uuid4().hex}-{uuid4().hex}"

        return gr_user_manager.create(
            sub=sub,
            is_superuser=is_superuser,
        )

    return _inner


@pytest.fixture
def gr_user_cache(
    gr_user: GRUser,
    gr_db: PostgresConfig,
    thl_web_rr: PostgresConfig,
    gr_redis_config: RedisConfig,
) -> GRUser:
    gr_user.set_cache(
        pg_config=gr_db, thl_web_rr=thl_web_rr, redis_config=gr_redis_config
    )
    return gr_user


@pytest.fixture
def gr_business_bank_account_factory(
    gr_bbam: BusinessBankAccountManager,
) -> Callable[..., BusinessBankAccount]:

    def _inner(
        business_id: PositiveInt,
        uuid: UUIDStr | None = None,
        transfer_method: TransferMethod | None = None,
        account_number: str | None = None,
        routing_number: str | None = None,
        iban: str | None = None,
        swift: str | None = None,
    ):
        from generalresearch.models.gr.business import TransferMethod

        return gr_bbam.create(
            business_id=business_id,
            uuid=uuid or uuid4().hex,
            transfer_method=transfer_method or TransferMethod.ACH,
            account_number=account_number or uuid4().hex[:6],
            routing_number=routing_number or uuid4().hex[:6],
            iban=iban or uuid4().hex[:6],
            swift=swift or uuid4().hex[:6],
        )

    return _inner


@pytest.fixture
def gr_business_address_factory(
    gr_bam: BusinessAddressManager,
) -> Callable[..., BusinessAddress]:

    def _inner(
        business_id: PositiveInt,
        uuid: UUIDStr | None = None,
        line_1: str | None = None,
        line_2: str | None = None,
        city: str | None = None,
        state: str | None = None,
        postal_code: str | None = None,
        phone_number: PhoneNumber | None = None,
        country: str | None = None,
    ):
        uuid = uuid or uuid4().hex
        line_1 = line_1 or "abc"
        line_2 = line_2 or "bczx"
        city = city or "Downingtown"
        state = state or "CA"
        postal_code = postal_code or "94041"
        phone_number = None
        country = country or "US"

        return gr_bam.create(
            business_id=business_id,
            uuid=uuid,
            line_1=line_1,
            line_2=line_2,
            city=city,
            state=state,
            postal_code=postal_code,
            phone_number=phone_number,
            country=country,
        )

    return _inner


@pytest.fixture
def gr_business_factory(
    gr_bm: BusinessManager,
) -> Callable[..., Business]:

    def _inner(
        uuid: UUIDStr | None = None,
        name: str | None = None,
        team: Team | None = None,
        kind: BusinessType | None = None,
        tax_number: str | None = None,
    ) -> Business:
        from random import randint

        uuid = uuid or uuid4().hex
        name = name or "< Unknown >"
        tax_number = tax_number or str(randint(1, 999_999_999))

        return gr_bm.create(
            uuid=uuid, name=name, team=team, kind=kind, tax_number=tax_number
        )

    return _inner


@pytest.fixture
def gr_team(
    gr_tm: TeamManager,
) -> Callable[..., Team]:

    def _inner(uuid: UUIDStr | None = None, name: str | None = None) -> Team:
        uuid = uuid or uuid4().hex
        name = name or f"name-{uuid4().hex[:12]}"

        return gr_tm.create(uuid=uuid, name=name)

    return _inner


@pytest.fixture()
def gr_user_token(
    gr_user: GRUser, gr_tm: GRTokenManager, gr_db: PostgresConfig
) -> GRToken:
    gr_tm.create(user_id=gr_user.id)
    gr_user.prefetch_token(pg_config=gr_db)

    res = gr_user.token
    assert res is not None, "GRToken should exist after creation and prefetching"
    return res


@pytest.fixture()
def gr_user_token_header(gr_user_token: GRToken) -> dict[str, str]:
    return gr_user_token.auth_header


@pytest.fixture(scope="function")
def membership(team: Team, gr_user: GRUser, team_manager: TeamManager) -> Membership:
    assert team.id, "Team must be saved"
    assert gr_user.id, "GRUser must be saved"
    return team_manager.add_user(team=team, gr_user=gr_user)


@pytest.fixture(scope="function")
def membership_factory(
    team: Team,
    gr_user: GRUser,
    membership_manager: MembershipManager,
    team_manager: TeamManager,
    gr_um: GRUserManager,
) -> Callable[..., Membership]:

    def _inner(**kwargs) -> Membership:
        _team = kwargs.get("team", team_manager.create_dummy())
        _gr_user = kwargs.get("gr_user", gr_um.create_dummy())

        return membership_manager.create(team=_team, gr_user=_gr_user)

    return _inner
