from __future__ import annotations

from collections.abc import Callable
from random import randint
from typing import TYPE_CHECKING
from uuid import uuid4

import pytest
from pydantic import PositiveInt
from pydantic_extra_types.phone_numbers import PhoneNumber

from generalresearch.models.custom_types import UUIDStr

if TYPE_CHECKING:
    from generalresearch.managers.gr.authentication import GRTokenManager, GRUserManager
    from generalresearch.managers.gr.business import (
        BusinessAddressManager,
        BusinessBankAccountManager,
        BusinessManager,
    )
    from generalresearch.managers.gr.team import MembershipManager, TeamManager
    from generalresearch.models.gr.authentication import GRToken, GRUser
    from generalresearch.models.gr.business import (
        Business,
        BusinessAddress,
        BusinessBankAccount,
    )
    from generalresearch.models.gr.definitions import TransferMethod
    from generalresearch.models.gr.team import Membership, Team
    from generalresearch.pg_helper import PostgresConfig
    from generalresearch.redis_helper import RedisConfig

# --- Static ---


# --- Factory / Database ---


# --- Business Bank Account ---


@pytest.fixture
def gr_business_bank_account_factory(
    gr_business_bank_account_manager: BusinessBankAccountManager,
) -> Callable[..., BusinessBankAccount]:

    def _inner(
        business_id: PositiveInt,
        save: bool = True,
        uuid: UUIDStr | None = None,
        transfer_method: TransferMethod | None = None,
        account_number: str | None = None,
        routing_number: str | None = None,
        iban: str | None = None,
        swift: str | None = None,
        **kwargs,
    ) -> BusinessBankAccount:

        if save:
            return gr_business_bank_account_manager.create(
                business_id=business_id,
                uuid=uuid or uuid4().hex,
                transfer_method=transfer_method or TransferMethod.ACH,
                account_number=account_number or uuid4().hex[:6],
                routing_number=routing_number or uuid4().hex[:6],
                iban=iban or uuid4().hex[:6],
                swift=swift or uuid4().hex[:6],
                **kwargs,
            )
        else:
            raise ValueError("Unsaved BusinessBankAccount not supported yet")

    return _inner


@pytest.fixture
def gr_business_bank_account(gr_business_factory: Callable[..., Business]) -> Business:
    return gr_business_factory(save=True)


@pytest.fixture
def unsaved_gr_business_bank_account(
    gr_business_factory: Callable[..., Business],
) -> Business:
    return gr_business_factory(save=False)


# --- Business Address ---


@pytest.fixture
def gr_business_address_factory(
    gr_business_address_manager: BusinessAddressManager,
) -> Callable[..., BusinessAddress]:

    def _inner(
        business_id: PositiveInt,
        save: bool = True,
        uuid: UUIDStr | None = None,
        line_1: str | None = None,
        line_2: str | None = None,
        city: str | None = None,
        state: str | None = None,
        postal_code: str | None = None,
        phone_number: PhoneNumber | None = None,
        country: str | None = None,
    ) -> BusinessAddress:
        uuid = uuid or uuid4().hex
        line_1 = line_1 or "abc"
        line_2 = line_2 or "bczx"
        city = city or "Downingtown"
        state = state or "CA"
        postal_code = postal_code or "94041"
        phone_number = None
        country = country or "US"

        if save:
            return gr_business_address_manager.create(
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
        else:
            raise ValueError("Unsaved BusinessAddress not supported yet")

    return _inner


# @pytest.fixture
# def business_address(
#     gr_business: Business, business_address_manager: BusinessAddressManager
# ) -> :
#     return business_address_manager.create_dummy(business_id=gr_business.id)


@pytest.fixture
def gr_business_address(
    gr_business_address_factory: Callable[..., BusinessAddress],
) -> BusinessAddress:
    return gr_business_address_factory(save=True)


@pytest.fixture
def unsaved_gr_business_address(
    gr_business_address_factory: Callable[..., BusinessAddress],
) -> BusinessAddress:
    return gr_business_address_factory(save=False)


# --- Business ---


@pytest.fixture
def gr_business_factory(
    gr_business_manager: BusinessManager,
) -> Callable[..., Business]:

    def _inner(
        save: bool = True, name: str | None = None, team: Team | None = None, **kwargs
    ) -> Business:
        name = name or f"<Unknown {uuid4().hex[:12]}>"
        tax_number = str(randint(1, 999_999_999))

        if save:
            return gr_business_manager.create(
                name=name,
                kind="c",
                uuid=uuid4().hex,
                team=team,
                tax_number=tax_number,
                **kwargs,
            )
        else:
            raise ValueError("Unsaved Business not supported yet")

    return _inner


@pytest.fixture
def gr_business(gr_business_factory: Callable[..., Business]) -> Business:
    return gr_business_factory(save=True)


@pytest.fixture
def unsaved_gr_business(gr_business_factory: Callable[..., Business]) -> Business:
    return gr_business_factory(save=False)


# --- GR Team ---


@pytest.fixture
def gr_team_factory(
    gr_team_manager: TeamManager,
) -> Callable[..., Team]:

    def _inner(
        save: bool = True,
        uuid: UUIDStr | None = None,
        name: str | None = None,
        **kwargs,
    ) -> Team:

        if save:
            return gr_team_manager.create(uuid=uuid, name=name, **kwargs)

        else:
            raise ValueError("BusinessBankAccount Business not supported yet")

    return _inner


@pytest.fixture
def gr_team(gr_team_factory: Callable[..., Team]) -> Team:
    return gr_team_factory(save=True)


@pytest.fixture
def unsaved_gr_team(
    gr_team_factory: Callable[..., Team],
) -> Team:
    return gr_team_factory(save=False)


# --- GR User ---


@pytest.fixture
def gr_user_factory(gr_user_manager: GRUserManager) -> Callable[..., GRUser]:

    def _inner(
        save: bool = True,
        sub: str | None = None,
        is_superuser: bool = False,
    ) -> GRUser:
        sub = sub or f"{uuid4().hex}-{uuid4().hex}"

        if save:
            return gr_user_manager.create(
                sub=sub,
                is_superuser=is_superuser,
            )
        else:
            raise ValueError("Unsaved GR User not supported yet")

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
def gr_user(gr_user_factory: Callable[..., GRUser]) -> GRUser:
    return gr_user_factory(save=True)


@pytest.fixture
def unsaved_gr_user(
    gr_user_factory: Callable[..., GRUser],
) -> GRUser:
    return gr_user_factory(save=False)


# --- GR User Token ---


@pytest.fixture
def gr_user_token_factory(
    gr_user: GRUser, gr_user_token_manager: GRUser, gr_db: PostgresConfig
) -> Callable[..., GRToken]:

    def _inner(
        save: bool = True,
    ) -> GRToken:

        if save:
            gr_user_token_manager.create(user_id=gr_user.id)
            gr_user.prefetch_token(pg_config=gr_db)

            res = gr_user.token
            assert (
                res is not None
            ), "GRToken should exist after creation and prefetching"
            return res

        else:
            raise ValueError("Unsaved GR User not supported yet")

    return _inner


@pytest.fixture
def gr_user_token(gr_user_token_factory: Callable[..., GRToken]) -> GRToken:
    return gr_user_token_factory(save=True)


@pytest.fixture
def unsaved_gr_user_token(gr_user_token_factory: Callable[..., GRToken]) -> GRToken:
    return gr_user_token_factory(save=False)


@pytest.fixture()
def gr_user_token_header(gr_user_token: GRToken) -> dict[str, str]:
    return gr_user_token.auth_header


# --- GR Membership ---


@pytest.fixture()
def gr_membership_factory(
    gr_team: Team,
    gr_user: GRUser,
    gr_membership_manager: MembershipManager,
) -> Callable[..., Membership]:

    def _inner(save: bool = True, **kwargs) -> Membership:
        if save:
            return gr_membership_manager.create(team=gr_team, gr_user=gr_user, **kwargs)
        else:
            raise ValueError("Unsaved GR Membership not supported yet")

    return _inner


@pytest.fixture()
def gr_membership(gr_membership_factory: Callable[..., Membership]) -> Membership:
    return gr_membership_factory(save=True)


@pytest.fixture()
def unsaved_gr_membership(
    gr_membership_factory: Callable[..., Membership],
) -> Membership:
    return gr_membership_factory(save=False)
