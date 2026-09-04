from collections.abc import Callable
from typing import TYPE_CHECKING
from uuid import uuid4

import pytest

from generalresearch.models.gr.business import (
    Business,
    BusinessAddress,
    BusinessBankAccount,
)
from generalresearch.models.gr.definitions import TransferMethod
from generalresearch.models.gr.team import Team

if TYPE_CHECKING:
    from generalresearch.managers.gr.business import (
        BusinessAddressManager,
        BusinessBankAccountManager,
        BusinessManager,
    )
    from generalresearch.managers.gr.team import MembershipManager, TeamManager
    from generalresearch.models.gr.authentication import GRUser
    from generalresearch.pg_helper import PostgresConfig


class TestBusinessBankAccountManager:

    def test_init(
        self,
        gr_business_bank_account_manager: BusinessBankAccountManager,
        gr_db: PostgresConfig,
    ):
        assert gr_business_bank_account_manager.pg_config == gr_db

    def test_create(
        self,
        gr_business: Business,
        gr_business_bank_account_manager: BusinessBankAccountManager,
    ):

        instance = gr_business_bank_account_manager.create(
            business_id=gr_business.id,
            uuid=uuid4().hex,
            transfer_method=TransferMethod.ACH,
        )
        assert isinstance(instance, BusinessBankAccount)
        assert isinstance(instance.id, int)

        res = gr_business_bank_account_manager.get_by_business_id(
            business_id=instance.business_id
        )
        assert isinstance(res, list)
        assert len(res) == 1
        assert isinstance(res[0], BusinessBankAccount)
        assert res[0].business_id == instance.business_id


class TestBusinessAddressManager:

    def test_create(
        self, gr_business: Business, gr_business_address_manager: BusinessAddressManager
    ):
        assert gr_business.id
        res = gr_business_address_manager.create(
            uuid=uuid4().hex, business_id=gr_business.id
        )
        assert isinstance(res, BusinessAddress)
        assert isinstance(res.id, int)


class TestBusinessManager:

    def test_create(self, gr_business_factory: Callable[..., Business]):

        instance = gr_business_factory()
        assert isinstance(instance, Business)
        assert isinstance(instance.id, int)

    def test_get_or_create(self, gr_business_manager: BusinessManager):
        uuid_key = uuid4().hex

        assert gr_business_manager.get_by_uuid(business_uuid=uuid_key) is None

        instance = gr_business_manager.get_or_create(
            uuid=uuid_key,
            name=f"name-{uuid4().hex[:6]}",
        )

        res = gr_business_manager.get_by_uuid(business_uuid=uuid_key)
        assert isinstance(res, Business)
        assert res.id == instance.id

    def test_get_all(
        self,
        gr_business_manager: BusinessManager,
        gr_business_factory: Callable[..., Business],
    ):
        res1 = gr_business_manager.get_all()
        assert isinstance(res1, list)

        gr_business_factory()
        res2 = gr_business_manager.get_all()
        assert len(res1) == len(res2) - 1

    @pytest.mark.skip(reason="TODO")
    def test_get_by_team(self):
        pass

    def test_get_by_user_id(
        self,
        gr_business_manager: BusinessManager,
        gr_user: GRUser,
        gr_team_manager: TeamManager,
        gr_membership_manager: MembershipManager,
        gr_business_factory: Callable[..., Business],
        gr_team_factory: Callable[..., Team],
    ):
        res = gr_business_manager.get_by_user_id(user_id=gr_user.id)
        assert len(res) == 0

        # Create a business: Business, but don't add it to anything
        b1 = gr_business_factory()
        res = gr_business_manager.get_by_user_id(user_id=gr_user.id)
        assert len(res) == 0

        # Create a Team, but don't create any Memberships
        t1 = gr_team_factory()
        res = gr_business_manager.get_by_user_id(user_id=gr_user.id)
        assert len(res) == 0

        # Create a Membership for the gr_user to the Team... but it doesn't
        #   matter because the Team doesn't have any Business yet
        _ = gr_membership_manager.create(team=t1, gr_user=gr_user)
        res = gr_business_manager.get_by_user_id(user_id=gr_user.id)
        assert len(res) == 0

        # Add the Business to the Team... now the Business should be available
        # to the gr_user
        gr_team_manager.add_business(team=t1, business=b1)
        res = gr_business_manager.get_by_user_id(user_id=gr_user.id)
        assert len(res) == 1

        # Add another Business to the Team!
        b2 = gr_business_factory()
        gr_team_manager.add_business(team=t1, business=b2)
        res = gr_business_manager.get_by_user_id(user_id=gr_user.id)
        assert len(res) == 2

    @pytest.mark.skip(reason="TODO")
    def test_get_uuids_by_user_id(self):
        pass

    def test_get_by_uuid(
        self, gr_business: Business, gr_business_manager: BusinessManager
    ):
        instance = gr_business_manager.get_by_uuid(business_uuid=gr_business.uuid)
        assert isinstance(instance, Business)
        assert gr_business.id == instance.id

    def test_get_by_id(
        self, gr_business: Business, gr_business_manager: BusinessManager
    ):
        instance = gr_business_manager.get_by_id(business_id=gr_business.id)
        assert isinstance(instance, Business)
        assert gr_business.uuid == instance.uuid

    def test_cache_key(self, gr_business: Business):
        assert "business:" in gr_business.cache_key

    # def test_create_raise_on_duplicate(self):
    #     b_uuid = uuid4().hex
    #
    #     # Make the first one
    #     business = BusinessManager.create(
    #         uuid=b_uuid,
    #         name=f"test-{b_uuid[:6]}")
    #     assert isinstance(gr_business: Business, Business)
    #
    #     # Try to make it again
    #     with pytest.raises(expected_exception=psycopg.errors.UniqueViolation):
    #         business = BusinessManager.create(
    #             uuid=b_uuid,
    #             name=f"test-{b_uuid[:6]}")
    #
    # def test_get_by_team(self, team):
    #     for idx in range(5):
    #         BusinessManager.create(name=f"Business Name #{uuid4().hex[:6]}", team=team)
    #
    #     res = BusinessManager.get_by_team(team_id=team.id)
    #     assert isinstance(res, list)
    #     assert 5 == len(res)
