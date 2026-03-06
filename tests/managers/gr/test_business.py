from uuid import uuid4

import pytest

from test_utils.models.conftest import business


class TestBusinessBankAccountManager:

    def test_init(self, business_bank_account_manager, gr_db):
        assert business_bank_account_manager.pg_config == gr_db

    def test_create(self, business, business_bank_account_manager):
        from generalresearch.models.gr.business import (
            TransferMethod,
            BusinessBankAccount,
        )

        instance = business_bank_account_manager.create(
            business_id=business.id,
            uuid=uuid4().hex,
            transfer_method=TransferMethod.ACH,
        )
        assert isinstance(instance, BusinessBankAccount)
        assert isinstance(instance.id, int)

        res = business_bank_account_manager.get_by_business_id(
            business_id=instance.business_id
        )
        assert isinstance(res, list)
        assert len(res) == 1
        assert isinstance(res[0], BusinessBankAccount)
        assert res[0].business_id == instance.business_id


class TestBusinessAddressManager:

    def test_create(self, business, business_address_manager):
        from generalresearch.models.gr.business import BusinessAddress

        res = business_address_manager.create(uuid=uuid4().hex, business_id=business.id)
        assert isinstance(res, BusinessAddress)
        assert isinstance(res.id, int)


class TestBusinessManager:

    def test_create(self, business_manager):
        from generalresearch.models.gr.business import Business

        instance = business_manager.create_dummy()
        assert isinstance(instance, Business)
        assert isinstance(instance.id, int)

    def test_get_or_create(self, business_manager):
        uuid_key = uuid4().hex

        assert business_manager.get_by_uuid(business_uuid=uuid_key) is None

        instance = business_manager.get_or_create(
            uuid=uuid_key,
            name=f"name-{uuid4().hex[:6]}",
        )

        res = business_manager.get_by_uuid(business_uuid=uuid_key)
        assert res.id == instance.id

    def test_get_all(self, business_manager):
        res1 = business_manager.get_all()
        assert isinstance(res1, list)

        business_manager.create_dummy()
        res2 = business_manager.get_all()
        assert len(res1) == len(res2) - 1

    @pytest.mark.skip(reason="TODO")
    def test_get_by_team(self):
        pass

    def test_get_by_user_id(
        self, business_manager, gr_user, team_manager, membership_manager
    ):
        res = business_manager.get_by_user_id(user_id=gr_user.id)
        assert len(res) == 0

        # Create a Business, but don't add it to anything
        b1 = business_manager.create_dummy()
        res = business_manager.get_by_user_id(user_id=gr_user.id)
        assert len(res) == 0

        # Create a Team, but don't create any Memberships
        t1 = team_manager.create_dummy()
        res = business_manager.get_by_user_id(user_id=gr_user.id)
        assert len(res) == 0

        # Create a Membership for the gr_user to the Team... but it doesn't
        #   matter because the Team doesn't have any Business yet
        m1 = membership_manager.create(team=t1, gr_user=gr_user)
        res = business_manager.get_by_user_id(user_id=gr_user.id)
        assert len(res) == 0

        # Add the Business to the Team... now the Business should be available
        # to the gr_user
        team_manager.add_business(team=t1, business=b1)
        res = business_manager.get_by_user_id(user_id=gr_user.id)
        assert len(res) == 1

        # Add another Business to the Team!
        b2 = business_manager.create_dummy()
        team_manager.add_business(team=t1, business=b2)
        res = business_manager.get_by_user_id(user_id=gr_user.id)
        assert len(res) == 2

    @pytest.mark.skip(reason="TODO")
    def test_get_uuids_by_user_id(self):
        pass

    def test_get_by_uuid(self, business, business_manager):
        instance = business_manager.get_by_uuid(business_uuid=business.uuid)
        assert business.id == instance.id

    def test_get_by_id(self, business, business_manager):
        instance = business_manager.get_by_id(business_id=business.id)
        assert business.uuid == instance.uuid

    def test_cache_key(self, business):
        assert "business:" in business.cache_key

    # def test_create_raise_on_duplicate(self):
    #     b_uuid = uuid4().hex
    #
    #     # Make the first one
    #     business = BusinessManager.create(
    #         uuid=b_uuid,
    #         name=f"test-{b_uuid[:6]}")
    #     assert isinstance(business, Business)
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
