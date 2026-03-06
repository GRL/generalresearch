from uuid import uuid4

import pytest

from generalresearch.models.thl.user import User
from test_utils.models.conftest import product, user_manager, user_factory


class TestUserManagerFetch:

    def test_fetch(self, user_factory, product, user_manager):
        user1: User = user_factory(product=product)
        user2: User = user_factory(product=product)
        res = user_manager.fetch_by_bpuids(
            product_id=product.uuid,
            product_user_ids=[user1.product_user_id, user2.product_user_id],
        )
        assert len(res) == 2

        res = user_manager.fetch(user_ids=[user1.user_id, user2.user_id])
        assert len(res) == 2

        res = user_manager.fetch(user_uuids=[user1.uuid, user2.uuid])
        assert len(res) == 2

        # filter including bogus values
        res = user_manager.fetch(user_uuids=[user1.uuid, uuid4().hex])
        assert len(res) == 1

        res = user_manager.fetch(user_uuids=[uuid4().hex])
        assert len(res) == 0

    def test_fetch_invalid(self, user_manager):
        with pytest.raises(AssertionError) as e:
            user_manager.fetch(user_uuids=[], user_ids=None)
        assert "Must pass ONE of user_ids, user_uuids" in str(e.value)

        with pytest.raises(AssertionError) as e:
            user_manager.fetch(user_uuids=uuid4().hex)
        assert "must pass a collection of user_uuids" in str(e.value)

        with pytest.raises(AssertionError) as e:
            user_manager.fetch(user_uuids=[uuid4().hex], user_ids=[1, 2, 3])
        assert "Must pass ONE of user_ids, user_uuids" in str(e.value)

        with pytest.raises(AssertionError) as e:
            user_manager.fetch(user_ids=list(range(501)))
        assert "limit 500 user_ids" in str(e.value)
