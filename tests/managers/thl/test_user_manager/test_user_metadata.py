from uuid import uuid4

import pytest

from generalresearch.models.thl.user_profile import UserMetadata
from test_utils.models.conftest import user, user_manager, user_factory


class TestUserMetadataManager:

    def test_get_notset(self, user, user_manager, user_metadata_manager):
        # The row in the db won't exist. It just returns the default obj with everything None (except for the user_id)
        um1 = user_metadata_manager.get(user_id=user.user_id)
        assert um1 == UserMetadata(user_id=user.user_id)

    def test_create(self, user_factory, product, user_metadata_manager):
        from generalresearch.models.thl.user import User

        u1: User = user_factory(product=product)

        email_address = f"{uuid4().hex}@example.com"
        um = UserMetadata(user_id=u1.user_id, email_address=email_address)
        # This happens in the model itself, nothing to do with the manager (a model_validator)
        assert um.email_sha256 is not None

        user_metadata_manager.update(um)
        um2 = user_metadata_manager.get(email_address=email_address)
        assert um == um2

    def test_create_no_email(self, product, user_factory, user_metadata_manager):
        from generalresearch.models.thl.user import User

        u1: User = user_factory(product=product)
        um = UserMetadata(user_id=u1.user_id)
        assert um.email_sha256 is None

        user_metadata_manager.update(um)
        um2 = user_metadata_manager.get(user_id=u1.user_id)
        assert um == um2

    def test_update(self, product, user_factory, user_metadata_manager):
        from generalresearch.models.thl.user import User

        u: User = user_factory(product=product)

        email_address = f"{uuid4().hex}@example1.com"
        um = UserMetadata(user_id=u.user_id, email_address=email_address)
        user_metadata_manager.update(user_metadata=um)

        um.email_address = email_address.replace("example1", "example2")
        user_metadata_manager.update(user_metadata=um)

        um2 = user_metadata_manager.get(email_address=um.email_address)
        assert um2.email_address != email_address

        assert um2 == UserMetadata(
            user_id=u.user_id,
            email_address=email_address.replace("example1", "example2"),
        )

    def test_filter(self, user_factory, product, user_metadata_manager):
        from generalresearch.models.thl.user import User

        user1: User = user_factory(product=product)
        user2: User = user_factory(product=product)

        email_address = f"{uuid4().hex}@example.com"
        res = user_metadata_manager.filter(email_addresses=[email_address])
        assert len(res) == 0

        # Create 2 user metadata with the same email address
        user_metadata_manager.update(
            user_metadata=UserMetadata(
                user_id=user1.user_id, email_address=email_address
            )
        )
        user_metadata_manager.update(
            user_metadata=UserMetadata(
                user_id=user2.user_id, email_address=email_address
            )
        )

        res = user_metadata_manager.filter(email_addresses=[email_address])
        assert len(res) == 2

        with pytest.raises(expected_exception=ValueError) as e:
            res = user_metadata_manager.get(email_address=email_address)
        assert "More than 1 result returned!" in str(e.value)
