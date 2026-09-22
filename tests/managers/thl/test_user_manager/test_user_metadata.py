from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING
from uuid import uuid4

import pytest

from generalresearch.models.thl.user_profile import UserMetadata

if TYPE_CHECKING:
    from generalresearch.managers.thl.user_manager.user_metadata_manager import (
        UserMetadataManager,
    )
    from generalresearch.models.thl.product import Product
    from generalresearch.models.thl.user import User


class TestUserMetadataManager:
    def test_get_notset(
        self,
        user: User,
        user_metadata_manager: UserMetadataManager,
    ):
        # The row in the db won't exist. It just returns the default obj with everything None (except for the user_id)
        um1 = user_metadata_manager.get(user_id=user.user_id)
        assert um1 == UserMetadata(user_id=user.user_id)

    def test_create(
        self,
        user_factory: Callable[..., User],
        product: Product,
        user_metadata_manager: UserMetadataManager,
    ):

        u1: User = user_factory(product=product)

        email_address = f"{uuid4().hex}@example.com"
        um = UserMetadata(user_id=u1.user_id, email_address=email_address)
        # This happens in the model itself, nothing to do with the manager (a model_validator)
        assert um.email_sha256 is not None

        user_metadata_manager.update(um)
        um2 = user_metadata_manager.get(email_address=email_address)
        assert um == um2

    def test_create_no_email(
        self,
        product: Product,
        user_factory: Callable[..., User],
        user_metadata_manager: UserMetadataManager,
    ):

        u1: User = user_factory(product=product)
        um = UserMetadata(user_id=u1.user_id)
        assert um.email_sha256 is None

        user_metadata_manager.update(um)
        um2 = user_metadata_manager.get(user_id=u1.user_id)
        assert um == um2

    def test_update(
        self,
        product: Product,
        user_factory: Callable[..., User],
        user_metadata_manager: UserMetadataManager,
    ):

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

    def test_filter(
        self, user_factory: Callable[..., User], product: Product, user_metadata_manager
    ):

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

    def test_canonical(
        self,
        product: Product,
        user_factory: Callable[..., User],
        user_metadata_manager: UserMetadataManager,
    ):

        u: User = user_factory(product=product)

        rand_part = uuid4().hex[:12]
        local = f"Example.{rand_part}"
        expected_canonical = f"example{rand_part}@gmail.com"

        um = UserMetadata(
            user_id=u.user_id, email_address=f"{local}+123@googlemail.com"
        )
        assert um.canonical_email == expected_canonical
        user_metadata_manager.update(user_metadata=um)

        um.email_address = f"{local}+456@googlemail.com"
        user_metadata_manager.update(user_metadata=um)

        um2 = user_metadata_manager.get(email_address=um.email_address)
        assert um2.email_address == f"{local}+456@googlemail.com"
        assert um2.canonical_email == expected_canonical

        res = user_metadata_manager.filter(canonical_emails=[expected_canonical])
        assert len(res) == 1

        res = user_metadata_manager.filter_by_email_aliases(
            email_addresses=[f"{local}+789@googlemail.com", expected_canonical]
        )
        assert len(res) == 1

        with pytest.raises(
            ValueError, match="canonical email must already be normalized"
        ):
            user_metadata_manager.filter(
                canonical_emails=[f"{local}+789@googlemail.com"]
            )
