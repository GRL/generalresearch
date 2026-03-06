import json
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from random import randint, choice as rand_choice
from uuid import uuid4

import pytest
from pydantic import ValidationError


class TestUserUserID:

    def test_valid(self):
        from generalresearch.models.thl.user import User

        val = randint(1, 2**30)
        user = User(user_id=val)
        assert user.user_id == val

    def test_type(self):
        from generalresearch.models.thl.user import User

        # It will cast str to int
        assert User(user_id="1").user_id == 1

        # It will cast float to int
        assert User(user_id=1.0).user_id == 1

        # It will cast Decimal to int
        assert User(user_id=Decimal("1.0")).user_id == 1

        # pydantic Validation error is a ValueError, let's check both..
        with pytest.raises(expected_exception=ValueError) as cm:
            User(user_id=Decimal("1.00000001"))
        assert "1 validation error for User" in str(cm.value)
        assert "user_id" in str(cm.value)
        assert "Input should be a valid integer," in str(cm.value)

        with pytest.raises(expected_exception=ValidationError) as cm:
            User(user_id=Decimal("1.00000001"))
        assert "1 validation error for User" in str(cm.value)
        assert "user_id" in str(cm.value)
        assert "Input should be a valid integer," in str(cm.value)

    def test_zero(self):
        from generalresearch.models.thl.user import User

        with pytest.raises(expected_exception=ValidationError) as cm:
            User(user_id=0)
        assert "1 validation error for User" in str(cm.value)
        assert "Input should be greater than 0" in str(cm.value)

    def test_negative(self):
        from generalresearch.models.thl.user import User

        with pytest.raises(expected_exception=ValidationError) as cm:
            User(user_id=-1)
        assert "1 validation error for User" in str(cm.value)
        assert "Input should be greater than 0" in str(cm.value)

    def test_too_big(self):
        from generalresearch.models.thl.user import User

        val = 2**31
        with pytest.raises(expected_exception=ValidationError) as cm:
            User(user_id=val)
        assert "1 validation error for User" in str(cm.value)
        assert "Input should be less than 2147483648" in str(cm.value)

    def test_identifiable(self):
        from generalresearch.models.thl.user import User

        val = randint(1, 2**30)
        user = User(user_id=val)
        assert user.is_identifiable


class TestUserProductID:
    user_id = randint(1, 2**30)

    def test_valid(self):
        from generalresearch.models.thl.user import User

        product_id = uuid4().hex

        user = User(user_id=self.user_id, product_id=product_id)
        assert user.user_id == self.user_id
        assert user.product_id == product_id

    def test_type(self):
        from generalresearch.models.thl.user import User

        with pytest.raises(expected_exception=ValueError) as cm:
            User(user_id=self.user_id, product_id=0)
        assert "1 validation error for User" in str(cm.value)
        assert "Input should be a valid string" in str(cm.value)

        with pytest.raises(expected_exception=ValueError) as cm:
            User(user_id=self.user_id, product_id=0.0)
        assert "1 validation error for User" in str(cm.value)
        assert "Input should be a valid string" in str(cm.value)

        with pytest.raises(expected_exception=ValueError) as cm:
            User(user_id=self.user_id, product_id=Decimal("0"))
        assert "1 validation error for User" in str(cm.value)
        assert "Input should be a valid string" in str(cm.value)

    def test_empty(self):
        from generalresearch.models.thl.user import User

        with pytest.raises(expected_exception=ValueError) as cm:
            User(user_id=self.user_id, product_id="")
        assert "1 validation error for User" in str(cm.value)
        assert "String should have at least 32 characters" in str(cm.value)

    def test_invalid_len(self):
        from generalresearch.models.thl.user import User

        # Valid uuid4s are 32 char long
        product_id = uuid4().hex[:31]
        with pytest.raises(expected_exception=ValueError) as cm:
            User(user_id=self.user_id, product_id=product_id)
        assert "1 validation error for User", str(cm.value)
        assert "String should have at least 32 characters", str(cm.value)

        product_id = uuid4().hex * 2
        with pytest.raises(ValueError) as cm:
            User(user_id=self.user_id, product_id=product_id)
        assert "1 validation error for User" in str(cm.value)
        assert "String should have at most 32 characters" in str(cm.value)

        product_id = uuid4().hex
        product_id *= 2
        with pytest.raises(expected_exception=ValueError) as cm:
            User(user_id=self.user_id, product_id=product_id)
        assert "1 validation error for User" in str(cm.value)
        assert "String should have at most 32 characters" in str(cm.value)

    def test_invalid_uuid(self):
        from generalresearch.models.thl.user import User

        # Modify the UUID to break it
        product_id = uuid4().hex[:31] + "x"

        with pytest.raises(expected_exception=ValueError) as cm:
            User(user_id=self.user_id, product_id=product_id)
        assert "1 validation error for User" in str(cm.value)
        assert "Invalid UUID" in str(cm.value)

    def test_invalid_hex_form(self):
        from generalresearch.models.thl.user import User

        # Sure not in hex form, but it'll get caught for being the
        # wrong length before anything else
        product_id = str(uuid4())  # '1a93447e-c77b-4cfa-b58e-ed4777d57110'
        with pytest.raises(expected_exception=ValueError) as cm:
            User(user_id=self.user_id, product_id=product_id)
        assert "1 validation error for User" in str(cm.value)
        assert "String should have at most 32 characters" in str(cm.value)

    def test_identifiable(self):
        """Can't create a User with only a product_id because it also
        needs to the product_user_id"""
        from generalresearch.models.thl.user import User

        product_id = uuid4().hex
        with pytest.raises(expected_exception=ValueError) as cm:
            User(product_id=product_id)
        assert "1 validation error for User" in str(cm.value)
        assert "Value error, User is not identifiable" in str(cm.value)


class TestUserProductUserID:
    user_id = randint(1, 2**30)

    def randomword(self, length: int = 50):
        # Raw so nothing is escaped to add additional backslashes
        _bpuid_allowed = r"0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ!#$%&()*+,-.:;<=>?@[]^_{|}~"
        return "".join(rand_choice(_bpuid_allowed) for i in range(length))

    def test_valid(self):
        from generalresearch.models.thl.user import User

        product_user_id = uuid4().hex[:12]
        user = User(user_id=self.user_id, product_user_id=product_user_id)

        assert user.user_id == self.user_id
        assert user.product_user_id == product_user_id

    def test_type(self):
        from generalresearch.models.thl.user import User

        with pytest.raises(expected_exception=ValueError) as cm:
            User(user_id=self.user_id, product_user_id=0)
        assert "1 validation error for User" in str(cm.value)
        assert "Input should be a valid string" in str(cm.value)

        with pytest.raises(expected_exception=ValueError) as cm:
            User(user_id=self.user_id, product_user_id=0.0)
        assert "1 validation error for User" in str(cm.value)
        assert "Input should be a valid string" in str(cm.value)

        with pytest.raises(ValueError) as cm:
            User(user_id=self.user_id, product_user_id=Decimal("0"))
        assert "1 validation error for User" in str(cm.value)
        assert "Input should be a valid string" in str(cm.value)

    def test_empty(self):
        from generalresearch.models.thl.user import User

        with pytest.raises(expected_exception=ValueError) as cm:
            User(user_id=self.user_id, product_user_id="")
        assert "1 validation error for User" in str(cm.value)
        assert "String should have at least 3 characters" in str(cm.value)

    def test_invalid_len(self):
        from generalresearch.models.thl.user import User

        product_user_id = self.randomword(251)
        with pytest.raises(expected_exception=ValueError) as cm:
            User(user_id=self.user_id, product_user_id=product_user_id)
        assert "1 validation error for User" in str(cm.value)
        assert "String should have at most 128 characters" in str(cm.value)

        product_user_id = self.randomword(2)
        with pytest.raises(expected_exception=ValueError) as cm:
            User(user_id=self.user_id, product_user_id=product_user_id)
        assert "1 validation error for User" in str(cm.value)
        assert "String should have at least 3 characters" in str(cm.value)

    def test_invalid_chars_space(self):
        from generalresearch.models.thl.user import User

        product_user_id = f"{self.randomword(50)} {self.randomword(50)}"
        with pytest.raises(expected_exception=ValueError) as cm:
            User(user_id=self.user_id, product_user_id=product_user_id)
        assert "1 validation error for User" in str(cm.value)
        assert "String cannot contain spaces" in str(cm.value)

    def test_invalid_chars_slash(self):
        from generalresearch.models.thl.user import User

        product_user_id = f"{self.randomword(50)}\{self.randomword(50)}"
        with pytest.raises(expected_exception=ValueError) as cm:
            User(user_id=self.user_id, product_user_id=product_user_id)
        assert "1 validation error for User" in str(cm.value)
        assert "String cannot contain backslash" in str(cm.value)

        product_user_id = f"{self.randomword(50)}/{self.randomword(50)}"
        with pytest.raises(expected_exception=ValueError) as cm:
            User(user_id=self.user_id, product_user_id=product_user_id)
        assert "1 validation error for User" in str(cm.value)
        assert "String cannot contain slash" in str(cm.value)

    def test_invalid_chars_backtick(self):
        """Yes I could keep doing these specific character checks. However,
        I wanted a test that made sure the regex was hit. I do not know
        how we want to provide with the level of specific String checks
        we do in here for specific error messages."""
        from generalresearch.models.thl.user import User

        product_user_id = f"{self.randomword(50)}`{self.randomword(50)}"
        with pytest.raises(expected_exception=ValueError) as cm:
            User(user_id=self.user_id, product_user_id=product_user_id)
        assert "1 validation error for User" in str(cm.value)
        assert "String is not valid regex" in str(cm.value)

    def test_unique_from_product_id(self):
        # We removed this filter b/c these users already exist. the manager checks for this
        # though and we can't create new users like this
        pass
        # product_id = uuid4().hex
        #
        # with pytest.raises(ValueError) as cm:
        #     User(product_id=product_id, product_user_id=product_id)
        # assert "1 validation error for User", str(cm.exception))
        # assert "product_user_id must not equal the product_id", str(cm.exception))

    def test_identifiable(self):
        """Can't create a User with only a product_user_id because it also
        needs to the product_id"""
        from generalresearch.models.thl.user import User

        product_user_id = uuid4().hex
        with pytest.raises(ValueError) as cm:
            User(product_user_id=product_user_id)
        assert "1 validation error for User" in str(cm.value)
        assert "Value error, User is not identifiable" in str(cm.value)


class TestUserUUID:
    user_id = randint(1, 2**30)

    def test_valid(self):
        from generalresearch.models.thl.user import User

        uuid_pk = uuid4().hex

        user = User(user_id=self.user_id, uuid=uuid_pk)
        assert user.user_id == self.user_id
        assert user.uuid == uuid_pk

    def test_type(self):
        from generalresearch.models.thl.user import User

        with pytest.raises(ValueError) as cm:
            User(user_id=self.user_id, uuid=0)
        assert "1 validation error for User" in str(cm.value)
        assert "Input should be a valid string" in str(cm.value)

        with pytest.raises(ValueError) as cm:
            User(user_id=self.user_id, uuid=0.0)
        assert "1 validation error for User" in str(cm.value)
        assert "Input should be a valid string" in str(cm.value)

        with pytest.raises(ValueError) as cm:
            User(user_id=self.user_id, uuid=Decimal("0"))
        assert "1 validation error for User", str(cm.value)
        assert "Input should be a valid string" in str(cm.value)

    def test_empty(self):
        from generalresearch.models.thl.user import User

        with pytest.raises(ValueError) as cm:
            User(user_id=self.user_id, uuid="")
        assert "1 validation error for User", str(cm.value)
        assert "String should have at least 32 characters", str(cm.value)

    def test_invalid_len(self):
        from generalresearch.models.thl.user import User

        # Valid uuid4s are 32 char long
        uuid_pk = uuid4().hex[:31]
        with pytest.raises(ValueError) as cm:
            User(user_id=self.user_id, uuid=uuid_pk)
        assert "1 validation error for User" in str(cm.value)
        assert "String should have at least 32 characters" in str(cm.value)

        # Valid uuid4s are 32 char long
        uuid_pk = uuid4().hex
        uuid_pk *= 2
        with pytest.raises(ValueError) as cm:
            User(user_id=self.user_id, uuid=uuid_pk)
        assert "1 validation error for User" in str(cm.value)
        assert "String should have at most 32 characters" in str(cm.value)

    def test_invalid_uuid(self):
        from generalresearch.models.thl.user import User

        # Modify the UUID to break it
        uuid_pk = uuid4().hex[:31] + "x"

        with pytest.raises(ValueError) as cm:
            User(user_id=self.user_id, uuid=uuid_pk)
        assert "1 validation error for User" in str(cm.value)
        assert "Invalid UUID" in str(cm.value)

    def test_invalid_hex_form(self):
        from generalresearch.models.thl.user import User

        # Sure not in hex form, but it'll get caught for being the
        # wrong length before anything else
        uuid_pk = str(uuid4())  # '1a93447e-c77b-4cfa-b58e-ed4777d57110'
        with pytest.raises(ValueError) as cm:
            User(user_id=self.user_id, uuid=uuid_pk)
        assert "1 validation error for User" in str(cm.value)
        assert "String should have at most 32 characters" in str(cm.value)

        uuid_pk = str(uuid4())[:32]  # '1a93447e-c77b-4cfa-b58e-ed4777d57110'
        with pytest.raises(ValueError) as cm:
            User(user_id=self.user_id, uuid=uuid_pk)
        assert "1 validation error for User" in str(cm.value)
        assert "Invalid UUID" in str(cm.value)

    def test_identifiable(self):
        from generalresearch.models.thl.user import User

        user_uuid = uuid4().hex
        user = User(uuid=user_uuid)
        assert user.is_identifiable


class TestUserCreated:
    user_id = randint(1, 2**30)

    def test_valid(self):
        from generalresearch.models.thl.user import User

        user = User(user_id=self.user_id)
        dt = datetime.now(tz=timezone.utc)
        user.created = dt

        assert user.created == dt

    def test_tz_naive_throws_init(self):
        from generalresearch.models.thl.user import User

        with pytest.raises(ValueError) as cm:
            User(user_id=self.user_id, created=datetime.now(tz=None))
        assert "1 validation error for User" in str(cm.value)
        assert "Input should have timezone info" in str(cm.value)

    def test_tz_naive_throws_setter(self):
        from generalresearch.models.thl.user import User

        user = User(user_id=self.user_id)
        with pytest.raises(ValueError) as cm:
            user.created = datetime.now(tz=None)
        assert "1 validation error for User" in str(cm.value)
        assert "Input should have timezone info" in str(cm.value)

    def test_tz_utc(self):
        from generalresearch.models.thl.user import User

        with pytest.raises(ValueError) as cm:
            User(
                user_id=self.user_id,
                created=datetime.now(tz=timezone(-timedelta(hours=8))),
            )
        assert "1 validation error for User" in str(cm.value)
        assert "Timezone is not UTC" in str(cm.value)

    def test_not_in_future(self):
        from generalresearch.models.thl.user import User

        the_future = datetime.now(tz=timezone.utc) + timedelta(minutes=1)
        with pytest.raises(ValueError) as cm:
            User(user_id=self.user_id, created=the_future)
        assert "1 validation error for User" in str(cm.value)
        assert "Input is in the future" in str(cm.value)

    def test_after_anno_domini(self):
        from generalresearch.models.thl.user import User

        before_ad = datetime(
            year=2015, month=1, day=1, tzinfo=timezone.utc
        ) + timedelta(minutes=1)
        with pytest.raises(ValueError) as cm:
            User(user_id=self.user_id, created=before_ad)
        assert "1 validation error for User" in str(cm.value)
        assert "Input is before Anno Domini" in str(cm.value)


class TestUserLastSeen:
    user_id = randint(1, 2**30)

    def test_valid(self):
        from generalresearch.models.thl.user import User

        user = User(user_id=self.user_id)
        dt = datetime.now(tz=timezone.utc)
        user.last_seen = dt

        assert user.last_seen == dt

    def test_tz_naive_throws_init(self):
        from generalresearch.models.thl.user import User

        with pytest.raises(ValueError) as cm:
            User(user_id=self.user_id, last_seen=datetime.now(tz=None))
        assert "1 validation error for User" in str(cm.value)
        assert "Input should have timezone info" in str(cm.value)

    def test_tz_naive_throws_setter(self):
        from generalresearch.models.thl.user import User

        user = User(user_id=self.user_id)
        with pytest.raises(ValueError) as cm:
            user.last_seen = datetime.now(tz=None)
        assert "1 validation error for User" in str(cm.value)
        assert "Input should have timezone info" in str(cm.value)

    def test_tz_utc(self):
        from generalresearch.models.thl.user import User

        with pytest.raises(ValueError) as cm:
            User(
                user_id=self.user_id,
                last_seen=datetime.now(tz=timezone(-timedelta(hours=8))),
            )
        assert "1 validation error for User" in str(cm.value)
        assert "Timezone is not UTC" in str(cm.value)

    def test_not_in_future(self):
        from generalresearch.models.thl.user import User

        the_future = datetime.now(tz=timezone.utc) + timedelta(minutes=1)
        with pytest.raises(ValueError) as cm:
            User(user_id=self.user_id, last_seen=the_future)
        assert "1 validation error for User" in str(cm.value)
        assert "Input is in the future" in str(cm.value)

    def test_after_anno_domini(self):
        from generalresearch.models.thl.user import User

        before_ad = datetime(
            year=2015, month=1, day=1, tzinfo=timezone.utc
        ) + timedelta(minutes=1)
        with pytest.raises(ValueError) as cm:
            User(user_id=self.user_id, last_seen=before_ad)
        assert "1 validation error for User" in str(cm.value)
        assert "Input is before Anno Domini" in str(cm.value)


class TestUserBlocked:
    user_id = randint(1, 2**30)

    def test_valid(self):
        from generalresearch.models.thl.user import User

        user = User(user_id=self.user_id, blocked=True)
        assert user.blocked

    def test_str_casting(self):
        """We don't want any of these to work, and that's why
        we set strict=True on the column"""
        from generalresearch.models.thl.user import User

        with pytest.raises(ValueError) as cm:
            User(user_id=self.user_id, blocked="true")
        assert "1 validation error for User" in str(cm.value)
        assert "Input should be a valid boolean" in str(cm.value)

        with pytest.raises(ValueError) as cm:
            User(user_id=self.user_id, blocked="True")
        assert "1 validation error for User" in str(cm.value)
        assert "Input should be a valid boolean" in str(cm.value)

        with pytest.raises(ValueError) as cm:
            User(user_id=self.user_id, blocked="1")
        assert "1 validation error for User" in str(cm.value)
        assert "Input should be a valid boolean" in str(cm.value)

        with pytest.raises(ValueError) as cm:
            User(user_id=self.user_id, blocked="yes")
        assert "1 validation error for User" in str(cm.value)
        assert "Input should be a valid boolean" in str(cm.value)

        with pytest.raises(ValueError) as cm:
            User(user_id=self.user_id, blocked="no")
        assert "1 validation error for User" in str(cm.value)
        assert "Input should be a valid boolean" in str(cm.value)

        with pytest.raises(ValueError) as cm:
            User(user_id=self.user_id, blocked=uuid4().hex)
        assert "1 validation error for User" in str(cm.value)
        assert "Input should be a valid boolean" in str(cm.value)


class TestUserTiming:
    user_id = randint(1, 2**30)

    def test_valid(self):
        from generalresearch.models.thl.user import User

        created = datetime.now(tz=timezone.utc) - timedelta(minutes=60)
        last_seen = datetime.now(tz=timezone.utc) - timedelta(minutes=59)

        user = User(user_id=self.user_id, created=created, last_seen=last_seen)
        assert user.created == created
        assert user.last_seen == last_seen

    def test_created_first(self):
        from generalresearch.models.thl.user import User

        created = datetime.now(tz=timezone.utc) - timedelta(minutes=60)
        last_seen = datetime.now(tz=timezone.utc) - timedelta(minutes=59)

        with pytest.raises(ValueError) as cm:
            User(user_id=self.user_id, created=last_seen, last_seen=created)
        assert "1 validation error for User" in str(cm.value)
        assert "User created time invalid" in str(cm.value)


class TestUserModelVerification:
    """Tests that may be dependent on more than 1 attribute"""

    def test_identifiable(self):
        from generalresearch.models.thl.user import User

        product_id = uuid4().hex
        product_user_id = uuid4().hex
        user = User(product_id=product_id, product_user_id=product_user_id)
        assert user.is_identifiable

    def test_valid_helper(self):
        from generalresearch.models.thl.user import User

        user_bool = User.is_valid_ubp(
            product_id=uuid4().hex, product_user_id=uuid4().hex
        )
        assert user_bool

        user_bool = User.is_valid_ubp(product_id=uuid4().hex, product_user_id=" - - - ")
        assert not user_bool


class TestUserSerialization:

    def test_basic_json(self):
        from generalresearch.models.thl.user import User

        product_id = uuid4().hex
        product_user_id = uuid4().hex

        user = User(
            product_id=product_id,
            product_user_id=product_user_id,
            created=datetime.now(tz=timezone.utc),
            blocked=False,
        )

        d = json.loads(user.to_json())
        assert d.get("product_id") == product_id
        assert d.get("product_user_id") == product_user_id
        assert not d.get("blocked")

        assert d.get("product") is None
        assert d.get("created").endswith("Z")

    def test_basic_dict(self):
        from generalresearch.models.thl.user import User

        product_id = uuid4().hex
        product_user_id = uuid4().hex

        user = User(
            product_id=product_id,
            product_user_id=product_user_id,
            created=datetime.now(tz=timezone.utc),
            blocked=False,
        )

        d = user.to_dict()
        assert d.get("product_id") == product_id
        assert d.get("product_user_id") == product_user_id
        assert not d.get("blocked")

        assert d.get("product") is None
        assert d.get("created").tzinfo == timezone.utc

    def test_from_json(self):
        from generalresearch.models.thl.user import User

        product_id = uuid4().hex
        product_user_id = uuid4().hex

        user = User(
            product_id=product_id,
            product_user_id=product_user_id,
            created=datetime.now(tz=timezone.utc),
            blocked=False,
        )

        u = User.model_validate_json(user.to_json())
        assert u.product_id == product_id
        assert u.product is None
        assert u.created.tzinfo == timezone.utc


class TestUserMethods:

    def test_audit_log(self, user, audit_log_manager):
        assert user.audit_log is None
        user.prefetch_audit_log(audit_log_manager=audit_log_manager)
        assert user.audit_log == []

        audit_log_manager.create_dummy(user_id=user.user_id)
        user.prefetch_audit_log(audit_log_manager=audit_log_manager)
        assert len(user.audit_log) == 1

    def test_transactions(
        self, user_factory, thl_lm, session_with_tx_factory, product_user_wallet_yes
    ):
        u1 = user_factory(product=product_user_wallet_yes)

        assert u1.transactions is None
        u1.prefetch_transactions(thl_lm=thl_lm)
        assert u1.transactions == []

        session_with_tx_factory(user=u1)

        u1.prefetch_transactions(thl_lm=thl_lm)
        assert len(u1.transactions) == 1

    @pytest.mark.skip(reason="TODO")
    def test_location_history(self, user):
        assert user.location_history is None
