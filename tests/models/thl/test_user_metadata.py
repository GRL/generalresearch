import pytest

from generalresearch.models import MAX_INT32
from generalresearch.models.thl.user_profile import UserMetadata


class TestUserMetadata:

    def test_default(self):
        # You can initialize it with nothing
        um = UserMetadata()
        assert um.email_address is None
        assert um.email_sha1 is None

    def test_user_id(self):
        # This does NOT validate that the user_id exists. When we attempt a db operation,
        #   at that point it will fail b/c of the foreign key constraint.
        UserMetadata(user_id=MAX_INT32 - 1)

        with pytest.raises(expected_exception=ValueError) as cm:
            UserMetadata(user_id=MAX_INT32)
        assert "Input should be less than 2147483648" in str(cm.value)

    def test_email(self):
        um = UserMetadata(email_address="e58375d80f5f4a958138004aae44c7ca@example.com")
        assert (
            um.email_sha256
            == "fd219d8b972b3d82e70dc83284027acc7b4a6de66c42261c1684e3f05b545bc0"
        )
        assert um.email_sha1 == "a82578f02b0eed28addeb81317417cf239ede1c3"
        assert um.email_md5 == "9073a7a3c21cfd6160d1899fb736cd1c"

        # You cannot set the hashes directly
        with pytest.raises(expected_exception=AttributeError) as cm:
            um.email_md5 = "x" * 32
        # assert "can't set attribute 'email_md5'" in str(cm.value)
        assert "property 'email_md5' of 'UserMetadata' object has no setter" in str(
            cm.value
        )

        # assert it hasn't changed anything
        assert um.email_md5 == "9073a7a3c21cfd6160d1899fb736cd1c"

        # If you update the email, all the hashes change
        um.email_address = "greg@example.com"
        assert um.email_md5 != "9073a7a3c21cfd6160d1899fb736cd1c"
