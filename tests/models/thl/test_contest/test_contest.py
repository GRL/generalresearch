import pytest
from generalresearch.models.thl.user import User


class TestContest:
    """In many of the Contest related tests, we often want a consistent
    Product throughout, and multiple different users that may be
    involved in the Contest... so redefine the product fixture along with
    some users in here that are scoped="class" so they stay around for
    each of the test functions
    """

    @pytest.fixture(scope="function")
    def user_1(self, user_factory, product) -> User:
        return user_factory(product=product)

    @pytest.fixture(scope="function")
    def user_2(self, user_factory, product) -> User:
        return user_factory(product=product)

    @pytest.fixture(scope="function")
    def user_3(self, user_factory, product) -> User:
        return user_factory(product=product)
