from itertools import groupby
from random import shuffle as rshuffle

from generalresearch.models.thl.product import (
    UserWalletConfig,
)

from generalresearch.models.thl.wallet import PayoutType


def all_equal(iterable):
    g = groupby(iterable)
    return next(g, True) and not next(g, False)


class TestProductUserWalletConfig:

    def test_init(self):
        instance = UserWalletConfig()

        assert isinstance(instance, UserWalletConfig)

        # Check the defaults
        assert not instance.enabled
        assert not instance.amt

        assert isinstance(instance.supported_payout_types, set)
        assert len(instance.supported_payout_types) == 3

        assert instance.min_cashout is None

    def test_model_dump(self):
        instance = UserWalletConfig()

        # If we use the defaults, the supported_payout_types are always
        #   in the same order because they're the same
        assert isinstance(instance.model_dump_json(), str)
        res = []
        for idx in range(100):
            res.append(instance.model_dump_json())
        assert all_equal(res)

    def test_model_dump_payout_types(self):
        res = []
        for idx in range(100):

            # Generate a random order of PayoutTypes each time
            payout_types = [e for e in PayoutType]
            rshuffle(payout_types)
            instance = UserWalletConfig.model_validate(
                {"supported_payout_types": payout_types}
            )

            res.append(instance.model_dump_json())

        assert all_equal(res)
