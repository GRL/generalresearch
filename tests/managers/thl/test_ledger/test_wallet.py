from decimal import Decimal
from uuid import uuid4

import pytest

from generalresearch.models.thl.product import (
    UserWalletConfig,
    PayoutConfig,
    PayoutTransformation,
    PayoutTransformationPercentArgs,
)
from generalresearch.models.thl.user import User


@pytest.fixture()
def schrute_product(product_manager):
    return product_manager.create_dummy(
        user_wallet_config=UserWalletConfig(enabled=True, amt=False),
        payout_config=PayoutConfig(
            payout_transformation=PayoutTransformation(
                f="payout_transformation_percent",
                kwargs=PayoutTransformationPercentArgs(pct=0.4),
            ),
            payout_format="{payout:,.0f} Schrute Bucks",
        ),
    )


class TestGetUserWalletBalance:
    def test_get_user_wallet_balance_non_managed(self, user, thl_lm):
        with pytest.raises(
            AssertionError,
            match="Can't get wallet balance on non-managed account.",
        ):
            thl_lm.get_user_wallet_balance(user=user)

    def test_get_user_wallet_balance_managed_0(
        self, schrute_product, user_factory, thl_lm
    ):
        assert (
            schrute_product.payout_config.payout_format == "{payout:,.0f} Schrute Bucks"
        )
        user: User = user_factory(schrute_product)
        balance = thl_lm.get_user_wallet_balance(user=user)
        assert balance == 0
        balance_string = user.product.format_payout_format(Decimal(balance) / 100)
        assert balance_string == "0 Schrute Bucks"
        redeemable_balance = thl_lm.get_user_redeemable_wallet_balance(
            user=user, user_wallet_balance=balance
        )
        assert redeemable_balance == 0
        redeemable_balance_string = user.product.format_payout_format(
            Decimal(redeemable_balance) / 100
        )
        assert redeemable_balance_string == "0 Schrute Bucks"

    def test_get_user_wallet_balance_managed(
        self, schrute_product, user_factory, thl_lm, session_with_tx_factory
    ):
        user: User = user_factory(schrute_product)
        thl_lm.create_tx_user_bonus(
            user=user,
            amount=Decimal(1),
            ref_uuid=uuid4().hex,
            description="cheese",
        )
        session_with_tx_factory(user=user, wall_req_cpi=Decimal("1.23"))

        # This product has a payout xform of 40% and commission of 5%
        # 1.23 * 0.05 = 0.06 of commission
        # 1.17 of payout * 0.40 = 0.47 of user pay and (1.17-0.47) 0.70 bp pay
        balance = thl_lm.get_user_wallet_balance(user=user)
        assert balance == 47 + 100  # plus the $1 bribe

        redeemable_balance = thl_lm.get_user_redeemable_wallet_balance(
            user=user, user_wallet_balance=balance
        )
        assert redeemable_balance == 20 + 100
