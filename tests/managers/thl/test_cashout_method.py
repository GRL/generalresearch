import pytest

from generalresearch.models.thl.wallet import PayoutType
from generalresearch.models.thl.wallet.cashout_method import (
    CashMailCashoutMethodData,
    PaypalCashoutMethodData,
    USDeliveryAddress,
)
from test_utils.managers.cashout_methods import (
    EXAMPLE_TANGO_CASHOUT_METHODS,
)


class TestTangoCashoutMethods:

    def test_create_and_get(self, cashout_method_manager, setup_cashoutmethod_db):
        res = cashout_method_manager.filter(payout_types=[PayoutType.TANGO])
        assert len(res) == 2
        cm = [x for x in res if x.ext_id == "U025035"][0]
        assert EXAMPLE_TANGO_CASHOUT_METHODS[0] == cm

    def test_user(
        self, cashout_method_manager, user_with_wallet, setup_cashoutmethod_db
    ):
        res = cashout_method_manager.get_cashout_methods(user_with_wallet)
        # This user ONLY has the two tango cashout methods, no AMT
        assert len(res) == 2


class TestAMTCashoutMethods:

    def test_create_and_get(self, cashout_method_manager, setup_cashoutmethod_db):
        res = cashout_method_manager.filter(payout_types=[PayoutType.AMT])
        assert len(res) == 2

        cm = [x for x in res if x.name == "AMT Assignment"][0]
        assert AMT_ASSIGNMENT_CASHOUT_METHOD == cm

        cm = [x for x in res if x.name == "AMT Bonus"][0]
        assert AMT_BONUS_CASHOUT_METHOD == cm

    def test_user(
        self, cashout_method_manager, user_with_wallet_amt, setup_cashoutmethod_db
    ):
        res = cashout_method_manager.get_cashout_methods(user_with_wallet_amt)
        # This user has the 2 tango, plus amt bonus & assignment
        assert len(res) == 4


class TestUserCashoutMethods:

    def test(self, cashout_method_manager, user_with_wallet, delete_cashoutmethod_db):
        delete_cashoutmethod_db()

        res = cashout_method_manager.get_cashout_methods(user_with_wallet)
        assert len(res) == 0

    def test_cash_in_mail(
        self, cashout_method_manager, user_with_wallet, delete_cashoutmethod_db
    ):
        delete_cashoutmethod_db()

        data = CashMailCashoutMethodData(
            delivery_address=USDeliveryAddress.model_validate(
                {
                    "name_or_attn": "Josh Ackerman",
                    "address": "123 Fake St",
                    "city": "San Francisco",
                    "state": "CA",
                    "postal_code": "12345",
                }
            )
        )
        cashout_method_manager.create_cash_in_mail_cashout_method(
            data=data, user=user_with_wallet
        )

        res = cashout_method_manager.get_cashout_methods(user_with_wallet)
        assert len(res) == 1
        assert res[0].data.delivery_address.postal_code == "12345"

        # try to create the same one again. should just do nothing
        cashout_method_manager.create_cash_in_mail_cashout_method(
            data=data, user=user_with_wallet
        )
        res = cashout_method_manager.get_cashout_methods(user_with_wallet)
        assert len(res) == 1

        # Create with a new address, will create a new one
        data.delivery_address.postal_code = "99999"
        cashout_method_manager.create_cash_in_mail_cashout_method(
            data=data, user=user_with_wallet
        )
        res = cashout_method_manager.get_cashout_methods(user_with_wallet)
        assert len(res) == 2

    def test_paypal(
        self, cashout_method_manager, user_with_wallet, delete_cashoutmethod_db
    ):
        delete_cashoutmethod_db()

        data = PaypalCashoutMethodData(email="test@example.com")
        cashout_method_manager.create_paypal_cashout_method(
            data=data, user=user_with_wallet
        )
        res = cashout_method_manager.get_cashout_methods(user_with_wallet)
        assert len(res) == 1
        assert res[0].data.email == "test@example.com"

        # try to create the same one again. should just do nothing
        cashout_method_manager.create_paypal_cashout_method(
            data=data, user=user_with_wallet
        )
        res = cashout_method_manager.get_cashout_methods(user_with_wallet)
        assert len(res) == 1

        # Create with a new email, will error! must delete the old one first.
        # We can only have one paypal active
        data.email = "test2@example.com"
        with pytest.raises(
            ValueError,
            match="User already has a cashout method of this type. Delete the existing one and try again.",
        ):
            cashout_method_manager.create_paypal_cashout_method(
                data=data, user=user_with_wallet
            )
        res = cashout_method_manager.get_cashout_methods(user_with_wallet)
        assert len(res) == 1

        cashout_method_manager.delete_cashout_method(res[0].id)
        res = cashout_method_manager.get_cashout_methods(user_with_wallet)
        assert len(res) == 0

        cashout_method_manager.create_paypal_cashout_method(
            data=data, user=user_with_wallet
        )
        res = cashout_method_manager.get_cashout_methods(user_with_wallet)
        assert len(res) == 1
        assert res[0].data.email == "test2@example.com"
