from collections.abc import Callable
from decimal import Decimal
from typing import TYPE_CHECKING

import pytest

from generalresearch.currency import USDCent
from generalresearch.models.thl.wallet.definitions import Currency

if TYPE_CHECKING:
    from generalresearch.managers.thl.cashout_method import CashoutMethodManager
    from generalresearch.models.thl.user import User
    from generalresearch.models.thl.wallet.cashout_method import CashoutMethod

def user_with_ten_dollar_minimum(user_with_wallet: User) -> User:
    user_with_wallet.product.user_wallet_config.min_cashout = Decimal("10.00")
    return user_with_wallet


def test_usd_tango_cashout_method_uses_product_minimum(
    cashout_method_manager: CashoutMethodManager,
    user_with_wallet: User,
    delete_cashoutmethod_db: Callable[..., None],
    example_tango_cashout_methods: list[CashoutMethod],
) -> None:
    delete_cashoutmethod_db()
    method = example_tango_cashout_methods[0].model_copy(
        update={
            "data": example_tango_cashout_methods[0].data.model_copy(
                update={"value_type": "variable"}
            ),
            "min_value": 5_00,
            "max_value": 25_00,
        }
    )
    cashout_method_manager.create(method)

    methods = cashout_method_manager.get_user_cashout_methods(
        user=user_with_ten_dollar_minimum(user_with_wallet),
        country_iso="us",
        usd_exchange_rate={},
    )
    result = methods[method.id]

    assert result.usd_exchange_rate == 1.0
    assert result.min_value == 10_00
    assert result.min_value_usd == USDCent(10_00)
    assert result.max_value == 25_00
    assert result.max_value_usd == USDCent(25_00)
    assert result.validate_requested_amount(USDCent(10_00))
    with pytest.raises(ValueError, match="Must be between"):
        result.validate_requested_amount(USDCent(9_99))
    with pytest.raises(ValueError, match="Must be between"):
        result.validate_requested_amount(USDCent(25_01))


def test_eur_tango_cashout_method_converts_product_minimum(
    cashout_method_manager: CashoutMethodManager,
    user_with_wallet: User,
    delete_cashoutmethod_db: Callable[..., None],
    example_tango_cashout_methods: list[CashoutMethod],
) -> None:
    delete_cashoutmethod_db()
    method = example_tango_cashout_methods[1]
    cashout_method_manager.create(method)
    eur_to_usd = 1.14792

    methods = cashout_method_manager.get_user_cashout_methods(
        user=user_with_ten_dollar_minimum(user_with_wallet),
        country_iso="it",
        usd_exchange_rate={Currency.EUR: eur_to_usd},
    )
    result = methods[method.id]

    assert result.usd_exchange_rate == eur_to_usd
    assert result.min_value == round(10_00 / eur_to_usd)
    assert result.min_value_usd == USDCent(10_00)
    assert result.max_value == 100_00
    assert result.max_value_usd == USDCent(round(100_00 * eur_to_usd))

    # $10.00 min = 8.71 EUR which is over the card minimum
    assert result.validate_requested_amount(USDCent(10_00))
    # $101 USD = 87 EURO which is below $100 max
    assert result.validate_requested_amount(USDCent(101_00))

    # $9.99 is below the BP's USD min
    with pytest.raises(ValueError, match="Must be between"):
        result.validate_requested_amount(USDCent(9_99))
    # $115 USD = 100.18 EURO which is over the card's max
    with pytest.raises(ValueError, match="Must be between"):
        result.validate_requested_amount(USDCent(115_00))
