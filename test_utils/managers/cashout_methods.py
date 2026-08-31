from __future__ import annotations

import random
from collections.abc import Callable
from uuid import uuid4

import pytest

from generalresearch.models.thl.wallet import Currency, PayoutType
from generalresearch.models.thl.wallet.cashout_method import (
    CashoutMethod,
    TangoCashoutMethodData,
)


@pytest.fixture(scope="session")
def random_ext_id_factory(base: str = "U02") -> Callable[..., str]:

    def _inner() -> str:
        suffix = random.randint(0, 99999)
        return f"{base}{suffix:05d}"

    return _inner


@pytest.fixture(scope="session")
def example_tango_cashout_methods(
    random_ext_id_factory: Callable[..., str],
) -> list[CashoutMethod]:
    return [
        CashoutMethod(
            id=uuid4().hex,
            last_updated="2021-06-23T20:45:38.239182Z",
            is_live=True,
            type=PayoutType.TANGO,
            ext_id=random_ext_id_factory(),
            name="Safeway eGift Card $25",
            data=TangoCashoutMethodData(
                value_type="fixed", countries=["US"], utid=random_ext_id_factory()
            ),
            user=None,
            image_url="https://d30s7yzk2az89n.cloudfront.net/images/brands/b694446-1200w-326ppi.png",
            original_currency=Currency.USD,
            min_value=2500,
            max_value=2500,
        ),
        CashoutMethod(
            id=uuid4().hex,
            last_updated="2021-06-23T20:45:38.239182Z",
            is_live=True,
            type=PayoutType.TANGO,
            ext_id=random_ext_id_factory(),
            name="Amazon.it Gift Certificate",
            data=TangoCashoutMethodData(
                value_type="variable", countries=["IT"], utid="U006961"
            ),
            user=None,
            image_url="https://d30s7yzk2az89n.cloudfront.net/images/brands/b405753-1200w-326ppi.png",
            original_currency=Currency.EUR,
            min_value=1,
            max_value=10000,
        ),
    ]


# AMT_ASSIGNMENT_CASHOUT_METHOD = CashoutMethod(
#     id=uuid4().hex,
#     last_updated="2021-06-23T20:45:38.239182Z",
#     is_live=True,
#     type=PayoutType.AMT,
#     ext_id=None,
#     name="AMT Assignment",
#     data=AmtCashoutMethodData(),
#     user=None,
#     min_value=1,
#     max_value=5,
# )

# AMT_BONUS_CASHOUT_METHOD = CashoutMethod(
#     id=uuid4().hex,
#     last_updated="2021-06-23T20:45:38.239182Z",
#     is_live=True,
#     type=PayoutType.AMT,
#     ext_id=None,
#     name="AMT Bonus",
#     data=AmtCashoutMethodData(),
#     user=None,
#     min_value=7,
#     max_value=4000,
# )
