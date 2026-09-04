from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING
from uuid import uuid4

import pytest
from pytest import FixtureRequest as Request

from generalresearch.models.definitions import Source
from generalresearch.models.thl.survey.model import Buyer, Survey

if TYPE_CHECKING:
    from generalresearch.managers.thl.buyer import BuyerManager
    from generalresearch.managers.thl.survey import SurveyManager
    from generalresearch.models.thl.product import (
        PayoutConfig,
        Product,
    )

# === THL ===


@pytest.fixture()
def payout_config(request: Request) -> PayoutConfig:
    from generalresearch.models.thl.product import (
        PayoutConfig,
        PayoutTransformation,
        PayoutTransformationPercentArgs,
    )

    return (
        request.param
        if hasattr(request, "payout_config")
        else PayoutConfig(
            payout_format="${payout/100:.2f}",
            payout_transformation=PayoutTransformation(
                f="payout_transformation_percent",
                kwargs=PayoutTransformationPercentArgs(pct=0.40),
            ),
        )
    )


@pytest.fixture
def product_user_wallet_yes(
    product_factory: Callable[..., Product],
    payout_config: PayoutConfig,
) -> Product:
    from generalresearch.models.thl.product import UserWalletConfig

    return product_factory(
        payout_config=payout_config, user_wallet_config=UserWalletConfig(enabled=True)
    )


@pytest.fixture
def product_user_wallet_no(
    product_factory: Callable[..., Product],
) -> Product:
    from generalresearch.models.thl.product import UserWalletConfig

    return product_factory(user_wallet_config=UserWalletConfig(enabled=False))


@pytest.fixture
def product_amt_true(
    product_factory: Callable[..., Product],
    payout_config: PayoutConfig,
) -> Product:
    from generalresearch.models.thl.product import UserWalletConfig

    return product_factory(
        user_wallet_config=UserWalletConfig(amt=True, enabled=True),
        payout_config=payout_config,
    )


@pytest.fixture(scope="session")
def buyer(buyer_manager: BuyerManager) -> Buyer:
    buyer_code = uuid4().hex
    buyer_manager.bulk_get_or_create(source=Source.TESTING, codes=[buyer_code])
    b = Buyer(
        source=Source.TESTING, code=buyer_code, label=f"test-buyer-{buyer_code[:8]}"
    )
    buyer_manager.update(b)
    return b


@pytest.fixture(scope="session")
def buyer_factory(buyer_manager: BuyerManager) -> Callable[..., Buyer]:

    def _inner() -> Buyer:
        return buyer_manager.bulk_get_or_create(
            source=Source.TESTING, codes=[uuid4().hex]
        )[0]

    return _inner


@pytest.fixture(scope="session")
def survey(survey_manager: SurveyManager, buyer: Buyer) -> Survey:
    s = Survey(source=Source.TESTING, survey_id=uuid4().hex, buyer_code=buyer.code)
    survey_manager.create_bulk([s])
    return s


@pytest.fixture(scope="session")
def survey_factory(
    survey_manager: SurveyManager, buyer_factory: Callable[..., Buyer]
) -> Callable[..., Survey]:

    def _inner(buyer: Buyer | None = None) -> Survey:
        buyer = buyer or buyer_factory()
        s = Survey(
            source=Source.TESTING,
            survey_id=uuid4().hex,
            buyer_code=buyer.code,
            buyer_id=buyer.id,
        )
        survey_manager.create_bulk([s])
        return s

    return _inner
