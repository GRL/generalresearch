from decimal import Decimal
from typing import Optional

from generalresearch.models.thl.finance import (
    ProductBalances,
    POPFinancial,
)
from generalresearch.models.thl.payout import (
    BrokerageProductPayoutEvent,
    PayoutEvent,
)
from generalresearch.models.thl.product import Product

_ = (
    Product,
    PayoutEvent,
    BrokerageProductPayoutEvent,
    ProductBalances,
    POPFinancial,
)

Product.model_rebuild()
PayoutEvent.model_rebuild()
BrokerageProductPayoutEvent.model_rebuild()


def decimal_to_int_cents(usd: Optional[Decimal]) -> Optional[int]:
    return round(usd * 100) if usd is not None else None


def int_cents_to_decimal(value: Optional[int], decimals: int = 2) -> Optional[Decimal]:
    if value is None:
        return None
    return (Decimal(value) / Decimal(100)).quantize(Decimal(10) ** -decimals)
