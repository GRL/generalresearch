from decimal import Decimal

# from generalresearch.models.thl.finance import (
#     POPFinancial,
#     ProductBalances,
# )
# from generalresearch.models.thl.payout import (
#     BrokerageProductPayoutEvent,
#     PayoutEvent,
# )
# from generalresearch.models.thl.product import Product

# _ = (
#     Product,
#     PayoutEvent,
#     BrokerageProductPayoutEvent,
#     ProductBalances,
#     POPFinancial,
# )

# Product.model_rebuild()
# PayoutEvent.model_rebuild()
# BrokerageProductPayoutEvent.model_rebuild()


def decimal_to_int_cents(usd: Decimal | None) -> int | None:
    return round(usd * 100) if usd is not None else None


def int_cents_to_decimal(value: int | None, decimals: int = 2) -> Decimal | None:
    if value is None:
        return None
    return (Decimal(value) / Decimal(100)).quantize(Decimal(10) ** -decimals)
