from generalresearch.models.thl.finance import (
    POPFinancial,
    ProductBalances,
)
from generalresearch.models.thl.ledger import LedgerAccount
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
LedgerAccount.model_rebuild()
PayoutEvent.model_rebuild()
BrokerageProductPayoutEvent.model_rebuild()
