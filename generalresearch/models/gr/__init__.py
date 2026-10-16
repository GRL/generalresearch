from generalresearch.models.gr.authentication import GRToken, GRUser
from generalresearch.models.gr.business import Business, BusinessCreate
from generalresearch.models.gr.team import Team, TeamCreate
from generalresearch.models.thl.finance import BusinessBalances
from generalresearch.models.thl.payout import BrokerageProductPayoutEvent
from generalresearch.models.thl.product import Product

# To prevent type checkers from removing these imports from this file
_ = Product, BrokerageProductPayoutEvent, BusinessBalances

GRUser.model_rebuild()
GRToken.model_rebuild()
BusinessCreate.model_rebuild()
Business.model_rebuild()
TeamCreate.model_rebuild()
Team.model_rebuild()
