from enum import Enum

from generalresearch.utils.enum import ReprEnumMeta


class PayoutType(str, Enum, metaclass=ReprEnumMeta):
    """
    The method in which the requested payout is delivered.
    """

    # The max size of the db field that holds this value is 14, so please
    #     don't add new values longer than that!

    # User is paid out to their personal PayPal email address
    PAYPAL = "PAYPAL"
    # User is paid out via a Tango Gift Card
    TANGO = "TANGO"
    # DWOLLA
    DWOLLA = "DWOLLA"
    # A payment is made to a bank account using ACH
    ACH = "ACH"
    # A payment is made to a bank account using ACH
    WIRE = "WIRE"
    # A payment is made in cash and mailed to the user.
    CASH_IN_MAIL = "CASH_IN_MAIL"
    # A payment is made as a prize with some monetary value
    PRIZE = "PRIZE"

    # This is used to designate either AMT_BONUS or AMT_HIT
    AMT = "AMT"
    # Amazon Mechanical Turk as a Bonus
    AMT_BONUS = "AMT_BONUS"
    # Amazon Mechanical Turk for a HIT
    AMT_HIT = "AMT_ASSIGNMENT"
    AMT_ASSIGNMENT = "AMT_ASSIGNMENT"


class Currency(str, Enum):
    # United States Dollar
    USD = "USD"
    # Canadian Dollar
    CAD = "CAD"
    # British Pound Sterling
    GBP = "GBP"
    # Euro
    EUR = "EUR"
    # Indian Rupee
    INR = "INR"
    # Australian Dollar
    AUD = "AUD"
    # Polish Zloty
    PLN = "PLN"
    # Swedish Krona
    SEK = "SEK"
    # Singapore Dollar
    SGD = "SGD"
    # Mexican Peso
    MXN = "MXN"


CURRENCY_FORMATTER = {
    "USD": lambda x: f"${x / 100:,.2f}",
    "CAD": lambda x: f"${x / 100:,.2f} CAD",
    "GBP": lambda x: f"{x / 100:,.2f} £",
    "EUR": lambda x: f"€{x / 100:,.2f}",
    "INR": lambda x: f"₹{x / 100:,.2f}",
    "AUD": lambda x: f"${x / 100:,.2f} AUD",
    "PLN": lambda x: f"{x / 100:,.2f} zł",
    "SEK": lambda x: f"{x / 100:,.2f} kr",
    "SGD": lambda x: f"${x / 100:,.2f} SGD",
    "MXN": lambda x: f"${x / 100:,.2f} MXN",
}

# The max value user can redeem in one go in foreign currencies. should be < $250
#   in order to avoid exchange rate issues
CURRENCY_MAX_VALUE = {
    "USD": 250,
    "CAD": 200,
    "GBP": 100,
    "EUR": 100,
    "INR": 10000,
    "AUD": 200,
    "PLN": 500,
    "SEK": 1000,
    "SGD": 200,
    "MXN": 4000,
}
