from datetime import datetime, timezone
from enum import Enum
from typing import Annotated, Any, Dict, List, Literal, Optional, Union
from uuid import uuid4

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    NonNegativeInt,
    PositiveInt,
    computed_field,
    field_validator,
    model_validator,
)
from typing_extensions import Self

from generalresearch.models.custom_types import (
    AwareDatetimeISO,
    HttpsUrlStr,
    UUIDStr,
    check_valid_uuid,
)
from generalresearch.models.thl.ledger_example import (
    _example_user_tx_adjustment,
    _example_user_tx_bonus,
    _example_user_tx_complete,
    _example_user_tx_payout,
)
from generalresearch.models.thl.pagination import Page
from generalresearch.models.thl.payout_format import (
    PayoutFormatType,
    format_payout_format,
)
from generalresearch.utils.enum import ReprEnumMeta


class Direction(int, Enum, metaclass=ReprEnumMeta):
    """Entries on the debit side will increase debit normal accounts, while
    entries on the credit side will decrease them. Conversely, entries on
    the credit side will increase credit normal accounts, while entries on
    the debit side will decrease them.

    By convention (?), the db will store transactions as debit-normal. For
    a credit-normal account, we should flip the signs.
    """

    CREDIT = -1
    DEBIT = 1


class OrderBy(str, Enum, metaclass=ReprEnumMeta):
    ASC = "ASC"

    DESC = "DESC"


class AccountType(str, Enum, metaclass=ReprEnumMeta):
    # Revenue from BP payment commission
    BP_COMMISSION = "bp_commission"
    # BP wallets (owed balance)
    BP_WALLET = "bp_wallet"
    # User's wallet
    USER_WALLET = "user_wallet"
    # Cash account
    CASH = "cash"
    # Revenue (money coming in)
    REVENUE = "revenue"
    # Expense
    EXPENSE = "expense"
    # Contest wallet (holds money entered into Contests)
    CONTEST_WALLET = "contest_wallet"
    # Line of Credit (LOC) account
    CREDIT_LINE = "credit_line"
    # wxet account operational funds
    WA_WALLET = "wa_wallet"
    # wxet account monies used to fund work
    WA_BUDGET_POOL = "wa_budget_pool"
    # wxet account funds which are being temporarily held
    WA_HELD = "wa_held"
    # wxet account credit line (LOC)
    WA_CREDIT_LINE = "wa_credit_line"


class TransactionMetadataColumns(str, Enum):
    BONUS = "bonus_id"
    # Note: EVENT & EVENT2 represent the same concept. I accidentally made
    # this inconsistent.
    EVENT = "event_payout"
    EVENT2 = "payoutevent"

    PAYOUT_TYPE = "payout_type"
    SOURCE = "source"

    SESSION = "thl_session"
    WALL = "thl_wall"

    TX_TYPE = "tx_type"
    USER = "user"

    CONTEST = "contest"


class TransactionType(str, Enum):
    """These are used in the Ledger to annotate the type of transaction (in
    metadata: tx_type)
    """

    # We receive payment from a marketplace for a task complete
    MP_PAYMENT = "mp_payment"

    # We pay a Brokerage Product for a session complete
    BP_PAYMENT = "bp_payment"

    # A marketplace adjusts the payment for a task complete
    MP_ADJUSTMENT = "mp_adjustment"

    # We adjust the payment to a BP for a session complete
    BP_ADJUSTMENT = "bp_adjustment"

    # We pay out a Brokerage Product their balance
    BP_PAYOUT = "bp_payout"

    # A user is paid (or penalized!) into their wallet balance for some reason,
    #   such as a leaderboard award, or reward for reporting a task. (This
    #   might be called "expenses" in finance reports).
    USER_BONUS = "user_bonus"

    # A transaction is made to plug accounting imbalances
    PLUG = "plug"

    # Transactions for a user requesting redemption of their wallet balance.
    USER_PAYOUT_REQUEST = "user_payout_request"
    USER_PAYOUT_COMPLETE = "user_payout_complete"
    USER_PAYOUT_CANCEL = "user_payout_cancel"

    # User is entering a Contest (typically a Raffle)
    USER_ENTER_CONTEST = "user_enter_contest"
    CLOSE_CONTEST = "close_contest"

    # User won a milestone contest
    USER_MILESTONE = "user_milestone"


class LedgerAccount(BaseModel, validate_assignment=True, frozen=True):
    uuid: UUIDStr = Field(
        default_factory=lambda: uuid4().hex,
        description="A unique identifier for this Ledger Account",
        examples=["c3c3566b5b1b4961b63a5670a2dc923d"],
    )

    display_name: str = Field(
        max_length=64,
        description="Human-readable description of the Ledger Account",
        examples=["BP Wallet c3c3566b5b1b4961b63a5670a2dc923d"],
    )

    qualified_name: str = Field(max_length=255)

    account_type: AccountType = Field(
        description=AccountType.as_openapi(),
        examples=[AccountType.BP_WALLET.value],
    )

    normal_balance: Direction = Field(description=Direction.as_openapi())

    reference_type: Optional[str] = Field(default=None)

    reference_uuid: Optional[UUIDStr] = Field(
        default=None,
        description="The associated Product ID or other parent account that"
        "this Ledger Account is intended to track transactions for."
        "If Wallet mode is enabled, this can also handle tracking"
        "individual users.",
        examples=["61dd0b086fd048518762757612b4a6d3"],
    )

    currency: str = Field(
        default="USD",
        max_length=32,
        description="GRL's Ledger system allows tracking of transactions in"
        "any currency possible. This is useful for tracking"
        "points, stars, coins, or any other currency that may be"
        "used in a Supplier's platform.",
    )

    @model_validator(mode="after")
    def check_qualified_name(self) -> Self:
        assert self.qualified_name.startswith(
            f"{self.currency}:{self.account_type.value}"
        ), "qualified name should start with {currency}:{account_type}"
        return self

    @field_validator("currency", mode="after")
    def check_currency(cls, currency: str) -> str:
        # The currency should be either USD (or "test") or a valid uuid.
        from generalresearch.currency import LedgerCurrency

        if currency not in [e.value for e in LedgerCurrency]:
            check_valid_uuid(currency)
        return currency


class LedgerEntry(BaseModel):
    id: Optional[int] = Field(default=None)

    direction: Direction
    account_uuid: UUIDStr

    amount: PositiveInt = Field(
        lt=2**63 - 1,
        strict=True,
        description="The USDCent amount. A LedgerEntry cannot be made for"
        "0 USDCent and it cannot be negative.",
        examples=[531],
    )

    # This really shouldn't be Optional, but it has to be in order to
    #   instantiate this class before the LedgerTransaction exists
    transaction_id: Optional[int] = Field(default=None)

    @classmethod
    def from_amount(cls, account_uuid: UUIDStr, amount: int):
        if amount > 0:
            return cls(
                direction=Direction.CREDIT,
                amount=amount,
                account_uuid=account_uuid,
            )
        elif amount < 0:
            return cls(
                direction=Direction.DEBIT,
                amount=abs(amount),
                account_uuid=account_uuid,
            )
        else:
            raise ValueError("amount must not be 0")

    # @property
    # def hash(self):
    #     # we'll use this to prevent duplicate transactions.
    #     s = ';'.join(sorted([self.direction, self.account_uuid, self.amount],
    #                         key=lambda x: (x.direction, x.account_uuid, x.amount)))
    #     hash_object = hashlib.sha256(s.encode())
    #     return hash_object.hexdigest()


class LedgerTransaction(BaseModel):
    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    id: Optional[int] = Field(default=None)

    created: AwareDatetimeISO = Field(
        default_factory=lambda: datetime.now(tz=timezone.utc),
        description="When the Transaction (TX) was created into the database."
        "This does not represent the exact time for any action"
        "which may be responsible for this Transaction (TX), and "
        "TX timestamps will likely be a few milliseconds delayed",
    )

    ext_description: Optional[str] = Field(default=None, max_length=255)
    tag: Optional[str] = Field(default=None, max_length=255)
    metadata: Dict[str, str] = Field(default_factory=dict)

    entries: List[LedgerEntry] = Field(
        default_factory=list,
        description="A Transaction (TX) is composed of multiple Entry events.",
    )

    # @property
    # def hash(self):
    #     # we'll use this to prevent duplicate transactions.
    #     metadata_str = ",".join(sorted([f"{k}={v}" for k, v in self.metadata.items()]))
    #     s = ';'.join([metadata_str] + [entry.hash for entry in self.entries])
    #     hash_object = hashlib.sha256(s.encode())
    #     return hash_object.hexdigest()

    @field_validator("created", mode="after")
    @classmethod
    def check_created_future(cls, created: AwareDatetimeISO) -> AwareDatetimeISO:
        """Created should not be in the future. This will mess up
        LedgerAccountStatement / groupby rollups.
        """
        assert (
            datetime.now(tz=timezone.utc) > created
        ), "created cannot be in the future"
        return created

    @field_validator("entries", mode="after")
    @classmethod
    def check_entries(cls, entries: List[LedgerEntry]) -> List[LedgerEntry]:
        """Transactions should enforce double-entry upon creation. Each
        transaction needs to have at least two entries, which, in aggregate,
        must affect credit and debit sides in equal amounts.
        """
        if entries:
            assert len(entries) >= 2, "ledger transaction must have 2 or more entries"
            assert (
                sum(x.amount * x.direction for x in entries) == 0
            ), "ledger entries must balance"
        return entries

    def model_dump_mysql(self, *args, **kwargs) -> Dict[str, Any]:
        d = self.model_dump(mode="json", *args, **kwargs)
        if "created" in d:
            d["created"] = self.created.replace(tzinfo=None)
        return d

    def to_user_tx(
        self, user_account: LedgerAccount, product_id: str, payout_format: str
    ):
        from generalresearch.models.thl.wallet import PayoutType

        d = self.model_dump(include={"created"})
        d["tx_type"] = self.metadata.get("tx_type")
        d["product_id"] = product_id
        d["payout_format"] = payout_format
        debits = [
            x
            for x in self.entries
            if x.direction == Direction.DEBIT and x.account_uuid == user_account.uuid
        ]
        credits = [
            x
            for x in self.entries
            if x.direction == Direction.CREDIT and x.account_uuid == user_account.uuid
        ]

        if d["tx_type"] == TransactionType.USER_PAYOUT_REQUEST.value:
            assert len(debits) == 1
            d["amount"] = debits[0].amount * -1
            d["payout_id"] = self.metadata["payoutevent"]
            payout_type = PayoutType(self.metadata["payout_type"].upper())
            if payout_type == PayoutType.AMT_ASSIGNMENT:
                d["description"] = "HIT Reward"
            elif payout_type == PayoutType.AMT_BONUS:
                d["description"] = "HIT Bonus"
            else:
                raise ValueError(payout_type)
            return UserLedgerTransactionUserPayout.model_validate(d)
        elif d["tx_type"] == TransactionType.BP_PAYMENT.value:
            assert len(credits) == 1
            d["amount"] = credits[0].amount
            d["tsid"] = self.metadata.get("thl_session")
            return UserLedgerTransactionTaskComplete.model_validate(d)
        elif d["tx_type"] == TransactionType.USER_BONUS.value:
            assert len(credits) == 1
            d["amount"] = credits[0].amount
            return UserLedgerTransactionUserBonus.model_validate(d)
        elif d["tx_type"] == TransactionType.BP_ADJUSTMENT.value:
            assert len(debits) == 1 or len(credits) == 1
            if len(debits) == 1:
                # complete -> fail
                d["amount"] = debits[0].amount * -1
            else:
                # fail -> complete
                d["amount"] = credits[0].amount
            d["tsid"] = self.metadata.get("thl_session")
            return UserLedgerTransactionTaskAdjustment.model_validate(d)


class UserLedgerTransaction(BaseModel):
    """
    Represents a LedgerTransaction item that would get shown to a user. This
    is only used in wallet-managed accounts. Everything (especially the
    amount) is w.r.t the user.
    """

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    created: AwareDatetimeISO = Field(description="When the Transaction was created")

    description: str = Field(
        max_length=255, description="External description suitable for UI"
    )

    # This should be a USDCent, but we need to support negative numbers here ...
    amount: int = Field(
        strict=True,
        description=(
            "The net amount affecting the user's wallet, in USDCents. "
            "Positive means the user's balance increased; negative means it decreased."
        ),
        examples=[+500, -250],
    )

    # Needed to generate urls
    product_id: Optional[str] = Field(default=None, exclude=True)
    # Needed to generate amount_string
    payout_format: Optional[PayoutFormatType] = Field(default=None, exclude=True)
    # The balance in this account immediately after this tx.
    # It is optional b/c we'll calculate this from the query
    balance_after: Optional[int] = Field(default=None)

    def create_url(self, product_id: str):
        raise NotImplementedError()

    @computed_field(
        description="A link to where the user can get more details about this transaction",
    )
    def url(self) -> Optional[HttpsUrlStr]:
        if self.product_id is None:
            return None
        return self.create_url(product_id=self.product_id)

    @computed_field(
        description="The 'amount' with the payout_format applied.",
    )
    def amount_string(self) -> Optional[HttpsUrlStr]:
        if self.payout_format is None:
            return None
        return format_payout_format(
            payout_format=self.payout_format, payout_int=self.amount
        )


class UserLedgerTransactionUserPayout(UserLedgerTransaction):
    model_config = ConfigDict(
        extra="forbid",
        validate_assignment=True,
        json_schema_extra=_example_user_tx_payout,
    )

    tx_type: Literal[TransactionType.USER_PAYOUT_REQUEST] = Field(
        default=TransactionType.USER_PAYOUT_REQUEST
    )

    payout_id: UUIDStr = Field(
        description="A unique identifier for the payout",
        examples=["a3848e0a53d64f68a74ced5f61b6eb68"],
    )

    def create_url(self, product_id: str):
        return f"https://fsb.generalresearch.com/{product_id}/cashout/{self.payout_id}/"

    @model_validator(mode="after")
    def validate_amount(self):
        assert self.amount < 0, (
            "In a user payout, the amount should be negative. This represents the user's "
            "wallet balance decreasing because this amount was actually dispersed to them."
        )
        return self


class UserLedgerTransactionUserBonus(UserLedgerTransaction):
    model_config = ConfigDict(
        extra="forbid",
        validate_assignment=True,
        json_schema_extra=_example_user_tx_bonus,
    )

    tx_type: Literal[TransactionType.USER_BONUS] = Field(
        default=TransactionType.USER_BONUS
    )
    description: str = Field(
        max_length=255,
        description="External description suitable for UI",
        default="Compensation Bonus",
    )

    def create_url(self, product_id: str):
        return None

    @model_validator(mode="after")
    def validate_amount(self):
        assert self.amount > 0, f"UserLedgerTransactionUserBonus: {self.amount=}"
        return self


class UserLedgerTransactionTaskComplete(UserLedgerTransaction):
    """
    In a BP with user wallet enabled, the task-complete transaction would have
    line items for both the credit to the bp_wallet_account and credit to
    user_account. This is the user-detail, so we've only caring about the
    user's payment.
    """

    model_config = ConfigDict(
        extra="forbid",
        validate_assignment=True,
        json_schema_extra=_example_user_tx_complete,
    )

    tx_type: Literal[TransactionType.BP_PAYMENT] = Field(
        default=TransactionType.BP_PAYMENT
    )

    description: str = Field(
        max_length=255,
        description="External description suitable for UI",
        default="Task Complete",
    )

    tsid: UUIDStr = Field(
        description="A unique identifier for the session",
        examples=["a3848e0a53d64f68a74ced5f61b6eb68"],
    )

    def create_url(self, product_id: str):
        return f"https://fsb.generalresearch.com/{product_id}/status/{self.tsid}/"

    @model_validator(mode="after")
    def validate_amount(self):
        assert self.amount >= 0, f"UserLedgerTransactionTaskComplete: {self.amount=}"
        return self


class UserLedgerTransactionTaskAdjustment(UserLedgerTransaction):
    model_config = ConfigDict(
        extra="forbid",
        validate_assignment=True,
        json_schema_extra=_example_user_tx_adjustment,
    )

    tx_type: Literal[TransactionType.BP_ADJUSTMENT] = Field(
        default=TransactionType.BP_ADJUSTMENT
    )

    description: str = Field(
        max_length=255,
        description="External description suitable for UI",
        default="Task Adjustment",
    )

    tsid: UUIDStr = Field(
        description="A unique identifier for the session",
        examples=["a3848e0a53d64f68a74ced5f61b6eb68"],
    )

    def create_url(self, product_id: str):
        return f"https://fsb.generalresearch.com/{product_id}/status/{self.tsid}/"


UserLedgerTransactionType = Annotated[
    Union[
        UserLedgerTransactionUserPayout,
        UserLedgerTransactionUserBonus,
        UserLedgerTransactionTaskAdjustment,
        UserLedgerTransactionTaskComplete,
    ],
    Field(discriminator="tx_type"),
]


class UserLedgerTransactionTypeSummary(BaseModel):
    entry_count: NonNegativeInt = Field(default=0)
    min_amount: Optional[int] = Field(
        description="positive or negative USDCent", default=None
    )
    max_amount: Optional[int] = Field(
        description="positive or negative USDCent", default=None
    )
    total_amount: Optional[int] = Field(
        description="positive or negative USDCent", default=None
    )


class UserLedgerTransactionTypesSummary(BaseModel):
    # Each key is a possible value of the TransactionType enum
    bp_adjustment: UserLedgerTransactionTypeSummary = Field(
        default_factory=UserLedgerTransactionTypeSummary
    )
    bp_payment: UserLedgerTransactionTypeSummary = Field(
        default_factory=UserLedgerTransactionTypeSummary
    )
    user_bonus: UserLedgerTransactionTypeSummary = Field(
        default_factory=UserLedgerTransactionTypeSummary
    )
    user_payout_request: UserLedgerTransactionTypeSummary = Field(
        default_factory=UserLedgerTransactionTypeSummary
    )


class UserLedgerTransactions(Page):
    """
    A (paginated) collection that holds transaction models that can be shown to a (wallet-managed) user.
    """

    transactions: List[UserLedgerTransactionType] = Field(default_factory=list)
    # The summary is w.r.t an optional time-filter. The transactions are
    # paginated so the counts won't necesarily match. In other words, the
    # summary is across all transaction in all pages, not this the transactions
    # in this page.
    summary: UserLedgerTransactionTypesSummary = Field()

    @classmethod
    def from_txs(
        cls,
        user_account: LedgerAccount,
        txs: List[LedgerTransaction],
        product_id: str,
        payout_format: str,
        summary: UserLedgerTransactionTypesSummary,
        page: int,
        size: int,
        total: int,
    ):
        user_txs = [
            tx.to_user_tx(
                user_account=user_account,
                product_id=product_id,
                payout_format=payout_format,
            )
            for tx in txs
        ]
        return cls.model_validate(
            {
                "transactions": user_txs,
                "summary": summary,
                "total": total,
                "page": page,
                "size": size,
            }
        )


class LedgerAccountStatement(BaseModel):
    id: Optional[int] = Field(default=None)
    account_uuid: UUIDStr
    filter_str: Optional[str] = Field(default=None)
    effective_at_lower_bound: AwareDatetimeISO
    effective_at_upper_bound: AwareDatetimeISO
    starting_balance: int = Field(lt=2**63 - 1, ge=0)
    ending_balance: int = Field(lt=2**63 - 1, ge=0)
    sql_query: Optional[str] = Field(default=None)
