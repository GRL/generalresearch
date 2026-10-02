from __future__ import annotations

import random
from datetime import UTC
from typing import TYPE_CHECKING
from uuid import uuid4

import pandas as pd
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    NonNegativeInt,
    computed_field,
    field_validator,
    model_validator,
)
from pydantic.json_schema import SkipJsonSchema

from generalresearch.config import is_debug
from generalresearch.currency import USDCent
from generalresearch.decorators import LOG
from generalresearch.models.custom_types import AwareDatetimeISO, UUIDStr
from generalresearch.models.thl.definitions import SessionAdjustedStatus
from generalresearch.models.thl.user_identifiers import BPUIDStr

payout_example = random.randint(150, 750 * 100)
adjustment_example = random.randint(-1_000, 50 * 100)

# Keep these boundaries stable so Prometheus can aggregate histograms across
# Products and Grafana can compare the same ranges over time. Values are USD
# cents; ``None`` represents the required +Inf bucket.
USER_WALLET_BALANCE_BUCKETS: tuple[int | None, ...] = (
    -1000_00,
    -500_00,
    -250_00,
    -100_00,
    -50_00,
    -25_00,
    -10_00,
    -5_00,
    -1_00,
    -1,
    0,
    1,
    5,
    1_00,
    2_00,
    3_00,
    4_00,
    5_00,
    6_00,
    7_00,
    8_00,
    9_00,
    10_00,
    15_00,
    20_00,
    25_00,
    30_00,
    40_00,
    50_00,
    75_00,
    100_00,
    250_00,
    500_00,
    None,
)

USER_WALLET_CREDIT_COLUMNS = (
    "bp_payment.CREDIT",
    "bp_adjustment.CREDIT",
    "user_bonus.CREDIT",
    "user_payout_cancel.CREDIT",
    "close_contest.CREDIT",
    "user_milestone.CREDIT",
)
USER_WALLET_DEBIT_COLUMNS = (
    "bp_adjustment.DEBIT",
    "user_bonus.DEBIT",
    "user_payout_request.DEBIT",
    "user_enter_contest.DEBIT",
)

if TYPE_CHECKING:
    from generalresearch.managers.thl.product import ProductManager
    from generalresearch.models.thl.ledger import LedgerAccount
    from generalresearch.models.thl.product import Product


class AdjustmentType(BaseModel):
    amount: int = Field(
        description="The total amount (in USD cents) that the Brokerage Product"
        "has earned within a respective time period from a specific"
        "Source of Tasks."
    )

    adjustment: SessionAdjustedStatus = Field(
        description=SessionAdjustedStatus.as_openapi(),
        examples=[SessionAdjustedStatus.ADJUSTED_TO_FAIL.value],
    )


class POPFinancial(BaseModel):
    """
    We can't use our USDCent class in here because aside from it not
        supporting negative values for our adjustments, FastAPI also
        complains because it doesn't know how to generate documentation
        for it.  - Max 2024-06-25
    """

    # --- Tracking / Tagging ---
    product_id: UUIDStr | None = Field(default=None, examples=[uuid4().hex])

    time: AwareDatetimeISO = Field(
        description="The starting time block for the respective 'Period' that"
        "this grouping is on. The `time` could be the start of a "
        "1 minute or 1 hour block for example."
    )

    # --- Numeric ---

    payout: NonNegativeInt = Field(
        default=0,
        description="The total amount (in USD cents) that the Brokerage Product"
        "has earned within a respective time period.",
        examples=[payout_example],
    )

    adjustment: int = Field(
        description="The total amount (in USD cents) that the Brokerage Product"
        "has had adjusted within a respective time period. Most of"
        "the time, this will be negative due to Complete to "
        "Incomplete reconciliations. However, it can also be "
        "positive due to Incomplete to Complete adjustments.",
        examples=[adjustment_example],
    )

    adjustment_types: list[AdjustmentType] = Field()

    expense: int = Field(
        description="For Product accounts that are setup with Respondent payouts,"
        "competitions, user bonuses, or other associated 'costs', those"
        "expenses are accounted for here. This will be negative for"
        "those types of costs."
    )

    net: int = Field(
        description="This is the sum of the Payout total, Adjustment and any "
        "Expenses total. It can be positive or negative for any "
        "specific time period.",
        examples=[payout_example + adjustment_example],
    )

    payment: int = Field(
        description="Any ACH or Wire amount that was issued between GRL and "
        "the Supplier.",
        examples=[3_408_288],
    )

    @staticmethod
    def list_from_pandas(
        input_data: pd.DataFrame, accounts: list[LedgerAccount]
    ) -> list[POPFinancial]:
        """
        This list can either be for a Product or a Business. The difference
        is that the list of accounts will either be len()=1 (Product) or
        len()>1 (Business), it's also possible that the business only
        has a single Product.

        """
        from generalresearch.config import is_debug
        from generalresearch.incite.schemas.mergers.pop_ledger import (
            numerical_col_names,
        )

        # Validate the input accounts
        assert len(accounts) > 0, "Must provide accounts"
        from generalresearch.models.thl.ledger import (
            AccountType,
            Direction,
        )

        assert all(a.account_type == AccountType.BP_WALLET for a in accounts)
        assert all(a.normal_balance == Direction.CREDIT for a in accounts)
        if not is_debug():
            assert all(a.currency == "USD" for a in accounts)

        if input_data.empty:
            return []

        assert isinstance(input_data.index, pd.MultiIndex)
        assert list(input_data.index.names) == ["time_idx", "account_id"]
        assert input_data.columns.to_list() == numerical_col_names
        uniq_acct_cnt: int = input_data.index.get_level_values(1).unique().size

        # https://grl.sentry.io/issues/5704598444/?project=4507416823332864
        # I changed this to <= because it is (I think) okay to have missing
        # events if there was no period financial activity -- Max 2024-08-12
        assert uniq_acct_cnt <= len(accounts)

        account_product_map = {a.uuid: a.reference_uuid for a in accounts}

        res = []
        for index, row in input_data.reset_index().iterrows():
            index: int  # Not useful, just a RangeIndex
            row: pd.DataFrame

            row["time_idx"] = row.time_idx.to_pydatetime().replace(tzinfo=UTC)
            instance = ProductBalances.from_pandas(row)

            res.append(
                POPFinancial(
                    product_id=account_product_map[row.account_id],
                    time=row.time_idx,
                    payout=instance.payout,
                    adjustment=instance.adjustment,
                    adjustment_types=[
                        AdjustmentType.model_validate(
                            {
                                "adjustment": SessionAdjustedStatus.ADJUSTED_TO_COMPLETE,
                                "amount": instance.adjustment_credit,
                            }
                        ),
                        AdjustmentType.model_validate(
                            {
                                "adjustment": SessionAdjustedStatus.ADJUSTED_TO_FAIL,
                                "amount": instance.adjustment_debit,
                            }
                        ),
                    ],
                    expense=instance.expense,
                    net=instance.net,
                    payment=instance.payment,
                )
            )

        return res


class UserWalletBalances(BaseModel):
    """Cumulative ledger activity and balance for one user's USD wallet.

    All monetary values are integer USD cents and are expressed from the user's
    perspective: credits increase the wallet balance and debits decrease it.
    A positive balance is an outstanding liability for the Brokerage Product;
    a negative balance is tracked separately and must not offset liabilities to
    other users when producing Product-level totals.
    """

    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    product_id: UUIDStr = Field(
        description="Brokerage Product that owns this user wallet.",
        examples=[uuid4().hex],
    )
    product_user_id: BPUIDStr = Field()

    last_event: AwareDatetimeISO | None = Field(
        default=None,
        description=(
            "Timestamp of the most recent ledger event included in these totals, "
            "or null when the wallet has no events."
        ),
    )

    bp_payment_credit: NonNegativeInt = Field(
        default=0,
        validation_alias="bp_payment.CREDIT",
        description="Total task-completion earnings credited to this wallet.",
        examples=[18_837],
    )

    adjustment_credit: NonNegativeInt = Field(
        default=0,
        validation_alias="bp_adjustment.CREDIT",
        description="Total positive task reconciliations credited to this wallet.",
        examples=[2],
    )

    adjustment_debit: NonNegativeInt = Field(
        default=0,
        validation_alias="bp_adjustment.DEBIT",
        description="Total negative task reconciliations debited from this wallet.",
        examples=[753],
    )

    user_bonus_credit: NonNegativeInt = Field(
        default=0,
        validation_alias="user_bonus.CREDIT",
        description="Total non-task bonuses credited to this wallet.",
        examples=[0],
    )

    user_bonus_debit: NonNegativeInt = Field(
        default=0,
        validation_alias="user_bonus.DEBIT",
        description="Total bonus reversals debited from this wallet.",
        examples=[2_745],
    )

    user_payout_request: NonNegativeInt = Field(
        default=0,
        validation_alias="user_payout_request.DEBIT",
        description=(
            "Total payout requests debited from this wallet. These amounts are no "
            "longer part of the unredeemed wallet balance."
        ),
        examples=[18_837],
    )

    user_payout_cancel: NonNegativeInt = Field(
        default=0,
        validation_alias="user_payout_cancel.CREDIT",
        description="Total cancelled payout requests returned to this wallet.",
        examples=[18_837],
    )

    user_enter_contest_debit: NonNegativeInt = Field(
        default=0,
        validation_alias="user_enter_contest.DEBIT",
        description="Total cash entries transferred from this wallet to contests.",
        examples=[500],
    )

    close_contest_credit: NonNegativeInt = Field(
        default=0,
        validation_alias="close_contest.CREDIT",
        description="Total cash prizes credited when contests closed.",
        examples=[2_500],
    )

    user_milestone_credit: NonNegativeInt = Field(
        default=0,
        validation_alias="user_milestone.CREDIT",
        description="Total cash milestone awards credited to this wallet.",
        examples=[1_000],
    )

    @computed_field(description="Total debits that decreased this wallet.")
    @property
    def debit(self) -> int:
        return (
            self.adjustment_debit
            + self.user_bonus_debit
            + self.user_payout_request
            + self.user_enter_contest_debit
        )

    @computed_field(description="Total credits that increased this wallet.")
    @property
    def credit(self) -> int:
        return (
            self.bp_payment_credit
            + self.adjustment_credit
            + self.user_bonus_credit
            + self.user_payout_cancel
            + self.close_contest_credit
            + self.user_milestone_credit
        )

    @computed_field(
        title="Wallet Balance",
        description=(
            "Current wallet balance from the user's perspective. A positive value "
            "is owed to the user; a negative value means the user owes or must earn "
            "back that amount."
        ),
        examples=[5_341],
    )
    @property
    def balance(self) -> int:
        return self.credit + (self.debit * -1)

    @classmethod
    def from_pop_ledger(
        cls,
        input_data: pd.DataFrame,
        product_id: UUIDStr,
        product_user_id: BPUIDStr,
    ) -> UserWalletBalances:
        """Build one user's wallet balance from ``PopLedgerSchema`` rows."""
        amount_columns = [
            *USER_WALLET_CREDIT_COLUMNS,
            *USER_WALLET_DEBIT_COLUMNS,
        ]
        required_columns = {
            "time_idx",
            "product_id",
            "product_user_id",
            *amount_columns,
        }
        missing_columns = required_columns.difference(input_data.columns)
        assert not missing_columns, (
            f"PopLedgerSchema columns missing: {sorted(missing_columns)}"
        )

        wallet_rows = input_data.loc[
            input_data["product_id"].eq(product_id)
            & input_data["product_user_id"].eq(product_user_id)
        ]
        wallet_rows = wallet_rows.loc[
            wallet_rows[amount_columns].sum(axis="columns").gt(0)
        ]

        data = wallet_rows[amount_columns].sum().to_dict()
        data.update(
            product_id=product_id,
            product_user_id=product_user_id,
            last_event=(
                None if wallet_rows.empty else wallet_rows["time_idx"].max()
            ),
        )
        return cls.model_validate(data)


class UserWalletBalanceHistogramBucket(BaseModel):
    """One cumulative bucket in the user-wallet balance distribution.

    Monetary boundaries are integer USD cents. ``upper_bound=None`` represents
    the required +Inf bucket containing every wallet in the distribution.
    """

    upper_bound: int | None = Field(
        description="Inclusive upper boundary, or null for +Inf."
    )
    cumulative_count: NonNegativeInt = Field(
        description="Number of wallets at or below the upper boundary."
    )


def _empty_user_wallet_balance_histogram() -> list[UserWalletBalanceHistogramBucket]:
    return [
        UserWalletBalanceHistogramBucket(upper_bound=bound, cumulative_count=0)
        for bound in USER_WALLET_BALANCE_BUCKETS
    ]


class ProductUserWalletBalances(BaseModel):
    """Aggregate user-wallet activity for one Brokerage Product.

    All monetary values are integer USD cents.

    These totals should be computed directly by the data layer rather than by
    materializing every ``UserWalletBalances`` instance. Products may have many
    thousands of user wallets.

    Positive and negative wallet balances are aggregated separately because
    a negative user balance MUST not reduce the BP's outstanding liability to
    users with positive balances.

    Pending payouts are reported separately because a payout request removes funds
    from the user's wallet before disbursement.
    """

    product_id: UUIDStr = Field(
        description="Brokerage Product represented by this aggregation.",
        examples=[uuid4().hex],
    )

    debit: NonNegativeInt = Field(
        default=0,
        description="Sum of ledger debits across the Product's user wallets.",
    )
    # Note the credit won't equal the net user_task_payments, b/c a user wallet
    #  could also have credits from things such as contests or bribes.
    credit: NonNegativeInt = Field(
        default=0,
        description="Sum of ledger credits across the Product's user wallets.",
    )

    user_task_payment: NonNegativeInt = Field(
        default=0,
        description="Total task-completion payments credited to user wallets.",
    )
    user_task_adjustment_credit: NonNegativeInt = Field(
        default=0,
        description="Total positive task adjustments credited to user wallets.",
    )
    user_task_adjustment_debit: NonNegativeInt = Field(
        default=0,
        description="Total negative task adjustments debited from user wallets.",
    )

    outstanding_liability: NonNegativeInt = Field(
        default=0,
        description="Sum of positive user-wallet balances owed by the Product.",
    )
    negative_balance_total: NonNegativeInt = Field(
        default=0,
        description=(
            "Absolute sum of negative user-wallet balances. This does not reduce "
            "outstanding_liability."
        ),
    )

    positive_wallet_count: NonNegativeInt = Field(
        default=0,
        description="Number of user wallets with a positive balance.",
    )
    zero_wallet_count: NonNegativeInt = Field(
        default=0,
        description="Number of user wallets with a zero balance.",
    )
    negative_wallet_count: NonNegativeInt = Field(
        default=0,
        description="Number of user wallets with a negative balance.",
    )

    wallet_balance_buckets: list[UserWalletBalanceHistogramBucket] = Field(
        default_factory=_empty_user_wallet_balance_histogram,
        description=(
            "Cumulative current-balance buckets for a Prometheus gauge histogram."
        ),
    )

    oldest_event: AwareDatetimeISO | None = Field(
        default=None,
        description=(
            "Oldest wallet-event timestamp included in this aggregation, or null "
            "when no events were included."
        ),
    )
    newest_event: AwareDatetimeISO | None = Field(
        default=None,
        description=(
            "Newest wallet-event timestamp included in this aggregation, or null "
            "when no events were included."
        ),
    )

    # We can't determine the amount per payout method without querying the
    # payout-event table.
    pending_payout_amount: NonNegativeInt = Field(
        default=0,
        description=(
            "Total value of requested payouts that have not completed or been "
            "cancelled."
        ),
    )

    # We cannot determine the pending payout count, only the balance, b/c individual
    # txs are aggregated in the POP ledger. We'd have to query the payout-event table.
    pending_payout_count: NonNegativeInt | None = Field(
        default=None,
        description=(
            "Number of payout requests that have not completed or been cancelled, "
            "or null when the source cannot determine pending status."
        ),
    )

    @computed_field(description="Number of wallets represented by the histogram.")
    @property
    def wallet_balance_count(self) -> int:
        return (
            self.positive_wallet_count
            + self.zero_wallet_count
            + self.negative_wallet_count
        )

    @computed_field(description="Sum of balances represented by the histogram.")
    @property
    def wallet_balance_sum(self) -> int:
        return self.balance

    @model_validator(mode="after")
    def validate_wallet_balance_buckets(self) -> ProductUserWalletBalances:
        boundaries = tuple(bucket.upper_bound for bucket in self.wallet_balance_buckets)
        assert boundaries == USER_WALLET_BALANCE_BUCKETS, (
            "wallet_balance_buckets must use USER_WALLET_BALANCE_BUCKETS"
        )

        counts = [bucket.cumulative_count for bucket in self.wallet_balance_buckets]
        assert counts == sorted(counts), (
            "wallet balance bucket counts must be cumulative"
        )
        assert counts[-1] == self.wallet_balance_count, (
            "+Inf bucket must equal wallet_balance_count"
        )
        return self

    @classmethod
    def from_pop_ledger(
        cls,
        input_data: pd.DataFrame,
        product_id: UUIDStr,
    ) -> ProductUserWalletBalances:
        """Build a current wallet-liability snapshot from ``PopLedgerSchema`` rows.

        The caller can pass rows for multiple Products. This method selects the
        requested Product and rows associated with a user, then collapses the
        minute/account grain to one lifetime balance per ``product_user_id``.

        Pending payout metrics are intentionally left at their defaults. The POP
        ledger merge drops payout identity, so it cannot distinguish individual
        payout requests or reliably determine whether each request is still open.
        """
        required_columns = {
            "time_idx",
            "product_id",
            "product_user_id",
            *USER_WALLET_CREDIT_COLUMNS,
            *USER_WALLET_DEBIT_COLUMNS,
        }
        missing_columns = required_columns.difference(input_data.columns)
        assert not missing_columns, (
            f"PopLedgerSchema columns missing: {sorted(missing_columns)}"
        )

        amount_columns = [
            *USER_WALLET_CREDIT_COLUMNS,
            *USER_WALLET_DEBIT_COLUMNS,
        ]
        wallet_rows = input_data.loc[
            input_data["product_id"].eq(product_id)
            & input_data["product_user_id"].notna()
        ]
        wallet_rows = wallet_rows.loc[
            wallet_rows[amount_columns].sum(axis="columns").gt(0)
        ]
        if wallet_rows.empty:
            return cls(product_id=product_id)

        users = wallet_rows.groupby("product_user_id", observed=True)[
            amount_columns
        ].sum()
        credits = users[list(USER_WALLET_CREDIT_COLUMNS)].sum(axis="columns")
        debits = users[list(USER_WALLET_DEBIT_COLUMNS)].sum(axis="columns")
        balances = credits - debits

        buckets = [
            UserWalletBalanceHistogramBucket(
                upper_bound=upper_bound,
                cumulative_count=(
                    len(balances)
                    if upper_bound is None
                    else int(balances.le(upper_bound).sum())
                ),
            )
            for upper_bound in USER_WALLET_BALANCE_BUCKETS
        ]

        return cls(
            product_id=product_id,
            debit=int(debits.sum()),
            credit=int(credits.sum()),
            user_task_payment=int(users["bp_payment.CREDIT"].sum()),
            user_task_adjustment_credit=int(users["bp_adjustment.CREDIT"].sum()),
            user_task_adjustment_debit=int(users["bp_adjustment.DEBIT"].sum()),
            outstanding_liability=int(balances[balances > 0].sum()),
            negative_balance_total=abs(int(balances[balances < 0].sum())),
            positive_wallet_count=int(balances.gt(0).sum()),
            zero_wallet_count=int(balances.eq(0).sum()),
            negative_wallet_count=int(balances.lt(0).sum()),
            wallet_balance_buckets=buckets,
            oldest_event=wallet_rows["time_idx"].min(),
            newest_event=wallet_rows["time_idx"].max(),
        )

    @computed_field(
        title="Wallet Balance",
        description=(
            "Net balance across all user wallets (credits minus debits). This is a "
            "net position, not the BP's outstanding liability when any individual "
            "wallet has a negative balance."
        ),
        examples=[5_341],
    )
    @property
    def balance(self) -> int:
        return self.credit + (self.debit * -1)


class ProductBalances(BaseModel):
    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    # --- Tracking / Tagging ---
    product_id: UUIDStr | None = Field(default=None, examples=[uuid4().hex])
    last_event: AwareDatetimeISO | None = Field(default=None)

    # --- Numeric ---

    # TODO: will these ever NOT be 0?
    mp_payment_credit: SkipJsonSchema[NonNegativeInt] = Field(
        default=0, exclude=True, validation_alias="mp_payment.CREDIT"
    )
    mp_payment_debit: SkipJsonSchema[NonNegativeInt] = Field(
        default=0, exclude=True, validation_alias="mp_payment.DEBIT"
    )
    mp_adjustment_credit: SkipJsonSchema[NonNegativeInt] = Field(
        default=0, exclude=True, validation_alias="mp_adjustment.CREDIT"
    )
    mp_adjustment_debit: SkipJsonSchema[NonNegativeInt] = Field(
        default=0, exclude=True, validation_alias="mp_adjustment.DEBIT"
    )
    bp_payment_debit: SkipJsonSchema[NonNegativeInt] = Field(
        default=0, exclude=True, validation_alias="bp_payment.DEBIT"
    )
    plug_credit: SkipJsonSchema[NonNegativeInt] = Field(
        default=0, exclude=True, validation_alias="plug.CREDIT"
    )
    plug_debit: SkipJsonSchema[NonNegativeInt] = Field(
        default=0, exclude=True, validation_alias="plug.DEBIT"
    )

    bp_payment_credit: NonNegativeInt = Field(
        default=0,
        validation_alias="bp_payment.CREDIT",
        description="The total amount that has been earned by the Task "
        "completes, for this Brokerage Product account.",
        examples=[18_837],
    )

    adjustment_credit: NonNegativeInt = Field(
        default=0,
        validation_alias="bp_adjustment.CREDIT",
        description="Positive reconciliations issued back to the Brokerage "
        "Product account.",
        examples=[2],
    )

    adjustment_debit: NonNegativeInt = Field(
        default=0,
        validation_alias="bp_adjustment.DEBIT",
        description="Negative reconciliations for any Task completes",
        examples=[753],
    )

    supplier_credit: NonNegativeInt = Field(
        default=0,
        validation_alias="bp_payout.CREDIT",
        description="ACH or Wire amounts issued to GRL from a Supplier to recoup "
        "for a negative Brokerage Product balance",
        examples=[0],
    )

    supplier_debit: NonNegativeInt = Field(
        default=0,
        validation_alias="bp_payout.DEBIT",
        description="ACH or Wire amounts sent to a Supplier",
        examples=[10_000],
    )

    user_bonus_credit: NonNegativeInt = Field(
        default=0,
        validation_alias="user_bonus.CREDIT",
        # TODO: @greg - when would this ever NOT be 0
        description="If a respondent ever pays back an product account.",
        examples=[0],
    )

    user_bonus_debit: NonNegativeInt = Field(
        default=0,
        validation_alias="user_bonus.DEBIT",
        description="Pay a user into their wallet balance. There is no fee "
        "here. There is only a fee when the user requests a payout."
        "The bonus could be as a bribe, winnings for a contest, "
        "leaderboard, etc.",
        examples=[2_745],
    )

    user_payout_complete_debit: NonNegativeInt = Field(
        default=0,
        validation_alias="user_payout_complete.DEBIT",
        description="Payout processing fees charged to the Product.",
    )

    # --- Hidden helper values ---

    issued_payment: NonNegativeInt = Field(
        default=0,
        description="This is the amount that we decide to credit as having"
        "taken from this Product. If there is any amount not issued"
        "it is summed up over the Business to offset any negative"
        "balances elsewhere.",
    )

    # --- Validate ---
    @model_validator(mode="after")
    def check_unknown_fields(self) -> ProductBalances:
        """
        I don't fully understand what these fields are supposed to be
        when looking at bp_wallet accounts. However, I know that they're
        always 0 so far, so let's assert that so it'll fail if they're
        not.. then figure out why...
        """
        val = sum(
            [
                self.mp_payment_credit,
                self.mp_payment_debit,
                self.mp_adjustment_credit,
                self.mp_adjustment_debit,
                self.bp_payment_debit,
                self.plug_credit,
            ]
        )

        if val > 0:
            raise ValueError("review data: unknown field not 0")

        return self

    # --- Properties ---
    @computed_field(
        title="Task Payouts",
        description="The sum amount of all Task payouts",
        examples=[18_837],
        return_type=int,
    )
    @property
    def payout(self) -> int:
        return self.bp_payment_credit

    @computed_field(
        title="Task Payouts USD Str",
        examples=["$18,837.00"],
        return_type=str,
    )
    @property
    def payout_usd_str(self) -> str:
        return USDCent(self.payout).to_usd_str()

    @computed_field(
        title="Task Adjustments",
        description="The sum amount of all Task Adjustments",
        examples=[-751],
        return_type=int,
    )
    @property
    def adjustment(self) -> int:
        return (self.adjustment_credit - self.plug_debit) + (self.adjustment_debit * -1)

    @computed_field(
        title="Product Expenses",
        description="The sum amount of any associated Product Expenses (eg: "
        "user bonuses)",
        examples=[-2_745],
        return_type=int,
    )
    @property
    def expense(self) -> int:
        return self.user_bonus_credit - self.user_bonus_debit - self.user_payout_complete_debit

    # --- Properties: account related ---
    @computed_field(
        title="Net Earnings",
        description="The Product's Net Earnings which is equal to the total"
        "amount of Task Payouts, with Task Adjustments and any"
        "Product Expenses deducted. This can be positive or"
        "negative.",
        examples=[15341],
        return_type=int,
    )
    @property
    def net(self) -> int:
        return self.payout + self.adjustment + self.expense

    @computed_field(
        title="Supplier Payments",
        description="The sum amount of all Supplier Payments (eg ACH or Wire "
        "transfers)",
        examples=[10_000],
        return_type=NonNegativeInt,
    )
    @property
    def payment(self):
        """We'll consider this positive, even though it's really a deduction
        from their balance... they'll want to see it as positive.
        """
        return (self.supplier_credit * -1) + self.supplier_debit

    @computed_field(
        title="Supplier Payments",
        examples=["$10,000"],
        return_type=str,
    )
    @property
    def payment_usd_str(self):
        return USDCent(self.payment).to_usd_str()

    @computed_field(
        title="Product Balance",
        description="The Product's Balance which is equal to the Product's Net"
        "amount with already issued Supplier Payments deducted. "
        "This can be positive or negative.",
        examples=[5_341],
        return_type=int,
    )
    @property
    def balance(self) -> int:
        return self.net + (self.payment * -1)

    @computed_field(
        title="Smart Retainer",
        description="The Smart Retainer is an about of money that is held by"
        "GRL to account for any Task Adjustments that may occur"
        "in the future. The amount will always be positive, and"
        "if the Product's balance is negative, the retainer will "
        "be $0.00 as the Product is not eligible for any Supplier"
        "Payments either way.",
        examples=[1_335],
        return_type=NonNegativeInt,
    )
    @property
    def retainer(self) -> NonNegativeInt:
        if self.balance <= 0:
            # We don't need to show a retainer amount if the account is already
            # in a financial deficit
            return 0

        return abs(int(self.balance * 0.25))

    @computed_field(
        title="Smart Retainer USD Str",
        examples=["$1,335.00"],
        return_type=str,
    )
    @property
    def retainer_usd_str(self) -> str:
        return USDCent(self.retainer).to_usd_str()

    @computed_field(
        title="Available Balance",
        description="The Available Balance is the amount that is currently, and"
        "immediately available for withdraw from the Supplier's"
        "balance. Supplier Payments are made every Friday for "
        "Businesses with an ACH connected Bank Account to GRL, "
        "while a Business that requires an International Wire "
        "are issued on the last Friday of every Month.",
        examples=[4_006],
        return_type=NonNegativeInt,
    )
    @property
    def available_balance(self) -> NonNegativeInt:
        if self.balance <= 0:
            return 0

        ab = self.balance - self.retainer
        if ab <= 0:
            return 0

        return ab

    @computed_field(
        title="Available Balance USD Str",
        examples=["$4,006.00"],
        return_type=str,
    )
    @property
    def available_balance_usd_str(self) -> str:
        return USDCent(self.available_balance).to_usd_str()

    @computed_field(
        title="Recoup",
        description="The amount that must be held back to recover a negative "
        "Product balance. This is $0.00 when the Product balance is "
        "positive or zero; otherwise it is the absolute value of the "
        "negative balance. Business-level payout calculations use this "
        "amount to offset deficits from one Product against available "
        "funds from other Products before issuing Supplier Payments.",
        examples=[282],
        return_type="USDCent",
    )
    @property
    def recoup(self) -> USDCent:
        if self.balance >= 0:
            return USDCent(0)

        return USDCent(abs(self.balance))

    @computed_field(
        title="Recoup Str",
        examples=["$2.04"],
        return_type=str,
    )
    @property
    def recoup_usd_str(self) -> str:
        return self.recoup.to_usd_str()

    # --- Properties: account related ---
    @computed_field(
        title="Adjustment Percentage",
        description="The percentage of USDCent value that has been adjusted"
        "over all time for this Product.",
        examples=[0.064938],
        return_type=float,
    )
    @property
    def adjustment_percent(self) -> float:
        if self.payout <= 0:
            return 0.00

        return abs(self.adjustment) / self.payout

    @staticmethod
    def from_pandas(
        input_data: pd.DataFrame | pd.Series,
    ) -> ProductBalances:
        LOG.debug(f"ProductBalances.from_pandas(input_data={input_data.shape})")

        if isinstance(input_data, pd.Series):
            return ProductBalances.model_validate(input_data.to_dict())

        else:
            assert isinstance(input_data.index, pd.DatetimeIndex), "Invalid input data"

            # The pop merge is grouped by 1min intervals. Therefore, if we take
            #   the maximum of the dt.floor("1min") value and add 1min to it, we
            #   can assume that the parquet files from incite will include any
            #   events up to that timestamp
            pq_last_event_close = input_data.index.max() + pd.Timedelta(minutes=1)

            pb = ProductBalances.model_validate(input_data.sum().to_dict())
            pb.last_event = pq_last_event_close.to_pydatetime()
            return pb

    def __str__(self) -> str:
        return (
            f"Product: {self.product_id or '—'}\n"
            f"Total Payout: ${self.payout / 100:,.2f}\n"
            f"Total Adjustment: ${self.adjustment / 100:,.2f}\n"
            f"Total Expense: ${self.expense / 100:,.2f}\n"
            f"–––\n"
            f"Net: ${self.net / 100:,.2f}\n"
            f"Balance: ${self.balance / 100:,.2f}\n"
            f"Smart Retainer: ${self.retainer / 100:,.2f}\n"
            f"Available Balance: ${self.available_balance / 100:,.2f}"
        ).replace("$-", "-$")


class PrivateProductBalances(ProductBalances):
    """Product balances with internal revenue visible to administrative APIs."""

    commission: int = Field(
        default=0,
        description=(
            "Net commission revenue earned by GRL from this Brokerage Product. "
            "Positive adjustments increase this value and reversals decrease it."
        ),
        examples=[5_038],
    )


class BusinessBalances(BaseModel):
    product_balances: list[ProductBalances] = Field(default_factory=list)

    # --- Validators ---
    @field_validator("product_balances")
    def required_product_ids(cls, v: list[ProductBalances]):
        """The BusinessBalances needs to be able to distinguish between all
        the child Products; in order to do this, we need to assert that
        they all explicitly are set
        """

        if any(pb.product_id is None for pb in v):
            raise ValueError("'product_id' must be set for BusinessBalance children.")

        return v

    # --- Properties ---
    @computed_field(
        title="Task Payouts",
        description="The sum amount of all Task payouts",
        examples=[18_837],
        return_type=int,
    )
    @property
    def payout(self) -> int:
        return sum([i.payout for i in self.product_balances])

    @computed_field(
        title="Task Payouts USD Str",
        examples=["$18,837"],
        return_type=str,
    )
    @property
    def payout_usd_str(self) -> str:
        return USDCent(self.payout).to_usd_str()

    @computed_field(
        title="Task Adjustments",
        description="The sum amount of all Task Adjustments",
        examples=[-751],
        return_type=int,
    )
    @property
    def adjustment(self) -> int:
        adjustment_credit = sum([pb.adjustment_credit for pb in self.product_balances])
        plug_debit = sum([pb.plug_debit for pb in self.product_balances])
        adjustment_debit = sum([pb.adjustment_debit for pb in self.product_balances])

        return (adjustment_credit - plug_debit) + (adjustment_debit * -1)

    @computed_field(
        title="Task Adjustments USD Str",
        examples=["-$2,745.00"],
        return_type=str,
    )
    @property
    def adjustment_usd_str(self) -> str:
        from generalresearch.currency import format_usd_cent

        return format_usd_cent(self.adjustment)

    @computed_field(
        title="Business Expenses",
        description="The sum amount of any associated Business Expenses (eg: "
        "user bonuses)",
        examples=[-2_745],
        return_type=int,
    )
    @property
    def expense(self) -> int:
        user_bonus_credit = sum([pb.user_bonus_credit for pb in self.product_balances])
        user_bonus_debit = sum([pb.user_bonus_debit for pb in self.product_balances])

        return user_bonus_credit + (user_bonus_debit * -1)

    @computed_field(
        title="Business Expenses USD Str",
        examples=["-$2,745.00"],
        return_type=str,
    )
    @property
    def expense_usd_str(self) -> str:
        from generalresearch.currency import format_usd_cent

        return format_usd_cent(self.expense)

    # --- Properties: account related ---
    @computed_field(
        title="Net Earnings",
        description="The Business's Net Earnings which is equal to the total"
        "amount of Task Payouts, with Task Adjustments and any"
        "Product Expenses deducted. This can be positive or"
        "negative.",
        examples=[15341],
        return_type=int,
    )
    @property
    def net(self) -> int:
        return self.payout + self.adjustment + self.expense

    @computed_field(
        title="Net Earnings USD Str",
        examples=["$15,341"],
        return_type=str,
    )
    @property
    def net_usd_str(self) -> str:
        from generalresearch.currency import format_usd_cent

        return format_usd_cent(self.net)

    @computed_field(
        title="Supplier Payments",
        description="The sum amount of all Supplier Payments (eg ACH or Wire "
        "transfers)",
        examples=[10_000],
        return_type=NonNegativeInt,
    )
    @property
    def payment(self):
        """We'll consider this positive, even though it's really a deduction
        from their balance... they'll want to see it as positive.
        """
        supplier_credit = sum([pb.supplier_credit for pb in self.product_balances])
        supplier_debit = sum([pb.supplier_debit for pb in self.product_balances])

        return (supplier_credit * -1) + supplier_debit

    @computed_field(
        title="Supplier Payments USD Str",
        examples=["$10,000.00"],
        return_type=str,
    )
    @property
    def payment_usd_str(self) -> str:
        return USDCent(self.payment).to_usd_str()

    @computed_field(
        title="Business Balance",
        description="The Business's Balance which is equal to the Business's Net"
        "amount with already issued Supplier Payments deducted. "
        "This can be positive or negative.",
        examples=[5_341],
        return_type=int,
    )
    @property
    def balance(self) -> int:
        return self.net + (self.payment * -1)

    @computed_field(
        title="Business Balance USD Str",
        examples=["$5,341.00"],
        return_type=str,
    )
    @property
    def balance_usd_str(self) -> str:
        from generalresearch.currency import format_usd_cent

        return format_usd_cent(self.balance)

    @computed_field(
        title="Smart Retainer",
        description="The Smart Retainer is an about of money that is held by"
        "GRL to account for any Task Adjustments that may occur"
        "in the future. The amount will always be positive, and"
        "if the Business's balance is negative, the retainer will "
        "be $0.00 as the Business is not eligible for any Supplier"
        "Payments either way.",
        examples=[1_335],
        return_type=NonNegativeInt,
    )
    @property
    def retainer(self) -> NonNegativeInt:
        return sum([pb.retainer for pb in self.product_balances])

    @computed_field(
        title="Smart Retainer USD Str",
        examples=["$1,335.00"],
        return_type=str,
    )
    @property
    def retainer_usd_str(self) -> str:
        return USDCent(self.retainer).to_usd_str()

    @computed_field(
        title="Available Balance",
        description="The Available Balance is the amount that is currently, and"
        "immediately available for withdraw from the Supplier's"
        "balance. Supplier Payments are made every Friday for "
        "Businesses with an ACH connected Bank Account to GRL, "
        "while a Business that requires an International Wire "
        "are issued on the last Friday of every Month.",
        examples=[4_006],
        return_type=NonNegativeInt,
    )
    @property
    def available_balance(self) -> NonNegativeInt:
        if self.balance <= 0:
            return 0

        ab = self.balance - self.retainer
        if ab <= 0:
            return 0

        return ab

    @computed_field(
        title="Available Balance USD Str",
        examples=["$4,006.00"],
        return_type=str,
    )
    @property
    def available_balance_usd_str(self) -> str:
        return USDCent(self.available_balance).to_usd_str()

    # --- Properties: account related ---
    @computed_field(
        title="Adjustment Percentage",
        description="The percentage of USDCent value that has been adjusted"
        "over all time for this Product. This is not an aggregation"
        "of each of the children Product Balances, but calculated"
        "across all traffic of the children",
        examples=[0.064938],
        return_type=float,
    )
    @property
    def adjustment_percent(self) -> float:
        if self.payout <= 0:
            return 0.00

        return abs(self.adjustment) / self.payout

    @computed_field(
        title="Business Recoup Hold",
        description="The total amount held back across all child Products to "
        "recover negative Product balances. This is the sum of each "
        "Product's recoup amount and represents funds that should reduce "
        "the Business's payable amount so deficits are recovered before "
        "additional Supplier Payments are issued.",
        examples=[10_000],
        return_type="USDCent",
    )
    @property
    def recoup(self) -> USDCent:
        """Returns the sum of this Business' recouped amount from any
        children Products.
        """
        return USDCent(sum([i.recoup for i in self.product_balances]))

    @computed_field(
        title="Business Recoup Hold Str",
        examples=["$2.04"],
        return_type=str,
    )
    @property
    def recoup_usd_str(self) -> str:
        return self.recoup.to_usd_str()

    # --- Methods ---

    def __str__(self) -> str:
        return (
            f"Products: {len(self.product_balances)}\n"
            f"Total Payout: ${self.payout / 100:,.2f}\n"
            f"Total Adjustment: ${self.adjustment / 100:,.2f}\n"
            f"Total Expense: ${self.expense / 100:,.2f}\n"
            f"–––\n"
            f"Net: ${self.net / 100:,.2f}\n"
            f"Balance: ${self.balance / 100:,.2f}\n"
            f"Smart Retainer: ${self.retainer / 100:,.2f}\n"
            f"Available Balance: ${self.available_balance / 100:,.2f}"
        ).replace("$-", "-$")

    # --- Methods ---
    @staticmethod
    def from_pandas(
        input_data: pd.DataFrame,
        accounts: list[LedgerAccount],
        product_manager: ProductManager,
    ) -> BusinessBalances:
        LOG.debug(f"BusinessBalances.from_pandas(input_data={input_data.shape})")

        from generalresearch.incite.schemas.mergers.pop_ledger import (
            numerical_col_names,
        )
        from generalresearch.models.thl.ledger import (
            AccountType,
            Direction,
        )

        # Validate the input accounts
        assert len(accounts) > 0, "Must provide accounts"
        assert all(a.account_type == AccountType.BP_WALLET for a in accounts)
        assert all(a.normal_balance == Direction.CREDIT for a in accounts)

        if not is_debug():
            assert all(a.currency == "USD" for a in accounts)

        # Validate the input dataframe
        assert input_data.index.name == "account_id"
        assert len(input_data.index) <= len(accounts)
        assert input_data.columns.to_list() == numerical_col_names

        account_product_map = {a.uuid: a.reference_uuid for a in accounts}

        product_balances = []
        for account_id, series in input_data.iterrows():
            pb = ProductBalances.from_pandas(series)
            pb.product_id = account_product_map[account_id]
            product_balances.append(pb)

        # Sort the ProductBalances so that they're always in a consistent
        #   sorted order.

        products: list[Product] = product_manager.get_by_uuids(
            product_uuids=[pb.product_id for pb in product_balances]
        )
        sorted_products_uuids = [
            p.uuid for p in sorted(products, key=lambda x: x.created)
        ]
        product_uuid_order = {
            value: idx for idx, value in enumerate(sorted_products_uuids)
        }
        product_balances = sorted(
            product_balances, key=lambda pb: product_uuid_order[pb.product_id]
        )

        return BusinessBalances.model_validate({"product_balances": product_balances})
