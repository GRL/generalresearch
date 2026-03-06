import logging
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from typing import Optional, Callable, Collection, List, TYPE_CHECKING
from uuid import UUID

import numpy as np
import pandas as pd
from pydantic import AwareDatetime, PositiveInt

from generalresearch.config import (
    JAMES_BILLINGS_BPID,
    JAMES_BILLINGS_TX_CUTOFF,
)
from generalresearch.currency import USDCent
from generalresearch.managers.base import Permission
from generalresearch.managers.thl.ledger_manager.conditions import (
    generate_condition_mp_payment,
    generate_condition_bp_payment,
    generate_condition_bp_payout,
    generate_condition_user_payout_request,
    generate_condition_user_payout_action,
    generate_condition_tag_exists,
    generate_condition_enter_contest,
)
from generalresearch.managers.thl.ledger_manager.ledger import (
    LedgerManager,
)
from generalresearch.models.custom_types import UUIDStr
from generalresearch.models.thl.contest.contest import Contest
from generalresearch.models.thl.contest.definitions import (
    ContestPrizeKind,
    ContestType,
)
from generalresearch.models.thl.contest.milestone import MilestoneContest
from generalresearch.models.thl.contest.raffle import (
    ContestEntry,
    ContestEntryType,
    RaffleContest,
)
from generalresearch.models.thl.ledger import (
    LedgerAccount,
    Direction,
    LedgerTransaction,
    LedgerEntry,
    AccountType,
    TransactionType,
    TransactionMetadataColumns as tmc,
    UserLedgerTransactions,
)
from generalresearch.models.thl.payout import UserPayoutEvent
from generalresearch.models.thl.product import Product
from generalresearch.models.thl.session import Status, Session, Wall
from generalresearch.models.thl.user import User
from generalresearch.models.thl.wallet import PayoutType

if TYPE_CHECKING:
    from generalresearch.models.thl.contest.contest import ContestWinner

logging.basicConfig()
logger = logging.getLogger("LedgerManager")
logger.setLevel(logging.INFO)


class ThlLedgerManager(LedgerManager):

    def get_account_or_create_user_wallet(self, user: User) -> LedgerAccount:
        """
        TODO: In the future we could create a user wallet account with a
            currency other than USD (or test). This would be determined
            by some BP config
        """

        assert user.user_id, "User must be saved"

        account = LedgerAccount(
            display_name=f"User Wallet {user.uuid}",
            qualified_name=f"{self.currency.value}:user_wallet:{user.uuid}",
            normal_balance=Direction.CREDIT,
            account_type=AccountType.USER_WALLET,
            reference_type="user",
            reference_uuid=user.uuid,
            currency=self.currency,
        )

        return self.get_account_or_create(account=account)

    def get_account_or_create_bp_wallet_by_uuid(
        self, product_uuid: UUIDStr
    ) -> LedgerAccount:
        assert UUID(product_uuid).hex == product_uuid, "Must provide a product_uuid"
        account = LedgerAccount(
            display_name=f"BP Wallet {product_uuid}",
            qualified_name=f"{self.currency.value}:bp_wallet:{product_uuid}",
            normal_balance=Direction.CREDIT,
            account_type=AccountType.BP_WALLET,
            reference_type="bp",
            reference_uuid=product_uuid,
            currency=self.currency,
        )

        return self.get_account_or_create(account=account)

    def get_account_or_create_bp_wallet(self, product: Product) -> LedgerAccount:
        assert isinstance(product, Product), "Must provide a Product"
        return self.get_account_or_create_bp_wallet_by_uuid(product_uuid=product.uuid)

    def get_account_or_create_bp_commission_by_uuid(
        self, product_uuid: UUIDStr
    ) -> LedgerAccount:
        assert UUID(product_uuid).hex == product_uuid, "Must provide a product_uuid"
        account = LedgerAccount(
            display_name=f"Revenue from commission {product_uuid}",
            qualified_name=f"{self.currency.value}:revenue:bp_commission:{product_uuid}",
            normal_balance=Direction.CREDIT,
            account_type=AccountType.REVENUE,
            reference_type="bp",
            reference_uuid=product_uuid,
            currency=self.currency,
        )
        return self.get_account_or_create(account=account)

    def get_account_or_create_bp_commission(self, product: Product) -> LedgerAccount:
        assert isinstance(product, Product), "Must provide a Product"
        return self.get_account_or_create_bp_commission_by_uuid(
            product_uuid=product.uuid
        )

    def get_account_or_create_bp_expense(
        self, product: Product, expense_name: str
    ) -> LedgerAccount:
        """
            Used exclusively for BP with managed user wallets. This account
                tracks expenses associated with a BP, for e.g. 20% fee paid to
                Amazon / Tango to issue gift cards / paypal.

        :param product: Product
        :param expense_name: should be one of {'amt', 'tango', 'paypal'}. Could
            grow as more payout methods are supported.
        """
        return self.get_account_or_create_bp_expense_by_uuid(
            product_uuid=product.uuid, expense_name=expense_name
        )

    def get_account_or_create_bp_expense_by_uuid(
        self, product_uuid: UUIDStr, expense_name: str
    ) -> LedgerAccount:

        account = LedgerAccount(
            display_name=f"Expense {expense_name} {product_uuid}",
            qualified_name=f"{self.currency.value}:expense:{expense_name}:{product_uuid}",
            normal_balance=Direction.DEBIT,
            account_type=AccountType.EXPENSE,
            reference_type="bp",
            reference_uuid=product_uuid,
            currency=self.currency,
        )

        return self.get_account_or_create(account=account)

    def get_account_or_create_contest_wallet_by_uuid(
        self, contest_uuid: UUIDStr
    ) -> LedgerAccount:
        assert UUID(contest_uuid).hex == contest_uuid, "Must provide a contest_uuid"
        account = LedgerAccount(
            display_name=f"Contest Wallet {contest_uuid}",
            qualified_name=f"{self.currency.value}:{AccountType.CONTEST_WALLET.value}:{contest_uuid}",
            normal_balance=Direction.CREDIT,
            account_type=AccountType.CONTEST_WALLET,
            reference_type="contest",
            reference_uuid=contest_uuid,
            currency=self.currency,
        )

        return self.get_account_or_create(account=account)

    def get_account_or_create_contest_wallet(
        self, contest: RaffleContest
    ) -> "LedgerAccount":
        assert isinstance(contest, RaffleContest), "Must provide a RaffleContest"
        return self.get_account_or_create_contest_wallet_by_uuid(
            contest_uuid=contest.uuid
        )

    def get_or_create_bp_pending_payout_account(
        self, product: "Product"
    ) -> "LedgerAccount":
        """
        Used exclusively for BP with managed user wallets. This account
            holds funds that a BP's users have requested as payouts but are
            still pending. Once the payout request is approved, the funds
            move from here into an expense / cash account.
        """

        assert Permission.CREATE in self.permissions

        account = LedgerAccount(
            display_name=f"BP Wallet Pending {product.id}",
            qualified_name=f"{self.currency.value}:bp_wallet:pending:{product.id}",
            normal_balance=Direction.CREDIT,
            account_type=AccountType.BP_WALLET,
            reference_type="bp",
            reference_uuid=product.id,
            currency=self.currency,
        )

        return self.get_account_or_create(account=account)

    def get_account_task_complete_revenue(self) -> "LedgerAccount":
        return self.get_account(
            qualified_name=f"{self.currency.value}:revenue:task_complete"
        )

    def get_account_cash(self) -> "LedgerAccount":
        return self.get_account(qualified_name=f"{self.currency.value}:cash")

    def get_accounts_bp_wallet_for_products(
        self, product_uuids: Collection[UUIDStr]
    ) -> Collection[LedgerAccount]:
        accounts = self.get_account_many(
            qualified_names=[
                f"{self.currency.value}:bp_wallet:{p_uuid}" for p_uuid in product_uuids
            ]
        )
        assert len(accounts) == len(product_uuids)

        return accounts

    def get_tx_bp_payouts(
        self,
        account_uuids: Collection[UUIDStr],
        time_start: Optional[datetime] = None,
        time_end: Optional[datetime] = None,
    ):
        if time_start is None:
            time_start = datetime(year=2017, month=1, day=1, tzinfo=timezone.utc)

        if time_end is None:
            time_end = datetime.now(tz=timezone.utc)

        assert all(
            isinstance(item, str) for item in account_uuids
        ), "Must pass account_uuid as str"

        params = {
            "time_start": time_start,
            "time_end": time_end,
            "tag_like": f"{self.currency.value}:bp_payout:%",
            "account_uuids": list(account_uuids),
        }
        query = """
            SELECT lt.id, lt.tag, lt.created
            FROM ledger_transaction AS lt
            JOIN ledger_entry le ON lt.id = le.transaction_id
            WHERE lt.created BETWEEN %(time_start)s AND %(time_end)s
                AND tag LIKE %(tag_like)s
                AND account_id = ANY(%(account_uuids)s);
        """
        return self.pg_config.execute_sql_query(query=query, params=params)

    def create_tx_task_complete(
        self,
        wall: Wall,
        user: User,
        created: Optional[datetime] = None,
        force=False,
    ) -> PositiveInt:
        """
        Create a transaction when we complete a task from a marketplace,
            showing the marketplace paying us for the task complete.

        :param wall: the wall event that was completed
        :param user: user who completed this wall event
        :param created: should only be used for back-fill / testing.
            Otherwise, == datetime.now()
        :param force: If True, we skip the flag check to allow for retry of
            a failed previous call. The locking and condition check still runs.
        """
        f = lambda: self.create_tx_task_complete_(wall=wall, user=user, created=created)

        condition = generate_condition_mp_payment(wall=wall)
        lock_key = f"{self.currency.value}:thl_wall:{wall.uuid}"

        return self.create_tx_protected(
            lock_key=lock_key,
            condition=condition,
            create_tx_func=f,
            skip_flag_check=force,
        )

    def create_tx_task_complete_(
        self, wall: Wall, user: User, created: Optional[datetime] = None
    ) -> LedgerTransaction:

        revenue_account = self.get_account_task_complete_revenue()
        cash_account = self.get_account_cash()
        metadata = {
            tmc.USER: user.uuid,
            tmc.WALL: wall.uuid,
            tmc.SOURCE: wall.source,
            tmc.TX_TYPE: TransactionType.MP_PAYMENT,
        }
        # This tag should uniquely identify this transaction (which should only happen once!)
        tag = f"{self.currency.value}:mp_payment:{wall.uuid}"
        amount = round(wall.cpi * 100)
        entries = [
            LedgerEntry(
                direction=Direction.CREDIT,
                account_uuid=revenue_account.uuid,
                amount=amount,
            ),
            LedgerEntry(
                direction=Direction.DEBIT,
                account_uuid=cash_account.uuid,
                amount=amount,
            ),
        ]
        ext_description = f"Task Complete {wall.source.name} {wall.survey_id}"
        t = self.create_tx(
            entries=entries,
            metadata=metadata,
            tag=tag,
            ext_description=ext_description,
            created=created,
        )

        return t

    def create_tx_bp_payment(
        self, session: Session, created: Optional[datetime] = None, force=False
    ) -> LedgerTransaction:
        """
            Create a transaction when we decide to report a session as complete
            and make a payment to the BP and optionally to the user's wallet.

        :param session: the session event that was completed
        :param created: should only be used for back-fill / testing.
            Otherwise, == datetime.now()
        :param force: If True, we skip the flag check to allow for retry of a
            failed previous call. The locking and condition check still runs.
        """
        assert session.status == Status.COMPLETE
        assert session.payout > 0, "call session.determine_payments() first"

        f = lambda: self.create_tx_bp_payment_(session=session, created=created)

        condition = generate_condition_bp_payment(session)
        lock_key = f"{self.currency.value}:thl_session:{session.uuid}"

        return self.create_tx_protected(
            lock_key=lock_key,
            condition=condition,
            create_tx_func=f,
            skip_flag_check=force,
        )

    def create_tx_bp_payment_(
        self, session: Session, created: Optional[datetime] = None
    ) -> LedgerTransaction:
        user = session.user
        assert user.product, "user.prefetch_product()"
        assert session.payout > 0, "call session.determine_payments() first"
        assert session.wall_events, "set session.wall_events first"

        metadata = {
            tmc.USER: user.uuid,
            tmc.SESSION: session.uuid,
            tmc.TX_TYPE: TransactionType.BP_PAYMENT,
        }
        # This tag should uniquely identify this transaction (which should only happen once!)
        tag = f"{self.currency.value}:bp_payment:{session.uuid}"
        revenue_account = self.get_account_task_complete_revenue()
        bp_wallet_account = self.get_account_or_create_bp_wallet(user.product)
        bp_commission_account = self.get_account_or_create_bp_commission(user.product)

        # Don't use session.determine_payments() here, b/c during back-pop this may be changed
        thl_net = Decimal(
            sum(wall.cpi for wall in session.wall_events if wall.is_visible_complete())
        )
        thl_net = round(thl_net * 100)
        bp_pay = round(session.payout * 100)
        user_pay = (
            round(session.user_payout * 100) if session.user_payout is not None else 0
        )
        if bp_pay > thl_net:
            # There are back-population issues (e.g. 5afcf8063ccb4662902ac727c2471202)
            #   were we paid the BP $0.39 for a $0.385 cpi complete. This is
            #   wrong because the round algorithm we use is HALF_EVEN, and so
            #   0.385 should round to 0.38.
            #   https://en.wikipedia.org/wiki/Rounding#Rounding_half_to_even
            logger.warning(
                f"bp_pay {bp_pay} > thl_net {thl_net}. Capping bp_pay to thl_net."
            )
            bp_pay = thl_net
            if user_pay > bp_pay:
                user_pay = bp_pay

        commission_amount = round(thl_net - bp_pay)

        entries = [
            LedgerEntry(
                direction=Direction.DEBIT,
                account_uuid=revenue_account.uuid,
                amount=thl_net,
            )
        ]

        if commission_amount:
            entries.append(
                LedgerEntry(
                    direction=Direction.CREDIT,
                    account_uuid=bp_commission_account.uuid,
                    amount=commission_amount,
                )
            )

        if user.product.user_wallet_enabled:
            bp_pay -= user_pay
            user_account = self.get_account_or_create_user_wallet(user)

            if bp_pay:
                entries.append(
                    LedgerEntry(
                        direction=Direction.CREDIT,
                        account_uuid=bp_wallet_account.uuid,
                        amount=bp_pay,
                    )
                )

            if user_pay:
                entries.append(
                    LedgerEntry(
                        direction=Direction.CREDIT,
                        account_uuid=user_account.uuid,
                        amount=user_pay,
                    )
                )
            ext_description = f"BP & User Payment {session.uuid}"

        else:
            entries.append(
                LedgerEntry(
                    direction=Direction.CREDIT,
                    account_uuid=bp_wallet_account.uuid,
                    amount=bp_pay,
                )
            )
            ext_description = f"BP Payment {session.uuid}"

        t = self.create_tx(
            entries=entries,
            metadata=metadata,
            tag=tag,
            ext_description=ext_description,
            created=created,
        )

        return t

    def create_tx_task_adjustment(
        self, wall: Wall, user: User, created: Optional[datetime] = None
    ) -> Optional[LedgerTransaction]:
        """
        How is this different then create_tx_bp_adjustment

        """

        if created is None:
            created = wall.adjusted_timestamp

        revenue_account = self.get_account_task_complete_revenue()
        cash_account = self.get_account_cash()
        metadata = {
            tmc.USER: user.uuid,
            tmc.WALL: wall.uuid,
            tmc.SOURCE: wall.source,
            tmc.TX_TYPE: TransactionType.MP_ADJUSTMENT,
        }
        # This tag may not uniquely identify this tx, b/c it could get adjusted multiple times.
        tag = f"{self.currency.value}:mp_adjustment:{wall.uuid}"
        new_amount = round(wall.get_cpi_after_adjustment() * 100)
        current_amount = self.get_account_filtered_balance(
            account=revenue_account,
            metadata_key="thl_wall",
            metadata_value=wall.uuid,
        )
        change_amount = new_amount - current_amount

        if change_amount > 0:
            # Fail -> Complete: new_amt = 1, current_amt = 0, change = 1
            logger.info(
                f"create_transaction_task_adjustment. current_amt: {current_amount}, new:amt: {new_amount}"
            )
            entries = [
                LedgerEntry(
                    direction=Direction.CREDIT,
                    account_uuid=revenue_account.uuid,
                    amount=change_amount,
                ),
                LedgerEntry(
                    direction=Direction.DEBIT,
                    account_uuid=cash_account.uuid,
                    amount=change_amount,
                ),
            ]

        elif change_amount < 0:
            # Complete -> Fail: new_amt = 0, current_amt = 1, change = -1
            logger.info(
                f"create_transaction_task_adjustment. current_amt: {current_amount}, new:amt: {new_amount}"
            )
            entries = [
                LedgerEntry(
                    direction=Direction.DEBIT,
                    account_uuid=revenue_account.uuid,
                    amount=abs(change_amount),
                ),
                LedgerEntry(
                    direction=Direction.CREDIT,
                    account_uuid=cash_account.uuid,
                    amount=abs(change_amount),
                ),
            ]

        else:
            logger.info(f"create_transaction_task_adjustment. No transactions needed.")
            return None

        amt_str = f"${abs(change_amount) / 100:,.2f}"
        amt_str = amt_str if change_amount > 0 else "-" + amt_str
        ext_description = (
            f"Task Adjustment {amt_str} {wall.source.name} {wall.survey_id}"
        )

        t = self.create_tx(
            entries=entries,
            metadata=metadata,
            tag=tag,
            ext_description=ext_description,
            created=created,
        )

        return t

    def create_tx_bp_adjustment(
        self, session: Session, created: Optional[datetime] = None
    ) -> Optional[LedgerTransaction]:
        """
        How is this different then create_tx_task_adjustment
        """

        if created is None:
            created = session.adjusted_timestamp
        user = session.user
        assert user.product, "user.prefetch_product()"
        metadata = {
            tmc.USER: user.uuid,
            tmc.SESSION: session.uuid,
            tmc.TX_TYPE: TransactionType.BP_ADJUSTMENT,
        }
        # This tag may not uniquely identify this tx, b/c it could get adjusted multiple times.
        tag = f"{self.currency.value}:bp_adjustment:{session.uuid}"
        revenue_account = self.get_account_task_complete_revenue()
        bp_wallet_account = self.get_account_or_create_bp_wallet(product=user.product)
        bp_commission_account = self.get_account_or_create_bp_commission(
            product=user.product
        )

        new_payout = round(session.get_payout_after_adjustment() * 100)
        thl_net = session.get_thl_net()
        new_commission = round(user.product.determine_bp_commission(thl_net) * 100)

        current_commission = self.get_account_filtered_balance(
            account=bp_commission_account,
            metadata_key="thl_session",
            metadata_value=session.uuid,
        )
        change_commission = new_commission - current_commission
        logger.info(
            [
                "commissions: ",
                new_commission,
                current_commission,
                change_commission,
            ]
        )

        user_amt_str = ""
        if user.product.user_wallet_enabled:
            # If the user wallet is enabled, the user_payout "comes out" of
            #   the payout
            payout_after_adj: Optional[Decimal] = (
                session.get_user_payout_after_adjustment()
            )
            if payout_after_adj is None:
                logger.info("session.get_user_payout_after_adjustment() return None")
                return None

            new_user_payout = round(payout_after_adj * 100)
            new_bp_payout = new_payout - new_user_payout
            current_bp_payout = self.get_account_filtered_balance(
                account=bp_wallet_account,
                metadata_key="thl_session",
                metadata_value=session.uuid,
            )
            user_account = self.get_account_or_create_user_wallet(user)
            current_user_payout = self.get_account_filtered_balance(
                account=user_account,
                metadata_key="thl_session",
                metadata_value=session.uuid,
            )
            change_bp_payout = new_bp_payout - current_bp_payout
            change_user_payout = new_user_payout - current_user_payout
            logger.info(
                f"changes: {change_bp_payout}, {change_user_payout}, {change_commission}"
            )
            user_amt_str = f"${abs(change_user_payout) / 100:,.2f}"
            user_amt_str = (
                user_amt_str if change_user_payout > 0 else "-" + user_amt_str
            )
            if change_bp_payout != 0:
                entries = [
                    LedgerEntry.from_amount(
                        account_uuid=revenue_account.uuid,
                        amount=(
                            change_bp_payout + change_commission + change_user_payout
                        )
                        * -1,
                    ),
                    LedgerEntry.from_amount(
                        account_uuid=bp_wallet_account.uuid,
                        amount=change_bp_payout,
                    ),
                ]

                if change_commission:
                    entries.append(
                        LedgerEntry.from_amount(
                            account_uuid=bp_commission_account.uuid,
                            amount=change_commission,
                        )
                    )

                if change_user_payout:
                    entries.append(
                        LedgerEntry.from_amount(
                            account_uuid=user_account.uuid,
                            amount=change_user_payout,
                        )
                    )

            else:
                logger.info(
                    f"create_transaction_bp_adjustment. No transactions needed."
                )
                return None
        else:
            new_bp_payout = new_payout
            current_bp_payout = self.get_account_filtered_balance(
                account=bp_wallet_account,
                metadata_key="thl_session",
                metadata_value=session.uuid,
            )
            change_bp_payout = new_bp_payout - current_bp_payout
            logger.info(f"changes: {change_bp_payout}, {change_commission}")
            if change_bp_payout > 0:
                # Fail -> Complete
                entries = [
                    LedgerEntry(
                        direction=Direction.DEBIT,
                        account_uuid=revenue_account.uuid,
                        amount=change_bp_payout + change_commission,
                    ),
                    LedgerEntry(
                        direction=Direction.CREDIT,
                        account_uuid=bp_wallet_account.uuid,
                        amount=change_bp_payout,
                    ),
                ]

                # This is a very rare occurrence, but the change_commission
                #   could be negative if the BP's commission pct changed, and
                #   now the commission amount is lower even though a complete
                #   happened. This would only happen if the session had a
                #   complete already.
                #
                #   e.x. $5 complete, 10% commission -> $4.50 payout, $0.50
                #       comm. Now F->C a $1 event in the session, and the
                #       commission changed to 5%: total $6 complete, 5%
                #       commission -> $5.70 payout, $.30 comm.
                #       So the payout increased but the commission decreased.

                if change_commission:
                    entries.append(
                        LedgerEntry.from_amount(
                            account_uuid=bp_commission_account.uuid,
                            amount=change_commission,
                        )
                    )

            elif change_bp_payout < 0:
                # Complete -> Fail
                entries = [
                    LedgerEntry(
                        direction=Direction.CREDIT,
                        account_uuid=revenue_account.uuid,
                        amount=abs(change_bp_payout + change_commission),
                    ),
                    LedgerEntry(
                        direction=Direction.DEBIT,
                        account_uuid=bp_wallet_account.uuid,
                        amount=abs(change_bp_payout),
                    ),
                ]
                if change_commission:
                    entries.append(
                        LedgerEntry(
                            direction=Direction.DEBIT,
                            account_uuid=bp_commission_account.uuid,
                            amount=abs(change_commission),
                        )
                    )

            else:
                logger.info(
                    f"create_transaction_bp_adjustment. No transactions needed."
                )
                return None

        logger.info(entries)
        amt_str = f"${abs(change_bp_payout) / 100:,.2f}"
        amt_str = amt_str if change_bp_payout > 0 else "-" + amt_str
        ext_description = f"Session BP Payment Adj. {amt_str} {session.uuid}"
        if user_amt_str:
            ext_description += f" User Payment Adj. {user_amt_str}"

        t = self.create_tx(
            entries=entries,
            metadata=metadata,
            tag=tag,
            ext_description=ext_description,
            created=created,
        )

        return t

    def create_tx_bp_payout(
        self,
        product: Product,
        amount: USDCent,
        payoutevent_uuid: UUIDStr,
        created: AwareDatetime,
        skip_wallet_balance_check=False,
        skip_one_per_day_check=False,
        skip_flag_check=False,
    ) -> LedgerTransaction:
        """This is when we pay "OUT" a BP their wallet balance. (Not a
            payment for a task complete)

        - We're by default allowing 1 tx per BP per day. Set
            allow_multiple_per_day to allow 1 tx per BP per minute.
        - Checks to make sure the BP has at least amount in their wallet.
            Set skip_wallet_balance_check to skip this check.

        :param product: The BP to pay
        :param amount: The amount to pay out of the BP's wallet
        :param payoutevent_uuid: Associates the ledger tx with a payout
            event. This is also used to de-duplicate (only 1 tx per
            payoutevent).
        :param created: When this was paid. Can not be in the future.
        :param skip_wallet_balance_check: Skips the condition checking the
            BP has >= amount in their wallet.
        :param skip_one_per_day_check: Skips the condition check of only
            allowing 1 tx per BP per day.
        :param skip_flag_check: If True, we skip the redis flag check to allow
            for retry of a failed previous call. The Locking and condition
            checks still run.
        """

        assert isinstance(amount, int)
        assert isinstance(amount, USDCent)

        if skip_one_per_day_check or skip_wallet_balance_check:
            skip_flag_check = True

        assert (
            datetime.now(tz=timezone.utc) > created
        ), "created cannot be in the future"
        f = lambda: self.create_tx_bp_payout_(
            product=product,
            amount=amount,
            payoutevent_uuid=payoutevent_uuid,
            created=created,
        )

        condition: Callable = generate_condition_bp_payout(
            product=product,
            amount=amount,
            payoutevent_uuid=payoutevent_uuid,
            skip_one_per_day_check=skip_one_per_day_check,
            skip_wallet_balance_check=skip_wallet_balance_check,
        )

        lock_key = f"{self.currency.value}:bp_payout:{product.id}"
        flag_key = f"{self.currency.value}:bp_payout:{payoutevent_uuid}"
        return self.create_tx_protected(
            lock_key=lock_key,
            condition=condition,
            create_tx_func=f,
            skip_flag_check=skip_flag_check,
            flag_key=flag_key,
        )

    def create_tx_bp_payout_(
        self,
        product: Product,
        amount: USDCent,
        payoutevent_uuid: UUIDStr,
        created: datetime,
    ) -> LedgerTransaction:

        metadata = {
            tmc.TX_TYPE: TransactionType.BP_PAYOUT,
            tmc.EVENT: payoutevent_uuid,
        }
        # This tag might will uniquely identify this tx
        tag = f"{self.currency.value}:bp_payout:{payoutevent_uuid}"
        cash_account = self.get_account_cash()
        bp_wallet_account = self.get_account_or_create_bp_wallet(product)

        entries = [
            LedgerEntry(
                direction=Direction.DEBIT,
                account_uuid=bp_wallet_account.uuid,
                amount=amount,
            ),
            LedgerEntry(
                direction=Direction.CREDIT,
                account_uuid=cash_account.uuid,
                amount=amount,
            ),
        ]

        ext_description = f"BP Payout"
        t = self.create_tx(
            entries=entries,
            metadata=metadata,
            tag=tag,
            ext_description=ext_description,
            created=created,
        )

        return t

    def create_tx_plug_bp_wallet(
        self,
        product: Product,
        amount: USDCent,
        created: AwareDatetime,
        direction: Direction = Direction.DEBIT,
        description: Optional[str] = None,
        skip_flag_check=False,
    ) -> LedgerTransaction:
        """https://en.wikipedia.org/wiki/Plug_(accounting)

        The typical use case here to create a transaction to make up for
        discrepancies in what our ledger shows versus what was actually paid
        out to a BP. This may be due to receiving reconciliations from a
        marketplace (which are in our ledger), but never actually being paid
        for them. As such, we did not pay our suppliers for them. The plug
        is temporary and can be reversed once marketplace payments are
        reconciled.

        :param product: The account to create the transaction for is the
            product's bp_wallet account. By default, the transaction is
            balanced with the Cash account.

        :param amount: The amount for the transaction in USDCents.

        :param created: When this was paid. Can not be in the future.

        :param direction: A Direction.DEBIT will decrease the BP's wall
            balance amount. A Direction.CREDIT will increase the BP's
            balance amount. By default, we will always want to decrease
            a BP Wallet amount.

        :param description

        :param skip_flag_check: If True, we skip the flag check to allow
            for retry of a failed previous call.
        """
        assert (
            datetime.now(tz=timezone.utc) > created
        ), "created cannot be in the future"
        assert isinstance(amount, int)
        assert isinstance(amount, USDCent)

        f = lambda: self.create_tx_plug_bp_wallet_(
            product=product,
            amount=amount,
            created=created,
            direction=direction,
            description=description,
        )

        # This tag won't necessarily uniquely identify this tx, as we could
        #   make multiple per year
        tag = f"{self.currency.value}:plug:{product.id}:{created.strftime('%Y-%m-%d')}"
        condition = lambda x: len(self.get_tx_ids_by_tag(tag)) == 0
        lock_key = f"{self.currency.value}:plug:{product.id}"
        flag_key = tag

        return self.create_tx_protected(
            lock_key=lock_key,
            condition=condition,
            create_tx_func=f,
            skip_flag_check=skip_flag_check,
            flag_key=flag_key,
        )

    def create_tx_plug_bp_wallet_(
        self,
        product: Product,
        amount: USDCent,
        created: AwareDatetime,
        direction: Direction,
        description: Optional[str] = None,
    ) -> LedgerTransaction:

        assert isinstance(amount, int)
        assert isinstance(amount, USDCent)

        tag = f"{self.currency.value}:plug:{product.id}:{created.strftime('%Y-%m-%d')}"
        metadata = {tmc.TX_TYPE: TransactionType.PLUG}
        cash_account = self.get_account_cash()
        bp_wallet_account = self.get_account_or_create_bp_wallet(product)

        match direction:
            case Direction.DEBIT:
                # Decrease the BP Wall balance (take away Supplier money)
                entries = [
                    LedgerEntry(
                        direction=Direction.DEBIT,
                        account_uuid=bp_wallet_account.uuid,
                        amount=amount,
                    ),
                    LedgerEntry(
                        direction=Direction.CREDIT,
                        account_uuid=cash_account.uuid,
                        amount=amount,
                    ),
                ]
            case Direction.CREDIT:
                # Increase the BP Wall balance (giving the Supplier money)
                entries = [
                    LedgerEntry(
                        direction=Direction.CREDIT,
                        account_uuid=bp_wallet_account.uuid,
                        amount=amount,
                    ),
                    LedgerEntry(
                        direction=Direction.DEBIT,
                        account_uuid=cash_account.uuid,
                        amount=amount,
                    ),
                ]
            case _:
                raise ValueError("Invalid Direction")

        if description is None:
            description = f"BP Plug"

        t = self.create_tx(
            entries=entries,
            metadata=metadata,
            tag=tag,
            ext_description=description,
            created=created,
        )

        return t

    def create_tx_user_payout_request(
        self,
        user: User,
        payout_event: UserPayoutEvent,
        created: Optional[datetime] = None,
        skip_flag_check: Optional[bool] = False,
        skip_wallet_balance_check: Optional[bool] = False,
    ) -> LedgerTransaction:
        """
        The funds move from the user's wallet into the BP's "pending"
            wallet. Then, once the cashout request is completed, the
            funds will be taken from the BP's pending wallet and the
            commission will be recorded.

        Note: We are assuming the user that is requesting the payout is
            requesting from their USD wallet. No other currencies are
            supported now.
        """
        assert (
            user.product.user_wallet_enabled
        ), "Can only call this on an wallet enabled BPs"
        amount = USDCent(payout_event.amount)

        amt_str = f"${int(amount) / 100:,.2f}"
        descriptions = {
            PayoutType.AMT_HIT: f"User Payout AMT Assignment Request {amt_str}",
            PayoutType.AMT_BONUS: f"User Payout AMT Bonus Request {amt_str}",
            PayoutType.PAYPAL: f"User Payout Paypal Request {amt_str}",
            PayoutType.CASH_IN_MAIL: f"User Payout Cash Request {amt_str}",
            PayoutType.TANGO: f"User Payout Tango Request {amt_str}",
        }
        description = descriptions[payout_event.payout_type]

        if payout_event.payout_type in {
            PayoutType.AMT_HIT,
            PayoutType.AMT_BONUS,
        }:
            """
            This is for AMT accounts only (currently JB). This is the
            payment of a either 1) 1c or 5c (typically) assignment or 2) a
            bonus for task complete to the user. The 20% commission will
            be taken from the BP's wallet once the tx is completed.
            """
            assert (
                user.product.user_wallet_amt
            ), "Can only call this on an AMT-enabled BPs"

        f = lambda: self.create_tx_user_payout_request_(
            user=user,
            payout_event=payout_event,
            description=description,
            created=created,
        )

        min_balance: Optional[int] = int(amount)
        if payout_event.payout_type == PayoutType.AMT_HIT:
            # We allow the user's balance to reach up to -$1.00.
            min_balance = -100 + amount

        if skip_wallet_balance_check:
            min_balance = None

        condition: Callable = generate_condition_user_payout_request(
            user=user,
            payoutevent_uuid=payout_event.uuid,
            min_balance=min_balance,
        )

        lock_key = f"{self.currency.value}:user_payout:{user.uuid}"
        flag_key = f"{self.currency.value}:user_payout:{payout_event.uuid}:request"

        return self.create_tx_protected(
            lock_key=lock_key,
            flag_key=flag_key,
            condition=condition,
            create_tx_func=f,
            skip_flag_check=skip_flag_check,
        )

    def create_tx_user_payout_complete(
        self,
        user: User,
        payout_event: UserPayoutEvent,
        created: Optional[datetime] = None,
        fee_amount: Optional[Decimal] = None,
        skip_flag_check: Optional[bool] = False,
    ) -> LedgerTransaction:
        """
        Once the cashout request is approved and completed, the funds
        are taken from the BP's pending wallet, the commission will be
        recorded, and the cash debited.
        """
        assert (
            user.product.user_wallet_enabled
        ), "Can only call this on an wallet enabled BPs"

        # Before we even do anything, we should check that a ledger tx exists for the request
        request_tag = f"{self.currency.value}:user_payout:{payout_event.uuid}:request"
        txs = self.get_tx_ids_by_tag(request_tag)
        if len(txs) != 1:
            raise ValueError(
                f"Trying to complete user payout {payout_event.uuid} with no request tx found."
            )

        amount_usd = Decimal(payout_event.amount) / 100
        amt_str = f"${amount_usd:,.2f}"
        descriptions = {
            PayoutType.AMT_HIT: f"User Payout AMT Assignment Complete {amt_str}",
            PayoutType.AMT_BONUS: f"User Payout AMT Bonus Complete {amt_str}",
            PayoutType.PAYPAL: f"User Payout Paypal Complete {amt_str}",
            PayoutType.CASH_IN_MAIL: f"User Payout Cash Complete {amt_str}",
            PayoutType.TANGO: f"User Payout Tango Complete {amt_str}",
        }
        description = descriptions[payout_event.payout_type]
        bp_wallet_account = self.get_account_or_create_bp_wallet(user.product)

        if payout_event.payout_type in {
            PayoutType.AMT_HIT,
            PayoutType.AMT_BONUS,
        }:
            assert (
                user.product.user_wallet_amt
            ), "Can only call this on an AMT-enabled BP"
            bp_expense_account = self.get_account_or_create_bp_expense(
                product=user.product, expense_name="amt"
            )

            if fee_amount is None:
                fee_amount = (amount_usd * Decimal("0.2")).quantize(
                    Decimal("0.01")
                ) or Decimal("0.01")

        elif payout_event.payout_type == PayoutType.PAYPAL:
            bp_expense_account = self.get_account_or_create_bp_expense(
                product=user.product, expense_name="paypal"
            )
            assert fee_amount is not None, "must set fee_amount"

        elif payout_event.payout_type == PayoutType.CASH_IN_MAIL:
            bp_expense_account = self.get_account_or_create_bp_expense(
                product=user.product, expense_name=PayoutType.CASH_IN_MAIL
            )
            assert fee_amount is not None, "must set fee_amount"

        elif payout_event.payout_type == PayoutType.TANGO:
            bp_expense_account = self.get_account_or_create_bp_expense(
                product=user.product, expense_name="tango"
            )
            if fee_amount is None:
                fee_amount = (amount_usd * Decimal("0.035")).quantize(Decimal("0.01"))
        else:
            raise NotImplementedError()

        f = lambda: self.create_tx_user_payout_complete_(
            user=user,
            payout_event=payout_event,
            fee_expense_account=bp_expense_account,
            fee_payer_account=bp_wallet_account,
            fee_amount=fee_amount,
            description=description,
            created=created,
        )

        condition = generate_condition_user_payout_action(
            payout_event.uuid, action="complete"
        )
        lock_key = f"{self.currency.value}:user_payout:{user.uuid}"
        flag_key = f"{self.currency.value}:user_payout:{payout_event.uuid}:complete"

        return self.create_tx_protected(
            lock_key=lock_key,
            flag_key=flag_key,
            condition=condition,
            create_tx_func=f,
            skip_flag_check=skip_flag_check,
        )

    def create_tx_user_payout_cancelled(
        self,
        user: User,
        payout_event: UserPayoutEvent,
        created: Optional[datetime] = None,
        skip_flag_check: Optional[bool] = False,
    ) -> LedgerTransaction:
        assert (
            user.product.user_wallet_enabled
        ), "Can only call this on an wallet enabled BPs"

        # Before we even do anything, we should check that a ledger tx exists for the request
        request_tag = f"{self.currency.value}:user_payout:{payout_event.uuid}:request"
        txs = self.get_tx_ids_by_tag(request_tag)
        if len(txs) != 1:
            raise ValueError(
                f"Trying to cancel user payout {payout_event.uuid} with no request tx found."
            )

        description = f"User Payout Cancelled"
        f = lambda: self.create_tx_user_payout_cancelled_(
            user=user,
            payout_event=payout_event,
            description=description,
            created=created,
        )

        condition = generate_condition_user_payout_action(
            payoutevent_uuid=payout_event.uuid, action="cancel"
        )
        lock_key = f"{self.currency.value}:user_payout:{user.uuid}"
        flag_key = f"{self.currency.value}:user_payout:{payout_event.uuid}:cancel"

        return self.create_tx_protected(
            lock_key=lock_key,
            flag_key=flag_key,
            condition=condition,
            create_tx_func=f,
            skip_flag_check=skip_flag_check,
        )

    def create_tx_user_payout_request_(
        self,
        user: User,
        payout_event: UserPayoutEvent,
        description: str,
        created: Optional[datetime] = None,
    ) -> LedgerTransaction:
        # This is the same for all user payout requests, regardless of the
        #   payout_type (paypal, amt, tango)
        metadata = {
            tmc.USER: user.uuid,
            tmc.TX_TYPE: TransactionType.USER_PAYOUT_REQUEST,
            tmc.EVENT2: payout_event.uuid,
            tmc.PAYOUT_TYPE: payout_event.payout_type.value,
        }
        # This tag uniquely identifies this tx
        tag = f"{self.currency.value}:user_payout:{payout_event.uuid}:request"
        bp_pending_account = self.get_or_create_bp_pending_payout_account(
            product=user.product
        )
        # The USD assumption is "enforced" here, in that this call gets user's USD wallet.
        user_wallet_account = self.get_account_or_create_user_wallet(user=user)
        amount_cents = payout_event.amount
        entries = [
            LedgerEntry(
                direction=Direction.DEBIT,
                account_uuid=user_wallet_account.uuid,
                amount=amount_cents,
            ),
            LedgerEntry(
                direction=Direction.CREDIT,
                account_uuid=bp_pending_account.uuid,
                amount=amount_cents,
            ),
        ]

        t = self.create_tx(
            entries=entries,
            metadata=metadata,
            tag=tag,
            ext_description=description,
            created=created,
        )

        return t

    def create_tx_user_payout_complete_(
        self,
        user: User,
        payout_event: UserPayoutEvent,
        fee_expense_account: LedgerAccount,
        fee_payer_account: LedgerAccount,
        fee_amount: Decimal,
        description: str,
        created: Optional[datetime] = None,
    ) -> LedgerTransaction:
        """
            Creates the LedgerTransaction for a completed user payout request.

        :param user: The user who is requesting the payout. The `amount` comes
            from this user's wallet
        :param payout_event: The payout event associated with this tx
        :param fee_expense_account: Which account records the expense
            associated with the transaction fee.
        :param fee_payer_account: Which account actually pays the transaction
            fee. Typically, this is the BP's wallet.
        :param fee_amount: The amount of the transaction fee.
        :param created: Whe the payout was completed

        :return: the ledger transaction
        """

        # TODO: The fee_payer_account must be the bp_wallet_account, or else
        #   we must change the user_payout_request logic to hold the fee
        #   amount from the user's wallet as well.
        bp_wallet_account = self.get_account_or_create_bp_wallet(user.product)
        assert fee_payer_account == bp_wallet_account, "unsupported fee_payer_account"

        metadata = {
            tmc.USER: user.uuid,
            tmc.TX_TYPE: TransactionType.USER_PAYOUT_COMPLETE,
            tmc.EVENT2: payout_event.uuid,
            tmc.PAYOUT_TYPE: payout_event.payout_type,
        }
        # This tag uniquely identifies this tx
        tag = f"{self.currency.value}:user_payout:{payout_event.uuid}:complete"
        cash_account = self.get_account_cash()
        bp_pending_account = self.get_or_create_bp_pending_payout_account(
            product=user.product
        )

        amount_cents = payout_event.amount
        fee_cents = round(fee_amount * 100)
        entries = [
            LedgerEntry(
                direction=Direction.DEBIT,
                account_uuid=bp_pending_account.uuid,
                amount=amount_cents,
            ),
            LedgerEntry(
                direction=Direction.CREDIT,
                account_uuid=cash_account.uuid,
                amount=amount_cents,
            ),
        ]

        if fee_cents:
            entries.extend(
                [
                    LedgerEntry(
                        direction=Direction.DEBIT,
                        account_uuid=fee_payer_account.uuid,
                        amount=fee_cents,
                    ),
                    LedgerEntry(
                        direction=Direction.CREDIT,
                        account_uuid=fee_expense_account.uuid,
                        amount=fee_cents,
                    ),
                ]
            )

        t = self.create_tx(
            entries=entries,
            metadata=metadata,
            tag=tag,
            ext_description=description,
            created=created,
        )

        return t

    def create_tx_user_payout_cancelled_(
        self,
        user: User,
        payout_event: UserPayoutEvent,
        description: str,
        created: Optional[datetime] = None,
    ) -> LedgerTransaction:
        assert user.product

        metadata = {
            tmc.USER: user.uuid,
            tmc.TX_TYPE: TransactionType.USER_PAYOUT_CANCEL,
            tmc.EVENT2: payout_event.uuid,
            tmc.PAYOUT_TYPE: payout_event.payout_type,
        }
        # This tag uniquely identifies this tx
        tag = f"{self.currency.value}:user_payout:{payout_event.uuid}:cancel"
        bp_pending_account = self.get_or_create_bp_pending_payout_account(user.product)
        user_wallet_account = self.get_account_or_create_user_wallet(user)
        amount_cents: int = payout_event.amount

        entries = [
            LedgerEntry(
                direction=Direction.CREDIT,
                account_uuid=user_wallet_account.uuid,
                amount=amount_cents,
            ),
            LedgerEntry(
                direction=Direction.DEBIT,
                account_uuid=bp_pending_account.uuid,
                amount=amount_cents,
            ),
        ]

        t = self.create_tx(
            entries=entries,
            metadata=metadata,
            tag=tag,
            ext_description=description,
            created=created,
        )

        return t

    def create_tx_user_bonus(
        self,
        user: User,
        amount: Decimal,
        ref_uuid: UUIDStr,
        description: str,
        source_account: Optional[LedgerAccount] = None,
        created: Optional[datetime] = None,
        skip_flag_check: Optional[bool] = False,
    ) -> LedgerTransaction:
        """
        Pay a user into their wallet balance. There is no fee here. There
            is only a fee when the user requests a payout. The bonus could
            be as a bribe, winnings for a contest, leaderboard, etc.

        :param source_account: Is this paid from the bp's wallet? or from us?
        """
        assert (
            user.product.user_wallet_enabled
        ), "Can only call this on an wallet enabled BPs"
        assert user.product, "user.prefetch_product()"

        # This tag should uniquely id this tx.
        f = lambda: self.create_tx_user_bonus_(
            user=user,
            amount=amount,
            ref_uuid=ref_uuid,
            description=description,
            source_account=source_account,
            created=created,
        )

        tag = f"{self.currency.value}:user_bonus:{ref_uuid}"
        condition = generate_condition_tag_exists(tag)

        return self.create_tx_protected(
            lock_key=tag,
            condition=condition,
            create_tx_func=f,
            skip_flag_check=skip_flag_check,
        )

    def create_tx_user_bonus_(
        self,
        user: User,
        amount: Decimal,
        ref_uuid: UUIDStr,
        description: str,
        source_account: Optional[LedgerAccount] = None,
        created: Optional[datetime] = None,
    ) -> LedgerTransaction:

        metadata = {
            tmc.USER: user.uuid,
            tmc.TX_TYPE: TransactionType.USER_BONUS,
            tmc.BONUS: ref_uuid,
        }
        tag = f"{self.currency.value}:user_bonus:{ref_uuid}"
        user_account = self.get_account_or_create_user_wallet(user)

        # TODO: the source_account could be a separate account than the
        #  BP's main wallet account ..
        bp_account = self.get_account_or_create_bp_wallet(product=user.product)
        if source_account:
            assert source_account == bp_account, "not supported"

        amount_cents = round(amount * 100)
        entries = [
            LedgerEntry(
                direction=Direction.DEBIT,
                account_uuid=bp_account.uuid,
                amount=amount_cents,
            ),
            LedgerEntry(
                direction=Direction.CREDIT,
                account_uuid=user_account.uuid,
                amount=amount_cents,
            ),
        ]

        return self.create_tx(
            entries=entries,
            metadata=metadata,
            tag=tag,
            ext_description=description,
            created=created,
        )

    def create_tx_user_enter_contest(
        self,
        contest_uuid: UUIDStr,
        contest_entry: ContestEntry,
        skip_flag_check: Optional[bool] = False,
    ) -> LedgerTransaction:
        """
        User is requesting to enter a Raffle Contest. We'll DEBIT
        funds from their wallet and CREDIT the contest wallet.
        """
        assert (
            contest_entry.entry_type == ContestEntryType.CASH
        ), "Can only call this for CASH Contests"
        user = contest_entry.user
        assert (
            user.product.user_wallet_enabled
        ), "Can only call this on an wallet enabled BPs"
        assert user.product, "user.prefetch_product()"
        amount = contest_entry.amount
        entry_uuid = contest_entry.uuid
        created = contest_entry.created_at

        f = lambda: self.create_tx_user_enter_contest_(
            user=user,
            amount=amount,
            contest_uuid=contest_uuid,
            tag=tag,
            created=created,
        )

        # This tag should uniquely id this tx.
        tag = f"{self.currency.value}:enter_contest:{entry_uuid}"
        # Checks that the user has at least this balance and that a tx with tag doesn't exist
        condition = generate_condition_enter_contest(
            user=user, tag=tag, min_balance=amount
        )
        # Lock the whole thing along with any tx that the user can do to spend money
        lock_key = f"{self.currency.value}:user_payout:{user.uuid}"
        return self.create_tx_protected(
            lock_key=lock_key,
            flag_key=tag,
            condition=condition,
            create_tx_func=f,
            skip_flag_check=skip_flag_check,
        )

    def create_tx_user_enter_contest_(
        self,
        user: User,
        amount: USDCent,
        contest_uuid: UUIDStr,
        tag: str,
        created: Optional[datetime] = None,
    ) -> LedgerTransaction:
        description = f"Enter contest {amount.to_usd_str()} {contest_uuid}"
        metadata = {
            tmc.USER: user.uuid,
            tmc.TX_TYPE: TransactionType.USER_ENTER_CONTEST,
            tmc.CONTEST: contest_uuid,
        }
        user_account = self.get_account_or_create_user_wallet(user)
        contest_account = self.get_account_or_create_contest_wallet_by_uuid(
            contest_uuid=contest_uuid
        )

        entries = [
            LedgerEntry(
                direction=Direction.DEBIT,
                account_uuid=user_account.uuid,
                amount=int(amount),
            ),
            LedgerEntry(
                direction=Direction.CREDIT,
                account_uuid=contest_account.uuid,
                amount=int(amount),
            ),
        ]

        return self.create_tx(
            entries=entries,
            metadata=metadata,
            tag=tag,
            ext_description=description,
            created=created,
        )

    def create_tx_contest_close(
        self,
        contest: Contest,
        skip_flag_check: Optional[bool] = False,
    ) -> LedgerTransaction:
        """
        Contest is over. For each winner, we make a transaction.
        If the prize is physical, the money goes into a prize-expense
            account (for that BP), and if the prize is monetary, the money
            goes into the winner's wallet.

        Any remaining money goes back into the BP's wallet ? todo
        """
        if contest.contest_type in {ContestType.RAFFLE, ContestType.MILESTONE}:
            assert (
                contest.entry_type == ContestEntryType.CASH
            ), "Can only call this for CASH Contests"

        contest_account = self.get_account_or_create_contest_wallet_by_uuid(
            contest_uuid=contest.uuid
        )
        bp_wallet = self.get_account_or_create_bp_wallet_by_uuid(contest.product_id)
        bp_prize_expense_account = self.get_account_or_create_bp_expense_by_uuid(
            contest.product_id, expense_name="Prize"
        )

        contest_account_balance = self.get_account_balance(contest_account)
        print(f"{contest_account_balance=}")

        entries = []
        for w in contest.all_winners:
            if w.prize.kind == ContestPrizeKind.CASH:
                user_wallet = self.get_account_or_create_user_wallet(w.user)
                entries.extend(
                    [
                        LedgerEntry(
                            direction=Direction.DEBIT,
                            account_uuid=contest_account.uuid,
                            amount=int(w.prize.estimated_cash_value),
                        ),
                        LedgerEntry(
                            direction=Direction.CREDIT,
                            account_uuid=user_wallet.uuid,
                            amount=int(w.prize.estimated_cash_value),
                        ),
                    ]
                )
            elif w.prize.kind == ContestPrizeKind.PHYSICAL:
                entries.extend(
                    [
                        LedgerEntry(
                            direction=Direction.DEBIT,
                            account_uuid=contest_account.uuid,
                            amount=int(w.prize.estimated_cash_value),
                        ),
                        LedgerEntry(
                            direction=Direction.CREDIT,
                            account_uuid=bp_prize_expense_account.uuid,
                            amount=int(w.prize.estimated_cash_value),
                        ),
                    ]
                )
            else:
                # The prize is a promotion. It has no cash value now! The money goes
                #   back into the BP's wallet. The BP will eventually (supposedly)
                #   have to pay the expense of the promotion (e.g. 50% bonus on completes)
                #   once the user actually "redeems" the promotion.
                pass
        prize_value = sum(
            [
                w.prize.estimated_cash_value
                for w in contest.all_winners
                if w.prize.kind in {ContestPrizeKind.CASH, ContestPrizeKind.PHYSICAL}
            ]
        )
        if prize_value > contest_account_balance:
            logger.warning("Paying out more than the balance!")
            extra_expense = prize_value - contest_account_balance
            # Debit this balance from the BP's wallet
            entries.extend(
                [
                    LedgerEntry(
                        direction=Direction.CREDIT,
                        account_uuid=contest_account.uuid,
                        amount=int(extra_expense),
                    ),
                    LedgerEntry(
                        direction=Direction.DEBIT,
                        account_uuid=bp_wallet.uuid,
                        amount=int(extra_expense),
                    ),
                ]
            )
        elif prize_value < contest_account_balance:
            extra_income = contest_account_balance - prize_value
            # BP's wallet gets the overage
            entries.extend(
                [
                    LedgerEntry(
                        direction=Direction.DEBIT,
                        account_uuid=contest_account.uuid,
                        amount=int(extra_income),
                    ),
                    LedgerEntry(
                        direction=Direction.CREDIT,
                        account_uuid=bp_wallet.uuid,
                        amount=int(extra_income),
                    ),
                ]
            )

        f = lambda: self.create_tx_contest_close_(
            entries=entries,
            contest_uuid=contest.uuid,
            tag=tag,
            created=contest.ended_at,
        )

        # This tag should uniquely id this tx.
        tag = f"{self.currency.value}:contest_close:{contest.uuid}"
        # Checks that a tx with tag doesn't exist
        condition = generate_condition_tag_exists(tag=tag)
        # Lock the whole thing by this contest id
        lock_key = tag
        tx = self.create_tx_protected(
            lock_key=lock_key,
            flag_key=tag,
            condition=condition,
            create_tx_func=f,
            skip_flag_check=skip_flag_check,
        )
        assert self.get_account_balance(contest_account) == 0
        return tx

    def create_tx_contest_close_(
        self,
        entries: List[LedgerEntry],
        contest_uuid: UUIDStr,
        tag: str,
        created: Optional[datetime] = None,
    ) -> LedgerTransaction:
        description = f"Close contest {contest_uuid}"
        metadata = {
            tmc.TX_TYPE: TransactionType.CLOSE_CONTEST,
            tmc.CONTEST: contest_uuid,
        }
        return self.create_tx(
            entries=entries,
            metadata=metadata,
            tag=tag,
            ext_description=description,
            created=created,
        )

    def create_tx_milestone_winner(
        self,
        contest: MilestoneContest,
        winners: List["ContestWinner"],
        skip_flag_check: Optional[bool] = False,
    ) -> LedgerTransaction:
        """
        A user has reached a milestone. Pay out any cash or physical prizes,
            coming from the BP's wallet.
        """
        assert isinstance(contest, MilestoneContest), "invalid contest type"
        assert all(w.user.user_id for w in winners), "user must be set"
        assert len({w.user.user_id for w in winners}) == 1, "Cannot mix users"
        user = winners[0].user
        created_at = winners[0].created_at

        bp_wallet = self.get_account_or_create_bp_wallet_by_uuid(contest.product_id)
        bp_prize_expense_account = self.get_account_or_create_bp_expense_by_uuid(
            contest.product_id, expense_name="Prize"
        )

        entries = []
        for w in winners:
            if w.prize.kind == ContestPrizeKind.CASH:
                user_wallet = self.get_account_or_create_user_wallet(w.user)
                entries.extend(
                    [
                        LedgerEntry(
                            direction=Direction.DEBIT,
                            account_uuid=bp_wallet.uuid,
                            amount=int(w.prize.estimated_cash_value),
                        ),
                        LedgerEntry(
                            direction=Direction.CREDIT,
                            account_uuid=user_wallet.uuid,
                            amount=int(w.prize.estimated_cash_value),
                        ),
                    ]
                )
            elif w.prize.kind == ContestPrizeKind.PHYSICAL:
                entries.extend(
                    [
                        LedgerEntry(
                            direction=Direction.DEBIT,
                            account_uuid=bp_wallet.uuid,
                            amount=int(w.prize.estimated_cash_value),
                        ),
                        LedgerEntry(
                            direction=Direction.CREDIT,
                            account_uuid=bp_prize_expense_account.uuid,
                            amount=int(w.prize.estimated_cash_value),
                        ),
                    ]
                )
            else:
                # The prize is a promotion. It has no cash value now!
                #   The BP will pay any expenses associated with it.
                pass

        f = lambda: self.create_tx_milestone_winner_(
            entries=entries,
            contest_uuid=contest.uuid,
            user_uuid=user.uuid,
            tag=tag,
            created=created_at,
        )

        # This tag should uniquely id this tx.
        tag = f"{self.currency.value}:contest_milestone:{contest.uuid}:{user.user_id}"
        # Checks that a tx with tag doesn't exist
        condition = generate_condition_tag_exists(tag=tag)
        lock_key = tag
        tx = self.create_tx_protected(
            lock_key=lock_key,
            flag_key=tag,
            condition=condition,
            create_tx_func=f,
            skip_flag_check=skip_flag_check,
        )
        return tx

    def create_tx_milestone_winner_(
        self,
        entries: List[LedgerEntry],
        contest_uuid: UUIDStr,
        user_uuid: UUIDStr,
        tag: str,
        created: Optional[datetime] = None,
    ) -> LedgerTransaction:
        description = f"Milestone award {contest_uuid}"
        metadata = {
            tmc.TX_TYPE: TransactionType.USER_MILESTONE,
            tmc.CONTEST: contest_uuid,
            tmc.USER: user_uuid,
        }
        return self.create_tx(
            entries=entries,
            metadata=metadata,
            tag=tag,
            ext_description=description,
            created=created,
        )

    def get_user_wallet_balance(
        self, user: User, since_days_ago: Optional[int] = None
    ) -> int:
        """
        Calculates all payments to user's wallet minus all payouts from
        user's wallet. The user's wallet is a credit normal account, so
        the balance is the sum of credits minus the sum of debits, which
        should typically be positive (if the user has money available).

        :param user: User
        :param since_days_ago: if None, we get over all time
        :returns wallet balance in integer cents
        """
        user.prefetch_product(self.pg_config)
        assert (
            user.product.user_wallet_config.enabled
        ), "Can't get wallet balance on non-managed account."

        now = datetime.now(tz=timezone.utc)
        wallet = self.get_account_or_create_user_wallet(user)
        if user.product_id == JAMES_BILLINGS_BPID:
            assert since_days_ago is None
            return self.get_account_balance_timerange(
                wallet, time_start=JAMES_BILLINGS_TX_CUTOFF, time_end=now
            )
        if since_days_ago:
            start_dt = now - timedelta(days=since_days_ago)
            return self.get_account_balance_timerange(
                wallet, time_start=start_dt, time_end=now
            )
        return self.get_account_balance(wallet)

    def get_user_redeemable_wallet_balance(
        self, user: User, user_wallet_balance: int
    ) -> PositiveInt:
        """
        Returns the amount (from the user's wallet) that is currently
            redeemable. This amount will be less than or equal to the
            user_wallet_balance and non-negative.

        In the future, we want to model the risk of recon by day and by survey
        buyer's historical recon behavior, but for now:

        Looking at historical data, we can expect that for the worst 5% of users
        to get ~30 % of their completes reconciled.
        After 3 days, about 25% of all "future" recons have happened,
        7 days: 50%, 14 days: 75%, till end of next month: 100%.
        """
        now = datetime.now(tz=timezone.utc)
        # The redeemable balance can NOT ever be more than the actual user_wallet_balance

        # Sum up the redeemable amount for each complete
        user_id = user.user_id
        wall = pd.DataFrame(
            self.pg_config.execute_sql_query(
                query="""
                    SELECT finished, COALESCE(adjusted_user_payout, user_payout) as user_payout 
                    FROM thl_session
                    WHERE user_id = %s AND status='c'
                """,
                params=[user_id],
            ),
            columns=["finished", "user_payout"],
        )
        if wall.empty:
            reserve = 0
        else:
            wall["user_payout"] = wall["user_payout"].astype(float)
            wall["user_payout_int"] = wall["user_payout"] * 100
            wall["days_since_complete"] = (now - wall["finished"]).dt.days
            wall["pct_rdm"] = wall["days_since_complete"].apply(self.get_redeemable_pct)
            wall.loc[wall["pct_rdm"] > 0.95, "pct_rdm"] = 1
            wall["redeemable"] = wall["pct_rdm"] * wall["user_payout_int"]
            # Calculate money needed to save in reserve to cover the difference between
            #   money earned from completes and $ redeemable, subtract that from the
            #   wall balance.
            reserve = round(wall["user_payout_int"].sum() - wall["redeemable"].sum())
        redeemable_balance = user_wallet_balance - reserve
        redeemable_balance = 0 if redeemable_balance < 0 else redeemable_balance

        if redeemable_balance > 0:
            # it is possible the user_wallet_balance is negative, in which case the redeemable
            #   balance is 0. Don't fail assertion if that happens.
            assert redeemable_balance <= user_wallet_balance
        return redeemable_balance

    def get_redeemable_pct(
        self, days_since_complete: float, user_trust: float = 0.0
    ) -> float:
        """
        Returns the percentage of a payment for a complete that should be redeemable given the
            number of days since the complete occurred.
        """
        days_since_complete = round(max([min([days_since_complete, 60]), 0]))
        # Redeemable pct by days since complete. Logistic growth model:
        # https://people.richland.edu/james/lecture/m116/logs/models.html
        # Starts at 40% and goes up to 95% by day 38 (we then round up to
        # 100% from there) with a 4-day hold at 40% (for an untrusted user).
        initial_value = 0.40 + (0.20 * user_trust)
        max_value = 1
        day_delay = 4 - (2 * user_trust)
        rate = 0.1 + (0.1 * user_trust)
        b = (max_value - initial_value) / initial_value
        days = max([(days_since_complete - day_delay), 0])
        y = 1 / (1 + (b * np.exp(-1 * rate * days)))
        pct_rdm = np.clip(y, a_min=0, a_max=0.95) / 0.95
        # We can plot it with ...
        # x = [timedelta(days=d) for d in range(60)]
        # plt.plot([d.days for d in x], [self.get_redeemable_amount(d) for d in x])
        return pct_rdm

    def get_user_txs(
        self,
        user: User,
        time_start: Optional[datetime] = None,
        time_end: Optional[datetime] = None,
        page: int = 1,
        size: int = 50,
        order_by: Optional[str] = "created,tag",
    ) -> UserLedgerTransactions:
        user.prefetch_product(self.pg_config)
        user_account = self.get_account_or_create_user_wallet(user)
        exclude_txs_before = None

        if user.product_id == JAMES_BILLINGS_BPID:
            time_start = (
                max([JAMES_BILLINGS_TX_CUTOFF, time_start])
                if time_start is not None
                else JAMES_BILLINGS_TX_CUTOFF
            )
            exclude_txs_before = JAMES_BILLINGS_TX_CUTOFF

        txs, total = self.get_tx_filtered_by_account_paginated(
            user_account.uuid,
            time_start=time_start,
            time_end=time_end,
            page=page,
            size=size,
            order_by=order_by,
        )
        summary = self.get_tx_filtered_by_account_summary(
            user_account.uuid, time_start=time_start, time_end=time_end
        )
        # the 'total' should equal the sum of the UserLedgerTransactionTypeSummary.entry_count for each field
        utx = UserLedgerTransactions.from_txs(
            user_account=user_account,
            txs=txs,
            product_id=user.product_id,
            payout_format=user.product.payout_config.payout_format,
            summary=summary,
            page=page,
            size=size,
            total=total,
        )
        # Now calculate the rolling balance. Modifies utx.transactions in place
        self.include_running_balance(
            txs=utx.transactions,
            account_uuid=user_account.uuid,
            exclude_txs_before=exclude_txs_before,
        )
        return utx
