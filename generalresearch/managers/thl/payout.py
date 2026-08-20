from __future__ import annotations

from collections import defaultdict
from collections.abc import Collection
from datetime import datetime, timezone
from random import choice as rand_choice, randint
from typing import Any
from uuid import uuid4

import numpy as np
import pandas as pd
from psycopg import sql
from pydantic import AwareDatetime, NonNegativeInt, PositiveInt

from generalresearch.currency import USDCent
from generalresearch.decorators import LOG
from generalresearch.managers.base import (
    PostgresManagerWithRedis,
)
from generalresearch.managers.thl.ledger_manager.thl_ledger import (
    ThlLedgerManager,
)
from generalresearch.managers.thl.product import ProductManager
from generalresearch.models.custom_types import AwareDatetimeISO, UUIDStr
from generalresearch.models.gr.business import Business
from generalresearch.models.thl.definitions import PayoutStatus
from generalresearch.models.thl.ledger import (
    Direction,
    LedgerAccount,
    OrderBy,
)
from generalresearch.models.thl.payout import (
    BrokerageProductPayoutEvent,
    BusinessPayoutEvent,
    PayoutEvent,
    UserPayoutEvent,
)
from generalresearch.models.thl.product import Product
from generalresearch.models.thl.wallet import PayoutType
from generalresearch.models.thl.wallet.cashout_method import (
    CashMailOrderData,
    CashoutRequestInfo,
)


class PayoutEventManager(PostgresManagerWithRedis):
    """This is the default base Payout Event Manger. It acts as a base for
    mixing up two different concepts:
        - User Payout Events (money to Users / respondents)
        - Brokerage Product Payout Events (money to Suppliers)

    """

    def set_account_lookup_table(self, thl_lm: ThlLedgerManager) -> None:
        """This needs to run from grl-flow or from somewhere that has thl-redis
        access
        """

        res = self.pg_config.execute_sql_query(
            query=f"""
                SELECT uuid, reference_uuid 
                FROM ledger_account
                WHERE qualified_name LIKE '{thl_lm.currency.value}:bp_wallet:%'
            """
        )
        account_to_product = {i["uuid"]: i["reference_uuid"] for i in res}
        product_to_account = {i["reference_uuid"]: i["uuid"] for i in res}

        rc = self.redis_client
        rc.hset(name="pem:account_to_product", mapping=account_to_product)
        rc.hset(name="pem:product_to_account", mapping=product_to_account)

        return None

    def get_by_uuid(self, pe_uuid: UUIDStr) -> PayoutEvent:
        res = self.pg_config.execute_sql_query(
            query="""
            SELECT  ep.uuid,
                    debit_account_uuid, 
                    cashout_method_uuid, 
                    ep.created, ep.amount, ep.status,
                    ep.ext_ref_id, ep.payout_type, 
                    ep.request_data::jsonb,
                    ep.order_data::jsonb
            FROM event_payout AS ep
            WHERE ep.uuid = %s
        """,
            params=[pe_uuid],
        )
        assert len(res) == 1, f"{pe_uuid} expected 1 result, got {len(res)}"
        return PayoutEvent.model_validate(res[0])

    def update(
        self,
        payout_event: UserPayoutEvent | BrokerageProductPayoutEvent | None,
        status: PayoutStatus,
        ext_ref_id: str | None = None,
        order_data: dict[str, Any] | None = None,
    ) -> None:
        # These 3 things are the only modifiable attributes
        ext_ref_id = ext_ref_id if ext_ref_id is not None else payout_event.ext_ref_id
        order_data = order_data if order_data is not None else payout_event.order_data
        payout_event.update(status=status, ext_ref_id=ext_ref_id, order_data=order_data)

        d = payout_event.model_dump_postgres()
        query = sql.SQL(
            """
        UPDATE event_payout SET 
            status = %(status)s,
            ext_ref_id = %(ext_ref_id)s,
            order_data = %(order_data)s
        WHERE uuid = %(uuid)s;
        """
        )
        with self.pg_config.make_connection() as conn:
            with conn.cursor() as c:
                c.execute(query=query, params=d)
                assert (
                    c.rowcount == 1
                ), "Nothing was updated! Are you sure this payout_event exists?"
            conn.commit()


class UserPayoutEventManager(PayoutEventManager):

    def get_by_uuid(self, pe_uuid: UUIDStr) -> UserPayoutEvent:

        res = self.pg_config.execute_sql_query(
            query="""
            SELECT  ep.uuid,
                    ep.debit_account_uuid, 
                    ep.cashout_method_uuid, 
                    ep.created, ep.amount, ep.status, ep.ext_ref_id, ep.payout_type, 
                    ep.request_data::jsonb,
                    ep.order_data::jsonb, 
                    -- User Payout specific
                    ac.name as description, 
                    la.reference_type as account_reference_type,
                    la.reference_uuid as account_reference_uuid
            FROM event_payout AS ep
            LEFT JOIN accounting_cashoutmethod AS ac 
                ON ep.cashout_method_uuid = ac.id
            LEFT JOIN ledger_account AS la 
                ON la.uuid = ep.debit_account_uuid
            WHERE ep.uuid = %s
        """,
            params=[pe_uuid],
        )

        assert len(res) == 1, f"{pe_uuid} expected 1 result, got {len(res)}"

        d = res[0]
        pe = UserPayoutEvent.model_validate(d)
        if pe.order_data and pe.payout_type == PayoutType.CASH_IN_MAIL:
            pe.order_data = CashMailOrderData.model_validate(pe.order_data)

        return pe

    def get_payout_detail(self, pe_uuid: UUIDStr) -> CashoutRequestInfo:
        # This gets the payout event, and then extracts information for
        #   the purposes of returning to the user.
        pe = self.get_by_uuid(pe_uuid=pe_uuid)

        transaction_info = dict()
        order: dict[str, Any] = pe.order_data
        if pe.payout_type == PayoutType.TANGO and pe.status == PayoutStatus.COMPLETE:
            reward = order["reward"]
            if "credentialList" in reward:
                reward["credential_list"] = reward.pop("credentialList")
            if "redemptionInstructions" in reward:
                reward["redemption_instructions"] = reward.pop("redemptionInstructions")
            transaction_info = order["reward"]
        elif pe.payout_type == PayoutType.PAYPAL and pe.status == PayoutStatus.COMPLETE:
            info = {"transaction_id": order["transaction_id"]}
            transaction_info = info
        elif (
            pe.payout_type == PayoutType.CASH_IN_MAIL
            and pe.status == PayoutStatus.COMPLETE
        ):
            transaction_info = pe.order_data.model_dump(mode="json")

        return CashoutRequestInfo(
            id=pe_uuid,
            status=pe.status,
            description=pe.description,
            transaction_info=transaction_info,
            message="",
        )

    def filter_by(
        self,
        reference_uuid: str | None = None,
        debit_account_uuids: Collection[UUIDStr] | None = None,
        amount: int | None = None,
        created: datetime | None = None,
        created_after: datetime | None = None,
        product_ids: str | None = None,
        bp_user_ids: Collection[str] | None = None,
        cashout_method_uuids: Collection[UUIDStr] | None = None,
        cashout_types: Collection[PayoutType] | None = None,
        statuses: Collection[PayoutStatus] | None = None,
    ) -> list[UserPayoutEvent]:
        """Try to retrieve payout events by the product_id/user_uuid, amount,
        and optionally timestamp.

        WARNING: This is only on the "payout events" table and nothing to
            do with the Ledger itself. Therefore, the product_ids query
            doesn't return Brokerage Product Payouts (the ACH or Wire events
            to Suppliers) as part of the query.

            *** IT IS ONLY FOR USER PAYOUTS ***

        Note: what used to be in thl-grpcs "ListCashoutRequests" calling
        "list_cashout_requests" was merged into this.
        """
        args = []
        filters = []

        if reference_uuid:
            # This could be a product_id or a user_uuid
            filters.append("la.reference_uuid = %s")
            args.append(reference_uuid)

        if debit_account_uuids:
            # Or we could use the bp_wallet or user_wallet's account uuid
            # instead of looking up by the product/user
            filters.append("ep.debit_account_uuid = ANY(%s)")
            args.append(debit_account_uuids)
        if amount:
            filters.append("ep.amount = %s")
            args.append(amount)
        if created:
            filters.append("ep.created = %s")
            args.append(created.replace(tzinfo=None))
        if created_after:
            filters.append("ep.created >= %s")
            args.append(created_after.replace(tzinfo=None))
        if product_ids:
            filters.append("product_id = ANY(%s)")
            args.append(product_ids)
        if bp_user_ids:
            filters.append("product_user_id = ANY(%s)")
            args.append(bp_user_ids)
        if cashout_method_uuids:
            filters.append("cashout_method_uuid = ANY(%s)")
            args.append(cashout_method_uuids)
        if cashout_types:
            filters.append("payout_type = ANY(%s)")
            args.append([x.value for x in cashout_types])
        if statuses:
            filters.append("status = ANY(%s)")
            args.append([x.value for x in statuses])

        assert len(filters) > 0, "must pass at least 1 filter"
        filter_str = "WHERE " + " AND ".join(filters)

        res = self.pg_config.execute_sql_query(
            query=f"""
            SELECT  
                ep.uuid, ep.debit_account_uuid,
                ep.created, ep.amount, ep.status,
                ep.ext_ref_id, ep.payout_type, ep.cashout_method_uuid,
                ep.order_data::jsonb,
                ep.request_data::jsonb,
                ac.name as description,
                la.reference_type as account_reference_type,
                la.reference_uuid as account_reference_uuid
            FROM event_payout AS ep
            LEFT JOIN accounting_cashoutmethod AS ac 
                ON ep.cashout_method_uuid = ac.id 
            LEFT JOIN ledger_account AS la
                ON la.uuid = ep.debit_account_uuid
            LEFT JOIN thl_user u
                ON la.reference_uuid = u.uuid
            {filter_str}
        """,
            params=args,
        )

        pes = []
        for d in res:
            pes.append(UserPayoutEvent.model_validate(d))
        return pes

    def create(
        self,
        debit_account_uuid: UUIDStr,
        cashout_method_uuid: UUIDStr,
        payout_type: PayoutType,
        amount: PositiveInt,
        # --- Optional: Default / Default Factory ---
        uuid: UUIDStr | None = None,
        status: PayoutStatus | None = None,
        created: AwareDatetimeISO | None = None,
        request_data: dict[str, Any] | None = None,
        # --- Optional: None  ---
        account_reference_type: str | None = None,
        account_reference_uuid: UUIDStr | None = None,
        description: str | None = None,
        ext_ref_id: str | None = None,
        order_data: dict[str, Any] | CashMailOrderData | None = None,
    ) -> UserPayoutEvent:

        payout_event = UserPayoutEvent(
            uuid=uuid or uuid4().hex,
            debit_account_uuid=debit_account_uuid,
            account_reference_type=account_reference_type,
            account_reference_uuid=account_reference_uuid,
            cashout_method_uuid=cashout_method_uuid,
            description=description,
            created=created or datetime.now(tz=timezone.utc),
            amount=amount,
            status=status or PayoutStatus.PENDING,
            ext_ref_id=ext_ref_id,
            payout_type=payout_type,
            request_data=request_data or {},
            order_data=order_data,
        )
        d = payout_event.model_dump_mysql()

        with self.pg_config.make_connection() as conn:
            with conn.cursor() as c:
                c.execute(
                    query="""
                    INSERT INTO event_payout (
                        uuid, debit_account_uuid, created, 
                        cashout_method_uuid, amount, status, 
                        ext_ref_id, payout_type, order_data, 
                        request_data
                    ) VALUES (
                        %(uuid)s, %(debit_account_uuid)s, %(created)s, 
                        %(cashout_method_uuid)s, %(amount)s, %(status)s, 
                        %(ext_ref_id)s, %(payout_type)s, %(order_data)s, 
                        %(request_data)s
                    );
                """,
                    params=d,
                )
                assert c.rowcount == 1, f"expected 1 row inserted, got {c.rowcount}"
            conn.commit()

        return payout_event

    def create_dummy(
        self,
        uuid: UUIDStr | None = None,
        debit_account_uuid: UUIDStr | None = None,
        account_reference_type: str | None = None,
        account_reference_uuid: UUIDStr | None = None,
        cashout_method_uuid: UUIDStr | None = None,
        description: str | None = None,
        created: AwareDatetimeISO | None = None,
        amount: PositiveInt | None = None,
        status: PayoutStatus | None = None,
        ext_ref_id: str | None = None,
        payout_type: PayoutType | None = None,
        request_data: dict[str, Any] | None = None,
        order_data: dict[str, Any] | CashMailOrderData | None = None,
    ) -> UserPayoutEvent:

        debit_account_uuid = debit_account_uuid or uuid4().hex
        cashout_method_uuid = cashout_method_uuid or uuid4().hex
        # account_reference_type = account_reference_type or f"acct-ref-{uuid4().hex}"
        # account_reference_uuid = account_reference_uuid or uuid4().hex
        # cashout_method_uuid = cashout_method_uuid or uuid4().hex
        amount = amount or randint(a=99, b=9_999)
        status = status or rand_choice(list(PayoutStatus))

        description = description or f"desc-{uuid4().hex[:12]}"
        # ext_ref_id = ext_ref_id or f"ext-ref-{uuid4().hex[:8]}"
        payout_type = payout_type or rand_choice(list(PayoutType))
        request_data = request_data or {}
        # order_data = order_data or None

        return self.create(
            uuid=uuid,
            debit_account_uuid=debit_account_uuid,
            account_reference_type=account_reference_type,
            account_reference_uuid=account_reference_uuid,
            cashout_method_uuid=cashout_method_uuid,
            description=description,
            created=created,
            amount=amount,
            status=status,
            ext_ref_id=ext_ref_id,
            payout_type=payout_type,
            request_data=request_data,
            order_data=order_data,
        )


class BrokerageProductPayoutEventManager(PayoutEventManager):
    # This is what makes a PayoutEvent a Brokerage Product Payout
    CASHOUT_METHOD_UUID = "602113e330cf43ae85c07d94b5100291"

    def get_by_uuid(
        self,
        pe_uuid: UUIDStr,
    ) -> BrokerageProductPayoutEvent:

        res = self.pg_config.execute_sql_query(
            query="""
            SELECT  ep.uuid, ep.debit_account_uuid, ep.cashout_method_uuid, 
                    ep.created, ep.amount, ep.status, ep.ext_ref_id, ep.payout_type, 
                    ep.request_data::jsonb,
                    ep.order_data::jsonb,
                    la.reference_uuid as product_id
            FROM event_payout AS ep
            JOIN ledger_account la on la.uuid = debit_account_uuid
            WHERE ep.uuid = %s
        """,
            params=[pe_uuid],
        )
        assert len(res) == 1, f"{pe_uuid} expected 1 result, got {len(res)}"
        return BrokerageProductPayoutEvent.model_validate(res[0])

    @staticmethod
    def check_for_ledger_tx(
        thl_ledger_manager: ThlLedgerManager,
        payout_event: BrokerageProductPayoutEvent,
    ) -> bool:
        """
        Checks if a ledger tx for this payout event exists properly in the DB.
        It looks up by the tag (which is uniquely specified by the payout event uuid),
        and then confirms that the associated transaction if a bp_payout, for the
        specified Product, for the same amount.

        Returns True if the tx exists and looks ok, False if no txs with that tag
            are found, and raises a ValueError if something is inconsistent.
        """
        tag = f"{thl_ledger_manager.currency.value}:bp_payout:{payout_event.uuid}"
        amount = USDCent(payout_event.amount)
        product_id = payout_event.product_id

        txs = thl_ledger_manager.get_tx_by_tag(tag)

        if not txs:
            return False

        if len(txs) != 1:
            raise ValueError(f"Two transactions found for tag: {tag}!")

        tx = txs[0]
        if (
            (len(tx.entries) != 2)
            or (tx.entries[0].amount != amount)
            or (tx.metadata["tx_type"] != "bp_payout")
            or (tx.metadata["event_payout"] != payout_event.uuid)
        ):
            raise ValueError(
                f"Found existing tx with tag: {tag}, but different than expected!"
            )
        bp_wallet_account = thl_ledger_manager.get_account_or_create_bp_wallet_by_uuid(
            product_uuid=product_id
        )
        entry = [x for x in tx.entries if x.direction == Direction.DEBIT][0]
        if entry.account_uuid != bp_wallet_account.uuid:
            raise ValueError(
                f"Found existing tx with tag: {tag}, but for a different account!"
            )

        return True

    def filter_by(
        self,
        ext_ref_id: str | None = None,
        debit_account_uuids: Collection[UUIDStr] | None = None,
        amount: int | None = None,
        created: datetime | None = None,
        created_after: datetime | None = None,
        product_ids: Collection[str] | None = None,
        cashout_types: Collection[PayoutType] | None = None,
        statuses: Collection[PayoutStatus] | None = None,
    ) -> list[BrokerageProductPayoutEvent]:
        """Try to retrieve BP payout events.

        WARNING: This is only on the "payout events" table and nothing to
            do with the Ledger itself

        *** IT IS ONLY FOR Brokerage Product PAYOUTS ***
        """
        params = dict()
        filters = []
        if ext_ref_id:
            # This is transaction id for tracking ACH/Wires with a banking institution
            filters.append("ep.ext_ref_id = %(ext_ref_id)s")
            params["ext_ref_id"] = ext_ref_id
        if debit_account_uuids:
            # Or we could use the bp_wallet's account uuid
            # instead of looking up by the product
            filters.append("ep.debit_account_uuid = ANY(%(debit_account_uuids)s)")
            params["debit_account_uuids"] = debit_account_uuids
        if amount:
            filters.append("ep.amount = %(amount)s")
            params["amount"] = amount
        if created:
            filters.append("ep.created = %(created)s")
            params["created"] = created
        if created_after:
            filters.append("ep.created >= %(created_after)s")
            params["created_after"] = created_after
        if product_ids is not None:
            filters.append("la.reference_uuid = ANY(%(product_ids)s)")
            params["product_ids"] = product_ids
        if cashout_types is not None:
            filters.append("payout_type = ANY(%(cashout_types)s)")
            params["cashout_types"] = [x.value for x in cashout_types]
        if statuses is not None:
            filters.append("status = ANY(%(statuses)s)")
            params["statuses"] = [x.value for x in statuses]

        assert len(filters) > 0, "must pass at least 1 filter"
        filter_str = " AND ".join(filters)
        params["cashout_method_uuid"] = self.CASHOUT_METHOD_UUID

        res = self.pg_config.execute_sql_query(
            query=f"""
                SELECT  ep.uuid, ep.debit_account_uuid, ep.cashout_method_uuid, 
                        ep.created, ep.amount, ep.status, ep.ext_ref_id, 
                        ep.payout_type, ep.supplier_payout_id,
                        ep.request_data::jsonb, ep.order_data::jsonb,
                        ac.name as description,
                        la.reference_uuid as product_id
                FROM event_payout AS ep
                LEFT JOIN accounting_cashoutmethod AS ac 
                    ON ep.cashout_method_uuid = ac.id 
                LEFT JOIN ledger_account AS la
                    ON la.uuid = ep.debit_account_uuid
                WHERE cashout_method_uuid = %(cashout_method_uuid)s
                    AND la.reference_type = 'bp'
                    AND {filter_str}
            """,
            params=params,
        )
        pes = []
        for row in res:
            pes.append(BrokerageProductPayoutEvent.model_validate(row))
        return pes

    def get_bp_payout_events_for_accounts(
        self, accounts: Collection[LedgerAccount]
    ) -> list[BrokerageProductPayoutEvent]:
        return self.filter_by(
            debit_account_uuids=[i.uuid for i in accounts],
            cashout_types=[PayoutType.ACH],
        )

    def get_bp_bp_payout_events_for_products(
        self,
        product_uuids: Collection[UUIDStr],
        order_by: OrderBy | None = OrderBy.ASC,
    ) -> list[BrokerageProductPayoutEvent]:
        """This is a terrible name, but it returns the
        BPPayoutEvent model type rather than a list of PayoutEvents.

        We do this for the Supplier-centric APIs where they don't know
        or care about the underlying ledger account structure.
        """
        assert len(product_uuids) > 0, "Must provide product_uuids"
        order_by = order_by or OrderBy.ASC

        payout_events = self.filter_by(
            product_ids=product_uuids,
            cashout_types=[PayoutType.ACH],
        )
        payout_events = sorted(
            payout_events, key=lambda x: x.created, reverse=order_by == OrderBy.DESC
        )
        return payout_events

    def retry_create_bp_payout_event_tx(
        self,
        thl_ledger_manager: ThlLedgerManager,
        product: Product,
        bp_pe: BrokerageProductPayoutEvent,
    ) -> BrokerageProductPayoutEvent:
        """
        If a create_bp_payout_event call fails, this can be called with
        the associated payoutevent.
        """
        assert bp_pe.status in {
            PayoutStatus.FAILED,
            PayoutStatus.PENDING,
        }, "Only use this on pending or failed payouts"

        if self.check_for_ledger_tx(
            thl_ledger_manager=thl_ledger_manager, payout_event=bp_pe
        ):
            LOG.warning(
                f"Transaction for {bp_pe.uuid=} {bp_pe.product_id=} already exists! "
                f"Marking the payout event status as complete."
            )
            self.update(payout_event=bp_pe, status=PayoutStatus.COMPLETE)
            return bp_pe

        return self._create_tx_bp_payout_from_payout_event(
            thl_ledger_manager=thl_ledger_manager,
            bp_pe=bp_pe,
            product=product,
            skip_one_per_day_check=True,
            skip_wallet_balance_check=True,
        )

    def create_pending_bp_payout_events(
        self,
        product: Product,
        amount: USDCent,
        payout_type: PayoutType = PayoutType.ACH,
        ext_ref_id: str | None = None,
        created: AwareDatetime | None = None,
    ):
        pass

    # def create_bp_payout_event(
    #     self,
    #     thl_ledger_manager: ThlLedgerManager,
    #     product: Product,
    #     amount: USDCent,
    #     payout_type: PayoutType = PayoutType.ACH,
    #     ext_ref_id: str | None = None,
    #     created: AwareDatetime | None = None,
    #     skip_wallet_balance_check: bool = False,
    #     skip_one_per_day_check: bool = False,
    # ) -> BrokerageProductPayoutEvent:
    #
    #     return self._create_tx_bp_payout_from_payout_event(
    #         thl_ledger_manager=thl_ledger_manager,
    #         bp_pe=bp_pe,
    #         product=product,
    #         amount=amount,
    #         created=created,
    #         skip_one_per_day_check=skip_one_per_day_check,
    #         skip_wallet_balance_check=skip_wallet_balance_check,
    #     )

    def _create_tx_bp_payout_from_payout_event(
        self,
        thl_ledger_manager: ThlLedgerManager,
        bp_pe: BrokerageProductPayoutEvent,
        product: Product,
        created: AwareDatetime | None = None,
        skip_wallet_balance_check: bool = False,
        skip_one_per_day_check: bool = False,
    ) -> BrokerageProductPayoutEvent:
        """
        This should not be called directly.
        Creates the ledger transaction for a BP Payout, given a PayoutEvent.
        Handles exceptions: Check if the ledger tx actually exists or not, and set the
            payout event status accordingly.
        """
        created = created if created else bp_pe.created
        try:
            thl_ledger_manager.create_tx_bp_payout(
                product=product,
                amount=USDCent(bp_pe.amount),
                payoutevent_uuid=bp_pe.uuid,
                created=created,
                skip_wallet_balance_check=skip_wallet_balance_check,
                skip_one_per_day_check=skip_one_per_day_check,
            )
        except Exception as e:
            e.pe_uuid = bp_pe.uuid
            if self.check_for_ledger_tx(
                thl_ledger_manager=thl_ledger_manager,
                payout_event=bp_pe,
            ):
                LOG.warning(f"Got exception {e} but ledger tx exists! Continuing ... ")
                self.update(payout_event=bp_pe, status=PayoutStatus.COMPLETE)
                return bp_pe
            else:
                LOG.warning(f"Got exception {e}. No ledger tx was created.")
                self.update(payout_event=bp_pe, status=PayoutStatus.FAILED)
                raise e

        self.update(payout_event=bp_pe, status=PayoutStatus.COMPLETE)
        return bp_pe

    def get_bp_payout_events_for_product(
        self,
        thl_ledger_manager: ThlLedgerManager,
        product: Product,
    ) -> list[BrokerageProductPayoutEvent]:
        account = thl_ledger_manager.get_account_or_create_bp_wallet(product=product)
        return self.get_bp_payout_events_for_accounts(accounts=[account])

    def get_bp_payout_events_for_account(
        self, account: LedgerAccount
    ) -> list[BrokerageProductPayoutEvent]:
        return self.get_bp_payout_events_for_accounts(accounts=[account])

    def get_bp_payout_events_for_products(
        self,
        thl_ledger_manager: ThlLedgerManager,
        product_uuids: Collection[UUIDStr],
    ) -> list[BrokerageProductPayoutEvent]:
        accounts = thl_ledger_manager.get_accounts_bp_wallet_for_products(
            product_uuids=product_uuids
        )
        return self.get_bp_payout_events_for_accounts(accounts=accounts)


class BusinessPayoutEventManager(PostgresManagerWithRedis):

    def get_by_ext_ref_id(self, ext_ref_id: str) -> BusinessPayoutEvent:
        res = self.pg_config.execute_sql_query(
            """
        SELECT
            sp.*,
            ep.bp_payouts
        FROM supplier_payout sp
        JOIN (
            SELECT
                ep_inner.supplier_payout_id,
                jsonb_agg(
                    to_jsonb(ep_inner)
                    || jsonb_build_object('product_id', la.reference_uuid)
                    ORDER BY ep_inner.created
                ) AS bp_payouts
            FROM event_payout ep_inner
            JOIN ledger_account la
                ON ep_inner.debit_account_uuid = la.uuid
            GROUP BY ep_inner.supplier_payout_id
        ) ep ON sp.id = ep.supplier_payout_id
        WHERE sp.ext_ref_id = %(ext_ref_id)s
        """,
            {"ext_ref_id": ext_ref_id},
        )
        assert len(res) == 1, f"No Business Payout found with ext ref: {ext_ref_id}"
        d = res[0]
        for bp_payout in d["bp_payouts"]:
            bp_payout["created"] = datetime.fromisoformat(bp_payout["created"])
        bpe = BusinessPayoutEvent.model_validate(d)
        assert (
            bpe.bp_payouts is not None and len(bpe.bp_payouts) > 0
        ), "No BP payouts found for this Business Payout Event. This shouldn't happen!"
        return bpe

    def filter_by(
        self,
        business_uuids: Collection[UUIDStr] | None = None,
    ) -> list[BusinessPayoutEvent]:

        params = dict()
        filters = []
        if business_uuids is not None:
            filters.append("business_id = ANY(%(business_uuids)s)")
            params["business_uuids"] = business_uuids

        assert len(filters) > 0, "must pass at least 1 filter"
        filter_str = " AND ".join(filters)

        res = self.pg_config.execute_sql_query(
            f"""
        SELECT
            sp.*,
            ep.bp_payouts
        FROM supplier_payout sp
        JOIN (
            SELECT
                ep_inner.supplier_payout_id,
                jsonb_agg(
                    to_jsonb(ep_inner)
                    || jsonb_build_object('product_id', la.reference_uuid)
                    ORDER BY ep_inner.created
                ) AS bp_payouts
            FROM event_payout ep_inner
            JOIN ledger_account la
                ON ep_inner.debit_account_uuid = la.uuid
            GROUP BY ep_inner.supplier_payout_id
        ) ep ON sp.id = ep.supplier_payout_id
        WHERE {filter_str}
        """,
            params,
        )
        bpes = []
        for row in res:
            for bp_payout in row["bp_payouts"]:
                bp_payout["created"] = datetime.fromisoformat(bp_payout["created"])
            bpe = BusinessPayoutEvent.model_validate(row)
            assert (
                bpe.bp_payouts is not None and len(bpe.bp_payouts) > 0
            ), "No BP payouts found for this Business Payout Event. This shouldn't happen!"
            bpes.append(bpe)
        return bpes

    def validate_business_payout_in_ledger(
        self, ext_ref_id: str, thl_lm: ThlLedgerManager
    ):
        """
        Check that there exist ledger TXs for the Brokerage Product payouts
            for this Business Payout Event.
        """
        bpe = self.get_by_ext_ref_id(ext_ref_id=ext_ref_id)
        tags = [
            f"{thl_lm.currency.value}:bp_payout:{bp_pe.uuid}"
            for bp_pe in bpe.bp_payouts
        ]
        txs = thl_lm.get_tx_ids_by_tags(tags=tags)
        assert len(txs) == len(
            bpe.bp_payouts
        ), f"Expected {len(bpe.bp_payouts)} BP payouts but found {len(txs)}!"
        return True

    def resume_failed_business_payout(
        self, ext_ref_id: str, thl_lm: ThlLedgerManager, pm: ProductManager
    ):
        """
        Sometimes a business payout's BP payouts fail due to multiple reasons
        (timeouts, BP having insufficient funds, etc). Grab the PENDING
        BP payout events and retry them.
        """
        bpe = self.get_by_ext_ref_id(ext_ref_id=ext_ref_id)
        assert bpe.id
        assert bpe.bp_payouts

        if all(bp_pe.status == PayoutStatus.COMPLETE for bp_pe in bpe.bp_payouts):
            try:
                self.validate_business_payout_in_ledger(
                    ext_ref_id=ext_ref_id, thl_lm=thl_lm
                )
            except AssertionError as e:
                raise AssertionError(
                    f"Business Payout Event {ext_ref_id} is COMPLETE but BP payouts are not in the ledger! {e} "
                    f"This typically shouldn't happen, as if the ledger TX fails, the event_payout "
                    f"status won't be COMPLETE. If it does, set all the bp statuses to FAILED, and "
                    f"then try again. Any that do exist in the ledger will be found and marked COMPLETE."
                ) from e
            if bpe.status != PayoutStatus.COMPLETE:
                self.update_business_payout_event(
                    pk=bpe.id, status=PayoutStatus.COMPLETE
                )
                LOG.warning(
                    "All BP payouts complete, setting Business Payout Event status to COMPLETE."
                )
            else:
                LOG.warning(
                    "Nothing to do! Business Payout is COMPLETE and all Brokerage Product payouts are also COMPLETE!"
                )
            return None

        for bp_pe in bpe.bp_payouts:
            if bp_pe.status in {PayoutStatus.PENDING, PayoutStatus.FAILED}:
                LOG.warning(
                    f"Found a {bp_pe.status} BP payout event: {bp_pe.uuid} - retrying ... "
                )
                product = pm.get_by_uuid(bp_pe.product_id)
                self.retry_create_bp_payout_event_tx(
                    thl_ledger_manager=thl_lm, bp_pe=bp_pe, product=product
                )
            if bp_pe.status != PayoutStatus.COMPLETE:
                raise ValueError(f"{bp_pe.uuid} has {bp_pe.status=}. Please check me.")

        self.validate_business_payout_in_ledger(ext_ref_id=ext_ref_id, thl_lm=thl_lm)
        self.update_business_payout_event(pk=bpe.id, status=PayoutStatus.COMPLETE)

        return None

    def get_business_payout_events_for_business(
        self,
        business_uuid: UUIDStr,
        order_by: OrderBy | None = OrderBy.ASC,
    ) -> list[BusinessPayoutEvent]:
        order_by = order_by or OrderBy.ASC
        bpes = self.filter_by(
            business_uuids=[business_uuid],
        )
        bpes = sorted(bpes, key=lambda x: x.created, reverse=order_by == OrderBy.DESC)
        return bpes

    @staticmethod
    def recoup_proportional(
        df: pd.DataFrame,
        target_amount: USDCent | NonNegativeInt,
    ) -> pd.DataFrame:
        """
        Recoup a target amount from rows proportionally based on a numeric column.

        Does not filter the dataframe. Length in == Length out

        Parameters:
        - df: pandas DataFrame
        - target_amount: total amount to recoup

        Returns:
        - A new DataFrame with columns:
            - original amounts
            - weights
            - proposed and actual deductions
            - remaining balances
        """
        w_df = df.copy(deep=True)
        target_amount = USDCent(target_amount)
        total_available = int(w_df["available_balance"].sum())

        if total_available == 0:
            raise ValueError("Total available amount is empty, cannot recoup")

        if int(target_amount) > total_available:
            raise ValueError(
                f"Target amount ({target_amount}) exceeds total available "
                f"({total_available})."
            )

        # Calculate weight and proportional deduction
        w_df["weight"] = w_df["available_balance"] / total_available
        w_df["raw_deduction"] = w_df["weight"] * target_amount
        w_df["deduction"] = np.floor(w_df["raw_deduction"]).astype(int)
        w_df["remainder"] = w_df["raw_deduction"] - w_df["deduction"]
        # While this is updated initially, we'll also update it on every
        #   loop to make sure we only pull from
        w_df["remaining_balance"] = w_df["available_balance"] - w_df["deduction"]

        shortfall: int = int(target_amount) - w_df["deduction"].sum()

        while shortfall > 0:
            # Distribute remaining cents to rows with the largest remainder
            extra_idxs = (
                w_df[w_df["remaining_balance"] >= 1]
                .sort_values(by="weight", ascending=False)
                .index[:shortfall]
            )
            w_df.loc[extra_idxs, "deduction"] += 1

            shortfall: int = int(target_amount) - w_df["deduction"].sum()
            w_df["remaining_balance"] = w_df["available_balance"] - w_df["deduction"]

        assert w_df[
            w_df["deduction"] > w_df["available_balance"]
        ].empty, "Trying to deduct more from an Product than what is available"

        return w_df

    @staticmethod
    def distribute_amount(
        df: pd.DataFrame,
        amount: USDCent,
        weight_col: str = "weight",
        balance_col: str = "remaining_balance",
    ) -> pd.Series:
        """
        Distributes an integer amount across dataframe rows proportionally,
        ensuring the total equals exactly the desired amount (in cents).

        Parameters:
        -----------
        df : pd.DataFrame
            The dataframe with product information
        amount : USDCent
            The total amount to distribute (in cents)
        weight_col : str
            Column name containing the weights
        balance_col : str
            Column name containing the balance constraint

        Returns:
        --------
        pd.Series
            A series with integer allocations that sum to exactly the amount
        """
        res_df = df.copy(deep=True)

        # Calculate ideal fractional allocation
        ideal_allocation = res_df[weight_col] * int(amount)

        # Ensure we don't exceed available balance
        ideal_allocation = np.minimum(ideal_allocation, res_df[balance_col])

        # Start with floor values
        allocation = np.floor(ideal_allocation).astype(int)

        # Calculate remainders
        remainders = ideal_allocation - allocation

        # Distribute the remaining cents to rows with largest remainders
        shortage = int(amount) - allocation.sum()

        if shortage > 0:

            assert shortage < len(remainders), (
                "The shortage cent amount must be less than or equal to the "
                "length of the remainders if we intend of taking a penny "
                "from each"
            )

            remainders.sort_values(ascending=False, inplace=True)
            from itertools import islice

            # Add 1 cent to the top 'shortage' rows
            for idx, _ in islice(remainders.items(), shortage):
                # Only add if it doesn't exceed the balance
                if allocation.loc[idx] < df[balance_col].loc[idx]:
                    allocation.loc[idx] += 1

        return allocation

    def create_business_payout_event(
        self,
        bpe: BusinessPayoutEvent,
    ):
        assert bpe.bp_payouts, "Must provide at least one BP Payout"
        assert {bp_pe.status for bp_pe in bpe.bp_payouts} == {
            PayoutStatus.PENDING
        }, "All BP Payouts must be PENDING"
        assert bpe.id is None, "Cannot create a BusinessPayoutEvent with an existing ID"

        with self.pg_config.make_connection() as conn:
            with conn.cursor() as c:
                # ext_ref_id has a unique constraint, so we don't need to even
                #   do an existence check first
                c.execute(
                    """
                INSERT INTO supplier_payout (
                    business_id, created, amount,
                    status, ext_ref_id, payout_type,
                    request_data, order_data
                ) VALUES (
                    %(business_id)s, %(created)s, %(amount)s, 
                    %(status)s, %(ext_ref_id)s, %(payout_type)s, 
                    %(request_data)s, %(order_data)s 
                ) RETURNING id;
                """,
                    bpe.model_dump_postgres(),
                )
                supplier_payout_pk = c.fetchone()["id"]
                bpe.id = supplier_payout_pk
                for bp_pe in bpe.bp_payouts:
                    c.execute(
                        """
                        INSERT INTO event_payout (
                            uuid, debit_account_uuid, created, cashout_method_uuid,
                            amount, status, ext_ref_id, payout_type, order_data,
                            request_data, supplier_payout_id
                        ) VALUES (
                            %(uuid)s, %(debit_account_uuid)s, %(created)s, %(cashout_method_uuid)s,
                            %(amount)s, %(status)s, %(ext_ref_id)s, %(payout_type)s, %(order_data)s,
                            %(request_data)s, %(supplier_payout_id)s
                        );
                        """,
                        bp_pe.model_dump_postgres()
                        | {"supplier_payout_id": supplier_payout_pk},
                    )
            conn.commit()

    def create_from_ach_or_wire(
        self,
        business: Business,
        amount: USDCent,
        transaction_id: str,
        pm: ProductManager,
        thl_lm: ThlLedgerManager,
        created: datetime | None = None,
    ) -> BusinessPayoutEvent | None:
        """This records a single banking transfer to a supplier. Takes a
        specific Business that was paid out and how much. It then determines
        how to distribute the amount to each Brokerage Product in the
        Business.
        """
        assert business.balance is not None, (
            "Must provide a full version of a Business in order to calculate"
            "the required Brokerage Product amounts."
        )

        assert amount >= 100_00, "Must issue Supplier Payouts at least $100 minimum."
        LOG.warning(f"Paying out {business.name} {amount.to_usd_str()}")

        if created:
            LOG.warning("Payouts in the past, require the parquet files to be rebuilt.")
            assert created.tzinfo == timezone.utc, "created must be UTC"
            assert created < datetime.now(
                tz=timezone.utc
            ), "created must be in the past"
        else:
            created = datetime.now(tz=timezone.utc)

        # Gather the total amount available balance from each and put into
        #   a simple DF. We're using the available balance because we need it
        #   to always be positive. We never want to get into a negative
        #   situation again, so it's best to be extra conservative.
        balances = {
            pb.product_id: pb.available_balance
            for pb in business.balance.product_balances
        }
        df = pd.DataFrame.from_dict(balances, orient="index").reset_index()
        df.columns = ["product_id", "available_balance"]

        df = BusinessPayoutEventManager.recoup_proportional(
            df=df, target_amount=business.balance.recoup
        )

        # Can't pay any Products that don't have a remaining balance
        df = df[df["remaining_balance"] > 0].copy()

        assert (
            df.deduction.sum() == business.balance.recoup
        ), "recoup_proportional failure"

        df["issue_amount"] = BusinessPayoutEventManager.distribute_amount(
            df=df, amount=amount
        )

        assert df.issue_amount.sum() == amount, "issue_amount failure"

        # Can't pay any Products that don't have an issue amount
        df = df[df["issue_amount"] > 0].copy()

        amounts: dict[str, dict[str, int]] = df.set_index("product_id")[
            ["remaining_balance", "issue_amount"]
        ].to_dict(orient="index")

        products = pm.get_by_uuids(product_uuids=list(amounts.keys()))
        product_lookup = {p.uuid: p for p in products}

        # Bulk version of this ---v
        # bp_wallet = thl_lm.get_account_or_create_bp_wallet(product=product)
        qualified_names = [
            f"{thl_lm.currency.value}:bp_wallet:{bp.id}" for bp in products
        ]
        bp_wallets = thl_lm.get_accounts(qualified_names)
        wallet_lookup = {bpw.reference_uuid: bpw.uuid for bpw in bp_wallets}

        bpe = BusinessPayoutEvent(
            id=None,
            business_id=business.uuid,
            payout_type=PayoutType.ACH,
            amount=amount,
            created=created,
            ext_ref_id=transaction_id,
            # The ACH payment was sent! We haven't yet recorded it
            #   in the ledger, but it was sent by the bank. This
            #   is kind of ambiguous the meaning, we'll say it
            #   is not yet COMPLETE b/c the bp payouts
            #   haven't all been created yet.
            status=PayoutStatus.APPROVED,
        )

        bp_payouts: list[BrokerageProductPayoutEvent] = []
        for product_id, item in amounts.items():
            product = product_lookup[product_id]
            bp_payouts.append(
                BrokerageProductPayoutEvent(
                    created=created,
                    payout_type=PayoutType.ACH,
                    status=PayoutStatus.PENDING,
                    uuid=uuid4().hex,
                    amount=USDCent(item["issue_amount"]),
                    ext_ref_id=transaction_id,
                    product_id=product.uuid,
                    cashout_method_uuid=self.CASHOUT_METHOD_UUID,
                    debit_account_uuid=wallet_lookup[product_id],
                )
            )
        bpe.bp_payouts = bp_payouts
        # The supplier_payout db row and all event_payout (BP rows) are all
        #   created in the same DB transaction.
        self.create_business_payout_event(bpe=bpe)
        assert bpe.id is not None, "Something failed creating BusinessPayoutEvent"

        # Now, go through each and create ledger txs. This is resumable
        #   from the BrokerageProductPayoutEvents
        for bp_pe in bpe.bp_payouts:
            product = product_lookup[bp_pe.product_id]
            self._create_tx_bp_payout_from_payout_event(
                thl_ledger_manager=thl_lm,
                bp_pe=bp_pe,
                product=product,
                skip_one_per_day_check=True,
                skip_wallet_balance_check=True,
            )

        self.update_business_payout_event(pk=bpe.id, status=PayoutStatus.COMPLETE)

        return bpe

    def update_business_payout_event(self, pk: int, status: PayoutStatus):
        with self.connection() as conn:
            with conn.cursor() as c:
                c.execute(
                    """
                UPDATE supplier_payout
                SET status = %(status)s
                WHERE id = %(pk)s""",
                    {"pk": pk, "status": status},
                )
                assert c.rowcount == 1, f"{id=} not found"
            conn.commit()
        return None


# import duckdb
# conn = duckdb.connect()
# conn.execute("""
# select * from read_parquet('/mnt/thl-incite/raw/df-collections/ledger/*/*.parquet')
# where event_payout is not null
#     and direction =1
#     and reference_uuid in ?
# """, [b.product_uuids])
# df = conn.fetch_df()
# df['ext_description'].value_counts()
#
# tx_ids = [35554404, 37210650]
# conn.execute("""
# select * from read_parquet('/mnt/thl-incite/raw/df-collections/ledger/*/*.parquet')
# where tx_id in ?
# """, [tx_ids])
# df = conn.fetch_df()
