from collections import defaultdict
from datetime import timezone, datetime, timedelta
from random import randint, choice as rand_choice
from time import sleep
from typing import Collection, Optional, Dict, List, Union
from uuid import UUID, uuid4

import numpy as np
import pandas as pd
from psycopg import sql
from pydantic import AwareDatetime, PositiveInt, NonNegativeInt

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
    LedgerAccount,
    Direction,
    OrderBy,
)
from generalresearch.models.thl.payout import (
    PayoutEvent,
    UserPayoutEvent,
    BrokerageProductPayoutEvent,
    BusinessPayoutEvent,
)
from generalresearch.models.thl.product import Product
from generalresearch.models.thl.wallet import PayoutType
from generalresearch.models.thl.wallet.cashout_method import (
    CashoutRequestInfo,
    CashMailOrderData,
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
        payout_event: Union[UserPayoutEvent, BrokerageProductPayoutEvent],
        status: PayoutStatus,
        ext_ref_id: Optional[str] = None,
        order_data: Optional[Dict] = None,
    ) -> None:
        # These 3 things are the only modifiable attributes
        ext_ref_id = ext_ref_id if ext_ref_id is not None else payout_event.ext_ref_id
        order_data = order_data if order_data is not None else payout_event.order_data
        payout_event.update(status=status, ext_ref_id=ext_ref_id, order_data=order_data)

        d = payout_event.model_dump_mysql()
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

        return None


class UserPayoutEventManager(PayoutEventManager):

    def get_by_uuid(self, pe_uuid: UUIDStr) -> UserPayoutEvent:

        res = self.pg_config.execute_sql_query(
            query=f"""
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
        order: Dict = pe.order_data
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
        reference_uuid: Optional[str] = None,
        debit_account_uuids: Optional[Collection[UUIDStr]] = None,
        amount: Optional[int] = None,
        created: Optional[datetime] = None,
        created_after: Optional[datetime] = None,
        product_ids: Collection[str] = None,
        bp_user_ids: Optional[Collection[str]] = None,
        cashout_method_uuids: Collection[UUIDStr] = None,
        cashout_types: Optional[Collection[PayoutType]] = None,
        statuses: Optional[Collection[PayoutStatus]] = None,
    ) -> List[UserPayoutEvent]:
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
        uuid: Optional[UUIDStr] = None,
        status: Optional[PayoutStatus] = None,
        created: Optional[AwareDatetimeISO] = None,
        request_data: Optional[Dict] = None,
        # --- Optional: None  ---
        account_reference_type: Optional[str] = None,
        account_reference_uuid: Optional[UUIDStr] = None,
        description: Optional[str] = None,
        ext_ref_id: Optional[str] = None,
        order_data: Optional[Dict | CashMailOrderData] = None,
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
                    query=f"""
                    INSERT INTO event_payout (
                        uuid, debit_account_uuid, created, cashout_method_uuid, amount,
                        status, ext_ref_id, payout_type, order_data, request_data
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
        uuid: Optional[UUIDStr] = None,
        debit_account_uuid: Optional[UUIDStr] = None,
        account_reference_type: Optional[str] = None,
        account_reference_uuid: Optional[UUIDStr] = None,
        cashout_method_uuid: Optional[UUIDStr] = None,
        description: Optional[str] = None,
        created: Optional[AwareDatetimeISO] = None,
        amount: Optional[PositiveInt] = None,
        status: Optional[PayoutStatus] = None,
        ext_ref_id: Optional[str] = None,
        payout_type: Optional[PayoutType] = None,
        request_data: Optional[Dict] = None,
        order_data: Optional[Dict | CashMailOrderData] = None,
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
        # --- Support resources ---
        account_product_mapping: Optional[Dict[UUIDStr, UUIDStr]] = None,
    ) -> BrokerageProductPayoutEvent:

        res = self.pg_config.execute_sql_query(
            query=f"""
            SELECT  ep.uuid,
                    ep.debit_account_uuid,
                    ep.cashout_method_uuid, 
                    ep.created, ep.amount, ep.status, ep.ext_ref_id, ep.payout_type, 
                    ep.request_data::jsonb,
                    ep.order_data::jsonb
            FROM event_payout AS ep
            WHERE ep.uuid = %s
        """,
            params=[pe_uuid],
        )
        assert len(res) == 1, f"{pe_uuid} expected 1 result, got {len(res)}"

        d = res[0]

        # This isn't really need for creation... but we're doing it so that
        #   it can return back a full BrokerageProductPayoutEvent instance
        if account_product_mapping is None:
            rc = self.redis_client
            account_product_mapping: Dict = rc.hgetall(name="pem:account_to_product")
            assert isinstance(account_product_mapping, dict)
        d["product_id"] = account_product_mapping[d["debit_account_uuid"]]

        return BrokerageProductPayoutEvent.model_validate(d)

    @staticmethod
    def check_for_ledger_tx(
        thl_ledger_manager: ThlLedgerManager,
        product_id: UUIDStr,
        amount: USDCent,
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

    def create(
        self,
        uuid: Optional[UUIDStr] = None,
        debit_account_uuid: Optional[UUIDStr] = None,
        created: AwareDatetimeISO = None,
        amount: PositiveInt = None,
        status: Optional[PayoutStatus] = None,
        ext_ref_id: Optional[str] = None,
        payout_type: PayoutType = None,
        request_data: Dict = None,
        order_data: Optional[Dict | CashMailOrderData] = None,
        # --- Support resources ---
        account_product_mapping: Optional[Dict[UUIDStr, UUIDStr]] = None,
    ) -> BrokerageProductPayoutEvent:
        if request_data is None:
            request_data = dict()

        # This isn't really need for creation... but we're doing it so that
        #   it can return back a full BrokerageProductPayoutEvent instance
        if account_product_mapping is None:
            rc = self.redis_client
            account_product_mapping: Dict = rc.hgetall(name="pem:account_to_product")
            assert isinstance(account_product_mapping, dict)
        product_id = account_product_mapping[debit_account_uuid]

        bp_payout_event = BrokerageProductPayoutEvent(
            uuid=uuid or uuid4().hex,
            debit_account_uuid=debit_account_uuid,
            cashout_method_uuid=self.CASHOUT_METHOD_UUID,
            created=created or datetime.now(tz=timezone.utc),
            amount=amount,
            status=status,
            ext_ref_id=ext_ref_id,
            payout_type=payout_type,
            request_data=request_data,
            order_data=order_data,
            product_id=product_id,
        )
        d = bp_payout_event.model_dump_mysql()

        self.pg_config.execute_write(
            query=f"""
            INSERT INTO event_payout (
                uuid, debit_account_uuid, created, cashout_method_uuid, amount,
                status, ext_ref_id, payout_type, order_data, request_data
            ) VALUES (
                %(uuid)s, %(debit_account_uuid)s, %(created)s, 
                %(cashout_method_uuid)s, %(amount)s, %(status)s, 
                %(ext_ref_id)s, %(payout_type)s, %(order_data)s, 
                %(request_data)s
            );
        """,
            params=d,
        )

        return bp_payout_event

    def filter_by(
        self,
        reference_uuid: Optional[str] = None,
        ext_ref_id: Optional[str] = None,
        debit_account_uuids: Optional[Collection[UUIDStr]] = None,
        amount: Optional[int] = None,
        created: Optional[datetime] = None,
        created_after: Optional[datetime] = None,
        product_ids: Collection[str] = None,
        bp_user_ids: Optional[Collection[str]] = None,
        cashout_types: Optional[Collection[PayoutType]] = None,
        statuses: Optional[Collection[PayoutStatus]] = None,
    ) -> List[BrokerageProductPayoutEvent]:
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
        if ext_ref_id:
            # This is transaction id for tracking ACH/Wires with a banking
            #   institution
            filters.append("ep.ext_ref_id = %s")
            args.append(ext_ref_id)
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
        if cashout_types:
            filters.append("payout_type = ANY(%s)")
            args.append([x.value for x in cashout_types])
        if statuses:
            filters.append("status = ANY(%s)")
            args.append([x.value for x in statuses])

        assert len(filters) > 0, "must pass at least 1 filter"
        filter_str = " AND ".join(filters)

        res = self.pg_config.execute_sql_query(
            query=f"""
                SELECT  ep.uuid, 
                        ep.debit_account_uuid, 
                        ep.cashout_method_uuid, 
                        ep.created, 
                        ep.amount, ep.status, ep.ext_ref_id, ep.payout_type, 
                        ep.request_data::jsonb, ep.order_data::jsonb,
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
                WHERE cashout_method_uuid = '{self.CASHOUT_METHOD_UUID}'
                    AND {filter_str}
            """,
            params=args,
        )

        rc = self.redis_client
        account_product_mapping = rc.hgetall(name="pem:account_to_product")

        pes = []
        for d in res:
            for k in [
                "uuid",
                "debit_account_uuid",
                "account_reference_uuid",
                "cashout_method_uuid",
            ]:
                if d[k] is not None:
                    d[k] = UUID(d[k]).hex

            d["product_id"] = account_product_mapping[d["debit_account_uuid"]]
            pes.append(BrokerageProductPayoutEvent.model_validate(d))

        return pes

    def get_bp_payout_events_for_accounts(
        self, accounts: Collection[LedgerAccount]
    ) -> List[BrokerageProductPayoutEvent]:
        return self.filter_by(
            debit_account_uuids=[i.uuid for i in accounts],
            cashout_types=[PayoutType.ACH],
        )

    def get_bp_bp_payout_events_for_products(
        self,
        thl_ledger_manager: ThlLedgerManager,
        product_uuids: Collection[UUIDStr],
        order_by: Optional[OrderBy] = OrderBy.ASC,
    ) -> List["BrokerageProductPayoutEvent"]:
        """This is a terrible name, but it returns the
        BPPayoutEvent model type rather than a list of PayoutEvents.

        We do this for the Supplier centric APIs where they don't know,
        or care about the underlying ledger account structure.
        """
        assert len(product_uuids) > 0, "Must provide product_uuids"
        accounts = thl_ledger_manager.get_accounts_bp_wallet_for_products(
            product_uuids=product_uuids
        )

        assert len(accounts) == len(product_uuids), "Unequal Product & Account lists"

        rc = self.redis_client
        account_product_mapping = rc.hgetall(name="pem:account_to_product")

        payout_events: List[BrokerageProductPayoutEvent] = (
            self.get_bp_payout_events_for_accounts(
                accounts=accounts,
            )
        )

        return BrokerageProductPayoutEvent.from_payout_events(
            payout_events=payout_events,
            account_product_mapping=account_product_mapping,
            order_by=order_by,
        )

    def retry_create_bp_payout_event_tx(
        self,
        thl_ledger_manager: ThlLedgerManager,
        product: Product,
        payout_event_uuid: UUIDStr,
        skip_wallet_balance_check: bool = False,
        skip_one_per_day_check: bool = False,
    ) -> BrokerageProductPayoutEvent:
        """If a create_bp_payout_event call fails, this can be called with
        the associated payoutevent.
        """
        bp_pe: BrokerageProductPayoutEvent = self.get_by_uuid(payout_event_uuid)
        assert bp_pe.status == PayoutStatus.FAILED, "Only use this on failed payouts"
        created = bp_pe.created

        assert not self.check_for_ledger_tx(
            thl_ledger_manager=thl_ledger_manager,
            payout_event=bp_pe,
            product_id=bp_pe.product_id,
            amount=bp_pe.amount_usd,
        ), "Transaction exists! You should mark the payout event status as complete"

        return self._create_tx_bp_payout_from_payout_event(
            thl_ledger_manager=thl_ledger_manager,
            bp_pe=bp_pe,
            product=product,
            amount=bp_pe.amount_usd,
            created=created,
            skip_one_per_day_check=skip_one_per_day_check,
            skip_wallet_balance_check=skip_wallet_balance_check,
        )

    def create_bp_payout_event(
        self,
        thl_ledger_manager: ThlLedgerManager,
        product: Product,
        amount: USDCent,
        payout_type: PayoutType = PayoutType.ACH,
        ext_ref_id: Optional[str] = None,
        created: Optional[AwareDatetime] = None,
        skip_wallet_balance_check: bool = False,
        skip_one_per_day_check: bool = False,
    ) -> BrokerageProductPayoutEvent:
        """This should be called when a BP is paid out money from their
            wallet. Typically, this is an ACH payment. This function creates
            the PayoutEvent and the Ledger entries.

        :param thl_ledger_manager:
        :param product: The BP being paid. Assuming we're paying them out
            of the balance of their USD wallet account.
        :param amount: We're assuming everything is in USD, and we're
            paying out a USD currency account. We could theoretically also
            pay, for e.g. a Bitcoin account with a bitcoin transfer, but
            this is not supported for now.
        :param payout_type: PayoutType. default ACH
        :param cashout_method_uuid: The entry in the
            accounting_cashoutmethod table that records payment method
            details. By default, the generic ACH cashout method (that has
            no actual banking details).

        :param ext_ref_id: This is a unique ID for the Supplier Payment.
            Typically it'll be from JP Morgan Chase, but may also just be
            random if we can retrieve anything

        :param created:

        :param skip_wallet_balance_check: By default, this will fail unless
            the BP's wallet actually has the amount requested.

        :param skip_one_per_day_check: Safety mechanism, checks if there
            has already been a payout to this wallet in the past 24 hours.

        :return:
        """

        assert isinstance(amount, USDCent), "Must provide a USDCent"

        if created:
            # Try to do a quick dupe check first before we create the payout event
            pes = self.filter_by(
                reference_uuid=product.id, amount=amount, created=created
            )
            if len(pes) > 0:
                raise ValueError(f"Payout event already exists!: {pes}")

        if created is None:
            created = datetime.now(tz=timezone.utc)

        # TODO: Explain why we're doing this. Why is it important to have
        #   Payout Events when the ledger has everything that should be
        #   needed.
        bp_wallet = thl_ledger_manager.get_account_or_create_bp_wallet(product=product)

        bp_pe: BrokerageProductPayoutEvent = self.create(
            debit_account_uuid=bp_wallet.uuid,
            payout_type=payout_type,
            amount=amount,
            ext_ref_id=ext_ref_id,
            created=created,
            status=PayoutStatus.PENDING,
        )
        return self._create_tx_bp_payout_from_payout_event(
            thl_ledger_manager=thl_ledger_manager,
            bp_pe=bp_pe,
            product=product,
            amount=amount,
            created=created,
            skip_one_per_day_check=skip_one_per_day_check,
            skip_wallet_balance_check=skip_wallet_balance_check,
        )

    def _create_tx_bp_payout_from_payout_event(
        self,
        thl_ledger_manager: ThlLedgerManager,
        bp_pe: BrokerageProductPayoutEvent,
        product: Product,
        amount: USDCent,
        created: Optional[AwareDatetime] = None,
        skip_wallet_balance_check: bool = False,
        skip_one_per_day_check: bool = False,
    ) -> BrokerageProductPayoutEvent:
        """
        This should not be called directly.
        Creates the ledger transaction for a BP Payout, given a PayoutEvent.
        Handles exceptions: Check if the ledger tx actually exists or not, and set the
            payout event status accordingly.
        """
        try:
            thl_ledger_manager.create_tx_bp_payout(
                product=product,
                amount=amount,
                payoutevent_uuid=bp_pe.uuid,
                created=created,
                skip_wallet_balance_check=skip_wallet_balance_check,
                skip_one_per_day_check=skip_one_per_day_check,
            )

        except Exception as e:
            e.pe_uuid = bp_pe.uuid
            if self.check_for_ledger_tx(
                thl_ledger_manager=thl_ledger_manager,
                product_id=product.uuid,
                amount=amount,
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
    ) -> List[BrokerageProductPayoutEvent]:
        account = thl_ledger_manager.get_account_or_create_bp_wallet(product=product)
        return self.get_bp_payout_events_for_accounts(accounts=[account])

    def get_bp_payout_events_for_account(
        self, account: LedgerAccount
    ) -> List[BrokerageProductPayoutEvent]:
        return self.get_bp_payout_events_for_accounts(accounts=[account])

    def get_bp_payout_events_for_products(
        self,
        thl_ledger_manager: ThlLedgerManager,
        product_uuids: Collection[UUIDStr],
    ) -> List[BrokerageProductPayoutEvent]:
        accounts = thl_ledger_manager.get_accounts_bp_wallet_for_products(
            product_uuids=product_uuids
        )
        return self.get_bp_payout_events_for_accounts(accounts=accounts)


class BusinessPayoutEventManager(BrokerageProductPayoutEventManager):

    def update_ext_reference_ids(
        self,
        new_value: str,
        current_value: Optional[str] = None,
    ) -> None:
        """
        There are scenarios where an ACH/Wire payout event was saved with
        a generic or anonymized reference identifier. We may want to be
        able to go back and update all of those transaction IDs.

        """

        if current_value is None:
            raise ValueError("Dangerous to do ambiguous updates")

        # SELECT first to check that records exist
        res = self.filter_by(ext_ref_id=current_value)
        if len(res) == 0:
            raise Warning("No event_payouts found to UPDATE")

        # As of 2025, no single Business has more than 10,000 Products,
        #   leave the limit in as an additional safeguard.
        query = """
            UPDATE event_payout
            SET ext_ref_id = %s
            WHERE ext_ref_id = %s
        """
        with self.pg_config.make_connection() as conn:
            with conn.cursor() as c:
                c.execute(query=query, params=[new_value, current_value])
                assert c.rowcount < 10000
            conn.commit()

        return None

    def delete_failed_business_payout(self, ext_ref_id: str, thl_lm: ThlLedgerManager):
        """
        Sometimes ACH/Wire payouts fail due to multiple reasons (timeouts,
        Business Product having insufficient funds, etc). This is a utility
        method that finds all event_payouts, and deletes them with all the
        associated:
            (1) Transactions
            (2) Transaction Metadata
            (3) Transaction Entries

        and then proceeds to delete them all in reverse order (so there is
        no orphan / FK constraint issues).
        """

        # (1) Find all by payout_event
        event_payouts = self.filter_by(ext_ref_id=ext_ref_id)
        if len(event_payouts) == 0:
            raise Warning("No event_payouts found to DELETE")

        # sum([i["amount"] for i in event_payouts])/100
        event_payout_uuids = [i.uuid for i in event_payouts]

        # (2) Find all ledger_transactions
        tags = [f"{thl_lm.currency.value}:bp_payout:{x}" for x in event_payout_uuids]
        transactions = thl_lm.get_txs_by_tags(tags=tags)
        transaction_ids = [tx.id for tx in transactions]
        print("XXX1", transaction_ids)
        # assert len(tags) == len(transactions)

        # (3) Find all ledger_transactionmetadata: assert two rows per tx
        tx_metadata_ids = thl_lm.get_tx_metadata_ids_by_txs(transactions=transactions)
        # assert len(tx_metadata) == len(transaction_ids)*2

        # (4) Find all ledger_entry: assert two rows per tx
        tx_entries = thl_lm.get_tx_entries_by_txs(transactions=transactions)
        tx_entry_ids = [tx_entry.id for tx_entry in tx_entries]
        # assert len(tx_entry) == len(transaction_ids)*2

        # (5) Delete records

        # DELETE: tx_entry
        self.pg_config.execute_write(
            query="""
                DELETE
                FROM ledger_entry
                WHERE transaction_id = ANY(%s)
                    AND id = ANY(%s)
            """,
            params=[transaction_ids, tx_entry_ids],
        )

        # DELETE: tx_metadata
        self.pg_config.execute_write(
            query="""
                DELETE
                FROM ledger_transactionmetadata 
                WHERE transaction_id = ANY(%s)
                    AND id = ANY(%s)
            """,
            params=[transaction_ids, list(tx_metadata_ids)],
        )

        # DELETE: transactions
        self.pg_config.execute_write(
            query="""
                DELETE
                FROM ledger_transaction
                WHERE id = ANY(%s)
            """,
            params=[transaction_ids],
        )

        # DELETE: event_payouts
        self.pg_config.execute_write(
            query="""
                DELETE
                FROM event_payout
                WHERE ext_ref_id = %s 
                    AND uuid = ANY(%s)
            """,
            params=[ext_ref_id, event_payout_uuids],
        )

        return None

    def get_business_payout_events_for_products(
        self,
        thl_ledger_manager: ThlLedgerManager,
        product_uuids: Collection[UUIDStr],
        order_by: Optional[OrderBy] = OrderBy.ASC,
    ) -> List["BusinessPayoutEvent"]:
        res = self.get_bp_bp_payout_events_for_products(
            thl_ledger_manager=thl_ledger_manager,
            product_uuids=product_uuids,
            order_by=order_by,
        )

        return self.from_bp_payout_events(bp_payout_events=res)

    @staticmethod
    def from_bp_payout_events(
        bp_payout_events: Collection["BrokerageProductPayoutEvent"],
    ) -> List["BusinessPayoutEvent"]:
        if len(bp_payout_events) == 0:
            return []

        grouped = defaultdict(list)
        for bp_pe in bp_payout_events:
            grouped[bp_pe.ext_ref_id].append(bp_pe)

        res = []
        for ex_ref_id, members in grouped.items():
            res.append(BusinessPayoutEvent.model_validate({"bp_payouts": members}))

        return res

    @staticmethod
    def recoup_proportional(
        df: pd.DataFrame,
        target_amount: Union[USDCent, NonNegativeInt],
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
        weight_col="weight",
        balance_col="remaining_balance",
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
            for idx, value in islice(remainders.items(), shortage):
                # Only add if it doesn't exceed the balance
                if allocation.loc[idx] < df[balance_col].loc[idx]:
                    allocation.loc[idx] += 1

        return allocation

    def create_from_ach_or_wire(
        self,
        business: Business,
        amount: USDCent,
        pm: ProductManager,
        thl_lm: ThlLedgerManager,
        created: Optional[datetime] = None,
        transaction_id: Optional[str] = None,
    ) -> Optional[BusinessPayoutEvent]:
        """This records a single banking transfer to a supplier. Takes a
        specific Business that was paid out and how much. It then determines
        how to distribute the amount to each Brokerage Product in the
        Business.

        :param business
        :param amount
        :param pm
        :param thl_lm: this must have rw permissions to add transactions to
            the ledger
        :param created
        :param transaction_id

        :return:
        """
        assert business.balance is not None, (
            "Must provide a full version of a Business in order to calculate"
            "the required Brokerage Product amounts."
        )

        assert amount > 100_00, "Must issue Supplier Payouts at least $100 minimum."
        LOG.warning("Paying out ")

        if created:
            LOG.warning("Payouts in the past, require the parquet files to be rebuilt.")
            assert created < datetime.now(tz=timezone.utc)

        else:
            created = datetime.now(tz=timezone.utc)

        # Gather the total amount available balance from each and put into
        #   a simple DF. We're using the available balance because we need it
        #   to always be positive.. and we never want to get into a negative
        #   situation again, so it's best to be extra conservative.
        res = {
            pb.product_id: pb.available_balance
            for pb in business.balance.product_balances
        }
        df = pd.DataFrame.from_dict(res, orient="index").reset_index()
        df.columns = ["product_id", "available_balance"]

        res = BusinessPayoutEventManager.recoup_proportional(
            df=df, target_amount=business.balance.recoup
        )

        # Can't pay any Products that don't have a remaining balance
        res = res[res["remaining_balance"] > 0]

        assert (
            res.deduction.sum() == business.balance.recoup
        ), "recoup_proportional failure"

        res["issue_amount"] = BusinessPayoutEventManager.distribute_amount(
            df=res, amount=amount
        )

        assert res.issue_amount.sum() == amount, "issue_amount failure"

        # Can't pay any Products that don't have an issue amount
        res = res[res["issue_amount"] > 0]

        recouped_amounts: List[Dict[str, int]] = res[
            ["product_id", "remaining_balance", "issue_amount"]
        ].to_dict(orient="records")

        # Get all of the products at once so we're not doing it for every interation
        products = pm.get_by_uuids(
            product_uuids=[i["product_id"] for i in recouped_amounts]
        )

        bp_payouts: List[BrokerageProductPayoutEvent] = []
        for idx, item in enumerate(recouped_amounts):
            product = next((p for p in products if p.uuid == item["product_id"]), None)
            assert product is not None

            try:
                bp_pe: BrokerageProductPayoutEvent = self.create_bp_payout_event(
                    thl_ledger_manager=thl_lm,
                    product=product,
                    amount=USDCent(item["issue_amount"]),
                    created=created + timedelta(milliseconds=idx + 1),
                    ext_ref_id=transaction_id,
                )

                assert bp_pe.status == PayoutStatus.COMPLETE
                bp_payouts.append(bp_pe)

            except (Exception,) as e:
                # Cleanup bp_payouts
                print("Exception", e)
                return None

            if bp_pe.status == PayoutStatus.FAILED:
                sleep(1)

                try:
                    bp_pe = self.retry_create_bp_payout_event_tx(
                        thl_ledger_manager=thl_lm,
                        product=product,
                        payout_event_uuid=bp_pe.uuid,
                    )
                    assert bp_pe.status == PayoutStatus.COMPLETE
                    bp_payouts.append(bp_pe)

                except (Exception,) as e:
                    # Cleanup bp_payouts
                    return None

        return BusinessPayoutEvent.model_validate({"bp_payouts": bp_payouts})
