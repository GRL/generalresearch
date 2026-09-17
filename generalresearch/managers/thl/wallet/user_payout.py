import uuid
from collections.abc import Collection
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import slack
from pydantic import PositiveInt
from redis import Redis

from generalresearch.currency import USDCent
from generalresearch.managers.thl.cashout_method import CashoutMethodManager
from generalresearch.managers.thl.ipinfo import GeoIpInfoManager
from generalresearch.managers.thl.ledger_manager.thl_ledger import ThlLedgerManager
from generalresearch.managers.thl.payout import PayoutEventManager
from generalresearch.managers.thl.userhealth import UserIpHistoryManager
from generalresearch.managers.thl.wallet.tango import TangoManager
from generalresearch.models.custom_types import AwareDatetimeISO, UUIDStr
from generalresearch.models.thl.definitions import PayoutStatus
from generalresearch.models.thl.payout import UserPayoutEvent
from generalresearch.models.thl.user import User
from generalresearch.models.thl.wallet.cashout_method import (
    CashMailOrderData,
    CashoutMethod,
    CashoutRequestInfo,
    PaypalCashoutMethodRequestData,
    TangoCashoutMethodRequestData,
    CashMailCashoutMethodRequestData,
)
from generalresearch.models.thl.wallet.definitions import PayoutType


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

        transaction_info = {}
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
            created=created or datetime.now(tz=UTC),
            amount=amount,
            status=status or PayoutStatus.PENDING,
            ext_ref_id=ext_ref_id,
            payout_type=payout_type,
            request_data=request_data or {},
            order_data=order_data,
        )
        d = payout_event.model_dump_postgres()

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

    def user_request_redeem(
        self,
        user: User,
        country_iso: str,
        cashout_method_id: str,
        amount: USDCent,
        tango_manager: TangoManager,
        cashout_method_manager: CashoutMethodManager,
        ledger_manager: ThlLedgerManager,
        user_ip_history_manager: UserIpHistoryManager,
        geoip_info_manager: GeoIpInfoManager,
            redis_client: Redis,
        slack_client: slack.WebClient | None = None,
    ) -> UserPayoutEvent:
        """
        A user has requested to be paid from their wallet balance.
        :param user: User
        :param cashout_method_id: the cashout method to use
        :param amount_usd: In decimal USD.
        """
        now = datetime.now(tz=UTC)
        user.prefetch_product(pg_config=self.pg_config)
        usd_exchange_rates = tango_manager.get_exchange_rates()
        cashout_methods = cashout_method_manager.get_user_cashout_methods(
            user, country_iso=country_iso, usd_exchange_rate=usd_exchange_rates
        )
        assert cashout_method_id in cashout_methods, (
            f"cashout method '{cashout_method_id}' not found"
        )
        cashout_method = cashout_methods[cashout_method_id]

        # Make sure the amount requested is allowed for this cashout method
        try:
            cashout_method.validate_requested_amount(amount=amount)
        except ValueError as e:
            # Outer function is expecting an assertion error.
            raise AssertionError(str(e))

        """
        These checks are run when a user requests any kind of payout:
            - user wallet enabled on BP
            - is user blocked
            - is user anonymous
            - does the user have a redeemable_balance <= amount requested (except for
                an AMT assignment request)
        """
        assert type(amount) is USDCent
        user.prefetch_product(pg_config=self.pg_config)
        assert user.product.user_wallet_enabled, (
            "SubmitUserCashout called on user without managed wallet"
        )
        assert not user.blocked, "Blocked user requesting redemption"
        assert 0 < amount <= 250_00, (
            f"Amount must be between 0 and $250.00. Got {amount.to_usd_str()}"
        )

        product = user.product
        banned_countries = user.product.user_health_config.banned_countries

        assert not user_ip_history_manager.is_user_anonymous(
            user, geoip_info_manager=geoip_info_manager
        ), "Anonymous user requesting redemption"
        ipr = user_ip_history_manager.get_user_latest_ip_record(
            user, geoip_info_manager=geoip_info_manager
        )
        if ipr is not None and ipr.country_iso in banned_countries:
            raise AssertionError("Banned country requesting redemption")

        wallet_balance = ledger_manager.get_user_wallet_balance(user)
        if product.user_wallet_config.balance_type == "wallet_balance":
            redeemable_amount = wallet_balance
        elif product.user_wallet_config.balance_type == "redeemable_balance":
            redeemable_amount = ledger_manager.get_user_redeemable_wallet_balance(user, wallet_balance)
        else:
            raise ValueError(f"unexpected balance_type={product.user_wallet_config.balance_type}")

        assert amount <= redeemable_amount, (
            f"User requesting more than their redeemable balance ({amount} > {redeemable_amount}}"
        )

        # Simple dedupe mechanism. Don't allow more than 1 per user_id per minute per cashout_method.
        flag_just_set = bool(
            redis_client.set(
                f"user_request_redeem:{user.user_id}:{cashout_method}",
                1,
                nx=True,
                ex=60,
            )
        )
        assert flag_just_set, "User requesting more than 1 per min per cashout method"

        payout_type = cashout_method.type

        if slack_client:
            slack_client.chat_postMessage(
                channel="cashouts",
                text=f"{user.product.name} {cashout_method.type} "
                f"{user.user_id} {amount.to_usd_str()}",
            )

        pe_uuid = uuid.uuid4().hex

        if payout_type == PayoutType.TANGO:
            request_data: TangoCashoutMethodRequestData = tango_manager.make_request(
                amount, cashout_method, pe_uuid
            )
        elif payout_type == PayoutType.PAYPAL:
            request_data: PaypalCashoutMethodRequestData = make_request_paypal(cashout_method)
        elif payout_type == PayoutType.CASH_IN_MAIL:
            request_data: CashMailCashoutMethodRequestData = CashMailCashoutMethodRequestData.model_validate(cashout_method.data.model_dump())
        else:
            raise ValueError(f"unknown {payout_type=}")

        account = ledger_manager.get_account_or_create_user_wallet(user)

        pe = self.create(
            uuid=pe_uuid,
            debit_account_uuid=account.uuid,
            cashout_method_uuid=cashout_method_id,
            amount=amount,
            created=now,
            payout_type=payout_type,
            request_data=request_data.model_dump(mode="json"),
        )

        ledger_manager.create_tx_user_payout_request(user, payout_event=pe, created=now)
        return pe





def make_request_paypal(
    cashout_method: CashoutMethod,
) -> PaypalCashoutMethodRequestData:
    return PaypalCashoutMethodRequestData.model_validate(
        {
            "email": cashout_method.data.email,
            "interface": "api",
        }
    )
