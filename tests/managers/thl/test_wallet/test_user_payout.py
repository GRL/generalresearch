from datetime import datetime
from decimal import Decimal
from random import randint
from unittest.mock import Mock
from uuid import uuid4

import pytest
from faker import Faker

from generalresearch.currency import USDCent
from generalresearch.managers.thl.cashout_method import CashoutMethodManager
from generalresearch.managers.thl.ipinfo import GeoIpInfoManager
from generalresearch.managers.thl.ledger_manager.thl_ledger import ThlLedgerManager
from generalresearch.managers.thl.userhealth import UserIpHistoryManager
from generalresearch.managers.thl.virtual_incentives_api import (
    VirtualIncentivesClient,
    VirtualIncentivesManager,
)
from generalresearch.managers.thl.wallet import manage_pending_cashout
from generalresearch.managers.thl.wallet.tango import TangoManager
from generalresearch.managers.thl.wallet.user_payout import (
    UserPayoutEventManager,
    make_request_paypal,
)
from generalresearch.models.thl.definitions import PayoutStatus
from generalresearch.models.thl.payout import UserPayoutEvent
from generalresearch.models.thl.product import UserWalletConfig
from generalresearch.models.thl.user import User
from generalresearch.models.thl.wallet.cashout_method import (
    CashMailCashoutMethodData,
    CashMailCashoutMethodRequestData,
    CashoutRequestDetail,
    PaypalCashoutMethodData,
    PaypalCashoutMethodRequestData,
    USDeliveryAddress,
)
from generalresearch.models.thl.wallet.definitions import PayoutType

fake = Faker()


class TestUserPayoutEventManager:
    def test_get_by_uuid_and_create(
        self,
        user: User,
        user_payout_event_manager: UserPayoutEventManager,
        thl_ledger_manager: ThlLedgerManager,
        utc_now: datetime,
    ):
        user_account = thl_ledger_manager.get_account_or_create_user_wallet(user=user)
        request_data = PaypalCashoutMethodRequestData.model_validate(
            {
                "email": fake.email(),
                "interface": "api",
            }
        )
        pe1: UserPayoutEvent = user_payout_event_manager.create(
            debit_account_uuid=user_account.uuid,
            payout_type=PayoutType.PAYPAL,
            cashout_method_uuid=uuid4().hex,
            amount=100,
            created=utc_now,
            request_data=request_data.model_dump(mode="json"),
        )
        # these get added by the query
        pe1.account_reference_type = "user"
        pe1.account_reference_uuid = user.uuid

        pe2 = user_payout_event_manager.get_by_uuid(pe_uuid=pe1.uuid)

        assert pe1 == pe2

    def test_get_payout_detail_cash_in_mail(
        self,
        user_with_wallet: User,
        user_payout_event_manager: UserPayoutEventManager,
        cashout_method_manager: CashoutMethodManager,
        thl_ledger_manager: ThlLedgerManager,
    ):
        user = user_with_wallet
        data = CashMailCashoutMethodData(
            delivery_address=USDeliveryAddress.model_validate(
                {
                    "name_or_attn": "Josh Ackerman",
                    "address": "123 Fake St",
                    "city": "San Francisco",
                    "state": "CA",
                    "postal_code": "12345",
                }
            )
        )
        cm = cashout_method_manager.create_cash_in_mail_cashout_method(
            data=data, user=user
        )
        user_account = thl_ledger_manager.get_account_or_create_user_wallet(user=user)

        rand_amount = randint(a=99, b=999)
        request_data = CashMailCashoutMethodRequestData.model_validate(
            cm.data.model_dump()
        )

        pe = user_payout_event_manager.create(
            debit_account_uuid=user_account.uuid,
            cashout_method_uuid=cm.id,
            amount=rand_amount,
            ext_ref_id=uuid4().hex,
            payout_type=PayoutType.CASH_IN_MAIL,
            request_data=request_data.model_dump(mode="json"),
        )

        res = user_payout_event_manager.get_payout_detail(pe_uuid=pe.uuid)
        assert isinstance(res, CashoutRequestDetail)

    def test_get_payout_detail_paypal(
        self,
        user_with_wallet: User,
        user_payout_event_manager: UserPayoutEventManager,
        cashout_method_manager: CashoutMethodManager,
        thl_ledger_manager: ThlLedgerManager,
    ):
        user = user_with_wallet
        data = PaypalCashoutMethodData(email=fake.email())
        cm = cashout_method_manager.create_paypal_cashout_method(data=data, user=user)
        user_account = thl_ledger_manager.get_account_or_create_user_wallet(user=user)

        rand_amount = randint(a=99, b=999)
        request_data = make_request_paypal(cm)
        pe = user_payout_event_manager.create(
            debit_account_uuid=user_account.uuid,
            cashout_method_uuid=cm.id,
            amount=rand_amount,
            ext_ref_id=uuid4().hex,
            payout_type=PayoutType.PAYPAL,
            request_data=request_data.model_dump(mode="json"),
        )

        res = user_payout_event_manager.get_payout_detail(pe_uuid=pe.uuid)
        assert isinstance(res, CashoutRequestDetail)


class TestUserRequestRedeem:
    @staticmethod
    def request_paypal_payout(
        *,
        user: User,
        amount: USDCent,
        user_payout_event_manager: UserPayoutEventManager,
        cashout_method_manager: CashoutMethodManager,
        thl_ledger_manager: ThlLedgerManager,
        user_iphistory_manager: UserIpHistoryManager,
        ip_record_factory,
        thl_redis_client,
        ip: str = "8.8.8.8",
        fund_amount: Decimal = Decimal("10.00"),
    ) -> UserPayoutEvent:
        email = fake.email()
        cashout_method = cashout_method_manager.create_paypal_cashout_method(
            data=PaypalCashoutMethodData(email=email),
            user=user,
        )
        ip_record_factory(user=user, ip=ip)
        thl_ledger_manager.create_tx_user_bonus(
            user=user,
            amount=fund_amount,
            ref_uuid=uuid4().hex,
            description="Fund PayPal redemption test",
        )

        tango_manager = Mock(spec=TangoManager)
        tango_manager.get_exchange_rates.return_value = {}

        return user_payout_event_manager.user_request_redeem(
            user=user,
            cashout_method_id=cashout_method.id,
            amount=amount,
            tango_manager=tango_manager,
            cashout_method_manager=cashout_method_manager,
            ledger_manager=thl_ledger_manager,
            user_ip_history_manager=user_iphistory_manager,
            redis_client=thl_redis_client,
        )

    def test_user_request_redeem_paypal(
        self,
        user_with_wallet: User,
        user_payout_event_manager: UserPayoutEventManager,
        cashout_method_manager: CashoutMethodManager,
        thl_ledger_manager: ThlLedgerManager,
        user_iphistory_manager: UserIpHistoryManager,
        geoip_info_manager: GeoIpInfoManager,
        ip_record_factory,
        thl_redis_client,
    ):
        user = user_with_wallet
        payout = self.request_paypal_payout(
            user=user,
            amount=USDCent(600),
            user_payout_event_manager=user_payout_event_manager,
            cashout_method_manager=cashout_method_manager,
            thl_ledger_manager=thl_ledger_manager,
            user_iphistory_manager=user_iphistory_manager,
            ip_record_factory=ip_record_factory,
            thl_redis_client=thl_redis_client,
        )

        assert payout.status == PayoutStatus.PENDING
        assert payout.payout_type == PayoutType.PAYPAL
        assert payout.amount == 600
        assert payout.request_data["interface"] == "api"
        assert thl_ledger_manager.get_user_wallet_balance(user) == 400
        assert user_payout_event_manager.get_by_uuid(payout.uuid) == payout

        detail = user_payout_event_manager.get_payout_detail(payout.uuid)
        assert not detail.transaction_info
        assert detail.description == "PayPal"

    def test_user_request_redeem_paypal_complete(
        self,
        user_with_wallet: User,
        user_payout_event_manager: UserPayoutEventManager,
        cashout_method_manager: CashoutMethodManager,
        thl_ledger_manager: ThlLedgerManager,
        user_iphistory_manager: UserIpHistoryManager,
        geoip_info_manager: GeoIpInfoManager,
        ip_record_factory,
        thl_redis_client,
        create_main_accounts,
    ):
        create_main_accounts()
        user = user_with_wallet
        payout = self.request_paypal_payout(
            user=user,
            amount=USDCent(600),
            user_payout_event_manager=user_payout_event_manager,
            cashout_method_manager=cashout_method_manager,
            thl_ledger_manager=thl_ledger_manager,
            user_iphistory_manager=user_iphistory_manager,
            ip_record_factory=ip_record_factory,
            thl_redis_client=thl_redis_client,
        )

        user_payout_event_manager.update(
            payout_event=payout,
            status=PayoutStatus.APPROVED,
            ext_ref_id="paypal-batch-id",
        )
        thl_ledger_manager.create_tx_user_payout_complete(
            user=user,
            payout_event=payout,
            fee_amount=Decimal("0.25"),
        )
        user_payout_event_manager.update(
            payout_event=payout,
            status=PayoutStatus.COMPLETE,
            order_data={"transaction_id": "paypal-transaction-id"},
        )

        detail = user_payout_event_manager.get_payout_detail(pe_uuid=payout.uuid)
        assert detail.status == PayoutStatus.COMPLETE
        assert detail.transaction_info == {"transaction_id": "paypal-transaction-id"}

        assert thl_ledger_manager.get_user_wallet_balance(user_with_wallet) == 400
        bp_expense = thl_ledger_manager.get_account_or_create_bp_expense_by_uuid(
            product_uuid=user.to_user_ref().product_id, expense_name="paypal"
        )
        assert thl_ledger_manager.get_account_balance(bp_expense) == -25

    def test_user_request_redeem_tango_complete(
        self,
        user_with_wallet: User,
        user_payout_event_manager: UserPayoutEventManager,
        cashout_method_manager: CashoutMethodManager,
        thl_ledger_manager: ThlLedgerManager,
        user_iphistory_manager: UserIpHistoryManager,
        ip_record_factory,
        thl_redis_client,
        create_main_accounts,
        setup_cashoutmethod_db,
        example_tango_cashout_methods,
    ):
        create_main_accounts()
        setup_cashoutmethod_db()
        user = user_with_wallet
        # This is the italian Amazon.it gift card
        cashout_method = example_tango_cashout_methods[1]
        # This is an IP that we have hard-coded by the geoip_info_manager as Italy
        ip_record_factory(user=user, ip="2.2.2.2")

        thl_ledger_manager.create_tx_user_bonus(
            user=user,
            amount=Decimal("5.00"),
            ref_uuid=uuid4().hex,
            description="Fund Tango redemption test",
        )

        tango_client = Mock()
        tango_client.get_exchange_rates.return_value = {
            "exchangeRates": [
                {"rewardCurrency": "USD", "baseCurrency": "EUR", "baseFx": 1.14792}
            ]
        }
        tango_manager = TangoManager(
            tango_client=tango_client,
            tango_account_id="test-account",
            tango_customer_id="test-customer",
            cashout_method_manager=cashout_method_manager,
        )
        request = tango_manager.make_request(
            amount=USDCent(2_00),
            cashout_method=cashout_method,
            payout_event_id=uuid4().hex,
        )
        assert request.amount == Decimal(2.0 / 1.14792).quantize(
            Decimal("0.01")
        )  # ~ 1.74 EUR

        payout = user_payout_event_manager.user_request_redeem(
            user=user,
            cashout_method_id=cashout_method.id,
            amount=USDCent(2_00),
            tango_manager=tango_manager,
            cashout_method_manager=cashout_method_manager,
            ledger_manager=thl_ledger_manager,
            user_ip_history_manager=user_iphistory_manager,
            redis_client=thl_redis_client,
        )
        assert payout.request_data["amount"] == "1.74"
        assert payout.request_data["externalRefID"] == payout.uuid

        user_payout_event_manager.update(
            payout_event=payout,
            status=PayoutStatus.APPROVED,
            ext_ref_id="tango-order-id",
        )
        thl_ledger_manager.create_tx_user_payout_complete(
            user=user,
            payout_event=payout,
        )
        user_payout_event_manager.update(
            payout_event=payout,
            status=PayoutStatus.COMPLETE,
            order_data={
                "reward": {
                    "credentialList": [
                        {
                            "credentialType": "giftCardCode",
                            "credentialValue": "TEST-CODE",
                        }
                    ],
                    "redemptionInstructions": "Use this code at checkout.",
                }
            },
        )

        detail = user_payout_event_manager.get_payout_detail(pe_uuid=payout.uuid)
        assert payout.payout_type == PayoutType.TANGO
        assert detail.status == PayoutStatus.COMPLETE
        assert detail.transaction_info == {
            "credential_list": [
                {
                    "credentialType": "giftCardCode",
                    "credentialValue": "TEST-CODE",
                }
            ],
            "redemption_instructions": "Use this code at checkout.",
        }
        assert thl_ledger_manager.get_user_wallet_balance(user) == 300
        bp_expense = thl_ledger_manager.get_account_or_create_bp_expense_by_uuid(
            product_uuid=user.to_user_ref().product_id,
            expense_name="tango",
        )
        assert thl_ledger_manager.get_account_balance(bp_expense) == -7

    def test_user_request_redeem_virtual_incentives(
        self,
        product_factory,
        payout_config,
        user_factory,
        user_payout_event_manager: UserPayoutEventManager,
        cashout_method_manager: CashoutMethodManager,
        thl_ledger_manager: ThlLedgerManager,
        user_iphistory_manager: UserIpHistoryManager,
        ip_record_factory,
        thl_redis_client,
        setup_cashoutmethod_db,
        example_vi_cashout_methods,
    ):
        setup_cashoutmethod_db()
        cm = example_vi_cashout_methods[0]
        product = product_factory(
            payout_config=payout_config,
            user_wallet_config=UserWalletConfig(
                enabled=True, supported_payout_types={PayoutType.VIRTUAL_INCENTIVES}
            ),
        )
        user = user_factory(product=product)

        ip_record_factory(user=user, ip="8.8.8.8")
        bonus_ref = uuid4().hex
        thl_ledger_manager.create_tx_user_bonus(
            user=user,
            amount=Decimal("10.00"),
            ref_uuid=bonus_ref,
            description="Fund Virtual Incentives redemption test",
        )
        tango_manager = Mock(spec=TangoManager)
        tango_manager.get_exchange_rates.return_value = {}
        virtual_incentives_manager = VirtualIncentivesManager.__new__(
            VirtualIncentivesManager
        )

        payout = user_payout_event_manager.user_request_redeem(
            user=user,
            cashout_method_id=cm.id,
            amount=USDCent(600),
            tango_manager=tango_manager,
            virtual_incentives_manager=virtual_incentives_manager,
            cashout_method_manager=cashout_method_manager,
            ledger_manager=thl_ledger_manager,
            user_ip_history_manager=user_iphistory_manager,
            redis_client=thl_redis_client,
        )

        assert payout.status == PayoutStatus.PENDING
        assert payout.payout_type == PayoutType.VIRTUAL_INCENTIVES
        assert payout.amount == 600
        assert payout.request_data == {
            "sku": "AMAZON-US",
            "amount": "6.00",
            "cashout_request_id": payout.uuid,
        }
        assert payout.cashout_method_uuid == cm.id
        assert thl_ledger_manager.get_user_wallet_balance(user) == 400
        assert user_payout_event_manager.get_by_uuid(payout.uuid) == payout

        bonus_tag = f"test:user_bonus:{bonus_ref}"
        request_tag = f"test:user_payout:{payout.uuid}:request"
        assert len(thl_ledger_manager.get_tx_ids_by_tag(bonus_tag)) == 1
        assert len(thl_ledger_manager.get_tx_ids_by_tag(request_tag)) == 1

        user_wallet = thl_ledger_manager.get_account_or_create_user_wallet(user)
        bp_wallet = thl_ledger_manager.get_account_or_create_bp_wallet(product)
        pending_payout = thl_ledger_manager.get_or_create_bp_pending_payout_account(
            product
        )
        assert thl_ledger_manager.get_account_balance(user_wallet) == 400
        assert thl_ledger_manager.get_account_balance(bp_wallet) == -1_000
        assert thl_ledger_manager.get_account_balance(pending_payout) == 600
        assert (
            thl_ledger_manager.get_account_filtered_balance(
                account=user_wallet,
                metadata_key="payoutevent",
                metadata_value=payout.uuid,
            )
            == -600
        )
        assert (
            thl_ledger_manager.get_account_filtered_balance(
                account=pending_payout,
                metadata_key="payoutevent",
                metadata_value=payout.uuid,
            )
            == 600
        )

        detail = user_payout_event_manager.get_payout_detail(payout.uuid)
        assert detail.status == PayoutStatus.PENDING
        assert detail.description == "Amazon.com Gift Card"
        assert not detail.transaction_info


class TestManagePendingCashout:
    def test_approve_virtual_incentives(
        self,
        product_factory,
        payout_config,
        user_factory,
        user_manager,
        user_payout_event_manager: UserPayoutEventManager,
        cashout_method_manager: CashoutMethodManager,
        thl_ledger_manager: ThlLedgerManager,
        user_iphistory_manager: UserIpHistoryManager,
        ip_record_factory,
        thl_redis_client,
        setup_cashoutmethod_db,
        example_vi_cashout_methods,
        create_main_accounts,
    ):
        create_main_accounts()
        setup_cashoutmethod_db()
        cashout_method = example_vi_cashout_methods[0]
        product = product_factory(
            payout_config=payout_config,
            user_wallet_config=UserWalletConfig(
                enabled=True,
                supported_payout_types={PayoutType.VIRTUAL_INCENTIVES},
            ),
        )
        user = user_factory(product=product)
        ip_record_factory(user=user, ip="8.8.8.8")
        thl_ledger_manager.create_tx_user_bonus(
            user=user,
            amount=Decimal("10.00"),
            ref_uuid=uuid4().hex,
            description="Fund Virtual Incentives approval test",
        )
        tango_manager = Mock(spec=TangoManager)
        tango_manager.get_exchange_rates.return_value = {}
        virtual_incentives_manager = VirtualIncentivesManager.__new__(
            VirtualIncentivesManager
        )
        virtual_incentives_manager.client = Mock(spec=VirtualIncentivesClient)

        payout = user_payout_event_manager.user_request_redeem(
            user=user,
            cashout_method_id=cashout_method.id,
            amount=USDCent(600),
            tango_manager=tango_manager,
            virtual_incentives_manager=virtual_incentives_manager,
            cashout_method_manager=cashout_method_manager,
            ledger_manager=thl_ledger_manager,
            user_ip_history_manager=user_iphistory_manager,
            redis_client=thl_redis_client,
        )

        pending_payout = thl_ledger_manager.get_or_create_bp_pending_payout_account(
            product
        )
        user_wallet = thl_ledger_manager.get_account_or_create_user_wallet(user)

        assert thl_ledger_manager.get_account_balance(user_wallet) == 400
        assert thl_ledger_manager.get_account_balance(pending_payout) == 600

        order = {
            "status": PayoutStatus.COMPLETE,
            "number": "vi-order-1",
            "accounts": [{"sku": "AMAZON-US", "amount": "6.00"}],
        }
        virtual_incentives_manager.client.create_order.return_value = order

        result = manage_pending_cashout(
            payout_id=payout.uuid,
            new_status=PayoutStatus.APPROVED,
            user_payout_event_manager=user_payout_event_manager,
            user_ip_history_manager=user_iphistory_manager,
            user_manager=user_manager,
            ledger_manager=thl_ledger_manager,
            virtual_incentives_manager=virtual_incentives_manager,
        )

        assert result.uuid == payout.uuid
        assert result.status == PayoutStatus.COMPLETE
        assert result.ext_ref_id == "vi-order-1"
        assert result.order_data == order
        virtual_incentives_manager.client.create_order.assert_called_once_with(
            cashout_request_id=payout.uuid,
            sku="AMAZON-US",
            amount=Decimal("6.00"),
        )

        stored_payout = user_payout_event_manager.get_by_uuid(payout.uuid)
        assert stored_payout.status == PayoutStatus.COMPLETE
        assert stored_payout.ext_ref_id == "vi-order-1"
        assert stored_payout.order_data == order

        request_tag = f"test:user_payout:{payout.uuid}:request"
        complete_tag = f"test:user_payout:{payout.uuid}:complete"
        assert len(thl_ledger_manager.get_tx_ids_by_tag(request_tag)) == 1
        assert len(thl_ledger_manager.get_tx_ids_by_tag(complete_tag)) == 1

        bp_wallet = thl_ledger_manager.get_account_or_create_bp_wallet(product)

        vi_expense = thl_ledger_manager.get_account_or_create_bp_expense_by_uuid(
            product_uuid=product.uuid,
            expense_name="virtual_incentives",
        )
        assert thl_ledger_manager.get_account_balance(user_wallet) == 400
        assert thl_ledger_manager.get_account_balance(pending_payout) == 0
        # $10 bribe, 17c expense (2.9% x $6)
        assert thl_ledger_manager.get_account_balance(bp_wallet) == -10_17
        assert thl_ledger_manager.get_account_balance(vi_expense) == -17
        assert thl_ledger_manager.check_ledger_balanced()

    def test_user_request_redeem_paypal_insufficient_balance(
        self,
        user_with_wallet: User,
        user_payout_event_manager: UserPayoutEventManager,
        cashout_method_manager: CashoutMethodManager,
        thl_ledger_manager: ThlLedgerManager,
        user_iphistory_manager: UserIpHistoryManager,
        ip_record_factory,
        thl_redis_client,
    ):
        with pytest.raises(AssertionError, match="more than their redeemable balance"):
            self.request_paypal_payout(
                user=user_with_wallet,
                amount=USDCent(1_100),
                user_payout_event_manager=user_payout_event_manager,
                cashout_method_manager=cashout_method_manager,
                thl_ledger_manager=thl_ledger_manager,
                user_iphistory_manager=user_iphistory_manager,
                ip_record_factory=ip_record_factory,
                thl_redis_client=thl_redis_client,
            )

    def test_user_request_redeem_paypal_blocked_user(
        self,
        user_with_wallet: User,
        user_payout_event_manager: UserPayoutEventManager,
        cashout_method_manager: CashoutMethodManager,
        thl_ledger_manager: ThlLedgerManager,
        user_iphistory_manager: UserIpHistoryManager,
        ip_record_factory,
        thl_redis_client,
    ):
        user_with_wallet.blocked = True

        with pytest.raises(AssertionError, match="Blocked user"):
            self.request_paypal_payout(
                user=user_with_wallet,
                amount=USDCent(600),
                user_payout_event_manager=user_payout_event_manager,
                cashout_method_manager=cashout_method_manager,
                thl_ledger_manager=thl_ledger_manager,
                user_iphistory_manager=user_iphistory_manager,
                ip_record_factory=ip_record_factory,
                thl_redis_client=thl_redis_client,
            )

    def test_user_request_redeem_paypal_anonymous_user(
        self,
        user_with_wallet: User,
        user_payout_event_manager: UserPayoutEventManager,
        cashout_method_manager: CashoutMethodManager,
        thl_ledger_manager: ThlLedgerManager,
        user_iphistory_manager: UserIpHistoryManager,
        ip_record_factory,
        thl_redis_client,
    ):
        with pytest.raises(AssertionError, match="Anonymous user"):
            self.request_paypal_payout(
                user=user_with_wallet,
                amount=USDCent(600),
                user_payout_event_manager=user_payout_event_manager,
                cashout_method_manager=cashout_method_manager,
                thl_ledger_manager=thl_ledger_manager,
                user_iphistory_manager=user_iphistory_manager,
                ip_record_factory=ip_record_factory,
                thl_redis_client=thl_redis_client,
                ip="1.1.1.1",
            )
