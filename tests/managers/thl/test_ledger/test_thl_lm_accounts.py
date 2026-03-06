from uuid import uuid4

import pytest


class TestThlLedgerManagerAccounts:

    def test_get_account_or_create_user_wallet(self, user, thl_lm, lm):
        from generalresearch.currency import LedgerCurrency
        from generalresearch.models.thl.ledger import (
            LedgerAccount,
            Direction,
            AccountType,
        )

        account = thl_lm.get_account_or_create_user_wallet(user=user)
        assert isinstance(account, LedgerAccount)

        assert user.uuid in account.qualified_name
        assert account.display_name == f"User Wallet {user.uuid}"
        assert account.account_type == AccountType.USER_WALLET
        assert account.normal_balance == Direction.CREDIT
        assert account.reference_type == "user"
        assert account.reference_uuid == user.uuid
        assert account.currency == LedgerCurrency.TEST

        # Actually query for it to confirm
        res = lm.get_account(qualified_name=account.qualified_name, raise_on_error=True)
        assert res.model_dump_json() == account.model_dump_json()

    def test_get_account_or_create_bp_wallet(self, product, thl_lm, lm):
        from generalresearch.currency import LedgerCurrency
        from generalresearch.models.thl.ledger import (
            LedgerAccount,
            Direction,
            AccountType,
        )

        account = thl_lm.get_account_or_create_bp_wallet(product=product)
        assert isinstance(account, LedgerAccount)

        assert product.uuid in account.qualified_name
        assert account.display_name == f"BP Wallet {product.uuid}"
        assert account.account_type == AccountType.BP_WALLET
        assert account.normal_balance == Direction.CREDIT
        assert account.reference_type == "bp"
        assert account.reference_uuid == product.uuid
        assert account.currency == LedgerCurrency.TEST

        # Actually query for it to confirm
        res = lm.get_account(qualified_name=account.qualified_name, raise_on_error=True)
        assert res.model_dump_json() == account.model_dump_json()

    def test_get_account_or_create_bp_commission(self, product, thl_lm, lm):
        from generalresearch.currency import LedgerCurrency
        from generalresearch.models.thl.ledger import (
            Direction,
            AccountType,
        )

        account = thl_lm.get_account_or_create_bp_commission(product=product)

        assert product.uuid in account.qualified_name
        assert account.display_name == f"Revenue from commission {product.uuid}"
        assert account.account_type == AccountType.REVENUE
        assert account.normal_balance == Direction.CREDIT
        assert account.reference_type == "bp"
        assert account.reference_uuid == product.uuid
        assert account.currency == LedgerCurrency.TEST

        # Actually query for it to confirm
        res = lm.get_account(qualified_name=account.qualified_name, raise_on_error=True)
        assert res.model_dump_json() == account.model_dump_json()

    @pytest.mark.parametrize("expense", ["tango", "paypal", "gift", "tremendous"])
    def test_get_account_or_create_bp_expense(self, product, expense, thl_lm, lm):
        from generalresearch.currency import LedgerCurrency
        from generalresearch.models.thl.ledger import (
            Direction,
            AccountType,
        )

        account = thl_lm.get_account_or_create_bp_expense(
            product=product, expense_name=expense
        )
        assert product.uuid in account.qualified_name
        assert account.display_name == f"Expense {expense} {product.uuid}"
        assert account.account_type == AccountType.EXPENSE
        assert account.normal_balance == Direction.DEBIT
        assert account.reference_type == "bp"
        assert account.reference_uuid == product.uuid
        assert account.currency == LedgerCurrency.TEST

        # Actually query for it to confirm
        res = lm.get_account(qualified_name=account.qualified_name, raise_on_error=True)
        assert res.model_dump_json() == account.model_dump_json()

    def test_get_or_create_bp_pending_payout_account(self, product, thl_lm, lm):
        from generalresearch.currency import LedgerCurrency
        from generalresearch.models.thl.ledger import (
            Direction,
            AccountType,
        )

        account = thl_lm.get_or_create_bp_pending_payout_account(product=product)

        assert product.uuid in account.qualified_name
        assert account.display_name == f"BP Wallet Pending {product.uuid}"
        assert account.account_type == AccountType.BP_WALLET
        assert account.normal_balance == Direction.CREDIT
        assert account.reference_type == "bp"
        assert account.reference_uuid == product.uuid
        assert account.currency == LedgerCurrency.TEST

        # Actually query for it to confirm
        res = lm.get_account(qualified_name=account.qualified_name, raise_on_error=True)
        assert res.model_dump_json() == account.model_dump_json()

    def test_get_account_task_complete_revenue_raises(
        self, delete_ledger_db, thl_lm, lm
    ):
        from generalresearch.managers.thl.ledger_manager.exceptions import (
            LedgerAccountDoesntExistError,
        )

        delete_ledger_db()

        with pytest.raises(expected_exception=LedgerAccountDoesntExistError):
            thl_lm.get_account_task_complete_revenue()

    def test_get_account_task_complete_revenue(
        self, account_cash, account_revenue_task_complete, thl_lm, lm
    ):
        from generalresearch.models.thl.ledger import (
            LedgerAccount,
            AccountType,
        )

        res = thl_lm.get_account_task_complete_revenue()
        assert isinstance(res, LedgerAccount)
        assert res.reference_type is None
        assert res.reference_uuid is None
        assert res.account_type == AccountType.REVENUE
        assert res.display_name == "Cash flow task complete"

    def test_get_account_cash_raises(self, delete_ledger_db, thl_lm, lm):
        from generalresearch.managers.thl.ledger_manager.exceptions import (
            LedgerAccountDoesntExistError,
        )

        delete_ledger_db()

        with pytest.raises(expected_exception=LedgerAccountDoesntExistError):
            thl_lm.get_account_cash()

    def test_get_account_cash(self, account_cash, thl_lm, lm):
        from generalresearch.models.thl.ledger import (
            LedgerAccount,
            AccountType,
        )

        res = thl_lm.get_account_cash()
        assert isinstance(res, LedgerAccount)
        assert res.reference_type is None
        assert res.reference_uuid is None
        assert res.account_type == AccountType.CASH
        assert res.display_name == "Operating Cash Account"

    def test_get_accounts(self, setup_accounts, product, user_factory, thl_lm, lm, lam):
        from generalresearch.models.thl.user import User
        from generalresearch.managers.thl.ledger_manager.exceptions import (
            LedgerAccountDoesntExistError,
        )

        user1: User = user_factory(product=product)
        user2: User = user_factory(product=product)

        account1 = thl_lm.get_account_or_create_bp_wallet(product=product)

        # (1) known account and confirm it comes back
        res = lm.get_account(qualified_name=account1.qualified_name)
        assert account1.model_dump_json() == res.model_dump_json()

        # (2) known accounts and confirm they both come back
        res = lam.get_accounts(qualified_names=[account1.qualified_name])
        assert isinstance(res, list)
        assert len(res) == 1
        assert account1 in res

        # Get 2 known and 1 made up qualified names, and confirm it raises
        # an error
        with pytest.raises(LedgerAccountDoesntExistError):
            lam.get_accounts(
                qualified_names=[
                    account1.qualified_name,
                    f"test:bp_wall:{uuid4().hex}",
                ]
            )

    def test_get_accounts_if_exists(self, product_factory, currency, thl_lm, lm):
        from generalresearch.models.thl.product import Product

        p1: Product = product_factory()
        p2: Product = product_factory()

        account1 = thl_lm.get_account_or_create_bp_wallet(product=p1)
        account2 = thl_lm.get_account_or_create_bp_wallet(product=p2)

        # (1) known account and confirm it comes back
        res = lm.get_account(qualified_name=account1.qualified_name)
        assert account1.model_dump_json() == res.model_dump_json()

        # (2) known accounts and confirm they both come back
        res = lm.get_accounts(
            qualified_names=[account1.qualified_name, account2.qualified_name]
        )
        assert isinstance(res, list)
        assert len(res) == 2
        assert account1 in res
        assert account2 in res

        # Get 2 known and 1 made up qualified names, and confirm only 2
        # come back
        lm.get_accounts_if_exists(
            qualified_names=[
                account1.qualified_name,
                account2.qualified_name,
                f"{currency.value}:bp_wall:{uuid4().hex}",
            ]
        )

        assert isinstance(res, list)
        assert len(res) == 2

        # Confirm an empty array comes back for all unknown qualified names
        res = lm.get_accounts_if_exists(
            qualified_names=[
                f"{lm.currency.value}:bp_wall:{uuid4().hex}" for i in range(5)
            ]
        )
        assert isinstance(res, list)
        assert len(res) == 0

    def test_get_accounts_for_products(self, product_factory, thl_lm, lm):
        from generalresearch.managers.thl.ledger_manager.exceptions import (
            LedgerAccountDoesntExistError,
        )
        from generalresearch.models.thl.ledger import (
            LedgerAccount,
        )

        # Create 5 Products
        product_uuids = []
        for i in range(5):
            _p = product_factory()
            product_uuids.append(_p.uuid)

        # Confirm that this fails.. because none of those accounts have been
        #   created yet
        with pytest.raises(expected_exception=LedgerAccountDoesntExistError):
            thl_lm.get_accounts_bp_wallet_for_products(product_uuids=product_uuids)

        # Create the bp_wallet accounts and then try again
        for p_uuid in product_uuids:
            thl_lm.get_account_or_create_bp_wallet_by_uuid(product_uuid=p_uuid)

        res = thl_lm.get_accounts_bp_wallet_for_products(product_uuids=product_uuids)
        assert len(res) == len(product_uuids)
        assert all([isinstance(i, LedgerAccount) for i in res])


class TestLedgerAccountManager:

    def test_get_or_create(self, thl_lm, lm, lam):
        from generalresearch.managers.thl.ledger_manager.exceptions import (
            LedgerAccountDoesntExistError,
        )
        from generalresearch.models.thl.ledger import (
            LedgerAccount,
            Direction,
            AccountType,
        )

        u = uuid4().hex
        name = f"test-{u[:8]}"

        account = LedgerAccount(
            display_name=name,
            qualified_name=f"test:bp_wallet:{u}",
            normal_balance=Direction.DEBIT,
            account_type=AccountType.BP_WALLET,
            currency="test",
            reference_type="bp",
            reference_uuid=u,
        )

        # First we want to validate that using the get_account method raises
        # an error for a random LedgerAccount which we know does not exist.
        with pytest.raises(LedgerAccountDoesntExistError):
            lam.get_account(qualified_name=account.qualified_name)

        # Now that we know it doesn't exist, get_or_create for it
        instance = lam.get_account_or_create(account=account)

        # It should always return
        assert isinstance(instance, LedgerAccount)
        assert instance.reference_uuid == u

    def test_get(self, user, thl_lm, lm, lam):
        from generalresearch.managers.thl.ledger_manager.exceptions import (
            LedgerAccountDoesntExistError,
        )
        from generalresearch.models.thl.ledger import (
            LedgerAccount,
            AccountType,
        )

        with pytest.raises(LedgerAccountDoesntExistError):
            lam.get_account(qualified_name=f"test:bp_wallet:{user.product.id}")

        thl_lm.get_account_or_create_bp_wallet(product=user.product)
        account = lam.get_account(qualified_name=f"test:bp_wallet:{user.product.id}")

        assert isinstance(account, LedgerAccount)
        assert AccountType.BP_WALLET == account.account_type
        assert user.product.uuid == account.reference_uuid

    def test_get_many(self, product_factory, thl_lm, lm, lam, currency):
        from generalresearch.models.thl.product import Product
        from generalresearch.managers.thl.ledger_manager.exceptions import (
            LedgerAccountDoesntExistError,
        )

        p1: Product = product_factory()
        p2: Product = product_factory()

        account1 = thl_lm.get_account_or_create_bp_wallet(product=p1)
        account2 = thl_lm.get_account_or_create_bp_wallet(product=p2)

        # Get 1 known account and confirm it comes back
        res = lam.get_account_many(
            qualified_names=[account1.qualified_name, account2.qualified_name]
        )
        assert isinstance(res, list)
        assert len(res) == 2
        assert account1 in res

        # Get 2 known accounts and confirm they both come back
        res = lam.get_account_many(
            qualified_names=[account1.qualified_name, account2.qualified_name]
        )
        assert isinstance(res, list)
        assert len(res) == 2
        assert account1 in res
        assert account2 in res

        # Get 2 known and 1 made up qualified names, and confirm only 2 come
        # back. Don't raise on error, so we can confirm the array is "short"
        res = lam.get_account_many(
            qualified_names=[
                account1.qualified_name,
                account2.qualified_name,
                f"test:bp_wall:{uuid4().hex}",
            ],
            raise_on_error=False,
        )
        assert isinstance(res, list)
        assert len(res) == 2

        # Same as above, but confirm the raise works on checking res length
        with pytest.raises(LedgerAccountDoesntExistError):
            lam.get_account_many(
                qualified_names=[
                    account1.qualified_name,
                    account2.qualified_name,
                    f"test:bp_wall:{uuid4().hex}",
                ],
                raise_on_error=True,
            )

        # Confirm an empty array comes back for all unknown qualified names
        res = lam.get_account_many(
            qualified_names=[f"test:bp_wall:{uuid4().hex}" for i in range(5)],
            raise_on_error=False,
        )
        assert isinstance(res, list)
        assert len(res) == 0

    def test_create_account(self, thl_lm, lm, lam):
        from generalresearch.models.thl.ledger import (
            LedgerAccount,
            Direction,
            AccountType,
        )

        u = uuid4().hex
        name = f"test-{u[:8]}"

        account = LedgerAccount(
            display_name=name,
            qualified_name=f"test:bp_wallet:{u}",
            normal_balance=Direction.DEBIT,
            account_type=AccountType.BP_WALLET,
            currency="test",
            reference_type="bp",
            reference_uuid=u,
        )

        lam.create_account(account=account)
        assert lam.get_account(f"test:bp_wallet:{u}") == account
        assert lam.get_account_or_create(account) == account
