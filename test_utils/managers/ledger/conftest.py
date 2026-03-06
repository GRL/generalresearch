from datetime import datetime
from decimal import Decimal
from random import randint
from typing import Optional, Dict, Callable, TYPE_CHECKING
from uuid import uuid4

import pytest

from generalresearch.currency import USDCent
from test_utils.models.conftest import (
    product_factory,
    user,
    product,
    user_factory,
    product_user_wallet_no,
    wall,
    product_amt_true,
    product_user_wallet_yes,
    session_factory,
    session,
    wall_factory,
    payout_config,
)

_ = (
    user_factory,
    product_user_wallet_no,
    wall,
    product_amt_true,
    product_user_wallet_yes,
    session_factory,
    session,
    wall_factory,
    payout_config,
)

if TYPE_CHECKING:
    from generalresearch.currency import LedgerCurrency
    from generalresearch.models.thl.ledger import (
        Direction,
        AccountType,
        LedgerTransaction,
    )
    from generalresearch.models.thl.ledger import (
        LedgerEntry,
        LedgerAccount,
    )
    from generalresearch.models.thl.payout import UserPayoutEvent


@pytest.fixture(scope="function")
def ledger_account(request, lm, currency) -> "LedgerAccount":
    from generalresearch.models.thl.ledger import (
        Direction,
        AccountType,
        LedgerAccount,
    )

    account_type = getattr(request, "account_type", AccountType.CASH)
    direction = getattr(request, "direction", Direction.CREDIT)

    acct_uuid = uuid4().hex
    qn = ":".join([currency, account_type, acct_uuid])

    acct_model = LedgerAccount(
        uuid=acct_uuid,
        display_name=f"test-{acct_uuid}",
        currency=currency,
        qualified_name=qn,
        account_type=account_type,
        normal_balance=direction,
    )
    return lm.create_account(account=acct_model)


@pytest.fixture(scope="function")
def ledger_account_factory(request, thl_lm, lm, currency) -> Callable:
    from generalresearch.models.thl.ledger import (
        Direction,
        AccountType,
        LedgerAccount,
    )

    def _ledger_account_factory(
        product,
        account_type: AccountType = AccountType.CASH,
        direction: Direction = Direction.CREDIT,
    ):
        thl_lm.get_account_or_create_bp_wallet(product=product)
        acct_uuid = uuid4().hex
        qn = ":".join([currency, account_type, acct_uuid])

        acct_model = LedgerAccount(
            uuid=acct_uuid,
            display_name=f"test-{acct_uuid}",
            currency=currency,
            qualified_name=qn,
            account_type=account_type,
            normal_balance=direction,
        )
        return lm.create_account(account=acct_model)

    return _ledger_account_factory


@pytest.fixture(scope="function")
def ledger_account_credit(request, lm, currency) -> "LedgerAccount":
    from generalresearch.models.thl.ledger import Direction, AccountType

    account_type = AccountType.REVENUE
    acct_uuid = uuid4().hex

    qn = ":".join([currency, account_type, acct_uuid])
    from generalresearch.models.thl.ledger import LedgerAccount

    acct_model = LedgerAccount(
        uuid=acct_uuid,
        display_name=f"test-{acct_uuid}",
        currency=currency,
        qualified_name=qn,
        account_type=account_type,
        normal_balance=Direction.CREDIT,
    )
    return lm.create_account(account=acct_model)


@pytest.fixture(scope="function")
def ledger_account_debit(request, lm, currency) -> "LedgerAccount":
    from generalresearch.models.thl.ledger import Direction, AccountType

    account_type = AccountType.EXPENSE
    acct_uuid = uuid4().hex

    qn = ":".join([currency, account_type, acct_uuid])
    from generalresearch.models.thl.ledger import LedgerAccount

    acct_model = LedgerAccount(
        uuid=acct_uuid,
        display_name=f"test-{acct_uuid}",
        currency=currency,
        qualified_name=qn,
        account_type=account_type,
        normal_balance=Direction.DEBIT,
    )
    return lm.create_account(account=acct_model)


@pytest.fixture(scope="function")
def tag(request, lm) -> str:
    from generalresearch.currency import LedgerCurrency

    return (
        request.param
        if hasattr(request, "tag")
        else f"{LedgerCurrency.TEST}:{uuid4().hex}"
    )


@pytest.fixture(scope="function")
def usd_cent(request) -> USDCent:
    amount = randint(99, 9_999)
    return request.param if hasattr(request, "usd_cent") else USDCent(amount)


@pytest.fixture(scope="function")
def bp_payout_event(
    product, usd_cent, business_payout_event_manager, thl_lm
) -> "UserPayoutEvent":
    return business_payout_event_manager.create_bp_payout_event(
        thl_ledger_manager=thl_lm,
        product=product,
        amount=usd_cent,
        skip_wallet_balance_check=True,
        skip_one_per_day_check=True,
    )


@pytest.fixture
def bp_payout_event_factory(brokerage_product_payout_event_manager, thl_lm) -> Callable:
    from generalresearch.models.thl.product import Product
    from generalresearch.currency import USDCent

    def _create_bp_payout_event(
        product: Product, usd_cent: USDCent, ext_ref_id: Optional[str] = None
    ):
        return brokerage_product_payout_event_manager.create_bp_payout_event(
            thl_ledger_manager=thl_lm,
            product=product,
            amount=usd_cent,
            ext_ref_id=ext_ref_id,
            skip_wallet_balance_check=True,
            skip_one_per_day_check=True,
        )

    return _create_bp_payout_event


@pytest.fixture(scope="function")
def currency(lm) -> "LedgerCurrency":
    # return request.param if hasattr(request, "currency") else LedgerCurrency.TEST
    return lm.currency


@pytest.fixture(scope="function")
def tx_metadata(request) -> Optional[Dict[str, str]]:
    return (
        request.param
        if hasattr(request, "tx_metadata")
        else {f"key-{uuid4().hex[:10]}": uuid4().hex}
    )


@pytest.fixture(scope="function")
def ledger_tx(
    request,
    ledger_account_credit,
    ledger_account_debit,
    tag,
    currency,
    tx_metadata,
    lm,
) -> "LedgerTransaction":
    from generalresearch.models.thl.ledger import Direction, LedgerEntry

    amount = int(Decimal("1.00") * 100)

    entries = [
        LedgerEntry(
            direction=Direction.CREDIT,
            account_uuid=ledger_account_credit.uuid,
            amount=amount,
        ),
        LedgerEntry(
            direction=Direction.DEBIT,
            account_uuid=ledger_account_debit.uuid,
            amount=amount,
        ),
    ]

    return lm.create_tx(entries=entries, tag=tag, metadata=tx_metadata)


@pytest.fixture(scope="function")
def create_main_accounts(lm, currency) -> Callable:
    def _create_main_accounts():
        from generalresearch.models.thl.ledger import (
            LedgerAccount,
            Direction,
            AccountType,
        )

        account = LedgerAccount(
            display_name="Cash flow task complete",
            qualified_name=f"{currency.value}:revenue:task_complete",
            normal_balance=Direction.CREDIT,
            account_type=AccountType.REVENUE,
            currency=lm.currency,
        )
        lm.get_account_or_create(account=account)

        account = LedgerAccount(
            display_name="Operating Cash Account",
            qualified_name=f"{currency.value}:cash",
            normal_balance=Direction.DEBIT,
            account_type=AccountType.CASH,
            currency=currency,
        )

        lm.get_account_or_create(account=account)

    return _create_main_accounts


@pytest.fixture(scope="function")
def delete_ledger_db(thl_web_rw) -> Callable:
    def _delete_ledger_db():
        for table in [
            "ledger_transactionmetadata",
            "ledger_entry",
            "ledger_transaction",
            "ledger_account",
        ]:
            thl_web_rw.execute_write(
                query=f"DELETE FROM {table};",
            )

    return _delete_ledger_db


@pytest.fixture(scope="function")
def wipe_main_accounts(thl_web_rw, lm, currency) -> Callable:
    def _wipe_main_accounts():
        db_table = thl_web_rw.db_name
        qual_names = [
            f"{currency.value}:revenue:task_complete",
            f"{currency.value}:cash",
        ]

        res = thl_web_rw.execute_sql_query(
            query=f"""
                SELECT lt.id as ltid, le.id as leid, tmd.id as tmdid, la.uuid as lauuid
                FROM `{db_table}`.`ledger_transaction` AS lt
                LEFT JOIN `{db_table}`.ledger_entry le 
                    ON lt.id = le.transaction_id
                LEFT JOIN `{db_table}`.ledger_account la
                    ON la.uuid = le.account_id
                LEFT JOIN `{db_table}`.ledger_transactionmetadata tmd 
                    ON lt.id = tmd.transaction_id
                WHERE la.qualified_name IN %s
            """,
            params=[qual_names],
        )

        lt = {x["ltid"] for x in res if x["ltid"]}
        le = {x["leid"] for x in res if x["leid"]}
        tmd = {x["tmdid"] for x in res if x["tmdid"]}
        la = {x["lauuid"] for x in res if x["lauuid"]}

        thl_web_rw.execute_sql_query(
            query=f"""
                DELETE FROM `{db_table}`.`ledger_transactionmetadata`
                WHERE id IN %s
            """,
            params=[tmd],
            commit=True,
        )

        thl_web_rw.execute_sql_query(
            query=f"""
                DELETE FROM `{db_table}`.`ledger_entry`
                WHERE id IN %s
            """,
            params=[le],
            commit=True,
        )

        thl_web_rw.execute_sql_query(
            query=f"""
                DELETE FROM `{db_table}`.`ledger_transaction`
                WHERE id IN %s
            """,
            params=[lt],
            commit=True,
        )

        thl_web_rw.execute_sql_query(
            query=f"""
                DELETE FROM `{db_table}`.`ledger_account`
                WHERE uuid IN %s
            """,
            params=[la],
            commit=True,
        )

    return _wipe_main_accounts


@pytest.fixture(scope="function")
def account_cash(lm, currency) -> "LedgerAccount":
    from generalresearch.models.thl.ledger import (
        LedgerAccount,
        Direction,
        AccountType,
    )

    account = LedgerAccount(
        display_name="Operating Cash Account",
        qualified_name=f"{currency.value}:cash",
        normal_balance=Direction.DEBIT,
        account_type=AccountType.CASH,
        currency=currency,
    )
    return lm.get_account_or_create(account=account)


@pytest.fixture(scope="function")
def account_revenue_task_complete(lm, currency) -> "LedgerAccount":
    from generalresearch.models.thl.ledger import (
        LedgerAccount,
        Direction,
        AccountType,
    )

    account = LedgerAccount(
        display_name="Cash flow task complete",
        qualified_name=f"{currency.value}:revenue:task_complete",
        normal_balance=Direction.CREDIT,
        account_type=AccountType.REVENUE,
        currency=currency,
    )
    return lm.get_account_or_create(account=account)


@pytest.fixture(scope="function")
def account_expense_tango(lm, currency) -> "LedgerAccount":
    from generalresearch.models.thl.ledger import (
        LedgerAccount,
        Direction,
        AccountType,
    )

    account = LedgerAccount(
        display_name="Tango Fee",
        qualified_name=f"{currency.value}:expense:tango_fee",
        normal_balance=Direction.DEBIT,
        account_type=AccountType.EXPENSE,
        currency=currency,
    )
    return lm.get_account_or_create(account=account)


@pytest.fixture(scope="function")
def user_account_user_wallet(lm, user, currency) -> "LedgerAccount":
    from generalresearch.models.thl.ledger import (
        LedgerAccount,
        Direction,
        AccountType,
    )

    account = LedgerAccount(
        display_name=f"{user.uuid} Wallet",
        qualified_name=f"{currency.value}:user_wallet:{user.uuid}",
        normal_balance=Direction.CREDIT,
        account_type=AccountType.USER_WALLET,
        reference_type="user",
        reference_uuid=user.uuid,
        currency=currency,
    )
    return lm.get_account_or_create(account=account)


@pytest.fixture(scope="function")
def product_account_bp_wallet(lm, product, currency) -> "LedgerAccount":
    from generalresearch.models.thl.ledger import (
        LedgerAccount,
        Direction,
        AccountType,
    )

    account = LedgerAccount.model_validate(
        dict(
            display_name=f"{product.name} Wallet",
            qualified_name=f"{currency.value}:bp_wallet:{product.uuid}",
            normal_balance=Direction.CREDIT,
            account_type=AccountType.BP_WALLET,
            reference_type="bp",
            reference_uuid=product.uuid,
            currency=currency,
        )
    )
    return lm.get_account_or_create(account=account)


@pytest.fixture(scope="function")
def setup_accounts(product_factory, lm, user, currency) -> None:
    from generalresearch.models.thl.ledger import (
        LedgerAccount,
        Direction,
        AccountType,
    )

    # BP's wallet and a revenue from their commissions account.
    p1 = product_factory()

    account = LedgerAccount(
        display_name=f"Revenue from {p1.name} commission",
        qualified_name=f"{currency.value}:revenue:bp_commission:{p1.uuid}",
        normal_balance=Direction.CREDIT,
        account_type=AccountType.REVENUE,
        reference_type="bp",
        reference_uuid=p1.uuid,
        currency=currency,
    )
    lm.get_account_or_create(account=account)

    account = LedgerAccount.model_validate(
        dict(
            display_name=f"{p1.name} Wallet",
            qualified_name=f"{currency.value}:bp_wallet:{p1.uuid}",
            normal_balance=Direction.CREDIT,
            account_type=AccountType.BP_WALLET,
            reference_type="bp",
            reference_uuid=p1.uuid,
            currency=currency,
        )
    )
    lm.get_account_or_create(account=account)

    # BP's wallet, user's wallet, and a revenue from their commissions account.
    p2 = product_factory()
    account = LedgerAccount(
        display_name=f"Revenue from {p2.name} commission",
        qualified_name=f"{currency.value}:revenue:bp_commission:{p2.uuid}",
        normal_balance=Direction.CREDIT,
        account_type=AccountType.REVENUE,
        reference_type="bp",
        reference_uuid=p2.uuid,
        currency=currency,
    )
    lm.get_account_or_create(account)

    account = LedgerAccount(
        display_name=f"{p2.name} Wallet",
        qualified_name=f"{currency.value}:bp_wallet:{p2.uuid}",
        normal_balance=Direction.CREDIT,
        account_type=AccountType.BP_WALLET,
        reference_type="bp",
        reference_uuid=p2.uuid,
        currency=currency,
    )
    lm.get_account_or_create(account)

    account = LedgerAccount(
        display_name=f"{user.uuid} Wallet",
        qualified_name=f"{currency.value}:user_wallet:{user.uuid}",
        normal_balance=Direction.CREDIT,
        account_type=AccountType.USER_WALLET,
        reference_type="user",
        reference_uuid=user.uuid,
        currency="test",
    )
    lm.get_account_or_create(account=account)


@pytest.fixture(scope="function")
def session_with_tx_factory(
    user_factory,
    product,
    session_factory,
    session_manager,
    wall_manager,
    utc_hour_ago,
    thl_lm,
) -> Callable:
    from generalresearch.models.thl.session import (
        Status,
        Session,
        StatusCode1,
    )
    from generalresearch.models.thl.user import User

    def _session_with_tx_factory(
        user: User,
        final_status: Status = Status.COMPLETE,
        wall_req_cpi: Decimal = Decimal(".50"),
        started: datetime = utc_hour_ago,
    ) -> Session:
        s: Session = session_factory(
            user=user,
            wall_count=2,
            final_status=final_status,
            wall_req_cpi=wall_req_cpi,
            started=started,
        )
        last_wall = s.wall_events[-1]

        wall_manager.finish(
            wall=last_wall,
            status=Status.COMPLETE,
            status_code_1=StatusCode1.COMPLETE,
            finished=last_wall.finished,
        )

        status, status_code_1 = s.determine_session_status()
        thl_net, commission_amount, bp_pay, user_pay = s.determine_payments()
        session_manager.finish_with_status(
            session=s,
            finished=last_wall.finished,
            payout=bp_pay,
            user_payout=user_pay,
            status=status,
            status_code_1=status_code_1,
        )

        thl_lm.create_tx_task_complete(
            wall=last_wall,
            user=user,
            created=last_wall.finished,
            force=True,
        )

        thl_lm.create_tx_bp_payment(session=s, created=last_wall.finished, force=True)

        return s

    return _session_with_tx_factory


@pytest.fixture(scope="function")
def adj_to_fail_with_tx_factory(session_manager, wall_manager, thl_lm) -> Callable:
    from generalresearch.models.thl.session import (
        Session,
    )
    from datetime import timedelta
    from generalresearch.models.thl.definitions import WallAdjustedStatus

    def _adj_to_fail_with_tx_factory(
        session: Session,
        created: datetime,
    ) -> None:
        w1 = wall_manager.get_wall_events(session_id=session.id)[-1]

        # This is defined in `thl-grpc/thl/user_quality_history/recons.py:150`
        #   so we can't use it as part of this test anyway to add rows to the
        #   thl_taskadjustment table anyway.. until we created a
        #   TaskAdjustment Manager to put into py-utils!

        # create_task_adjustment_event(
        #     wall,
        #     user,
        #     adjusted_status,
        #     amount_usd=amount_usd,
        #     alert_time=alert_time,
        #     ext_status_code=ext_status_code,
        # )

        wall_manager.adjust_status(
            wall=w1,
            adjusted_status=WallAdjustedStatus.ADJUSTED_TO_FAIL,
            adjusted_cpi=Decimal("0.00"),
            adjusted_timestamp=created,
        )

        thl_lm.create_tx_task_adjustment(
            wall=w1,
            user=session.user,
            created=created + timedelta(milliseconds=1),
        )

        session.wall_events = wall_manager.get_wall_events(session_id=session.id)
        session_manager.adjust_status(session=session)

        thl_lm.create_tx_bp_adjustment(
            session=session, created=created + timedelta(milliseconds=2)
        )

        return None

    return _adj_to_fail_with_tx_factory


@pytest.fixture(scope="function")
def adj_to_complete_with_tx_factory(session_manager, wall_manager, thl_lm) -> Callable:
    from generalresearch.models.thl.session import (
        Session,
    )
    from datetime import timedelta
    from generalresearch.models.thl.definitions import WallAdjustedStatus

    def _adj_to_complete_with_tx_factory(
        session: Session,
        created: datetime,
    ) -> None:
        w1 = wall_manager.get_wall_events(session_id=session.id)[-1]

        wall_manager.adjust_status(
            wall=w1,
            adjusted_status=WallAdjustedStatus.ADJUSTED_TO_COMPLETE,
            adjusted_cpi=w1.req_cpi,
            adjusted_timestamp=created,
        )

        thl_lm.create_tx_task_adjustment(
            wall=w1,
            user=session.user,
            created=created + timedelta(milliseconds=1),
        )

        session.wall_events = wall_manager.get_wall_events(session_id=session.id)
        session_manager.adjust_status(session=session)

        thl_lm.create_tx_bp_adjustment(
            session=session, created=created + timedelta(milliseconds=2)
        )

        return None

    return _adj_to_complete_with_tx_factory
