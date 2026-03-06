from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional
from uuid import uuid4

from generalresearch.managers.thl.ledger_manager.thl_ledger import (
    ThlLedgerManager,
)
from generalresearch.models.custom_types import UUIDStr
from generalresearch.models.thl.user import User


def user_compensate(
    ledger_manager: ThlLedgerManager,
    user: User,
    amount_int: int,
    ext_ref=None,
    description=None,
    skip_flag_check: Optional[bool] = False,
) -> UUIDStr:
    """
    Compensate a user. aka "bribe". The money is paid out of the BP's wallet balance.
    Amount is in USD cents.
    """
    pg_config = ledger_manager.pg_config
    redis_client = ledger_manager.redis_client

    now = datetime.now(tz=timezone.utc)
    assert type(amount_int) is int
    user.prefetch_product(pg_config=pg_config)
    assert (
        user.product.user_wallet_enabled
    ), "Trying to compensate user without managed wallet"

    # Simple dedupe mechanism. Don't allow more than 1 per user_id every 1 min.
    if not skip_flag_check:
        flag_just_set = bool(
            redis_client.set(
                f"thl-grpc:user_compensate:{user.user_id}", 1, nx=True, ex=60
            )
        )
        assert flag_just_set, "User already compensated within the past minute!"

    # If there is an external reference ID, don't allow it to be used twice
    if ext_ref:
        res = pg_config.execute_sql_query(
            query=f"""
                SELECT 1 
                FROM event_bribe
                WHERE ext_ref_id = %s
            """,
            params=[ext_ref],
        )
        assert not res, f"UserCompensate: ext_ref {ext_ref} already used!"

    # Create a Bribe instance, that stores info about this event
    bribe_uuid = uuid4().hex

    # Create a new bribe instance
    account = ledger_manager.get_account_or_create_user_wallet(user)
    pg_config.execute_write(
        query=f"""
        INSERT INTO event_bribe
            (uuid, credit_account_uuid, created, amount, ext_ref_id, description, data) 
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,
        params=[
            bribe_uuid,
            account.uuid,
            now,
            amount_int,
            ext_ref,
            description,
            None,
        ],
    )
    # For now, all Ledger Accounts are USD
    amount_usd = Decimal(amount_int) / 100
    if description is None:
        description = f"Bonus ${amount_usd:,.2f}"
    ledger_manager.create_tx_user_bonus(
        user,
        amount=amount_usd,
        ref_uuid=bribe_uuid,
        description=description,
        skip_flag_check=skip_flag_check,
    )

    return bribe_uuid
