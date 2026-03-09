from datetime import datetime, timezone
from typing import Any, Dict
from uuid import uuid4


def _example_user_tx_payout(schema: Dict[str, Any]) -> None:
    from generalresearch.models.thl.ledger import (
        UserLedgerTransactionUserPayout,
    )

    schema["example"] = UserLedgerTransactionUserPayout(
        product_id=uuid4().hex,
        payout_id=uuid4().hex,
        amount=-5,
        description="HIT Reward",
        payout_format="${payout/100:.2f}",
        created=datetime.now(tz=timezone.utc),
    ).model_dump(mode="json")


def _example_user_tx_bonus(schema: Dict[str, Any]) -> None:
    from generalresearch.models.thl.ledger import (
        UserLedgerTransactionUserBonus,
    )

    schema["example"] = UserLedgerTransactionUserBonus(
        product_id=uuid4().hex,
        amount=100,
        description="Compensation Bonus",
        payout_format="${payout/100:.2f}",
        created=datetime.now(tz=timezone.utc),
    ).model_dump(mode="json")


def _example_user_tx_complete(schema: Dict[str, Any]) -> None:
    from generalresearch.models.thl.ledger import (
        UserLedgerTransactionTaskComplete,
    )

    schema["example"] = UserLedgerTransactionTaskComplete(
        product_id=uuid4().hex,
        amount=38,
        description="Task Complete",
        payout_format="${payout/100:.2f}",
        created=datetime.now(tz=timezone.utc),
        tsid=uuid4().hex,
    ).model_dump(mode="json")


def _example_user_tx_adjustment(schema: Dict[str, Any]) -> None:
    from generalresearch.models.thl.ledger import (
        UserLedgerTransactionTaskAdjustment,
    )

    schema["example"] = UserLedgerTransactionTaskAdjustment(
        product_id=uuid4().hex,
        amount=-38,
        description="Task Adjustment",
        payout_format="${payout/100:.2f}",
        created=datetime.now(tz=timezone.utc),
        tsid=uuid4().hex,
    ).model_dump(mode="json")
