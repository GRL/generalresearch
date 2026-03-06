from datetime import datetime, timezone
from decimal import Decimal
from uuid import uuid4

import pytest
from pydantic import ValidationError

from generalresearch.models.thl.ledger import LedgerAccount, Direction, AccountType
from generalresearch.models.thl.ledger import LedgerTransaction, LedgerEntry


class TestLedgerTransaction:

    def test_create(self):
        # Can create with nothing ...
        t = LedgerTransaction()
        assert [] == t.entries
        assert {} == t.metadata
        t = LedgerTransaction(
            created=datetime.now(tz=timezone.utc),
            metadata={"a": "b", "user": "1234"},
            ext_description="foo",
        )

    def test_ledger_entry(self):
        with pytest.raises(expected_exception=ValidationError) as cm:
            LedgerEntry(
                direction=Direction.CREDIT,
                account_uuid="3f3735eaed264c2a9f8a114934afa121",
                amount=0,
            )
        assert "Input should be greater than 0" in str(cm.value)

        with pytest.raises(ValidationError) as cm:
            LedgerEntry(
                direction=Direction.CREDIT,
                account_uuid="3f3735eaed264c2a9f8a114934afa121",
                amount=2**65,
            )
        assert "Input should be less than 9223372036854775807" in str(cm.value)

        with pytest.raises(ValidationError) as cm:
            LedgerEntry(
                direction=Direction.CREDIT,
                account_uuid="3f3735eaed264c2a9f8a114934afa121",
                amount=Decimal("1"),
            )
        assert "Input should be a valid integer" in str(cm.value)

        with pytest.raises(ValidationError) as cm:
            LedgerEntry(
                direction=Direction.CREDIT,
                account_uuid="3f3735eaed264c2a9f8a114934afa121",
                amount=1.2,
            )
        assert "Input should be a valid integer" in str(cm.value)

    def test_entries(self):
        entries = [
            LedgerEntry(
                direction=Direction.CREDIT,
                account_uuid="3f3735eaed264c2a9f8a114934afa121",
                amount=100,
            ),
            LedgerEntry(
                direction=Direction.DEBIT,
                account_uuid="5927621462814f9893be807db850a31b",
                amount=100,
            ),
        ]
        LedgerTransaction(entries=entries)

    def test_raises_entries(self):
        entries = [
            LedgerEntry(
                direction=Direction.DEBIT,
                account_uuid="3f3735eaed264c2a9f8a114934afa121",
                amount=100,
            ),
            LedgerEntry(
                direction=Direction.DEBIT,
                account_uuid="5927621462814f9893be807db850a31b",
                amount=100,
            ),
        ]
        with pytest.raises(ValidationError) as e:
            LedgerTransaction(entries=entries)
        assert "ledger entries must balance" in str(e.value)

        entries = [
            LedgerEntry(
                direction=Direction.DEBIT,
                account_uuid="3f3735eaed264c2a9f8a114934afa121",
                amount=100,
            ),
            LedgerEntry(
                direction=Direction.CREDIT,
                account_uuid="5927621462814f9893be807db850a31b",
                amount=101,
            ),
        ]
        with pytest.raises(ValidationError) as cm:
            LedgerTransaction(entries=entries)
        assert "ledger entries must balance" in str(cm.value)


class TestLedgerAccount:

    def test_initialization(self):
        u = uuid4().hex
        name = f"test-{u[:8]}"

        with pytest.raises(ValidationError) as cm:
            LedgerAccount(
                display_name=name,
                qualified_name="bad bunny",
                normal_balance=Direction.DEBIT,
                account_type=AccountType.BP_WALLET,
            )
        assert "qualified name should start with" in str(cm.value)

        with pytest.raises(ValidationError) as cm:
            LedgerAccount(
                display_name=name,
                qualified_name="fish sticks:bp_wallet",
                normal_balance=Direction.DEBIT,
                account_type=AccountType.BP_WALLET,
                currency="fish sticks",
            )
        assert "Invalid UUID" in str(cm.value)
