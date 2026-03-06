import pytest
from pydantic import BaseModel

from generalresearch.models.thl.payout_format import (
    PayoutFormatType,
    PayoutFormatField,
    format_payout_format,
)


class PayoutFormatTestClass(BaseModel):
    payout_format: PayoutFormatType = PayoutFormatField


class TestPayoutFormat:
    def test_payout_format_cls(self):
        # valid
        PayoutFormatTestClass(payout_format="{payout*10:,.0f} Points")
        PayoutFormatTestClass(payout_format="{payout:.0f}")
        PayoutFormatTestClass(payout_format="${payout/100:.2f}")

        # invalid
        with pytest.raises(expected_exception=Exception) as e:
            PayoutFormatTestClass(payout_format="{payout10:,.0f} Points")

        with pytest.raises(expected_exception=Exception) as e:
            PayoutFormatTestClass(payout_format="payout:,.0f} Points")

        with pytest.raises(expected_exception=Exception):
            PayoutFormatTestClass(payout_format="payout")

        with pytest.raises(expected_exception=Exception):
            PayoutFormatTestClass(payout_format="{payout;import sys:.0f}")

    def test_payout_format(self):
        assert "1,230 Points" == format_payout_format(
            payout_format="{payout*10:,.0f} Points", payout_int=123
        )

        assert "123" == format_payout_format(
            payout_format="{payout:.0f}", payout_int=123
        )

        assert "$1.23" == format_payout_format(
            payout_format="${payout/100:.2f}", payout_int=123
        )
