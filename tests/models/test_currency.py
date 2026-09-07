"""These were taken from the wxet project's first use of this idea. Not all
functionality is the same, but pasting here so the tests are in the
correct spot...
"""

from __future__ import annotations

from decimal import Decimal
from random import randint

import pytest

from generalresearch.currency import USDCent, USDMill, format_usd_cent


class TestUSDCentModel:

    def test_construct_int(self):

        for _ in range(100):
            int_val = randint(0, 999_999)
            instance = USDCent(int_val)
            assert int_val == instance

    def test_construct_float(self):

        float_val: float = 10.6789
        with pytest.warns(expected_warning=Warning) as record:
            instance = USDCent(float_val)

        assert len(record) == 1
        assert "USDCent init with a float. Rounding behavior may be unexpected" in str(
            record[0].message
        )
        assert instance == USDCent(10)
        assert instance == 10

    def test_construct_decimal(self):

        decimal_val: Decimal = Decimal("10.0")
        with pytest.warns(expected_warning=Warning) as record:
            instance = USDCent(decimal_val)

        assert len(record) == 1
        assert (
            "USDCent init with a Decimal. Rounding behavior may be unexpected"
            in str(record[0].message)
        )

        assert instance == USDCent(10)
        assert instance == 10

        # Now with rounding
        decimal_val: Decimal = Decimal("10.6789")
        with pytest.warns(Warning) as record:
            instance = USDCent(decimal_val)

        assert len(record) == 1
        assert (
            "USDCent init with a Decimal. Rounding behavior may be unexpected"
            in str(record[0].message)
        )

        assert instance == USDCent(10)
        assert instance == 10

    def test_construct_negative(self):
        with pytest.raises(expected_exception=ValueError) as cm:
            USDCent(-1)
        assert "USDCent not be less than zero" in str(cm.value)

    def test_operation_add(self):
        for _ in range(100):
            int_val1 = randint(0, 999_999)
            int_val2 = randint(0, 999_999)

            instance1 = USDCent(int_val1)
            instance2 = USDCent(int_val2)

            assert int_val1 + int_val2 == instance1 + instance2

    def test_operation_subtract(self):
        for _ in range(100):
            int_val1 = randint(500_000, 999_999)
            int_val2 = randint(0, 499_999)

            instance1 = USDCent(int_val1)
            instance2 = USDCent(int_val2)

            assert int_val1 - int_val2 == instance1 - instance2

    def test_operation_subtract_to_neg(self):
        for _ in range(100):
            int_val = randint(0, 999_999)
            instance = USDCent(int_val)

            with pytest.raises(expected_exception=ValueError) as cm:
                _ = instance - USDCent(1_000_000)

            assert "USDCent not be less than zero" in str(cm.value)

    def test_operation_multiply(self):
        for _ in range(100):
            int_val1 = randint(0, 999_999)
            int_val2 = randint(0, 999_999)

            instance1 = USDCent(int_val1)
            instance2 = USDCent(int_val2)

            assert int_val1 * int_val2 == instance1 * instance2

    def test_operation_div(self):
        with pytest.raises(ValueError) as cm:
            _ = USDCent(10) / 2
        assert "Division not allowed for USDCent" in str(cm.value)

    def test_operation_result_type(self):
        int_val = randint(1, 999_999)
        instance = USDCent(int_val)

        res_add = instance + USDCent(1)
        assert isinstance(res_add, USDCent)

        res_sub = instance - USDCent(1)
        assert isinstance(res_sub, USDCent)

        res_multipy = instance * USDCent(2)
        assert isinstance(res_multipy, USDCent)

    def test_operation_partner_add(self):
        int_val = randint(1, 999_999)
        instance = USDCent(int_val)

        with pytest.raises(expected_exception=AssertionError):
            _ = instance + 0.10

        with pytest.raises(expected_exception=AssertionError):
            _ = instance + Decimal(".10")

        with pytest.raises(expected_exception=AssertionError):
            _ = instance + "9.9"

        with pytest.raises(expected_exception=AssertionError):
            _ = instance + True

    def test_abs(self):
        for _ in range(100):
            int_val = abs(randint(0, 999_999))
            instance = abs(USDCent(int_val))

            assert int_val == instance

    def test_str(self):
        for _ in range(100):
            int_val = randint(0, 999_999)
            instance = USDCent(int_val)

            assert str(int_val) == str(instance)

    def test_operation_result_type_unsupported(self):
        """There is no correct answer here, but we at least want to make sure
        that a USDCent is returned
        """
        res = USDCent(10) // 1.2
        assert not isinstance(res, USDCent)
        assert isinstance(res, float)

        res = USDCent(10) % 1
        assert not isinstance(res, USDCent)
        assert isinstance(res, int)

        res = pow(USDCent(10), 2)
        assert not isinstance(res, USDCent)
        assert isinstance(res, int)

        res = pow(USDCent(10), USDCent(2))
        assert not isinstance(res, USDCent)
        assert isinstance(res, int)

        res = float(USDCent(10))
        assert not isinstance(res, USDCent)
        assert isinstance(res, float)


class TestUSDMillModel:

    def test_construct_int(self):
        for _ in range(100):
            int_val = randint(0, 999_999)
            instance = USDMill(int_val)
            assert int_val == instance

    def test_construct_float(self):
        float_val: float = 10.6789
        with pytest.warns(expected_warning=Warning) as record:
            instance = USDMill(float_val)

        assert len(record) == 1
        assert "USDMill init with a float. Rounding behavior may be unexpected" in str(
            record[0].message
        )
        assert instance == USDMill(10)
        assert instance == 10

    def test_construct_decimal(self):
        decimal_val: Decimal = Decimal("10.0")
        with pytest.warns(expected_warning=Warning) as record:
            instance = USDMill(decimal_val)

        assert len(record) == 1
        assert (
            "USDMill init with a Decimal. Rounding behavior may be unexpected"
            in str(record[0].message)
        )

        assert instance == USDMill(10)
        assert instance == 10

        # Now with rounding
        decimal_val: Decimal = Decimal("10.6789")
        with pytest.warns(expected_warning=Warning) as record:
            instance = USDMill(decimal_val)

        assert isinstance(instance, USDMill)
        assert len(record) == 1
        assert (
            "USDMill init with a Decimal. Rounding behavior may be unexpected"
            in str(record[0].message)
        )

        assert instance == USDMill(10)
        assert instance == 10

    def test_construct_negative(self):
        with pytest.raises(expected_exception=ValueError) as cm:
            USDMill(-1)
        assert "USDMill not be less than zero" in str(cm.value)

    def test_operation_add(self):
        for _ in range(100):
            int_val1 = randint(0, 999_999)
            int_val2 = randint(0, 999_999)

            instance1 = USDMill(int_val1)
            instance2 = USDMill(int_val2)

            assert int_val1 + int_val2 == instance1 + instance2

    def test_operation_subtract(self):
        for _ in range(100):
            int_val1 = randint(500_000, 999_999)
            int_val2 = randint(0, 499_999)

            instance1 = USDMill(int_val1)
            instance2 = USDMill(int_val2)

            assert int_val1 - int_val2 == instance1 - instance2

    def test_operation_subtract_to_neg(self):
        for _ in range(100):
            int_val = randint(0, 999_999)
            instance = USDMill(int_val)

            with pytest.raises(expected_exception=ValueError) as cm:
                _ = instance - USDMill(1_000_000)

            assert "USDMill not be less than zero" in str(cm.value)

    def test_operation_multiply(self):
        for _ in range(100):
            int_val1 = randint(0, 999_999)
            int_val2 = randint(0, 999_999)

            instance1 = USDMill(int_val1)
            instance2 = USDMill(int_val2)

            assert int_val1 * int_val2 == instance1 * instance2

    def test_operation_div(self):
        with pytest.raises(ValueError) as cm:
            _ = USDMill(10) / 2
        assert "Division not allowed for USDMill" in str(cm.value)

    def test_operation_result_type(self):
        int_val = randint(1, 999_999)
        instance = USDMill(int_val)

        res_add = instance + USDMill(1)
        assert isinstance(res_add, USDMill)

        res_sub = instance - USDMill(1)
        assert isinstance(res_sub, USDMill)

        res_multipy = instance * USDMill(2)
        assert isinstance(res_multipy, USDMill)

    def test_operation_partner_add(self):
        int_val = randint(1, 999_999)
        instance = USDMill(int_val)

        with pytest.raises(expected_exception=AssertionError):
            _ = instance + 0.10

        with pytest.raises(expected_exception=AssertionError):
            _ = instance + Decimal(".10")

        with pytest.raises(expected_exception=AssertionError):
            _ = instance + "9.9"

        with pytest.raises(expected_exception=AssertionError):
            _ = instance + True

    def test_abs(self):
        for _ in range(100):
            int_val = abs(randint(0, 999_999))
            instance = abs(USDMill(int_val))

            assert int_val == instance

    def test_str(self):
        for _ in range(100):
            int_val = randint(0, 999_999)
            instance = USDMill(int_val)

            assert str(int_val) == str(instance)

    def test_operation_result_type_unsupported(self):
        """There is no correct answer here, but we at least want to make sure
        that a USDMill is returned
        """
        res = USDMill(10) // 1.2
        assert not isinstance(res, USDMill)
        assert isinstance(res, float)

        res = USDMill(10) % 1
        assert not isinstance(res, USDMill)
        assert isinstance(res, int)

        res = pow(USDMill(10), 2)
        assert not isinstance(res, USDMill)
        assert isinstance(res, int)

        res = pow(USDMill(10), USDMill(2))
        assert not isinstance(res, USDCent)
        assert isinstance(res, int)

        res = float(USDMill(10))
        assert not isinstance(res, USDMill)
        assert isinstance(res, float)


class TestNegativeFormatting:

    def test_pos(self):
        assert "-$987.65" == format_usd_cent(-98765)

    def test_neg(self):
        assert "-$123.45" == format_usd_cent(-12345)
