from datetime import timedelta
from decimal import Decimal

import pytest
from pydantic import ValidationError


class TestBucket:

    def test_raises_payout(self):
        from generalresearch.models.legacy.bucket import Bucket

        with pytest.raises(expected_exception=ValidationError) as e:
            Bucket(user_payout_min=123)
        assert "Must pass a Decimal" in str(e.value)

        with pytest.raises(expected_exception=ValidationError) as e:
            Bucket(user_payout_min=Decimal(1 / 3))
        assert "Must have 2 or fewer decimal places" in str(e.value)

        with pytest.raises(expected_exception=ValidationError) as e:
            Bucket(user_payout_min=Decimal(10000))
        assert "should be less than 1000" in str(e.value)

        with pytest.raises(expected_exception=ValidationError) as e:
            Bucket(user_payout_min=Decimal(1), user_payout_max=Decimal("0.01"))
        assert "user_payout_min should be <= user_payout_max" in str(e.value)

    def test_raises_loi(self):
        from generalresearch.models.legacy.bucket import Bucket

        with pytest.raises(expected_exception=ValidationError) as e:
            Bucket(loi_min=123)
        assert "Input should be a valid timedelta" in str(e.value)

        with pytest.raises(expected_exception=ValidationError) as e:
            Bucket(loi_min=timedelta(seconds=9999))
        assert "should be less than 90 minutes" in str(e.value)

        with pytest.raises(ValidationError) as e:
            Bucket(loi_min=timedelta(seconds=0))
        assert "should be greater than 0" in str(e.value)

        with pytest.raises(expected_exception=ValidationError) as e:
            Bucket(loi_min=timedelta(seconds=10), loi_max=timedelta(seconds=9))
        assert "loi_min should be <= loi_max" in str(e.value)

        with pytest.raises(expected_exception=ValidationError) as e:
            Bucket(
                loi_min=timedelta(seconds=10),
                loi_max=timedelta(seconds=90),
                loi_q1=timedelta(seconds=20),
            )
        assert "loi_q1, q2, and q3 should all be set" in str(e.value)
        with pytest.raises(expected_exception=ValidationError) as e:
            Bucket(
                loi_min=timedelta(seconds=10),
                loi_max=timedelta(seconds=90),
                loi_q1=timedelta(seconds=200),
                loi_q2=timedelta(seconds=20),
                loi_q3=timedelta(seconds=12),
            )
        assert "loi_q1 should be <= loi_q2" in str(e.value)

    def test_parse_1(self):
        from generalresearch.models.legacy.bucket import Bucket

        b1 = Bucket.parse_from_offerwall({"payout": {"min": 123}})
        b_exp = Bucket(
            user_payout_min=Decimal("1.23"),
            user_payout_max=None,
            loi_min=None,
            loi_max=None,
        )
        assert b_exp == b1

        b2 = Bucket.parse_from_offerwall({"payout": {"min": 123, "max": 230}})
        b_exp = Bucket(
            user_payout_min=Decimal("1.23"),
            user_payout_max=Decimal("2.30"),
            loi_min=None,
            loi_max=None,
        )
        assert b_exp == b2

        b3 = Bucket.parse_from_offerwall(
            {"payout": {"min": 123, "max": 230}, "duration": {"min": 600, "max": 1800}}
        )
        b_exp = Bucket(
            user_payout_min=Decimal("1.23"),
            user_payout_max=Decimal("2.30"),
            loi_min=timedelta(seconds=600),
            loi_max=timedelta(seconds=1800),
        )
        assert b_exp == b3

        b4 = Bucket.parse_from_offerwall(
            {
                "payout": {"max": 80, "min": 28, "q1": 43, "q2": 43, "q3": 56},
                "duration": {"max": 1172, "min": 266, "q1": 746, "q2": 918, "q3": 1002},
            }
        )
        b_exp = Bucket(
            user_payout_min=Decimal("0.28"),
            user_payout_max=Decimal("0.80"),
            user_payout_q1=Decimal("0.43"),
            user_payout_q2=Decimal("0.43"),
            user_payout_q3=Decimal("0.56"),
            loi_min=timedelta(seconds=266),
            loi_max=timedelta(seconds=1172),
            loi_q1=timedelta(seconds=746),
            loi_q2=timedelta(seconds=918),
            loi_q3=timedelta(seconds=1002),
        )
        assert b_exp == b4

    def test_parse_2(self):
        from generalresearch.models.legacy.bucket import Bucket

        b1 = Bucket.parse_from_offerwall({"min_payout": 123})
        b_exp = Bucket(
            user_payout_min=Decimal("1.23"),
            user_payout_max=None,
            loi_min=None,
            loi_max=None,
        )
        assert b_exp == b1

        b2 = Bucket.parse_from_offerwall({"min_payout": 123, "max_payout": 230})
        b_exp = Bucket(
            user_payout_min=Decimal("1.23"),
            user_payout_max=Decimal("2.30"),
            loi_min=None,
            loi_max=None,
        )
        assert b_exp == b2

        b3 = Bucket.parse_from_offerwall(
            {
                "min_payout": 123,
                "max_payout": 230,
                "min_duration": 600,
                "max_duration": 1800,
            }
        )
        b_exp = Bucket(
            user_payout_min=Decimal("1.23"),
            user_payout_max=Decimal("2.30"),
            loi_min=timedelta(seconds=600),
            loi_max=timedelta(seconds=1800),
        )
        assert b_exp, b3

        b4 = Bucket.parse_from_offerwall(
            {
                "min_payout": 28,
                "max_payout": 99,
                "min_duration": 205,
                "max_duration": 1113,
                "q1_payout": 43,
                "q2_payout": 43,
                "q3_payout": 46,
                "q1_duration": 561,
                "q2_duration": 891,
                "q3_duration": 918,
            }
        )
        b_exp = Bucket(
            user_payout_min=Decimal("0.28"),
            user_payout_max=Decimal("0.99"),
            user_payout_q1=Decimal("0.43"),
            user_payout_q2=Decimal("0.43"),
            user_payout_q3=Decimal("0.46"),
            loi_min=timedelta(seconds=205),
            loi_max=timedelta(seconds=1113),
            loi_q1=timedelta(seconds=561),
            loi_q2=timedelta(seconds=891),
            loi_q3=timedelta(seconds=918),
        )
        assert b_exp == b4

    def test_parse_3(self):
        from generalresearch.models.legacy.bucket import Bucket

        b1 = Bucket.parse_from_offerwall({"payout": 123})
        b_exp = Bucket(
            user_payout_min=Decimal("1.23"),
            user_payout_max=None,
            loi_min=None,
            loi_max=None,
        )
        assert b_exp == b1

        b2 = Bucket.parse_from_offerwall({"payout": 123, "duration": 1800})
        b_exp = Bucket(
            user_payout_min=Decimal("1.23"),
            user_payout_max=None,
            loi_min=None,
            loi_max=timedelta(seconds=1800),
        )
        assert b_exp == b2
