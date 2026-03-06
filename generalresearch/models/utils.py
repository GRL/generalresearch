from decimal import Decimal


def usd_cents_to_decimal(v: int) -> Decimal:
    return Decimal(Decimal(int(v)) / Decimal(100))


def decimal_to_usd_cents(d: Decimal) -> int:
    return round(d * Decimal(100))
