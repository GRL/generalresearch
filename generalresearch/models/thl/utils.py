from decimal import Decimal


def decimal_to_int_cents(usd: Decimal | None) -> int | None:
    return round(usd * 100) if usd is not None else None


def int_cents_to_decimal(value: int | None, decimals: int = 2) -> Decimal | None:
    if value is None:
        return None
    return (Decimal(value) / Decimal(100)).quantize(Decimal(10) ** -decimals)
