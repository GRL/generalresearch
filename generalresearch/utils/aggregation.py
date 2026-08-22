from collections import defaultdict
from typing import Any


def group_by_year(records: list[dict], datetime_field: str) -> dict[int, list[Any]]:
    """Memory efficient - processes records one at a time"""
    by_year = defaultdict(list)

    for record in records:
        # Extract year from ISO string without full datetime parsing
        year = int(record[datetime_field][:4])
        by_year[year].append(record)

    return dict(by_year)
