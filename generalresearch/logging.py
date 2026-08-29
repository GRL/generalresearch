import decimal
import json
from datetime import date
from typing import Any


class ThlJsonEncoder(json.JSONEncoder):
    """
    Converts:
      Decimal to str
      set to sorted list
      datetime/date to isoformat
    """

    def default(self, o: Any) -> Any:
        if isinstance(o, decimal.Decimal):
            return str(o)
        if isinstance(o, set):
            return sorted(o)
        if isinstance(o, date):
            return o.isoformat()
        return super().default(o)
