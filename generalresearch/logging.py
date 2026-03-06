import decimal
import json
from datetime import date


class ThlJsonEncoder(json.JSONEncoder):
    """
    Converts:
      Decimal to str
      set to sorted list
      datetime/date to isoformat
    """

    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return str(o)
        if isinstance(o, set):
            return sorted(list(o))
        if isinstance(o, date):
            return o.isoformat()
        return super().default(o)
