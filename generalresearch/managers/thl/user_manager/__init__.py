import csv
import logging
import os
import threading
import time
from pathlib import Path
from threading import RLock
from typing import Any, Dict, Union

from cachetools import TTLCache, cached

from generalresearch.models.thl.product import Product

logger = logging.getLogger()


class UserDoesntExistError(Exception):
    pass


class UserCreateNotAllowedError(Exception):
    pass


def download_bp_trust():
    raise DeprecationWarning("No more S3")


@cached(TTLCache(maxsize=1, ttl=5 * 60), lock=RLock())
def get_bp_trust_df():
    from importlib.resources import files

    fp = str(
        files("generalresearch.resources").joinpath("brokerage_trust_calculated.csv")
    )
    # cols = ['product_id', 'team_id', 'team_name', 'business_id', 'business_name',
    #         'product_name', 'bp_trust', 'team_trust', 'entrance_limit_expire_sec',
    #         'entrance_limit_value']

    if not os.path.exists(fp):
        Path(fp).touch()
        threading.Thread(target=download_bp_trust).start()
        # raise exception so its not cached
        raise FileNotFoundError()
    if time.time() - os.path.getmtime(fp) > 3600:
        Path(fp).touch()
        threading.Thread(target=download_bp_trust).start()
    bptrust = parse_bp_trust_df(fp)

    return bptrust


convert_int = lambda x: int(float(x))


def parse_bp_trust_df(fp: Union[str, Path]) -> Dict[str, Any]:
    dtype = {
        "bp_trust": float,
        "team_trust": float,
        "entrance_limit_expire_sec": convert_int,
        "entrance_limit_value": convert_int,
        "median_daily_completes_7d": convert_int,
    }
    bptrust = dict()

    with open(fp, newline="") as csvfile:
        reader = csv.reader(csvfile)
        header = next(reader)

        for row in reader:
            d = dict(zip(header, row))
            for k, v in dtype.items():
                if k in d:
                    d[k] = v(d[k])
            bptrust[d["product_id"]] = d

    return bptrust


def get_bp_user_create_limit_hourly(product: Product) -> int:
    """The BP's hour user creation limit is calculated as: 4 times the median
    daily completes over the past 7 days, with a default range of 60 to 1000
    per hour. For e.g. if a BP has 0 median daily completes, the user
    creation limit is 60/hr. We can also override the default 60-1000 range
    using the product.user_create_config.
    """
    global_default = 120
    default = product.user_create_config.clip_hourly_create_limit(global_default)

    try:
        bptrust = get_bp_trust_df()
    except (FileNotFoundError, StopIteration):
        return default

    if product.id not in bptrust:
        return default

    if "median_daily_completes_7d" not in bptrust[product.id]:
        logger.exception("missing median_daily_completes_7d column")
        return default

    user_create_limit_daily = bptrust[product.id]["median_daily_completes_7d"] * 8
    user_create_limit_hourly = user_create_limit_daily / 24
    user_create_limit_hourly = max(min(user_create_limit_hourly, 5000), global_default)
    user_create_limit_hourly = product.user_create_config.clip_hourly_create_limit(
        user_create_limit_hourly
    )
    return user_create_limit_hourly
