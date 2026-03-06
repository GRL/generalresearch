import logging
from typing import Optional

import geoip2.database
import geoip2.models
import geoip2.webservice
import slack
from geoip2.errors import (
    AddressNotFoundError,
    AuthenticationError,
    InvalidRequestError,
    OutOfQueriesError,
)

logger = logging.getLogger()


def get_insights_ip_information(
    ip_address: str,
    maxmind_account_id: str,
    maxmind_license_key: str,
) -> Optional[geoip2.models.Insights]:

    # (2) We want more information, proceed further ($0.002)
    client = geoip2.webservice.Client(
        account_id=maxmind_account_id, license_key=maxmind_license_key, timeout=1
    )
    logger.info(f"get_insights_ip_information: {ip_address}")
    try:
        res = client.insights(ip_address)

    except (AuthenticationError, OutOfQueriesError) as e:
        # TODO: Alert
        return None

    except (AddressNotFoundError, InvalidRequestError):
        return None
    else:
        return res


def should_call_insights(res: geoip2.models.Country) -> bool:
    """
    Call insights immediately if the IP is either:
        - in the continent of North America, Europe, or Oceania
        - in the country of Japan, Singapore, Israel, Hong Kong, Taiwan, South Korea
    """
    if res.continent.code.upper() in {"NA", "EU", "OC"}:
        return True
    if res.country.iso_code.upper() in {"JP", "SG", "IL", "HK", "TW", "KR"}:
        return True
    return False
