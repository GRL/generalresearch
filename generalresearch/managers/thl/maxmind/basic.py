import logging
import os
import subprocess
from datetime import timedelta
from pathlib import Path
from threading import RLock
from typing import Optional, Union
from uuid import uuid4

import geoip2.database
import geoip2.models
import requests
from cachetools import cached, TTLCache
from geoip2.errors import AddressNotFoundError

from generalresearch.managers.base import Manager
from generalresearch.models.custom_types import (
    IPvAnyAddressStr,
    CountryISOLike,
)


logger = logging.getLogger()


class MaxmindBasicManager(Manager):

    def __init__(
        self,
        data_dir: Union[str, Path],
        maxmind_account_id: str,
        maxmind_license_key: str,
    ):

        self.data_dir = data_dir
        self.maxmind_account_id = maxmind_account_id
        self.maxmind_license_key = maxmind_license_key

        self.run_update_geoip_db()
        super().__init__()

    @cached(
        cache=TTLCache(maxsize=1, ttl=timedelta(hours=1).total_seconds()),
        lock=RLock(),
    )
    def get_geoip_db(self):
        db_path = os.path.join(self.data_dir, "GeoIP2-Country.mmdb")
        return geoip2.database.Reader(fileish=db_path)

    def get_basic_ip_information(
        self, ip_address: IPvAnyAddressStr
    ) -> Optional[geoip2.models.Country]:
        try:
            return self.get_geoip_db().country(ip_address)
        except (ValueError, AddressNotFoundError):
            return None

    def get_country_iso_from_ip_geoip2db(
        self, ip: IPvAnyAddressStr
    ) -> Optional[CountryISOLike]:
        res = self.get_basic_ip_information(ip_address=ip)
        if res:
            return res.country.iso_code.lower()

    def run_update_geoip_db(self) -> None:
        # runs update_geoip_db with slack panic if fails
        db_path = os.path.join(self.data_dir, "GeoIP2-Country.mmdb")
        if os.path.exists(db_path):
            logger.info("GeoIP2-Country.mmdb already exists!")
        else:
            logger.info("Updating GeoIP2-Country.mmdb")
            try:
                self.update_geoip_db()
            except Exception as e:
                # TODO: Alert
                pass

    def update_geoip_db(self) -> None:
        """
        Download, checksum, extract from archive, confirm it works, then replace file on disk.
        # note: allowed 2,000 downloads per day, so I'm not bothering to implement
        #   last modified or whatever checks.
        # https://support.maxmind.com/geoip-faq/databases-and-database-updates/is-there-a-limit-to-how-often-i-can
        -download-a-database-from-my-maxmind-account/

        """
        db_url = (
            f"https://download.maxmind.com/app/geoip_download?edition_id=GeoIP2-Country&"
            f"license_key={self.maxmind_license_key}&suffix=tar.gz"
        )
        sha256_url = (
            f"https://download.maxmind.com/app/geoip_download?edition_id=GeoIP2-Country&"
            f"license_key={self.maxmind_license_key}&suffix=tar.gz.sha256"
        )
        u = uuid4().hex
        cwd = f"/tmp/{u}/"
        os.makedirs(name=cwd, exist_ok=True)

        res = requests.get(db_url)
        # db_file_name looks like "GeoIP2-Country_20210806.tar.gz"
        db_file_name = res.headers.get("Content-Disposition").split("filename=")[1]
        tmp_db_file = cwd + db_file_name
        with open(tmp_db_file, "wb") as f:
            f.write(res.content)
        res = requests.get(sha256_url)
        tmp_sha256_file = cwd + "db.sha256"
        with open(tmp_sha256_file, "wb") as f:
            f.write(res.content)
        subprocess.check_call(args=["sha256sum", "-c", tmp_sha256_file], cwd=cwd)
        # Extract
        db_name = db_file_name.replace(".tar.gz", "")
        subprocess.check_call(
            args=[
                "tar",
                "-xf",
                tmp_db_file,
                "--strip-components",
                "1",
                f"{db_name}/GeoIP2-Country.mmdb",
            ],
            cwd=cwd,
        )

        # Confirm it works
        g = geoip2.database.Reader(fileish=cwd + "GeoIP2-Country.mmdb")
        g.country("111.111.111.111").country.iso_code.lower()

        # update file on disk
        prod_db = os.path.join(self.data_dir, "GeoIP2-Country.mmdb")
        subprocess.check_call(["mv", cwd + "GeoIP2-Country.mmdb", prod_db])

        # clean up
        assert cwd.startswith("/tmp/")
        subprocess.check_call(["rm", "-r", cwd])
