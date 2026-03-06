import json
import logging
from datetime import datetime, timedelta
from typing import Literal, List
from urllib.parse import parse_qs, urlsplit

import dask.bag as db
import dask.dataframe as dd
from sentry_sdk import capture_exception

from generalresearch.incite.mergers import (
    MergeCollection,
    MergeCollectionItem,
    MergeType,
)
from generalresearch.incite.schemas.mergers.nginx import NGINXGRSSchema

LOG = logging.getLogger("incite")


uuid4hex = r"[a-f0-9]{8}-?[a-f0-9]{4}-?4[a-f0-9]{3}-?[89ab][a-f0-9]{3}-?[a-f0-9]{12}"


class NginxGRSMergeItem(MergeCollectionItem):

    def build(self) -> None:
        ir = self.interval
        coll: MergeCollection = self._collection
        is_partial = not self.should_archive()

        start, end = ir.left.to_pydatetime(), ir.right.to_pydatetime()
        __name__ = self._collection.merge_type.value

        reserved_kwargs = set(["39057c8b", "c184efc0", "0bb50182"])
        LOG.info(f"{__name__}: {coll._client} {reserved_kwargs=}")

        # --- READ ---
        _start = start.replace(hour=0)
        _end = end.replace(hour=0)
        days: List[str] = [
            (start + timedelta(days=i)).strftime("%Y-%m-%d")
            for i in range((end - start).days + 1)
        ]
        LOG.info(f"{__name__}: READ start: {days}")
        lines = db.read_text(
            urlpath=[f"/tmp/grs-logs/access.log-{day}-*.gz" for day in days],
            compression="gzip",
            include_path=False,
        )

        # --- PROCESS ---
        LOG.info(f"{MergeType.NGINX_GRS.value}: PROCESS start")

        def process_grs_entry(x: dict) -> dict:
            request: str = x["request"].split(" ")[1]  # GET full_url_path HTTP/1.1.1

            referer_split = urlsplit(x["referer"])
            referer_query_dict = parse_qs(referer_split.query)

            # -- couldn't get to work well with .astype. I know the 0 or 0.0 isn't good but too frustrating for now
            try:
                upstream_status = int(x["upstream_status"])
            except (Exception,):
                upstream_status = 0

            try:
                status = int(x["status"])
            except (Exception,):
                status = 0

            try:
                request_time = float(x["request_time"])
            except (Exception,):
                request_time = 0.00

            try:
                upstream_response_time = float(x["upstream_response_time"])
            except (Exception,):
                upstream_response_time = 0.0
            return {
                "time": datetime.fromtimestamp(float(x["time"])),
                "method": x.get("method", None),
                "user_agent": x.get("user_agent", None),
                "upstream_route": x.get("upstream_route", None),
                "host": x.get("host", None),
                "status": status,
                "upstream_status": upstream_status,
                "request_time": request_time,
                "upstream_response_time": upstream_response_time,
                "upstream_cache_hit": x.get("upstream_cache_hit") == "True",
                # GRL custom
                "product_id": referer_query_dict.get("39057c8b", [None])[0],
                "product_user_id": referer_query_dict.get("c184efc0", [None])[0],
                "wall_uuid": referer_query_dict.get("0bb50182", [None])[0],
                "custom_query_params": ",".join(
                    [
                        qk
                        for qk in referer_query_dict.keys()
                        if qk not in reserved_kwargs
                    ]
                ),
            }

        LOG.info(f"{__name__}: PROCESS - records maps")
        records = lines.map(json.loads).map(process_grs_entry)
        LOG.info(f"{__name__}: PROCESS - .to_dataframe()")
        ddf: dd.DataFrame = records.to_dataframe()

        # -- for "partition_on"
        LOG.info(f"{__name__}: PROCESS - cleanup")
        ddf = ddf[
            ddf["time"].between(ir.left.to_datetime64(), ir.right.to_datetime64())
        ]
        ddf = ddf.repartition(npartitions=1)

        # --- SAVE ---
        LOG.info(f"{__name__}: SAVE start")
        self.to_archive(ddf=ddf, is_partial=is_partial)
        LOG.info(f"{__name__}: SAVE finish")

        return None


class NginxGRSMerge(MergeCollection):
    merge_type: Literal[MergeType.NGINX_GRS] = MergeType.NGINX_GRS
    _schema = NGINXGRSSchema
    collection_item_class = NginxGRSMergeItem

    def build(self) -> None:
        LOG.info(f"NginxGRSMerge.build()")

        for item in reversed(self.items):
            item: NginxGRSMergeItem

            try:
                item.build()
            except (Exception,) as e:
                capture_exception(e)
                pass

        return None
