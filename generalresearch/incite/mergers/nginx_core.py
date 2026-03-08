import json
import logging
from datetime import datetime, timedelta
from typing import List, Literal
from urllib.parse import parse_qs, urlsplit

import dask.bag as db
import pandas as pd
from sentry_sdk import capture_exception

from generalresearch.incite.mergers import (
    MergeCollection,
    MergeCollectionItem,
    MergeType,
)
from generalresearch.incite.schemas.mergers.nginx import NGINXCoreSchema
from generalresearch.models.thl.definitions import ReservedQueryParameters

LOG = logging.getLogger("incite")

uuid4hex = r"[a-f0-9]{8}-?[a-f0-9]{4}-?4[a-f0-9]{3}-?[89ab][a-f0-9]{3}-?[a-f0-9]{12}"


class NginxCoreMergeItem(MergeCollectionItem):

    def build(self) -> None:
        ir: pd.Interval = self.interval
        is_partial = not self.should_archive()
        coll: MergeCollection = self._collection

        start, end = ir.left.to_pydatetime(), ir.right.to_pydatetime()
        __name__ = coll.merge_type.value

        reserved_kwargs = set([e.value for e in ReservedQueryParameters]) | set(
            ["AC5AD0DDBC0C", "66482fb"]
        )
        LOG.info(f"{__name__}: {self._collection._client} {reserved_kwargs=}")

        # --- READ ---
        _start = start.replace(hour=0)
        _end = end.replace(hour=0)
        days: List[str] = [
            (start + timedelta(days=i)).strftime("%Y-%m-%d")
            for i in range((end - start).days + 1)
        ]
        LOG.info(f"{__name__}: READ start")
        lines = db.read_text(
            urlpath=[f"/tmp/thl-core-logs/access.log-{day}-*.gz" for day in days],
            compression="gzip",
            include_path=False,
        )

        # --- PROCESS ---
        LOG.info(f"{__name__}: PROCESS start")

        def process_core_entry(x: dict) -> dict:
            request: str = x["request"].split(" ")[1]  # GET full_url_path HTTP/1.1.1
            referer: str = x["referer"]

            request_split = urlsplit(request)
            request_query_dict = parse_qs(request_split.query)

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
                request_time = 0.0

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
                "upstream_status": upstream_status,
                "status": status,
                "request_time": request_time,
                "upstream_response_time": upstream_response_time,
                "upstream_cache_hit": x.get("upstream_cache_hit") == "True",
                # GRL custom
                "request_path": request_split.path,
                "referer": referer,
                "session_id": request_query_dict.get("AC5AD0DDBC0C", [None])[0],
                "request_id": request_query_dict.get("66482fb", [None])[0],
                "nudge_id": request_query_dict.get("5e0e0323", [None])[0],
                "request_custom_query_params": ",".join(
                    [
                        qk
                        for qk in request_query_dict.keys()
                        if qk not in reserved_kwargs
                    ]
                ),
            }

        LOG.info(f"{__name__}: PROCESS - records maps")
        records = lines.map(json.loads).map(process_core_entry)
        LOG.info(f"{__name__}: PROCESS - .to_dataframe()")
        ddf = records.to_dataframe()

        # --- for "partition_on" ---
        ddf = ddf[
            ddf["time"].between(ir.left.to_datetime64(), ir.right.to_datetime64())
        ]
        ddf = ddf.repartition(npartitions=1)

        # --- SAVE ---
        LOG.info(f"{__name__}: SAVE start")
        self.to_archive(ddf=ddf, is_partial=is_partial)
        LOG.info(f"{__name__}: SAVE end")

        return None


class NginxCoreMerge(MergeCollection):
    merge_type: Literal[MergeType.NGINX_CORE] = MergeType.NGINX_CORE
    _schema = NGINXCoreSchema
    collection_item_class = NginxCoreMergeItem

    def build(self) -> None:
        LOG.info(f"NginxCoreMerge.build()")

        for item in reversed(self.items):
            item: NginxCoreMergeItem

            try:
                item.build()
            except (Exception,) as e:
                capture_exception(error=e)
                pass

        return None
