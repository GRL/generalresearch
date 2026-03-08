import json
import logging
import re
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
from generalresearch.incite.schemas.mergers.nginx import NGINXFSBSchema
from generalresearch.models.thl.definitions import ReservedQueryParameters

LOG = logging.getLogger("incite")

uuid4hex = r"[a-f0-9]{8}-?[a-f0-9]{4}-?4[a-f0-9]{3}-?[89ab][a-f0-9]{3}-?[a-f0-9]{12}"


class NginxFSBMergeItem(MergeCollectionItem):

    def build(self) -> None:
        ir: pd.Interval = self.interval
        is_partial = not self.should_archive()
        coll: MergeCollection = self._collection

        start, end = ir.left.to_pydatetime(), ir.right.to_pydatetime()
        __name__ = coll.merge_type.value

        reserved_kwargs = set([e.value for e in ReservedQueryParameters])
        LOG.info(f"{__name__}: {coll._client} {reserved_kwargs=}")

        # --- READ ---
        _start = start.replace(hour=0)
        _end = end.replace(hour=0)
        days: List[str] = [
            (start + timedelta(days=i)).strftime("%Y-%m-%d")
            for i in range((end - start).days + 1)
        ]
        LOG.info(f"{__name__}: READ start: {days=}")
        lines = db.read_text(
            urlpath=[f"/tmp/fsb-logs/access.log-{day}-*.gz" for day in days],
            compression="gzip",
            include_path=False,
        )

        # --- PROCESS ---
        LOG.info(f"{__name__}: PROCESS start")

        def process_fsb_entry(x: dict) -> dict:
            request: str = x["request"].split(" ")[1]  # GET full_url_path HTTP/1.1.1
            url_split = urlsplit(request)
            query_dict = parse_qs(url_split.query)
            product_ids = re.findall(uuid4hex, request)
            product_id = (
                product_ids[0] if len(product_ids) else "-"
            )  # Cannot (categorize) convert non-finite values
            is_offerwall = "/offerwall/" in url_split.path
            offerwall = "-"  # Cannot (categorize) convert non-finite values
            if is_offerwall:
                offerwall = url_split.path.split("/offerwall/")[1][:-1] or "-"
            is_report = "/report/" in url_split.path

            # -- couldn't get to work well with .astype. I know the 0 or 0.0 isn't good but too frustrating for now
            try:
                status = int(x["status"])
            except (Exception,):
                status = 0

            try:
                upstream_status = int(x["upstream_status"])
            except (Exception,):
                upstream_status = 0

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
                "status": status,
                "upstream_status": upstream_status,
                "request_time": request_time,
                "upstream_response_time": upstream_response_time,
                "upstream_cache_hit": x.get("upstream_cache_hit") == "True",
                # GRL custom
                "product_id": product_id,
                "product_user_id": query_dict.get("bpuid", [None])[0],
                "n_bins": query_dict.get("n_bins", [None])[0],
                "is_offerwall": is_offerwall,
                "offerwall": offerwall,
                "is_report": is_report,
                "custom_query_params": ",".join(
                    [qk for qk in query_dict.keys() if qk not in reserved_kwargs]
                ),
            }

        LOG.info(f"{__name__}: PROCESS - records maps")
        records = lines.map(json.loads).map(process_fsb_entry)
        LOG.info(f"{__name__}: PROCESS - .to_dataframe()")
        ddf = records.to_dataframe()

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


class NginxFSBMerge(MergeCollection):
    merge_type: Literal[MergeType.NGINX_FSB] = MergeType.NGINX_FSB
    _schema = NGINXFSBSchema
    collection_item_class = NginxFSBMergeItem

    def build(self) -> None:
        LOG.info(f"NginxFSBMerge.build()")

        for item in reversed(self.items):
            item: NginxFSBMergeItem

            try:
                item.build()
            except (Exception,) as e:
                capture_exception(error=e)
                pass

        return None
