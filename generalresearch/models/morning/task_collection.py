from typing import List, Set

import pandas as pd
from pandera import Column, DataFrameSchema, Check, Index

from generalresearch.locales import Localelator
from generalresearch.models.morning import MorningStatus
from generalresearch.models.morning.survey import MorningBid
from generalresearch.models.thl.survey.task_collection import (
    create_empty_df_from_schema,
    TaskCollection,
)

COUNTRY_ISOS: Set[str] = Localelator().get_all_countries()
LANGUAGE_ISOS: Set[str] = Localelator().get_all_languages()

bid_stats_columns = {
    "system_conversion": Column(float, Check.between(0, 1), nullable=True),
    "num_entrants": Column(int, Check.ge(0)),
    "num_screenouts": Column(int, Check.ge(0)),
    "bid_loi": Column("Int32", Check.between(0, 90 * 60), nullable=True),
}

# Used for "counts", should be a non-negative integer.
CountColumn = Column(int, Check.ge(0))
stats_columns = {
    "num_available": CountColumn,
    "num_completes": CountColumn,
    "num_failures": CountColumn,
    "num_in_progress": CountColumn,
    "num_over_quotas": CountColumn,
    "num_qualified": CountColumn,
    "num_quality_terminations": CountColumn,
    "num_timeouts": CountColumn,
    "obs_median_loi": Column("Int32", Check.between(0, 90 * 60), nullable=True),
    "qualified_conversion": Column(float, Check.between(0, 1), nullable=True),
}

bid_columns = {
    "bid.id": Column(str, Check.str_length(min_value=1, max_value=32)),  # uuid-hex
    "status": Column(str, Check.isin(MorningStatus)),
    "country_iso": Column(str, Check.isin(COUNTRY_ISOS)),  # 2 letter, lowercase
    "language_isos": Column(str),  # comma-separated list of [3 letter, lowercase]
    "buyer_account_id": Column(str),  # uuid-hex
    "buyer_id": Column(str),  # uuid-hex
    "name": Column(str, Check.str_length(min_value=1, max_value=256)),
    "supplier_exclusive": Column(bool),
    "survey_type": Column(str, Check.str_length(min_value=1, max_value=32)),
    "topic_id": Column(str, Check.str_length(min_value=1, max_value=64)),
    "timeout": Column(int, Check.ge(0)),
    "created_api": Column(dtype=pd.DatetimeTZDtype(tz="UTC")),
    "expected_end": Column(dtype=pd.DatetimeTZDtype(tz="UTC")),
    "updated": Column(dtype=pd.DatetimeTZDtype(tz="UTC")),
}
quota_columns = {
    "cpi": Column(float, Check.between(min_value=0, max_value=100)),
    "used_question_ids": Column(List[str]),
    "all_hashes": Column(List[str]),  # set >> list for column support
}

columns = (
    bid_columns
    | quota_columns
    | {"bid." + k: v for k, v in bid_stats_columns.items()}
    | {"bid." + k: v for k, v in stats_columns.items()}
    | {"quota." + k: v for k, v in stats_columns.items()}
)

# In Morning, each row is 1 quota!
MorningTaskCollectionSchema = DataFrameSchema(
    columns=columns,
    checks=[],
    # this should be a uuid-hex
    index=Index(
        str,
        name="quota_id",
        checks=Check.str_length(min_value=1, max_value=32),
        unique=True,
    ),
    strict=True,
    coerce=True,
    drop_invalid_rows=False,
)


class MorningTaskCollection(TaskCollection):
    items: List[MorningBid]
    _schema = MorningTaskCollectionSchema

    def to_rows(self, bid: MorningBid):
        stats_fields = list(stats_columns.keys())
        bid_stats_fields = list(bid_stats_columns.keys())
        bid_fields = [
            #  'id',  # we have to rename this
            "status",
            "country_iso",
            "language_isos",
            "buyer_account_id",
            "buyer_id",
            "name",
            "supplier_exclusive",
            "survey_type",
            "topic_id",
            "timeout",
            "created_api",
            "expected_end",
            "updated",
        ]
        quota_fields = list(quota_columns.keys())
        rows = []
        bid_dict = dict()
        for k in bid_fields:
            bid_dict[k] = getattr(bid, k)
            bid_dict["bid.id"] = bid.id
            bid_dict["language_isos"] = ",".join(sorted(bid.language_isos))
        for k in bid_stats_fields:
            bid_dict["bid." + k] = getattr(bid, k)
        for k in stats_fields:
            bid_dict["bid." + k] = getattr(bid, k)
        for quota in bid.quotas:
            d = bid_dict.copy()
            d["quota_id"] = quota.id
            for k in quota_fields:
                d[k] = getattr(quota, k)
            for k in stats_fields:
                d["quota." + k] = getattr(quota, k)
            d["cpi"] = float(quota.cpi)
            d["used_question_ids"] = list(quota.used_question_ids)
            d["all_hashes"] = list(quota.all_hashes)
            rows.append(d)
        return rows

    def to_df(self):
        rows = []
        for s in self.items:
            rows.extend(self.to_rows(s))
        if rows:
            return pd.DataFrame.from_records(rows, index="quota_id")
        else:
            return create_empty_df_from_schema(self._schema)
