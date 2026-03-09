from typing import Any, Dict, List

import pandas as pd
from pandera import Check, Column, DataFrameSchema, Index

from generalresearch.locales import Localelator
from generalresearch.models.prodege import ProdegeStatus
from generalresearch.models.prodege.survey import ProdegeSurvey
from generalresearch.models.thl.survey.task_collection import (
    TaskCollection,
    create_empty_df_from_schema,
)

COUNTRY_ISOS = Localelator().get_all_countries()
LANGUAGE_ISOS = Localelator().get_all_languages()

ProdegeTaskCollectionSchema = DataFrameSchema(
    columns={
        "status": Column(str, Check.isin(ProdegeStatus)),
        "cpi": Column(float, Check.between(min_value=0, max_value=100)),
        "country_iso": Column(str, Check.isin(COUNTRY_ISOS)),  # 2 letter, lowercase
        "language_iso": Column(str, Check.isin(LANGUAGE_ISOS)),  # 3 letter, lowercase
        "desired_count": Column(int, Check.greater_than(min_value=0)),
        "remaining_count": Column(int, Check.greater_than_or_equal_to(min_value=0)),
        "achieved_completes": Column(int, Check.greater_than_or_equal_to(min_value=0)),
        "bid_loi": Column(int, Check.between(0, 120 * 60), nullable=True),
        "bid_ir": Column(float, Check.between(0, 1), nullable=True),
        "actual_loi": Column(int, Check.between(0, 120 * 60), nullable=True),
        "actual_ir": Column(float, Check.between(0, 1), nullable=True),
        "conversion_rate": Column(float, Check.between(0, 1), nullable=True),
        "created": Column(dtype=pd.DatetimeTZDtype(tz="UTC")),
        "updated": Column(dtype=pd.DatetimeTZDtype(tz="UTC")),
        "used_question_ids": Column(List[str]),
        "all_hashes": Column(List[str]),  # set >> list for column support
        "is_recontact": Column(bool),
        # Not including here: entrance_url, max_clicks_settings, past_participation, include_psids, exclude_psids,
        #   quotas, source, conditions
        # Adding a derived field: is_recontact, which is True is include_psids is not None
    },
    checks=[],
    index=Index(
        str,
        name="survey_id",
        checks=Check.str_length(min_value=1, max_value=16),
        unique=True,
    ),
    strict=True,
    coerce=False,
    drop_invalid_rows=False,
)


class ProdegeTaskCollection(TaskCollection):
    items: List[ProdegeSurvey]
    _schema = ProdegeTaskCollectionSchema

    @staticmethod
    def to_row(s: ProdegeSurvey) -> Dict[str, Any]:
        fields = [
            "survey_id",
            "status",
            "country_iso",
            "language_iso",
            "cpi",
            "desired_count",
            "remaining_count",
            "achieved_completes",
            "bid_loi",
            "bid_ir",
            "actual_loi",
            "actual_ir",
            "conversion_rate",
            "created",
            "updated",
            "is_recontact",
            "used_question_ids",
            "all_hashes",
        ]
        d = dict()
        for k in fields:
            d[k] = getattr(s, k)
        d["cpi"] = float(d["cpi"])
        d["used_question_ids"] = list(d["used_question_ids"])
        d["all_hashes"] = list(d["all_hashes"])
        return d

    def to_df(self) -> pd.DataFrame:
        rows = []
        for s in self.items:
            rows.append(self.to_row(s))
        if rows:
            df = pd.DataFrame.from_records(rows, index="survey_id")
            df["bid_loi"] = df["bid_loi"].astype("Int64")
            df["actual_loi"] = df["actual_loi"].astype("Int64")
            return df
        else:
            return create_empty_df_from_schema(self._schema)
