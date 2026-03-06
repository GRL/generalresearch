from typing import List

import pandas as pd
from pandera import Column, DataFrameSchema, Check, Index

from generalresearch.locales import Localelator
from generalresearch.models import TaskCalculationType
from generalresearch.models.repdata import RepDataStatus
from generalresearch.models.repdata.survey import RepDataSurveyHashed
from generalresearch.models.thl.survey.task_collection import (
    TaskCollection,
    create_empty_df_from_schema,
)

COUNTRY_ISOS = Localelator().get_all_countries()
LANGUAGE_ISOS = Localelator().get_all_languages()

RepDataTaskCollectionSchema = DataFrameSchema(
    columns={
        # --- These fields come from the Survey object ---
        "survey_id": Column(
            str, Check.str_length(min_value=1, max_value=16), unique=False
        ),
        "survey_uuid": Column(
            str, Check.str_length(min_value=32, max_value=32), unique=False
        ),
        "survey_name": Column(str, Check.str_length(min_value=1, max_value=256)),
        "project_uuid": Column(str, Check.str_length(min_value=32, max_value=32)),
        "survey_status": Column(str, Check.isin(RepDataStatus)),
        "country_iso": Column(str, Check.isin(COUNTRY_ISOS)),  # 2 letter, lowercase
        "language_iso": Column(str, Check.isin(LANGUAGE_ISOS)),  # 3 letter, lowercase
        "estimated_loi": Column(int, Check.between(0, 90 * 60)),
        "estimated_ir": Column(int, Check.between(0, 100)),
        "collects_pii": Column(bool),
        "allowed_devices": Column(str),
        "created": Column(dtype=pd.DatetimeTZDtype(tz="UTC")),
        "last_updated": Column(dtype=pd.DatetimeTZDtype(tz="UTC")),
        # --- These come from the Stream object ---
        # This is the index ---v
        # "stream_id": Column(str, Check.str_length(min_value=1, max_value=16), unique=True),
        "stream_uuid": Column(
            str, Check.str_length(min_value=32, max_value=32), unique=True
        ),
        "stream_name": Column(str, Check.str_length(min_value=1, max_value=256)),
        "stream_status": Column(str, Check.isin(RepDataStatus)),
        "remaining_count": Column(int, Check.greater_than_or_equal_to(0)),
        "calculation_type": Column(str, Check.isin(TaskCalculationType)),
        "cpi": Column(float, Check.between(min_value=0, max_value=100)),
        "used_question_ids": Column(List[str]),
        "all_hashes": Column(List[str]),  # set >> list for column support
    },
    checks=[
        # # There's only 1 or 2 live surveys, so we can't really assert anything ...
        # Check(lambda df: df.shape[0] > 50,
        #       description="There should always be more than 50 surveys"),
        #
        # # Check(lambda df: 60 <= df.opp_obs_median_loi.mean() < 30 * 60,
        # #       description="Survey opp LOI should be 1 - 30 min on average."),
        # # Check(lambda df: 60 <= df.quota_obs_median_loi.mean() < 30 * 60,
        # #       description="Surveys opp quota LOI should be 1 - 30 min on average."),
        #
        # Check(lambda df: .25 <= df.cpi.mean() < 5,
        #       description="Surveys CPI should be $.25 - $5 on average."),
        #
        # Check(lambda df: "us" in df.country_iso.value_counts().index[:3],
        #       description="United States must be in the top 3 countries."),
    ],
    index=Index(
        str,
        name="stream_id",
        checks=Check.str_length(min_value=1, max_value=16),
        unique=True,
    ),
    strict=True,
    coerce=False,
    drop_invalid_rows=False,
)


class RepDataTaskCollection(TaskCollection):
    items: List[RepDataSurveyHashed]
    _schema = RepDataTaskCollectionSchema

    def to_rows(self, s: RepDataSurveyHashed):
        survey_fields = [
            "survey_id",
            "survey_uuid",
            "survey_name",
            "project_uuid",
            "survey_status",
            "country_iso",
            "language_iso",
            "estimated_loi",
            "estimated_ir",
            "collects_pii",
            "created",
            "last_updated",
        ]
        stream_fields = [
            "stream_id",
            "stream_uuid",
            "stream_name",
            "stream_status",
            "calculation_type",
            "cpi",
            "used_question_ids",
            "all_hashes",
            "remaining_count",
        ]
        rows = []
        d = dict()
        for k in survey_fields:
            d[k] = getattr(s, k)
        d["allowed_devices"] = s.allowed_devices_str
        for stream in s.hashed_streams:
            ds = d.copy()
            for k in stream_fields:
                ds[k] = getattr(stream, k)
            ds["cpi"] = float(ds["cpi"])
            ds["used_question_ids"] = list(ds["used_question_ids"])
            ds["all_hashes"] = list(ds["all_hashes"])
            rows.append(ds)
        return rows

    def to_df(self):
        rows = []
        for s in self.items:
            rows.extend(self.to_rows(s))
        if rows:
            return pd.DataFrame.from_records(rows, index="stream_id")
        else:
            return create_empty_df_from_schema(self._schema)
