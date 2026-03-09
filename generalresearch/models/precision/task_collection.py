from typing import Any, Dict, List

import pandas as pd
from pandera import Check, Column, DataFrameSchema, Index

from generalresearch.locales import Localelator
from generalresearch.models.precision import PrecisionStatus
from generalresearch.models.precision.survey import PrecisionSurvey
from generalresearch.models.thl.survey.task_collection import (
    TaskCollection,
    create_empty_df_from_schema,
)

COUNTRY_ISOS = Localelator().get_all_countries()
LANGUAGE_ISOS = Localelator().get_all_languages()

PrecisionTaskCollectionSchema = DataFrameSchema(
    columns={
        "status": Column(str, Check.isin(PrecisionStatus)),
        "cpi": Column(float, Check.between(min_value=0, max_value=100)),
        "group_id": Column(str),
        "name": Column(str),
        "survey_guid": Column(str),
        "category_id": Column(str),
        "buyer_id": Column(str),
        "country_iso": Column(str, Check.isin(COUNTRY_ISOS)),
        "country_isos": Column(str),  # comma-sep string
        "language_iso": Column(str, Check.isin(LANGUAGE_ISOS)),
        "language_isos": Column(str),  # comma-sep string
        "bid_loi": Column("Int32", Check.between(0, 90 * 60), nullable=True),
        "bid_ir": Column(float, Check.between(0, 1), nullable=True),
        "global_conversion": Column(float, Check.between(0, 1), nullable=True),
        "desired_count": Column(int),
        "achieved_count": Column(int),
        "allowed_devices": Column(str),
        "expected_end_date": Column(dtype=pd.DatetimeTZDtype(tz="UTC"), nullable=True),
        "created": Column(dtype=pd.DatetimeTZDtype(tz="UTC")),
        "updated": Column(dtype=pd.DatetimeTZDtype(tz="UTC")),
        "used_question_ids": Column(List[str]),
        "all_hashes": Column(List[str]),  # set >> list for column support
    },
    checks=[],
    index=Index(
        str,
        name="survey_id",
        checks=Check.str_length(min_value=1, max_value=16),
        unique=True,
    ),
    strict=True,
    coerce=True,
    drop_invalid_rows=False,
)


class PrecisionTaskCollection(TaskCollection):
    items: List[PrecisionSurvey]
    _schema = PrecisionTaskCollectionSchema

    def to_row(self, s: PrecisionSurvey) -> Dict[str, Any]:
        d = s.model_dump(
            mode="json",
            exclude={
                "qualifications",
                "quotas",
                "source",
                "conditions",
                "is_live",
                "excluded_surveys",
                "entry_link",
            },
        )
        d["cpi"] = float(s.cpi)
        return d

    def to_df(self):
        rows = []
        for s in self.items:
            rows.append(self.to_row(s))
        if rows:
            return pd.DataFrame.from_records(rows, index="survey_id")
        else:
            return create_empty_df_from_schema(self._schema)
