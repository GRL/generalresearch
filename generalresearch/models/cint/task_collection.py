from typing import List, Set

import pandas as pd
from pandera import Check, Column, DataFrameSchema, Index

from generalresearch.locales import Localelator
from generalresearch.models.cint.survey import CintSurvey
from generalresearch.models.thl.survey.task_collection import (
    TaskCollection,
    create_empty_df_from_schema,
)

COUNTRY_ISOS: Set[str] = Localelator().get_all_countries()
LANGUAGE_ISOS: Set[str] = Localelator().get_all_languages()

CintTaskCollectionSchema = DataFrameSchema(
    columns={
        "survey_name": Column(str, Check.str_length(min_value=1, max_value=128)),
        "is_live": Column(bool),
        "cpi": Column(float, Check.between(min_value=0, max_value=100)),
        "buyer_id": Column(str),
        "buyer_name": Column(str),
        "study_type": Column(str),
        "country_iso": Column(str, Check.isin(COUNTRY_ISOS)),  # 2 letter, lowercase
        "language_iso": Column(str, Check.isin(LANGUAGE_ISOS)),  # 3 letter, lowercase
        "total_client_entrants": Column(int),
        "overall_completes": Column(int),
        "length_of_interview": Column(
            "Int32", Check.between(0, 90 * 60), nullable=True
        ),
        "conversion": Column(float, Check.between(0, 1), nullable=True),
        "bid_loi": Column("Int32", Check.between(0, 90 * 60), nullable=True),
        "bid_ir": Column(float, Check.between(0, 1), nullable=True),
        "created_at": Column(dtype=pd.DatetimeTZDtype(tz="UTC")),
        "last_updated": Column(dtype=pd.DatetimeTZDtype(tz="UTC")),
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


class CintTaskCollection(TaskCollection):
    items: List[CintSurvey]
    _schema = CintTaskCollectionSchema

    def to_row(self, s: CintSurvey):
        d = s.model_dump(
            mode="json",
            include=set(CintTaskCollectionSchema.columns.keys()) | {"survey_id"},
        )
        d["cpi"] = float(s.cpi)
        return d

    def to_df(self) -> pd.DataFrame:
        rows = []
        for s in self.items:
            rows.append(self.to_row(s))
        if rows:
            return pd.DataFrame.from_records(rows, index="survey_id")
        else:
            return create_empty_df_from_schema(self._schema)
