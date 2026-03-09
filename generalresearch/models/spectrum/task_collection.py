from typing import Dict, List, Set

import pandas as pd
from pandera import Check, Column, DataFrameSchema, Index

from generalresearch.locales import Localelator
from generalresearch.models import TaskCalculationType
from generalresearch.models.spectrum import SpectrumStatus
from generalresearch.models.spectrum.survey import SpectrumSurvey
from generalresearch.models.thl.survey.task_collection import (
    TaskCollection,
    create_empty_df_from_schema,
)

COUNTRY_ISOS: Set[str] = Localelator().get_all_countries()
LANGUAGE_ISOS: Set[str] = Localelator().get_all_languages()

SpectrumTaskCollectionSchema = DataFrameSchema(
    columns={
        "survey_name": Column(str, Check.str_length(min_value=1, max_value=256)),
        "status": Column(int, Check.isin(SpectrumStatus)),
        "field_end_date": Column(dtype=pd.DatetimeTZDtype(tz="UTC")),
        "category_code": Column(),
        "calculation_type": Column(str, Check.isin(TaskCalculationType)),
        "requires_pii": Column(bool),
        "buyer_id": Column(str),
        "cpi": Column(float, Check.between(min_value=0, max_value=100)),
        "bid_loi": Column("Int32", Check.between(0, 90 * 60), nullable=True),
        "bid_ir": Column(float, Check.between(0, 1), nullable=True),
        "overall_loi": Column("Int32", Check.between(0, 90 * 60), nullable=True),
        "overall_ir": Column(float, Check.between(0, 1), nullable=True),
        "last_block_loi": Column("Int32", Check.between(0, 90 * 60), nullable=True),
        "last_block_ir": Column(float, Check.between(0, 1), nullable=True),
        "project_last_complete_date": Column(
            dtype=pd.DatetimeTZDtype(tz="UTC"), nullable=True
        ),
        "country_iso": Column(str, Check.isin(COUNTRY_ISOS)),  # 2 letter, lowercase
        "language_iso": Column(str, Check.isin(LANGUAGE_ISOS)),  # 3 letter, lowercase
        # exclude_psids is potentially large. We don't need these usually, we just want to know
        #   if include_psids is set, if so then this is a recontact
        # "exclude_psids": Column(bool),
        "include_psids": Column(str, nullable=True),
        "created_api": Column(dtype=pd.DatetimeTZDtype(tz="UTC")),
        "modified_api": Column(dtype=pd.DatetimeTZDtype(tz="UTC")),
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


class SpectrumTaskCollection(TaskCollection):
    items: List[SpectrumSurvey]
    _schema = SpectrumTaskCollectionSchema

    def to_rows(self, s: SpectrumSurvey) -> List[Dict]:
        fields = [
            "survey_name",
            "status",
            "field_end_date",
            "category_code",
            "calculation_type",
            "requires_pii",
            "buyer_id",
            "cpi",
            "bid_loi",
            "bid_ir",
            "overall_loi",
            "overall_ir",
            "last_block_loi",
            "last_block_ir",
            "project_last_complete_date",
            "country_iso",
            "language_iso",
            "include_psids",
            "created_api",
            "modified_api",
            "updated",
            "used_question_ids",
            "all_hashes",
            "survey_id",
        ]
        rows = []
        d = dict()
        for k in fields:
            d[k] = getattr(s, k) if hasattr(s, k) else None
        d["used_question_ids"] = list(s.used_question_ids)
        d["cpi"] = float(s.cpi)
        d["all_hashes"] = list(d["all_hashes"])
        rows.append(d)
        return rows

    def to_df(self) -> pd.DataFrame:
        rows = []
        for s in self.items:
            rows.extend(self.to_rows(s))
        if rows:
            return pd.DataFrame.from_records(rows, index="survey_id")
        else:
            return create_empty_df_from_schema(self._schema)
