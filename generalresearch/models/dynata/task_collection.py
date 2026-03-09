from typing import Any, Dict, List

import pandas as pd
from pandera import Check, Column, DataFrameSchema, Index

from generalresearch.locales import Localelator
from generalresearch.models import TaskCalculationType
from generalresearch.models.dynata import DynataStatus
from generalresearch.models.dynata.survey import DynataSurvey
from generalresearch.models.thl.survey.task_collection import (
    TaskCollection,
    create_empty_df_from_schema,
)

COUNTRY_ISOS = Localelator().get_all_countries()
LANGUAGE_ISOS = Localelator().get_all_languages()

DynataTaskCollectionSchema = DataFrameSchema(
    columns={
        "status": Column(str, Check.isin(DynataStatus)),
        "buyer_id": Column(str),
        "order_number": Column(str),
        "project_id": Column(str),
        "group_id": Column(str),
        "bid_loi": Column("Int32", Check.between(0, 90 * 60), nullable=True),
        "bid_ir": Column(float, Check.between(0, 1), nullable=True),
        "cpi": Column(float, Check.between(min_value=0, max_value=100)),
        "days_in_field": Column(int),
        "expected_count": Column(int),
        "calculation_type": Column(str, Check.isin(TaskCalculationType)),
        "category_ids": Column(str),
        "country_iso": Column(str, Check.isin(COUNTRY_ISOS)),  # 2 letter, lowercase
        "language_iso": Column(str, Check.isin(LANGUAGE_ISOS)),  # 3 letter, lowercase
        "allowed_devices": Column(str),
        "requirements": Column(str),  # json dumped str
        "created": Column(dtype=pd.DatetimeTZDtype(tz="UTC")),
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


class DynataTaskCollection(TaskCollection):
    items: List[DynataSurvey]
    _schema = DynataTaskCollectionSchema

    def to_row(self, s: DynataSurvey) -> Dict[str, Any]:
        d = s.model_dump(
            mode="json",
            exclude={
                "country_isos",
                "language_isos",
                "filters",
                "quotas",
                "source",
                "conditions",
                "is_live",
                "project_exclusions",
                "category_exclusions",
                "live_link",
                "client_id",
            },
        )
        d["cpi"] = float(s.cpi)
        d["requirements"] = s.requirements.model_dump_json()
        return d

    def to_df(self) -> pd.DataFrame:
        rows = []
        for s in self.items:
            rows.append(self.to_row(s))
        if rows:
            return pd.DataFrame.from_records(rows, index="survey_id")
        else:
            return create_empty_df_from_schema(self._schema)
