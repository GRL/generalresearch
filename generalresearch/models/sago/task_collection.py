from typing import Any, Dict, List, Set

import pandas as pd
from pandera import Check, Column, DataFrameSchema, Index

from generalresearch.locales import Localelator
from generalresearch.models.sago import SagoStatus
from generalresearch.models.sago.survey import SagoSurvey
from generalresearch.models.thl.survey.task_collection import (
    TaskCollection,
    create_empty_df_from_schema,
)

COUNTRY_ISOS: Set[str] = Localelator().get_all_countries()
LANGUAGE_ISOS: Set[str] = Localelator().get_all_languages()

SagoTaskCollectionSchema = DataFrameSchema(
    columns={
        "status": Column(str, Check.isin(SagoStatus)),
        "cpi": Column(float, Check.between(min_value=0, max_value=100)),
        "buyer_id": Column(str),
        "country_iso": Column(str, Check.isin(COUNTRY_ISOS)),  # 2 letter, lowercase
        "language_iso": Column(str, Check.isin(LANGUAGE_ISOS)),  # 3 letter, lowercase
        "account_id": Column(str),
        "study_type_id": Column(str),
        "industry_id": Column(str),
        "allowed_devices": Column(str),
        "collects_pii": Column(bool),
        "bid_loi": Column("Int32", Check.between(0, 90 * 60), nullable=True),
        "bid_ir": Column(float, Check.between(0, 1), nullable=True),
        "remaining_count": Column(int),
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


class SagoTaskCollection(TaskCollection):
    items: List[SagoSurvey]
    _schema = SagoTaskCollectionSchema

    def to_row(self, s: SagoSurvey) -> Dict[str, Any]:
        d = s.model_dump(
            mode="json",
            exclude={
                "country_isos",
                "language_isos",
                "qualifications",
                "quotas",
                "source",
                "conditions",
                "is_live",
                "survey_exclusions",
                "ip_exclusions",
                "live_link",
                "modified_api",
            },
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
