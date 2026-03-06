from typing import List, Set

import pandas as pd
from pandera import Column, DataFrameSchema, Check, Index

from generalresearch.locales import Localelator
from generalresearch.models.innovate import InnovateStatus
from generalresearch.models.innovate.survey import InnovateSurvey
from generalresearch.models.thl.survey.task_collection import (
    TaskCollection,
    create_empty_df_from_schema,
)

COUNTRY_ISOS: Set[str] = Localelator().get_all_countries()
LANGUAGE_ISOS: Set[str] = Localelator().get_all_languages()

InnovateTaskCollectionSchema = DataFrameSchema(
    columns={
        "survey_name": Column(str, Check.str_length(min_value=1, max_value=256)),
        "status": Column(str, Check.isin(InnovateStatus)),
        "cpi": Column(float, Check.between(min_value=0, max_value=100)),
        "buyer_id": Column(str),
        "country_iso": Column(str, Check.isin(COUNTRY_ISOS)),  # 2 letter, lowercase
        "language_iso": Column(str, Check.isin(LANGUAGE_ISOS)),  # 3 letter, lowercase
        "job_id": Column(str),
        "category": Column(str),
        "desired_count": Column(int),
        "remaining_count": Column(int),
        "supplier_completes_achieved": Column(int),
        "global_completes": Column(int),
        "global_starts": Column(int),
        "global_median_loi": Column("Int32", Check.between(0, 90 * 60), nullable=True),
        "global_conversion": Column(float, Check.between(0, 1), nullable=True),
        "bid_loi": Column("Int32", Check.between(0, 90 * 60), nullable=True),
        "bid_ir": Column(float, Check.between(0, 1), nullable=True),
        "allowed_devices": Column(str),
        "requires_pii": Column(bool),
        # exclude_pids is potentially large. We don't need these usually, we just want to know
        #   if include_pids is set, if so then this is a recontact
        # "exclude_pids": Column(bool),
        "include_pids": Column(str, nullable=True),
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


class InnovateTaskCollection(TaskCollection):
    items: List[InnovateSurvey]
    _schema = InnovateTaskCollectionSchema

    def to_row(self, s: InnovateSurvey):
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
                "excluded_surveys",
                "exclude_pids",
                "entry_link",
                "duplicate_check_level",
                "is_revenue_sharing",
                "group_type",
                "off_hour_traffic",
                "expected_end_date",
                "created",
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
