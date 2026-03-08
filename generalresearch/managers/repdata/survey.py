from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Collection, List, Optional

import pymysql

from generalresearch.managers.criteria import CriteriaManager
from generalresearch.managers.survey import SurveyManager
from generalresearch.models.repdata.survey import (
    RepDataCondition,
    RepDataStreamHashed,
    RepDataSurvey,
    RepDataSurveyHashed,
)


class RepDataCriteriaManager(CriteriaManager):
    CONDITION_MODEL = RepDataCondition
    TABLE_NAME = "repdata_criterion"


class RepDataSurveyManager(SurveyManager):
    SURVEY_FIELDS = [
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
        "allowed_devices",
    ]
    STREAM_FIELDS = [
        "stream_id",
        "stream_uuid",
        "stream_name",
        "stream_status",
        "calculation_type",
        "qualification_hashes",
        "hashed_quotas",
        "expected_count",
        "cpi",
        "days_in_field",
        "actual_ir",
        "actual_loi",
        "actual_conversion",
        "actual_complete_count",
        "actual_count",
        "used_question_ids",
        "survey_id",
        "remaining_count",
    ]

    def get_survey_library(
        self,
        country_iso: Optional[str] = None,
        language_iso: Optional[str] = None,
        survey_ids: Optional[Collection[str]] = None,
        is_live: Optional[bool] = None,
        updated_since: Optional[datetime] = None,
    ) -> List[RepDataSurveyHashed]:
        """
        Accepts lots of optional filters.
        :param country_iso: filters on country_iso field
        :param language_iso: filters on language_iso field
        :param is_live: filters on is_live field
        :param updated_since: filters on "> last_updated"
        """
        filters = []
        params = {}
        if country_iso:
            params["country_iso"] = country_iso
            filters.append("`country_iso` = %(country_iso)s")
        if language_iso:
            params["language_iso"] = language_iso
            filters.append("`language_iso` = %(language_iso)s")
        if survey_ids is not None:
            params["survey_ids"] = survey_ids
            filters.append("survey_id IN %(survey_ids)s")
        if is_live is not None:
            if is_live:
                filters.append("survey_status = 'LIVE'")
            else:
                filters.append("survey_status != 'LIVE'")
        if updated_since is not None:
            params["updated_since"] = updated_since
            filters.append("last_updated > %(updated_since)s")
        assert filters, "Must set at least 1 filter"
        filter_str = " AND ".join(filters)
        filter_str = "WHERE " + filter_str if filter_str else ""
        res = self.sql_helper.execute_sql_query(
            query=f"""
                SELECT *
                FROM `thl-repdata`.`repdata_survey` survey
                {filter_str}
            """,
            params=params,
        )
        surveys = [RepDataSurveyHashed.from_db(x) for x in res]
        surveys = {s.survey_id: s for s in surveys}
        if surveys:
            res = self.sql_helper.execute_sql_query(
                query=f"""
                    SELECT *
                    FROM `thl-repdata`.`repdata_surveystream`
                    WHERE survey_id IN %s
                """,
                params=[list(surveys.keys())],
            )
            for x in res:
                x["qualification_hashes"] = json.loads(x["qualification_hashes"])
                x["hashed_quotas"] = json.loads(x["hashed_quotas"])
                x["used_question_ids"] = json.loads(x["used_question_ids"])
            for x in res:
                survey = surveys[x["survey_id"]]
                survey.hashed_streams.append(RepDataStreamHashed.from_db(x, survey))
        return list(surveys.values())

    def create(self, survey: RepDataSurvey | RepDataSurveyHashed) -> bool:
        now = datetime.now(tz=timezone.utc)
        d = survey.to_mysql()
        conn: pymysql.Connection = self.sql_helper.make_connection()
        conn.autocommit(True)
        c = conn.cursor()
        create_fields = self.SURVEY_FIELDS + ["created", "last_updated"]

        fields_str = ", ".join([f"`{x}`" for x in create_fields])
        values_str = ", ".join([f"%({x})s" for x in create_fields])
        survey_data = {k: v for k, v in d.items() if k in create_fields}
        survey_data.update({"created": now, "last_updated": now})
        c.execute(
            query=f"""
            INSERT INTO `thl-repdata`.`repdata_survey`
            ({fields_str}) VALUES ({values_str})
            """,
            args=survey_data,
        )

        fields_str = ", ".join([f"`{x}`" for x in self.STREAM_FIELDS])
        values_str = ", ".join([f"%({x})s" for x in self.STREAM_FIELDS])
        stream_data = [
            {k: v for k, v in stream.items() if k in self.STREAM_FIELDS}
            for stream in d["streams"]
        ]
        for sd in stream_data:
            sd.update({"survey_id": survey.survey_id})
        c.executemany(
            query=f"""
            INSERT INTO `thl-repdata`.`repdata_surveystream`
            ({fields_str}) 
            VALUES ({values_str})
            """,
            args=stream_data,
        )
        return True

    def update(self, surveys: List[RepDataSurveyHashed]) -> bool:
        now = datetime.now(tz=timezone.utc)
        update_fields = self.SURVEY_FIELDS + ["last_updated"]

        data = [survey.to_mysql() for survey in surveys]
        survey_data = [[d[k] for k in self.SURVEY_FIELDS] + [now] for d in data]
        self.sql_helper.bulk_update(
            table_name="repdata_survey",
            field_names=update_fields,
            values_to_insert=survey_data,
        )

        stream_data = []
        for d in data:
            for stream in d["streams"]:
                stream["survey_id"] = d["survey_id"]
                stream_data.append([stream[k] for k in self.STREAM_FIELDS])

        self.sql_helper.bulk_update(
            table_name="repdata_surveystream",
            field_names=self.STREAM_FIELDS,
            values_to_insert=stream_data,
        )
        return True
