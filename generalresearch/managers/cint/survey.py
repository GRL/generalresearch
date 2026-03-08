from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Collection, List, Optional, Set

import pymysql
from pymysql import IntegrityError

from generalresearch.managers.criteria import CriteriaManager
from generalresearch.managers.survey import SurveyManager
from generalresearch.models.cint.survey import CintCondition, CintSurvey

logger = logging.getLogger()


class CintCriteriaManager(CriteriaManager):
    CONDITION_MODEL = CintCondition
    TABLE_NAME = "cint_criterion"


class CintSurveyManager(SurveyManager):
    SURVEY_FIELDS = (set(CintSurvey.model_fields.keys()) | {"is_live"}) - {
        "country_isos",
        "language_isos",
        "source",
        "conditions",
        "gross_cpi",
        "is_live_raw",
    }

    def get_survey_library(
        self,
        country_iso: Optional[str] = None,
        language_iso: Optional[str] = None,
        survey_ids: Optional[Collection[str]] = None,
        is_live: Optional[bool] = None,
        updated_since: Optional[datetime] = None,
        exclude_fields: Optional[Set[str]] = None,
    ) -> List[CintSurvey]:
        """
        Accepts lots of optional filters.

        :param country_iso: filters on country_iso field
        :param language_iso: filters on language_iso field
        :param is_live: filters on is_live field
        :param updated_since: filters on "> last_updated"
        :param exclude_fields: Optionally exclude fields from query. This only supports
            nullable fields, as the CintSurvey model validation will fail otherwise.
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
            params["is_live"] = is_live
            filters.append("is_live = %(is_live)s")

        if updated_since is not None:
            params["updated"] = updated_since
            filters.append("last_updated > %(updated)s")
        assert filters, "Must set at least 1 filter"
        filter_str = " AND ".join(filters)
        filter_str = "WHERE " + filter_str if filter_str else ""
        fields = set(self.SURVEY_FIELDS)
        if exclude_fields:
            fields -= exclude_fields
        fields_str = ", ".join([f"`{v}`" for v in fields])
        res = self.sql_helper.execute_sql_query(
            query=f"""
            SELECT {fields_str}
            FROM `thl-cint`.`cint_survey` survey
            {filter_str}
            """,
            params=params,
        )
        surveys = [CintSurvey.from_mysql(x) for x in res]
        return surveys

    def create(self, survey: CintSurvey) -> bool:
        d = survey.to_mysql()
        conn: pymysql.Connection = self.sql_helper.make_connection()
        conn.autocommit(True)
        c = conn.cursor()
        create_fields = list(self.SURVEY_FIELDS)

        fields_str = ", ".join([f"`{x}`" for x in create_fields])
        values_str = ", ".join([f"%({x})s" for x in create_fields])
        survey_data = {k: v for k, v in d.items() if k in create_fields}
        c.execute(
            query=f"""
                INSERT INTO `thl-cint`.`cint_survey`
                ({fields_str}) VALUES ({values_str})
            """,
            args=survey_data,
        )
        return True

    def update(self, surveys: List[CintSurvey]) -> bool:
        now = datetime.now(tz=timezone.utc)
        for survey in surveys:
            survey.last_updated = now

        survey_fields = list(self.SURVEY_FIELDS)
        data = [survey.to_mysql() for survey in surveys]
        survey_data = [[d[k] for k in survey_fields] for d in data]
        self.sql_helper.bulk_update("cint_survey", survey_fields, survey_data)
        return True

    def create_or_update(self, surveys: List[CintSurvey]):
        surveys = {s.survey_id: s for s in surveys}
        sns = set(surveys.keys())
        existing_sns = {
            x["survey_id"]
            for x in self.sql_helper.execute_sql_query(
                query="""
                    SELECT survey_id
                    FROM `thl-cint`.`cint_survey`
                    WHERE survey_id IN %s;
                """,
                params=[sns],
            )
        }
        create_sns = sns - existing_sns
        for sn in create_sns:
            survey = surveys[sn]
            try:
                self.create(survey)
            except IntegrityError as e:
                logger.info(e)
                if e.args[0] == 1062:
                    existing_sns.add(sn)
                else:
                    raise e
        self.update([surveys[sn] for sn in existing_sns])
