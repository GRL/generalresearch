from __future__ import annotations

import logging
from datetime import timezone, datetime
from typing import List, Collection, Optional, Set

import pymysql
from pymysql import IntegrityError

from generalresearch.managers.criteria import CriteriaManager
from generalresearch.managers.survey import SurveyManager
from generalresearch.models.innovate.survey import (
    InnovateSurvey,
    InnovateCondition,
)

logger = logging.getLogger()


class InnovateCriteriaManager(CriteriaManager):
    CONDITION_MODEL = InnovateCondition
    TABLE_NAME = "innovate_criterion"


class InnovateSurveyManager(SurveyManager):
    SURVEY_FIELDS = [
        "survey_id",
        "status",
        "country_iso",
        "language_iso",
        "cpi",
        "buyer_id",
        "job_id",
        "survey_name",
        "desired_count",
        "remaining_count",
        "supplier_completes_achieved",
        "global_completes",
        "global_starts",
        "global_median_loi",
        "global_conversion",
        "bid_loi",
        "bid_ir",
        "allowed_devices",
        "entry_link",
        "category",
        "requires_pii",
        "excluded_surveys",
        "duplicate_check_level",
        "exclude_pids",
        "include_pids",
        "is_revenue_sharing",
        "group_type",
        "off_hour_traffic",
        "qualifications",
        "quotas",
        "used_question_ids",
        "is_live",
        "modified_api",
        "created_api",
        "expected_end_date",
    ]

    def get_survey_library(
        self,
        country_iso: Optional[str] = None,
        language_iso: Optional[str] = None,
        survey_ids: Optional[Collection[str]] = None,
        is_live: Optional[bool] = None,
        updated_since: Optional[datetime] = None,
        exclude_fields: Optional[Set[str]] = None,
    ) -> List[InnovateSurvey]:
        """
        Accepts lots of optional filters.
        :param country_iso: filters on country_iso field
        :param language_iso: filters on language_iso field
        :param is_live: filters on is_live field
        :param updated_since: filters on "> updated"
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
                filters.append("is_live")
            else:
                filters.append("NOT is_live")
        if updated_since is not None:
            params["updated_since"] = updated_since
            filters.append("updated > %(updated_since)s")
        assert filters, "Must set at least 1 filter"
        filter_str = " AND ".join(filters)
        filter_str = "WHERE " + filter_str if filter_str else ""
        fields = set(self.SURVEY_FIELDS) | {"created", "updated"}
        if exclude_fields:
            fields -= exclude_fields
        fields_str = ", ".join([f"`{v}`" for v in fields])
        res = self.sql_helper.execute_sql_query(
            f"""
        SELECT {fields_str}
        FROM `{self.sql_helper.db}`.`innovate_survey` survey
        {filter_str}
        """,
            params,
        )
        surveys = [InnovateSurvey.from_db(x) for x in res]
        return surveys

    def create(self, survey: InnovateSurvey) -> bool:
        now = datetime.now(tz=timezone.utc)
        d = survey.to_mysql()
        conn: pymysql.Connection = self.sql_helper.make_connection()
        conn.autocommit(True)
        c = conn.cursor()
        create_fields = self.SURVEY_FIELDS + ["created", "updated"]

        fields_str = ", ".join([f"`{x}`" for x in create_fields])
        values_str = ", ".join([f"%({x})s" for x in create_fields])
        survey_data = {k: v for k, v in d.items() if k in create_fields}
        survey_data.update({"created": now, "updated": now})
        c.execute(
            f"""
        INSERT INTO `{self.sql_helper.db}`.`innovate_survey`
        ({fields_str}) VALUES ({values_str})
        """,
            survey_data,
        )
        return True

    def update(self, surveys: List[InnovateSurvey]) -> bool:
        now = datetime.now(tz=timezone.utc)
        update_fields = self.SURVEY_FIELDS + ["updated"]

        data = [survey.to_mysql() for survey in surveys]
        survey_data = [[d[k] for k in self.SURVEY_FIELDS] + [now] for d in data]
        self.sql_helper.bulk_update(
            table_name="innovate_survey",
            field_names=update_fields,
            values_to_insert=survey_data,
        )

        return True

    def create_or_update(self, surveys: List[InnovateSurvey]) -> None:
        surveys = {s.survey_id: s for s in surveys}
        sns = set(surveys.keys())
        existing_sns = {
            x["survey_id"]
            for x in self.sql_helper.execute_sql_query(
                query=f"""
                    SELECT survey_id
                    FROM `{self.sql_helper.db}`.`innovate_survey`
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

        return None
