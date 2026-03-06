from __future__ import annotations

import logging
from datetime import timezone, datetime
from typing import List, Collection, Optional

import pymysql
from pymysql import IntegrityError

from generalresearch.managers.criteria import CriteriaManager
from generalresearch.managers.survey import SurveyManager
from generalresearch.models.spectrum.survey import (
    SpectrumSurvey,
    SpectrumCondition,
)

logger = logging.getLogger()


class SpectrumCriteriaManager(CriteriaManager):
    CONDITION_MODEL = SpectrumCondition
    TABLE_NAME = "spectrum_criterion"


class SpectrumSurveyManager(SurveyManager):
    SURVEY_FIELDS = [
        "survey_id",
        "survey_name",
        "status",
        "country_iso",
        "language_iso",
        "cpi",
        "field_end_date",
        "category_code",
        "calculation_type",
        "requires_pii",
        "buyer_id",
        "survey_exclusions",
        "exclusion_period",
        "bid_loi",
        "bid_ir",
        "last_block_loi",
        "last_block_ir",
        "overall_ir",
        "overall_loi",
        "project_last_complete_date",
        "include_psids",
        "exclude_psids",
        "qualifications",
        "quotas",
        "used_question_ids",
        "is_live",
        "modified_api",
        "created_api",
    ]

    def get_survey_library(
        self,
        country_iso: Optional[str] = None,
        language_iso: Optional[str] = None,
        survey_ids: Optional[Collection[str]] = None,
        is_live: Optional[bool] = None,
        updated_since: Optional[datetime] = None,
        fields=None,
    ) -> List[SpectrumSurvey]:
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
        fields_str = "*"
        if fields:
            fields_str = ",".join(fields)
        filter_str = " AND ".join(filters)
        filter_str = "WHERE " + filter_str if filter_str else ""

        res = self.sql_helper.execute_sql_query(
            query=f"""
                SELECT {fields_str}
                FROM `{self.sql_helper.db_name}`.`spectrum_survey` survey
                {filter_str}
            """,
            params=params,
        )

        surveys = [SpectrumSurvey.from_db(x) for x in res]
        return surveys

    def create(self, survey: SpectrumSurvey) -> bool:
        now = datetime.now(tz=timezone.utc)
        d = survey.to_mysql()
        conn: pymysql.Connection = self.sql_helper.make_connection()
        conn.autocommit(True)
        c = conn.cursor()
        create_fields = self.SURVEY_FIELDS + ["updated"]

        fields_str = ", ".join([f"`{x}`" for x in create_fields])
        values_str = ", ".join([f"%({x})s" for x in create_fields])
        survey_data = {k: v for k, v in d.items() if k in create_fields}
        survey_data.update({"updated": now})

        c.execute(
            query=f"""
                INSERT INTO `{self.sql_helper.db_name}`.`spectrum_survey`
                    ({fields_str}) 
                VALUES ({values_str})
            """,
            args=survey_data,
        )

        return True

    def update(self, surveys: List[SpectrumSurvey]) -> bool:
        now = datetime.now(tz=timezone.utc)

        # Due to stupidity with bid/actual loi/ir values (last block nonsense),
        # we can't do a bulk update b/c the fields may be different in
        # different rows. Just do one at a time, there shouldn't be that many.
        for survey in surveys:
            self.update_one(survey, now=now)

        return True

    def update_one(self, survey: SpectrumSurvey, now=None) -> bool:
        if now is None:
            now = datetime.now(tz=timezone.utc)

        d = survey.to_mysql()
        # We have to have special logic for bid/actual loi/ir here. The api
        #   is stupid and only returns one set of them. If we just do the db
        #   update it'll overwrite the other with NULL. So, exclude them if
        #   they are null.

        for k in [
            "bid_loi",
            "bid_ir",
            "overall_loi",
            "overall_ir",
            "last_block_loi",
            "last_block_ir",
        ]:
            if d[k] is None:
                d.pop(k)
        d["updated"] = now
        set_str = ", ".join(
            [
                f"`{k}` = %({k})s"
                for k, v in d.items()
                if k not in {"survey_id", "created"}
            ]
        )

        conn: pymysql.Connection = self.sql_helper.make_connection()
        conn.autocommit(True)
        c = conn.cursor()
        c.execute(
            query=f"""
                UPDATE `{self.sql_helper.db_name}`.spectrum_survey
                SET {set_str}
                WHERE `survey_id`=%(survey_id)s
                LIMIT 1
            """,
            args=d,
        )

        return c.rowcount == 1

    def create_or_update(self, surveys: List[SpectrumSurvey]) -> None:
        surveys = {s.survey_id: s for s in surveys}
        sns = set(surveys.keys())
        existing_sns = {
            x["survey_id"]
            for x in self.sql_helper.execute_sql_query(
                query=f"""
                    SELECT ss.survey_id
                    FROM `{self.sql_helper.db_name}`.`spectrum_survey` AS ss
                    WHERE ss.survey_id IN %s;
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
