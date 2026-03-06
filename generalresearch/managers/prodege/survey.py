from __future__ import annotations

from datetime import timezone, datetime
from typing import List, Collection, Optional

import pymysql

from generalresearch.managers.criteria import CriteriaManager
from generalresearch.managers.survey import SurveyManager
from generalresearch.models.prodege.survey import ProdegeSurvey, ProdegeCondition


class ProdegeCriteriaManager(CriteriaManager):
    CONDITION_MODEL = ProdegeCondition
    TABLE_NAME = "prodege_criterion"


class ProdegeSurveyManager(SurveyManager):
    SURVEY_FIELDS = [
        "survey_id",
        "survey_name",
        "status",
        "country_iso",
        "language_iso",
        "cpi",
        "desired_count",
        "remaining_count",
        "achieved_completes",
        "bid_loi",
        "bid_ir",
        "actual_loi",
        "actual_ir",
        "conversion_rate",
        "entrance_url",
        "max_clicks_settings",
        "past_participation",
        "include_psids",
        "exclude_psids",
        "quotas",
        "used_question_ids",
        "is_live",
    ]

    def get_survey_library(
        self,
        country_iso: Optional[str] = None,
        language_iso: Optional[str] = None,
        survey_ids: Optional[Collection[str]] = None,
        is_live: Optional[bool] = None,
        updated_since: Optional[datetime] = None,
    ) -> List[ProdegeSurvey]:
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
                filters.append("status = 'LIVE'")
            else:
                filters.append("status != 'LIVE'")
        if updated_since is not None:
            params["updated_since"] = updated_since
            filters.append("updated > %(updated_since)s")
        assert filters, "Must set at least 1 filter"
        filter_str = " AND ".join(filters)
        filter_str = "WHERE " + filter_str if filter_str else ""
        res = self.sql_helper.execute_sql_query(
            f"""
        SELECT *
        FROM `thl-prodege`.`prodege_survey` survey
        {filter_str}
        """,
            params,
        )
        surveys = [ProdegeSurvey.from_db(x) for x in res]
        return surveys

    def create(self, survey: ProdegeSurvey) -> bool:
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
        INSERT INTO `thl-prodege`.`prodege_survey`
        ({fields_str}) VALUES ({values_str})
        """,
            survey_data,
        )
        return True

    def update(self, surveys: List[ProdegeSurvey]) -> None:
        now = datetime.now(tz=timezone.utc)

        # Do to stupidity with bid/actual loi/ir values (see ProdegeSurvey.to_mysql), we now
        #   can't do a bulk update b/c the fields may be different in different rows. Just do
        #   one at a time, there shouldn't be that many.
        for survey in surveys:
            self.update_one(survey, now=now)

    def update_one(self, survey: ProdegeSurvey, now=None) -> bool:
        if now is None:
            now = datetime.now(tz=timezone.utc)
        d = survey.to_mysql()
        # We have to have special logic for bid/actual loi/ir here. The api is
        #   stupid and only returns one set of them. If we just do the db
        #   update it'll overwrite the other with NULL. So, exclude them if
        #   they are null.

        for k in ["bid_loi", "bid_ir", "actual_loi", "actual_ir"]:
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
            f"""
        UPDATE `thl-prodege`.prodege_survey
        SET {set_str}
        WHERE `survey_id`=%(survey_id)s
        LIMIT 1""",
            d,
        )
        return c.rowcount == 1
