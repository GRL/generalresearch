from __future__ import annotations

import logging
from datetime import timezone, datetime
from typing import List, Collection, Optional

import pymysql
from pymysql import IntegrityError

from generalresearch.managers.criteria import CriteriaManager
from generalresearch.managers.survey import SurveyManager
from generalresearch.models.precision.survey import (
    PrecisionSurvey,
    PrecisionCondition,
)

logger = logging.getLogger()


class PrecisionCriteriaManager(CriteriaManager):
    CONDITION_MODEL = PrecisionCondition
    TABLE_NAME = "precision_criterion"


class PrecisionSurveyManager(SurveyManager):
    SURVEY_FIELDS = [
        # 'country_iso', 'language_iso',  # these come from join table
        "survey_id",
        "is_live",
        "status",
        "cpi",
        "group_id",
        "name",
        "survey_guid",
        "buyer_id",
        "category_id",
        "bid_loi",
        "bid_ir",
        "global_conversion",
        "desired_count",
        "achieved_count",
        "allowed_devices",
        "entry_link",
        "excluded_surveys",
        "quotas",
        "used_question_ids",
        "expected_end_date",
    ]

    def get_survey_library(
        self,
        country_iso: Optional[str] = None,
        language_iso: Optional[str] = None,
        survey_ids: Optional[Collection[str]] = None,
        is_live: Optional[bool] = None,
        updated_since: Optional[datetime] = None,
    ) -> List[PrecisionSurvey]:
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
            filters.append("s.survey_id IN %(survey_ids)s")
        if is_live is not None:
            params["is_live"] = is_live
            filters.append("is_live = %(is_live)s")
        if updated_since is not None:
            params["updated"] = updated_since
            filters.append("updated > %(updated)s")
        assert filters, "Must set at least 1 filter"
        filter_str = " AND ".join(filters)
        filter_str = "WHERE " + filter_str if filter_str else ""
        res = self.sql_helper.execute_sql_query(
            f"""
        SELECT *, 
            GROUP_CONCAT(DISTINCT country_iso SEPARATOR ',') as country_isos,
            GROUP_CONCAT(DISTINCT language_iso SEPARATOR ',') as language_isos
        FROM `thl-precision`.`precision_survey` s
        LEFT JOIN `thl-precision`.`precision_survey_country` sc on s.survey_id=sc.survey_id AND sc.is_active
        LEFT JOIN `thl-precision`.`precision_survey_language` sl on s.survey_id=sl.survey_id AND sl.is_active
        {filter_str}
        GROUP BY s.survey_id
        """,
            params,
        )
        for x in res:
            x["country_isos"] = x["country_isos"].split(",")
            x["language_isos"] = x["language_isos"].split(",")
        surveys = [PrecisionSurvey.from_db(x) for x in res]
        return surveys

    def create(self, survey: PrecisionSurvey) -> bool:
        now = datetime.now(tz=timezone.utc)
        d = survey.to_mysql()
        conn: pymysql.Connection = self.sql_helper.make_connection()
        conn.autocommit(False)
        c = conn.cursor()
        create_fields = self.SURVEY_FIELDS + ["created", "updated"]

        fields_str = ", ".join([f"`{x}`" for x in create_fields])
        values_str = ", ".join([f"%({x})s" for x in create_fields])
        survey_data = {k: v for k, v in d.items() if k in create_fields}
        survey_data.update({"created": now, "updated": now})
        c.execute(
            f"""
        INSERT INTO `thl-precision`.`precision_survey`
        ({fields_str}) VALUES ({values_str})
        """,
            survey_data,
        )

        country_data = [(survey.survey_id, c) for c in survey.country_isos]
        c.executemany(
            f"""
        INSERT INTO `thl-precision`.`precision_survey_country`
        (survey_id, country_iso, is_active) VALUES
        (%s, %s, TRUE)
        """,
            country_data,
        )
        lang_data = [(survey.survey_id, c) for c in survey.language_isos]
        c.executemany(
            f"""
        INSERT INTO `thl-precision`.`precision_survey_language`
        (survey_id, language_iso, is_active) VALUES
        (%s, %s, TRUE)
        """,
            lang_data,
        )
        conn.commit()

        return True

    def update(self, surveys: List[PrecisionSurvey]) -> bool:
        for survey in surveys:
            self.update_one(survey)
        return True

    def update_one(self, survey: PrecisionSurvey) -> bool:
        now = datetime.now(tz=timezone.utc)
        d = survey.to_mysql()
        d["updated"] = now

        conn: pymysql.Connection = self.sql_helper.make_connection()
        conn.autocommit(False)
        c = conn.cursor()

        # Update survey table
        set_str = ", ".join(
            [
                f"`{k}` = %({k})s"
                for k, v in d.items()
                if k not in {"survey_id", "created"}
            ]
        )
        c.execute(
            f"""
        UPDATE `thl-precision`.precision_survey
        SET {set_str}
        WHERE `survey_id`=%(survey_id)s
        LIMIT 1""",
            d,
        )

        # Turn off countries not in the current list, for this survey
        c.execute(
            """
        UPDATE `thl-precision`.`precision_survey_country`
        SET is_active = FALSE
        WHERE survey_id = %(survey_id)s AND country_iso NOT IN %(country_isos)s;
        """,
            {"survey_id": survey.survey_id, "country_isos": survey.country_isos},
        )
        country_data = [(survey.survey_id, c) for c in survey.country_isos]
        # Turn ON countries in this survey's list of countries, insert row, if already exists, set active.
        c.executemany(
            f"""
        INSERT INTO `thl-precision`.`precision_survey_country`
        (survey_id, country_iso, is_active) VALUES
        (%s, %s, TRUE) ON DUPLICATE KEY UPDATE is_active = TRUE;
        """,
            country_data,
        )

        # Same thing with languages
        c.execute(
            """
        UPDATE `thl-precision`.`precision_survey_language`
        SET is_active = FALSE
        WHERE survey_id = %(survey_id)s AND language_iso NOT IN %(language_isos)s;
        """,
            {"survey_id": survey.survey_id, "language_isos": survey.language_isos},
        )
        language_data = [(survey.survey_id, c) for c in survey.language_isos]
        c.executemany(
            f"""
        INSERT INTO `thl-precision`.`precision_survey_language`
        (survey_id, language_iso, is_active) VALUES
        (%s, %s, TRUE) ON DUPLICATE KEY UPDATE is_active = TRUE;
        """,
            language_data,
        )
        conn.commit()

        return True

    def create_or_update(self, surveys: List[PrecisionSurvey]):
        surveys = {s.survey_id: s for s in surveys}
        sns = set(surveys.keys())
        existing_sns = {
            x["survey_id"]
            for x in self.sql_helper.execute_sql_query(
                """
        SELECT survey_id
        FROM `thl-precision`.`precision_survey`
        WHERE survey_id IN %s""",
                [sns],
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
