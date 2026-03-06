from __future__ import annotations

import logging
from datetime import timezone, datetime
from typing import List, Collection, Optional, Set

import pymysql
from pymysql import IntegrityError

from generalresearch.managers.criteria import CriteriaManager
from generalresearch.managers.survey import SurveyManager
from generalresearch.models.sago.survey import SagoSurvey, SagoCondition

logger = logging.getLogger()


class SagoCriteriaManager(CriteriaManager):
    CONDITION_MODEL = SagoCondition
    TABLE_NAME = "sago_criterion"


class SagoSurveyManager(SurveyManager):
    SURVEY_FIELDS = [
        "survey_id",
        "is_live",
        "status",
        "country_iso",
        "language_iso",
        "cpi",
        "buyer_id",
        "account_id",
        "study_type_id",
        "industry_id",
        "allowed_devices",
        "collects_pii",
        "bid_loi",
        "bid_ir",
        "live_link",
        "survey_exclusions",
        "ip_exclusions",
        "remaining_count",
        "qualifications",
        "quotas",
        "used_question_ids",
        "modified_api",
    ]

    def get_survey_library(
        self,
        country_iso: Optional[str] = None,
        language_iso: Optional[str] = None,
        survey_ids: Optional[Collection[str]] = None,
        is_live: Optional[bool] = None,
        updated_since: Optional[datetime] = None,
        exclude_fields: Optional[Set[str]] = None,
    ) -> List[SagoSurvey]:
        """
        Accepts lots of optional filters.

        :param country_iso: filters on country_iso field
        :param language_iso: filters on language_iso field
        :param is_live: filters on is_live field
        :param updated_since: filters on "> last_updated"
        :param exclude_fields: Optionally exclude fields from query. This
            only supports nullable fields, as the SagoSurvey model validation
            will fail otherwise.
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
            filters.append("updated > %(updated)s")
        assert filters, "Must set at least 1 filter"
        filter_str = " AND ".join(filters)
        filter_str = "WHERE " + filter_str if filter_str else ""
        fields = set(self.SURVEY_FIELDS) | {"created", "updated"}
        if exclude_fields:
            fields -= exclude_fields
        fields_str = ", ".join([f"`{v}`" for v in fields])
        res = self.sql_helper.execute_sql_query(
            query=f"""
                SELECT {fields_str}
                FROM `thl-sago`.`sago_survey` survey
                {filter_str}
            """,
            params=params,
        )
        surveys = [SagoSurvey.from_db(x) for x in res]
        return surveys

    def create(self, survey: SagoSurvey) -> bool:
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
            query=f"""
                INSERT INTO `thl-sago`.`sago_survey`
                ({fields_str}) VALUES ({values_str})
            """,
            args=survey_data,
        )
        return True

    def update(self, surveys: List[SagoSurvey]) -> bool:
        now = datetime.now(tz=timezone.utc)
        update_fields = self.SURVEY_FIELDS + ["updated"]

        data = [survey.to_mysql() for survey in surveys]
        survey_data = [[d[k] for k in self.SURVEY_FIELDS] + [now] for d in data]
        self.sql_helper.bulk_update("sago_survey", update_fields, survey_data)
        return True

    def update_field(self, survey: SagoSurvey, field: str) -> bool:
        now = datetime.now(tz=timezone.utc)
        conn: pymysql.Connection = self.sql_helper.make_connection()
        value = survey.to_mysql()[field]
        c = conn.cursor()
        c.execute(
            f"""
        UPDATE `thl-sago`.`sago_survey`
        SET `{field}` = %(value)s,
        updated = %(now)s
        WHERE survey_id = %(survey_id)s
        LIMIT 2
        """,
            {"now": now, "value": value, "survey_id": survey.survey_id},
        )
        conn.commit()
        if c.rowcount == 0:
            raise ValueError(
                f"SagoSurveyManager.update_field: "
                f"survey {survey.survey_id} not found in db!"
            )
        elif c.rowcount == 2:
            raise ValueError("this should never happen")
        return True

    def create_or_update(self, surveys: List[SagoSurvey]):
        surveys = {s.survey_id: s for s in surveys}
        sns = set(surveys.keys())
        existing_sns = {
            x["survey_id"]
            for x in self.sql_helper.execute_sql_query(
                query="""
                    SELECT survey_id
                    FROM `thl-sago`.`sago_survey`
                    WHERE survey_id IN %s
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
