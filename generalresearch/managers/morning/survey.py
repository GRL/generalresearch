from __future__ import annotations

import json
import logging
from datetime import timezone, datetime
from typing import List, Collection, Optional

import pymysql
from pymysql import IntegrityError

from generalresearch.managers.criteria import CriteriaManager
from generalresearch.managers.survey import SurveyManager
from generalresearch.models.morning.survey import MorningBid, MorningCondition

logger = logging.getLogger()


class MorningCriteriaManager(CriteriaManager):
    CONDITION_MODEL = MorningCondition
    TABLE_NAME = "morning_criterion"


class MorningSurveyManager(SurveyManager):
    STAT_FIELDS = [
        "obs_median_loi",
        "qualified_conversion",
        "num_available",
        "num_completes",
        "num_failures",
        "num_in_progress",
        "num_over_quotas",
        "num_qualified",
        "num_quality_terminations",
        "num_timeouts",
    ]
    STAT_EXTENDED_FIELDS = ["system_conversion", "num_entrants", "num_screenouts"]
    BID_FIELDS = (
        [
            "id",
            "status",
            "country_iso",
            "language_isos",
            "buyer_account_id",
            "buyer_id",
            "name",
            "supplier_exclusive",
            "survey_type",
            "timeout",
            "topic_id",
            "bid_loi",
            "exclusions",
            "used_question_ids",
            "expected_end",
            "created_api",
            "is_live",
        ]
        + STAT_FIELDS
        + STAT_EXTENDED_FIELDS
    )
    QUOTA_FIELDS = [
        "id",
        "cpi",
        "condition_hashes",
    ] + STAT_FIELDS
    BID_DB_SOURCE = "`thl-morning`.`morning_surveybid`"
    QUOTA_DB_SOURCE = "`thl-morning`.`morning_surveyquota`"

    def get_survey_library(
        self,
        country_iso: Optional[str] = None,
        language_iso: Optional[str] = None,
        survey_ids: Optional[Collection[str]] = None,
        is_live: Optional[bool] = None,
        updated_since: Optional[datetime] = None,
    ) -> List[MorningBid]:
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
            filters.append("bid.id IN %(survey_ids)s")
        if is_live is not None:
            if is_live:
                filters.append("is_live")
            else:
                filters.append("NOT is_live")
        if updated_since is not None:
            params["updated_since"] = updated_since
            filters.append("updated > %(updated_since)s")
        assert filters, "Must set at least 1 filter"
        fields_str = """
        bid.*,
        JSON_ARRAYAGG(
            JSON_OBJECT(
                'id', quota.id,
                'cpi', quota.cpi,
                'condition_hashes', quota.condition_hashes,
                'num_available', quota.num_available,
                'num_completes', quota.num_completes,
                'num_failures', quota.num_failures,
                'num_in_progress', quota.num_in_progress,
                'num_over_quotas', quota.num_over_quotas,
                'num_qualified', quota.num_qualified,
                'num_quality_terminations', quota.num_quality_terminations,
                'num_timeouts', quota.num_timeouts,
                'obs_median_loi', quota.obs_median_loi,
                'qualified_conversion', quota.qualified_conversion
            )
        ) AS quotas
        """
        filter_str = " AND ".join(filters)
        filter_str = "WHERE " + filter_str if filter_str else ""
        res = self.sql_helper.execute_sql_query(
            f"""
        SELECT {fields_str}
        FROM {self.BID_DB_SOURCE} AS bid
        JOIN {self.QUOTA_DB_SOURCE} AS quota ON bid.id = quota.bid_id
        {filter_str}
        GROUP BY bid.id;
        """,
            params,
        )
        for bid in res:
            bid["quotas"] = json.loads(bid["quotas"])
        bids = [MorningBid.from_db(x) for x in res]
        return bids

    def create(self, bid: MorningBid) -> bool:
        now = datetime.now(tz=timezone.utc)
        d = bid.to_mysql()
        create_fields = self.BID_FIELDS + ["created", "updated"]

        fields_str = ", ".join([f"`{x}`" for x in create_fields])
        values_str = ", ".join([f"%({x})s" for x in create_fields])
        bid_data = {k: v for k, v in d.items() if k in create_fields}
        bid_data.update({"updated": now, "created": now})
        conn: pymysql.Connection = self.sql_helper.make_connection()
        conn.autocommit(True)
        c = conn.cursor()
        c.execute(
            query=f"""
            INSERT INTO {self.BID_DB_SOURCE}
                ({fields_str}) 
            VALUES ({values_str})
            """,
            args=bid_data,
        )

        quotas = d["quotas"]
        create_fields = self.QUOTA_FIELDS + ["bid_id"]
        fields_str = ", ".join([f"`{x}`" for x in create_fields])
        values_str = ", ".join([f"%({x})s" for x in create_fields])
        quota_data = [
            {k: v for k, v in quota.items() if k in create_fields} | {"bid_id": bid.id}
            for quota in quotas
        ]
        c = conn.cursor()
        c.executemany(
            query=f"""
                INSERT INTO {self.QUOTA_DB_SOURCE}
                    ({fields_str}) 
                VALUES ({values_str})
            """,
            args=quota_data,
        )

        return True

    def update(self, surveys: List[MorningBid]) -> None:
        now = datetime.now(tz=timezone.utc)

        for survey in surveys:
            self.update_one(survey, now=now)

    def update_one(self, bid: MorningBid, now=None) -> bool:
        if now is None:
            now = datetime.now(tz=timezone.utc)
        d = bid.to_mysql()
        d["updated"] = now

        bid_data = {k: v for k, v in d.items() if k in self.BID_FIELDS + ["updated"]}
        set_str = ", ".join(
            [
                f"`{k}` = %({k})s"
                for k, v in d.items()
                if k in self.BID_FIELDS + ["updated"] and k != "id"
            ]
        )

        conn: pymysql.Connection = self.sql_helper.make_connection()
        conn.autocommit(False)
        c = conn.cursor()
        c.execute(
            f"""
        UPDATE {self.BID_DB_SOURCE}
        SET {set_str}
        WHERE `id`=%(id)s
        LIMIT 1""",
            bid_data,
        )

        quota_data = [
            {k: v for k, v in quota.items() if k in self.QUOTA_FIELDS}
            for quota in d["quotas"]
        ]
        set_str = ", ".join(
            [
                f"`{k}` = %({k})s"
                for k, v in d["quotas"][0].items()
                if k in self.QUOTA_FIELDS and k != "id"
            ]
        )
        for quota in quota_data:
            c.execute(
                f"""
            UPDATE {self.QUOTA_DB_SOURCE}
            SET {set_str}
            WHERE `id`=%(id)s
            LIMIT 1""",
                quota,
            )

        conn.commit()
        return bool(c.rowcount >= 1)

    def create_or_update(self, surveys: List[MorningBid]):
        surveys = {s.id: s for s in surveys}
        sns = set(surveys.keys())
        existing_sns = {
            x["id"]
            for x in self.sql_helper.execute_sql_query(
                """
        SELECT id
        FROM `thl-morning`.`morning_surveybid`
        WHERE id IN %s""",
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
