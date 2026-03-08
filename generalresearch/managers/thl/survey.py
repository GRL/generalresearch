from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Collection, Dict, List, Optional, Tuple

import pandas as pd
from more_itertools import chunked
from psycopg import sql
from pydantic import NonNegativeInt

from generalresearch.managers.base import Permission, PostgresManager
from generalresearch.managers.thl.buyer import BuyerManager
from generalresearch.managers.thl.category import CategoryManager
from generalresearch.models import Source
from generalresearch.models.custom_types import SurveyKey
from generalresearch.models.thl.survey.model import (
    Survey,
    SurveyStat,
)
from generalresearch.pg_helper import PostgresConfig


class SurveyManager(PostgresManager):

    def __init__(
        self,
        pg_config: PostgresConfig,
        permissions: Collection[Permission] = None,
    ):
        super().__init__(pg_config=pg_config, permissions=permissions)
        self.buyer_manager = BuyerManager(pg_config=pg_config, permissions=permissions)
        self.category_manager = CategoryManager(pg_config=pg_config)

    def create_or_update(self, surveys: List[Survey]):
        """
        The only field that is checked for a possible update is `is_live`!
        """
        assert len({s.source for s in surveys}) == 1, "Only do one source at a time"
        source = surveys[0].source
        survey_ids = [s.survey_id for s in surveys]
        assert len(survey_ids) == len(set(survey_ids)), "duplicate survey_ids"

        # Handle the buyers
        buyer_codes = {s.buyer_code for s in surveys}
        self.buyer_manager.bulk_get_or_create(source=source, codes=buyer_codes)
        for s in surveys:
            s.buyer_id = self.buyer_manager.source_code_pk[s.buyer_natural_key]

        existing_surveys = self.filter_by_natural_key(
            source=source, survey_ids=survey_ids
        )
        existing_nks = {s.natural_key for s in existing_surveys}

        to_create = [
            survey for survey in surveys if survey.natural_key not in existing_nks
        ]
        if to_create:
            self.create_bulk(surveys=to_create)
            to_create_survey_ids = [s.survey_id for s in to_create]
            created_surveys = self.filter_by_natural_key(
                source=source, survey_ids=to_create_survey_ids
            )
            existing_surveys.extend(created_surveys)

        # Sometimes surveys get turned back on. Check that here
        potentially_update = [s for s in surveys if s.natural_key in existing_nks]
        existing_d = {s.survey_id: s for s in existing_surveys}
        to_update = []
        for s in potentially_update:
            if existing_d[s.survey_id].is_live != s.is_live:
                s.id = existing_d[s.survey_id].id
                to_update.append(s)
        if to_update:
            self.update_is_live(to_update)
        return {
            "survey_created_count": len(to_create),
            "survey_updated_count": len(to_update),
        }

    def create_bulk(self, surveys: List[Survey]):
        for chunk in chunked(surveys, 500):
            self.create_bulk_chunk(chunk)
        return None

    def create_bulk_chunk(self, surveys: List[Survey]):
        assert len(surveys) <= 500, "chunk me"

        query = """
        INSERT INTO marketplace_survey (
            source, survey_id, created_at, updated_at,
            is_live, is_recontact, buyer_id, eligibility_criteria
        ) VALUES (
            %(source)s, %(survey_id)s, %(created_at)s, %(updated_at)s,
            %(is_live)s, %(is_recontact)s, %(buyer_id)s, %(eligibility_criteria)s
        ) ON CONFLICT (source, survey_id) DO NOTHING;"""
        params = [s.model_dump_sql() for s in surveys]
        with self.pg_config.make_connection() as conn:
            with conn.cursor() as c:
                c.executemany(query=query, params_seq=params)
            conn.commit()
        return None

    def update_is_live(self, surveys: List[Survey]):
        ids_ON = [s.id for s in surveys if s.is_live]
        ids_OFF = [s.id for s in surveys if not s.is_live]
        query_ON = """
        UPDATE marketplace_survey
        SET is_live = TRUE, updated_at = NOW()
        WHERE id = ANY(%(ids)s);
        """
        query_OFF = """
        UPDATE marketplace_survey
        SET is_live = FALSE, updated_at = NOW()
        WHERE id = ANY(%(ids)s);
        """
        if ids_ON:
            self.pg_config.execute_write(
                query_ON,
                params={"ids": ids_ON},
            )
        if ids_OFF:
            self.pg_config.execute_write(
                query_OFF,
                params={"ids": ids_OFF},
            )

    def filter_by_keys(
        self,
        survey_keys: Collection[SurveyKey],
        include_categories: bool = False,
    ) -> List[Survey]:

        assert len(survey_keys) <= 1000
        if len(survey_keys) == 0:
            return []

        params = dict()
        survey_source_ids = defaultdict(set)

        for sk in survey_keys:
            source, survey_id = sk.split(":")
            survey_source_ids[Source(source).value].add(survey_id)

        sk_filters = []
        for source, survey_ids in survey_source_ids.items():
            sk_filters.append(
                f"(s.source = '{source}' AND s.survey_id = ANY(%(survey_ids_{source})s))"
            )
            params[f"survey_ids_{source}"] = list(survey_ids)

        filter_str = f"WHERE ({' OR '.join(sk_filters)})"

        if include_categories:
            CATEGORY_JOIN = """
            LEFT JOIN LATERAL (
                SELECT
                    jsonb_agg(
                        jsonb_build_object(
                            'category',
                            jsonb_build_object(
                                'id', c.id,
                                'uuid', replace(c.uuid::text, '-', ''),
                                'label', c.label,
                                'path', c.path,
                                'adwords_vertical_id', c.adwords_vertical_id,
                                'parent_id', c.parent_id
                            ),
                            'strength', sc.strength
                        )
                        ORDER BY c.id
                    ) AS categories
                FROM marketplace_surveycategory sc
                JOIN marketplace_category c
                    ON c.id = sc.category_id
                WHERE sc.survey_id = s.id
            ) cat ON TRUE
            """

            query = f"""
            SELECT
                s.*,
                b.code as buyer_code,
                COALESCE(cat.categories, '[]'::jsonb) AS categories
            FROM marketplace_survey s
            LEFT JOIN marketplace_buyer b on s.buyer_id = b.id
            {CATEGORY_JOIN}
            {filter_str};
            """
        else:
            query = f"""
            SELECT s.*, b.code as buyer_code
            FROM marketplace_survey s
            LEFT JOIN marketplace_buyer b on s.buyer_id = b.id
            {filter_str};
            """

        res = self.pg_config.execute_sql_query(
            query,
            params=params,
        )

        return [Survey.model_validate(x) for x in res]

    def filter_by_natural_key(
        self, source: Source, survey_ids: Collection[str]
    ) -> List[Survey]:
        res = []
        for chunk in chunked(survey_ids, 1000):
            res.extend(self.filter_by_natural_key_chunk(source, chunk))
        return res

    def filter_by_natural_key_chunk(
        self, source: Source, survey_ids: Collection[str]
    ) -> List[Survey]:
        query = """
        SELECT id, source, survey_id, created_at, updated_at,
            is_live, is_recontact, buyer_id, eligibility_criteria
        FROM marketplace_survey
        WHERE source = %(source)s AND
            survey_id = ANY(%(survey_ids)s);
        """
        res = self.pg_config.execute_sql_query(
            query,
            params={"survey_ids": list(survey_ids), "source": source.value},
        )
        return [Survey.model_validate(x) for x in res]

    def filter_by_source_live(self, source: Source) -> List[Survey]:
        """
        Return all live surveys for this source
        """
        query = """
        SELECT id, source, survey_id, created_at, updated_at,
            is_live, is_recontact, buyer_id, eligibility_criteria
        FROM marketplace_survey
        WHERE source = %(source)s AND is_live;
        """
        res = self.pg_config.execute_sql_query(query, params={"source": source.value})
        return [Survey.model_validate(x) for x in res]

    def filter_by_live(self, fields: Optional[List[str]] = None) -> List[Survey]:
        """
        Return all live surveys
        """
        fields_default = """id, source, survey_id, created_at, updated_at,
            is_live, is_recontact, buyer_id, eligibility_criteria"""
        fields = ", ".join(fields) if fields else fields_default
        query = f"""
        SELECT {fields}
        FROM marketplace_survey
        WHERE is_live;
        """
        res = self.pg_config.execute_sql_query(query)
        return [Survey.model_validate(x) for x in res]

    def turn_off_by_natural_key(
        self, source: Source, survey_ids: Collection[str]
    ) -> None:
        params = {"survey_ids": list(survey_ids), "source": source.value}
        query = """
        UPDATE marketplace_survey
        SET is_live = FALSE, updated_at = NOW()
        WHERE source = %(source)s AND
            survey_id = ANY(%(survey_ids)s)
        RETURNING id;
        """
        with self.pg_config.make_connection() as conn:
            with conn.cursor() as c:
                c.execute(query, params=params)
                survey_pks = [x["id"] for x in c.fetchall()]
            conn.commit()

        query = """
        UPDATE marketplace_surveystat
        SET survey_is_live = FALSE, updated_at = NOW()
        WHERE survey_is_live AND
            survey_id = ANY(%(survey_pks)s);
        """
        self.pg_config.execute_write(
            query=query,
            params={"survey_pks": survey_pks},
        )
        return None

    def update_surveys_categories(self, surveys: List[Survey] = None) -> None:
        for chunk in chunked(surveys, 500):
            self.update_surveys_categories_chunk(chunk)
        return None

    def update_surveys_categories_chunk(self, surveys: List[Survey] = None) -> None:
        assert len(surveys) <= 500, "chunk me"
        temp_table_sql = sql.SQL(
            """
        CREATE TEMP TABLE tmp_survey_categories (
            survey_id   bigint,
            category_id int,
            strength    float8
        ) ON COMMIT DROP;
        """
        )
        # noinspection SqlResolve
        insert_values_sql = sql.SQL(
            "INSERT INTO tmp_survey_categories VALUES (%s, %s, %s)"
        )
        # noinspection SqlResolve
        delete_sql = sql.SQL(
            """
        DELETE FROM marketplace_surveycategory sc
        WHERE NOT EXISTS (
            SELECT 1
            FROM tmp_survey_categories t
            WHERE t.survey_id = sc.survey_id
              AND t.category_id = sc.category_id
        )
        AND sc.survey_id IN (
            SELECT DISTINCT survey_id FROM tmp_survey_categories
        );"""
        )
        # noinspection SqlResolve
        upsert_sql = sql.SQL(
            """
        INSERT INTO marketplace_surveycategory (survey_id, category_id, strength)
        SELECT survey_id, category_id, strength
        FROM tmp_survey_categories
        ON CONFLICT (survey_id, category_id)
        DO UPDATE SET
            strength = EXCLUDED.strength;"""
        )

        rows = [
            (survey.id, c.category.id, c.strength)
            for survey in surveys
            for c in survey.categories
        ]
        with self.pg_config.make_connection() as conn:
            # noinspection PyArgumentList
            with conn.transaction():
                with conn.cursor() as c:
                    c.execute(temp_table_sql)
                    c.executemany(insert_values_sql, rows)
                    c.execute(delete_sql)
                    c.execute(upsert_sql)
            conn.commit()

    def get_survey_categories(self):
        query = """
        SELECT
            s.source, s.survey_id,
            jsonb_agg(
                jsonb_build_object(
                    'category_id', sc.category_id,
                    'strength', sc.strength
                )
            ) as categories
        FROM marketplace_survey s
        JOIN marketplace_surveycategory sc ON s.id = sc.survey_id
        WHERE is_live
        GROUP BY s.source, s.survey_id
        """
        return self.pg_config.execute_sql_query(query)


class SurveyStatManager(PostgresManager):
    KEYS = [
        "survey_id",
        "quota_id",
        "country_iso",
        "version",
        "cpi",
        "complete_too_fast_cutoff",
        "prescreen_conv_alpha",
        "prescreen_conv_beta",
        "conv_alpha",
        "conv_beta",
        "dropoff_alpha",
        "dropoff_beta",
        "completion_time_mu",
        "completion_time_sigma",
        "mobile_eligible_alpha",
        "mobile_eligible_beta",
        "desktop_eligible_alpha",
        "desktop_eligible_beta",
        "tablet_eligible_alpha",
        "tablet_eligible_beta",
        "long_fail_rate",
        "user_report_coeff",
        "recon_likelihood",
        "score_x0",
        "score_x1",
        "score",
        "updated_at",
        "survey_is_live",
        "survey_survey_id",
        "survey_source",
    ]

    SURVEY_STATS_COL_MAP = {
        "PRESCREEN_CONVERSION.alpha": "prescreen_conv_alpha",
        "PRESCREEN_CONVERSION.beta": "prescreen_conv_beta",
        "CONVERSION.alpha": "conv_alpha",
        "CONVERSION.beta": "conv_beta",
        "COMPLETION_TIME.mu": "completion_time_mu",
        "COMPLETION_TIME.sigma": "completion_time_sigma",
        "LONG_FAIL.value": "long_fail_rate",
        "USER_REPORT_COEFF.value": "user_report_coeff",
        "RECON_LIKELIHOOD.value": "recon_likelihood",
        "DROPOFF_RATE.alpha": "dropoff_alpha",
        "DROPOFF_RATE.beta": "dropoff_beta",
        "IS_MOBILE_ELIGIBLE.alpha": "mobile_eligible_alpha",
        "IS_MOBILE_ELIGIBLE.beta": "mobile_eligible_beta",
        "IS_DESKTOP_ELIGIBLE.alpha": "desktop_eligible_alpha",
        "IS_DESKTOP_ELIGIBLE.beta": "desktop_eligible_beta",
        "IS_TABLET_ELIGIBLE.alpha": "tablet_eligible_alpha",
        "IS_TABLET_ELIGIBLE.beta": "tablet_eligible_beta",
        "cpi": "cpi",
    }

    def __init__(
        self,
        pg_config: PostgresConfig,
        permissions: Collection[Permission] = None,
    ):
        super().__init__(pg_config=pg_config, permissions=permissions)
        self.survey_manager = SurveyManager(
            pg_config=pg_config, permissions=permissions
        )
        # self.ensure_surveystat_key_type()

    #
    # def ensure_surveystat_key_type(self):
    #     SQL = """
    #     DO $$
    #     BEGIN
    #         IF NOT EXISTS (
    #             SELECT 1
    #             FROM pg_type t
    #             JOIN pg_namespace n ON n.oid = t.typnamespace
    #             WHERE t.typname = 'surveystat_key'
    #               AND n.nspname = 'public'
    #         ) THEN
    #             CREATE TYPE public.surveystat_key AS (
    #                 survey_id   bigint,
    #                 quota_id    varchar(32),
    #                 country_iso varchar(2),
    #                 version     integer
    #             );
    #         END IF;
    #     END
    #     $$;"""
    #     with self.pg_config.make_connection() as conn:
    #         with conn.cursor() as c:
    #             c.execute(SQL)
    #         conn.commit()
    #     return None

    # def register_surveystat_key(self, conn):
    #     info = CompositeInfo.fetch(conn, "surveystat_key")
    #     info.register(conn)

    def update_or_create(
        self, survey_stats: List[SurveyStat]
    ) -> Optional[List[SurveyStat]]:
        """
        This manager is NOT responsible for creating surveys or buyers.
        It will check to make sure they exist
        """
        if len(survey_stats) == 0:
            return []

        assert all(s.survey_survey_id is not None for s in survey_stats)
        assert all(s.survey_source is not None for s in survey_stats)
        assert (
            len({s.survey_source for s in survey_stats}) == 1
        ), "Only do one source at a time"
        source = survey_stats[0].survey_source
        nks = [s.natural_key for s in survey_stats]
        assert len(nks) == len(set(nks)), "duplicate natural_keys"

        # Look up survey pks
        survey_ids = [s.survey_survey_id for s in survey_stats]
        surveys = self.survey_manager.filter_by_natural_key(
            source=source, survey_ids=survey_ids
        )
        nk_to_pk = {s.natural_key: s.id for s in surveys}
        for ss in survey_stats:
            try:
                ss.survey_id = nk_to_pk[ss.survey_natural_key]
            except KeyError as e:
                raise ValueError(
                    f"Survey {e.args[0]} does not exist. Must create surveys first"
                )
        # print(f"----aa-----: {datetime.now().isoformat()}")
        self.upsert_sql(survey_stats=survey_stats)

        # print(f"----ab-----: {datetime.now().isoformat()}")
        return None
        # keys = [s.unique_key for s in survey_stats]
        # print(keys[:4])
        # survey_stats = self.filter_by_unique_keys(keys)
        # print(len(survey_stats))
        # print(f"----ac-----: {datetime.now().isoformat()}")
        # # For testing/deterministic
        # survey_stats = sorted(survey_stats, key=lambda s: s.natural_key)
        # return survey_stats

    def upsert_sql(self, survey_stats: List[SurveyStat]) -> None:
        for chunk in chunked(survey_stats, 1000):
            self.upsert_sql_chunk(survey_stats=chunk)
        return None

    # def insert_sql(self, survey_stats: List[SurveyStat]):
    #     for chunk in chunked(survey_stats, 1000):
    #         self.insert_sql_chunk(survey_stats=chunk)
    #     return None
    #
    # def insert_sql_chunk(self, survey_stats: List[SurveyStat]):
    #     assert len(survey_stats) <= 1000, "chunk me"
    #     keys = self.keys
    #     keys_str = ", ".join(keys)
    #     values_str = ", ".join([f"%({k})s" for k in keys])
    #     unique_cols = ["survey_id", "quota_id", "country_iso", "version"]
    #     unique_cols_str = ", ".join(unique_cols)
    #
    #     query = f"""
    #     INSERT INTO marketplace_surveystat ({keys_str})
    #     VALUES ({values_str})
    #     ON CONFLICT ({unique_cols_str})
    #     DO NOTHING ;"""
    #     params = [ss.model_dump_sql() for ss in survey_stats]
    #     with self.pg_config.make_connection() as conn:
    #         with conn.cursor() as c:
    #             c.executemany(query=query, params_seq=params)
    #         conn.commit()
    #     return None

    def upsert_sql_chunk(self, survey_stats: List[SurveyStat]) -> None:
        assert len(survey_stats) <= 1000, "chunk me"
        keys = self.KEYS
        keys_str = ", ".join(keys)
        values_str = ", ".join([f"%({k})s" for k in keys])
        unique_cols = ["survey_id", "quota_id", "country_iso", "version"]
        unique_cols_str = ", ".join(unique_cols)
        update_cols = set(keys) - set(unique_cols) - {"updated_at", "is_live"}
        update_str = ", ".join(
            [f"{k} = EXCLUDED.{k}" for k in update_cols] + ["updated_at = NOW()"]
        )

        query = f"""
        INSERT INTO marketplace_surveystat ({keys_str})
        VALUES ({values_str})
        ON CONFLICT ({unique_cols_str})
        DO UPDATE SET {update_str};"""
        now = datetime.now(tz=timezone.utc)
        params = [ss.model_dump_sql() | {"updated_at": now} for ss in survey_stats]

        with self.pg_config.make_connection() as conn:
            with conn.cursor() as c:
                c.executemany(query=query, params_seq=params)
            conn.commit()

        return None

    def filter_by_unique_keys(self, keys: Collection[Tuple]) -> List[SurveyStat]:
        res = []
        for chunk in chunked(keys, 5000):
            res.extend(self.filter_by_unique_keys_chunk(chunk))

        return res

    def filter_by_unique_keys_chunk(self, keys: Collection[Tuple]):
        values_sql = ", ".join(["(%s, %s, %s, %s)"] * len(keys))
        query = f"""
        SELECT
            ss.*
        FROM marketplace_surveystat ss
        JOIN (
            VALUES {values_sql}
        ) AS v(survey_id, quota_id, country_iso, version)
          ON (ss.survey_id, ss.quota_id, ss.country_iso, ss.version)
           = (v.survey_id, v.quota_id, v.country_iso, v.version);
        """
        params = [item for row in keys for item in row]
        with self.pg_config.make_connection() as conn:
            # self.register_surveystat_key(conn)
            with conn.cursor() as c:
                c.execute(query, params=params)
                res = c.fetchall()
                # print('\n'.join([x['QUERY PLAN'] for x in res]))
        return [SurveyStat.model_validate(x) for x in res]

    def update_surveystats_for_source(
        self,
        source: Source,
        surveys: List[Survey],
        survey_stats: List[SurveyStat],
    ):
        """
        What ym-survey-stats actually calls.
        1. All surveys for this source not in this list of surveys
           get turned off
        2. Get or create all surveys and buyers
        3. Update survey stats
        """
        # Assert the surveys and surveystats we passed are all
        #   for this Source
        survey_source = {s.source for s in surveys}
        assert len(survey_source) == 1 and survey_source == {source}
        # And that the surveys in the surveystats match the passed in Surveys
        surveys_nks = {s.natural_key for s in surveys}
        ss_surveys_nks = {ss.survey_natural_key for ss in survey_stats}
        assert surveys_nks == ss_surveys_nks

        # Turn off not live surveys
        live_surveys = self.survey_manager.filter_by_source_live(source=source)
        live_ids = {s.survey_id for s in live_surveys}
        new_ids = {s.survey_id for s in surveys}
        turn_off_surveys = live_ids - new_ids
        self.survey_manager.turn_off_by_natural_key(
            source=source, survey_ids=turn_off_surveys
        )

        # Create or Update (is_live) Surveys
        res = self.survey_manager.create_or_update(surveys)

        # Update ss
        self.update_or_create(survey_stats=survey_stats)

        return res

    def filter_by_updated_since(self, since: datetime):
        return self.filter(updated_after=since, is_live=None)

    def filter_by_live(self):
        return self.filter(is_live=True)

    def make_filter_str(
        self,
        is_live: Optional[bool] = True,
        updated_after: Optional[datetime] = None,
        min_score: Optional[float] = None,
        survey_keys: Optional[Collection[SurveyKey]] = None,
        sources: Optional[Collection[Source]] = None,
        country_iso: Optional[str] = None,
    ) -> Tuple[str, Dict[str, Any]]:
        filters = []
        params = dict()
        if updated_after is not None:
            params["updated_after"] = updated_after
            filters.append("ss.updated_at >= %(updated_after)s")
        if min_score:
            params["min_score"] = min_score
            filters.append("score >= %(min_score)s")
        if is_live is not None:
            if is_live:
                filters.append("ss.survey_is_live")
            else:
                filters.append("NOT ss.survey_is_live")
        if sources:
            assert survey_keys is None
            params["sources"] = [s.value for s in sources]
            filters.append("survey_source = ANY(%(sources)s)")
        if country_iso:
            params["country_iso"] = country_iso
            filters.append("country_iso = %(country_iso)s")
        if survey_keys is not None:
            # Instead of doing a big IN with a big set of tuples, since we know
            #   we only have N possible sources, we just split by that and do
            #   a set of:
            #      ( (survey_source = 'x' and survey_survey_id IN ('1', '2') ) OR
            #        (survey_source = 'y' and survey_survey_id IN ('3', '4') ) ... )
            sk_filters = []
            survey_source_ids = defaultdict(set)
            for sk in survey_keys:
                source, survey_id = sk.split(":")
                survey_source_ids[Source(source).value].add(survey_id)
            for source, survey_ids in survey_source_ids.items():
                sk_filters.append(
                    f"(survey_source = '{source}' AND survey_survey_id = ANY(%(survey_ids_{source})s))"
                )
                params[f"survey_ids_{source}"] = list(survey_ids)
            # potential bug here ! --v Make sure this is wrapped in parentheses!
            filters.append(f"({' OR '.join(sk_filters)})")

        filter_str = "WHERE " + " AND ".join(filters) if filters else ""

        return filter_str, params

    def filter_count(
        self,
        is_live: Optional[bool] = True,
        updated_after: Optional[datetime] = None,
        min_score: Optional[float] = None,
        survey_keys: Optional[Collection[SurveyKey]] = None,
        sources: Optional[Collection[Source]] = None,
        country_iso: Optional[str] = None,
    ) -> NonNegativeInt:
        filter_str, params = self.make_filter_str(
            is_live=is_live,
            updated_after=updated_after,
            min_score=min_score,
            survey_keys=survey_keys,
            sources=sources,
            country_iso=country_iso,
        )
        query = f"""
        SELECT COUNT(1) as cnt
        FROM marketplace_surveystat ss
        {filter_str};
        """
        return self.pg_config.execute_sql_query(query, params=params)[0]["cnt"]

    def filter(
        self,
        is_live: Optional[bool] = True,
        updated_after: Optional[datetime] = None,
        min_score: Optional[float] = None,
        survey_keys: Optional[Collection[SurveyKey]] = None,
        sources: Optional[Collection[Source]] = None,
        country_iso: Optional[str] = None,
        page: Optional[int] = None,
        size: Optional[int] = None,
        order_by: Optional[str] = None,
        debug: Optional[bool] = False,
    ) -> List[SurveyStat]:
        filter_str, params = self.make_filter_str(
            is_live=is_live,
            updated_after=updated_after,
            min_score=min_score,
            survey_keys=survey_keys,
            sources=sources,
            country_iso=country_iso,
        )

        paginated_filter_str = ""
        if page is not None:
            assert page != 0, "page starts at 1"
            size = size if size is not None else 100
            params["offset"] = (page - 1) * size
            params["limit"] = size
            paginated_filter_str = " LIMIT %(limit)s OFFSET %(offset)s"

        order_by_str = ""
        if order_by:
            assert order_by in {"score DESC", "score", "updated_at DESC", "updated_at"}
            order_by_str = f"ORDER BY {order_by}"

        query = f"""
        SELECT
            quota_id, country_iso, cpi,
            complete_too_fast_cutoff,
            prescreen_conv_alpha, prescreen_conv_beta,
            conv_alpha, conv_beta,
            dropoff_alpha, dropoff_beta,
            completion_time_mu, completion_time_sigma,
            mobile_eligible_alpha, mobile_eligible_beta,
            desktop_eligible_alpha, desktop_eligible_beta,
            tablet_eligible_alpha, tablet_eligible_beta,
            long_fail_rate, user_report_coeff, recon_likelihood,
            score_x0, score_x1, updated_at, version, score,
            survey_is_live, survey_source, survey_survey_id
        FROM marketplace_surveystat ss
        {filter_str}
        {order_by_str}
        {paginated_filter_str} ;
        """

        if debug:
            print(query)
            print(params)

        with self.pg_config.make_connection() as conn:
            with conn.cursor() as c:
                c.execute("SET work_mem = '256MB';")
                c.execute("SET statement_timeout = '10s';")
                c.execute(query, params=params)
                res = c.fetchall()

        return [SurveyStat.model_validate(x) for x in res]

    def filter_to_merge_table(
        self,
        is_live: Optional[bool] = True,
        updated_after: Optional[datetime] = None,
        min_score: Optional[float] = 0.0001,
    ) -> Optional[pd.DataFrame]:

        survey_stats = self.filter(
            is_live=is_live, updated_after=updated_after, min_score=min_score
        )
        if not survey_stats:
            return None

        extra_cols = {
            "survey_id",
            "quota_id",
            "country_iso",
            "version",
            "updated_at",
            "score_x0",
            "score_x1",
            "survey_is_live",
            "survey_source",
            "survey_survey_id",
        }
        data = []
        for ss in survey_stats:
            d = {k: getattr(ss, v) for k, v in self.SURVEY_STATS_COL_MAP.items()}
            d.update({k: getattr(ss, k) for k in extra_cols})
            d["sid"] = ss.survey_natural_key
            data.append(d)
        df = pd.DataFrame(data)
        df = df.set_index("sid")
        df["cpi"] = df["cpi"].astype(float)
        return df
