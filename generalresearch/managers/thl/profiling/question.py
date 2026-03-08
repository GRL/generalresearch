import random
import threading
from typing import Any, Collection, Dict, List, Tuple

from cachetools import TTLCache, cached
from pydantic import ValidationError

from generalresearch.decorators import LOG
from generalresearch.managers.base import PostgresManager
from generalresearch.models.thl.profiling.upk_question import (
    UPKImportance,
    UpkQuestion,
)


class QuestionManager(PostgresManager):

    def get_multi_upk(self, question_ids: Collection[str]) -> List[UpkQuestion]:
        query = """
        SELECT data, property_code, explanation_template, explanation_fragment_template
        FROM marketplace_question
        WHERE id = ANY(%(question_ids)s);
        """
        res: List[Dict[str, Any]] = self.pg_config.execute_sql_query(
            query=query, params={"question_ids": list(question_ids)}
        )
        for x in res:
            x["data"]["ext_question_id"] = x["property_code"]
            x["data"]["explanation_template"] = x["explanation_template"]
            x["data"]["explanation_fragment_template"] = x[
                "explanation_fragment_template"
            ]
            x["data"].pop("categories", None)

        return [UpkQuestion.model_validate(x["data"]) for x in res]

    @cached(
        cache=TTLCache(maxsize=256, ttl=3600 + random.randint(-900, 900)),
        lock=threading.Lock(),
        info=True,
    )
    def get_questions_ranked(
        self, country_iso: str, language_iso: str
    ) -> List[UpkQuestion]:
        query = """
        SELECT data, property_code, explanation_template, explanation_fragment_template
        FROM marketplace_question
        WHERE country_iso = %(country_iso)s
            AND language_iso = %(language_iso)s
            AND property_code NOT LIKE 'gr:%%'
            AND property_code NOT LIKE 'g:%%'
            AND is_live
        """
        res: List[Dict[str, Any]] = self.pg_config.execute_sql_query(
            query=query,
            params={"country_iso": country_iso, "language_iso": language_iso},
        )
        qs: List[UpkQuestion] = []
        for x in res:
            x["data"]["ext_question_id"] = x["property_code"]
            x["data"]["explanation_template"] = x["explanation_template"]
            x["data"]["explanation_fragment_template"] = x[
                "explanation_fragment_template"
            ]
            x["data"].pop("categories", None)
            q = UpkQuestion.model_validate(x["data"])
            if not q.importance:
                q.importance = UPKImportance(
                    task_count=x["data"].get("task_count", 0),
                    task_score=x["data"].get("task_score", 0),
                )
            qs.append(q)

        res = sorted(qs, key=lambda x: x.importance.task_score, reverse=True)
        return res

    @cached(
        cache=TTLCache(maxsize=256, ttl=3600 + random.randint(-900, 900)),
        lock=threading.Lock(),
        info=True,
    )
    def lookup_by_property(
        self, property_code: str, country_iso: str, language_iso: str
    ) -> UpkQuestion:
        query = f"""
        SELECT data, property_code, explanation_template, explanation_fragment_template
        FROM marketplace_question
        WHERE property_code = %(property_code)s
            AND country_iso = %(country_iso)s
            AND language_iso = %(language_iso)s
        LIMIT 2;
        """
        params = {
            "property_code": property_code,
            "country_iso": country_iso,
            "language_iso": language_iso,
        }
        res: List[Dict[str, Any]] = self.pg_config.execute_sql_query(
            query=query, params=params
        )
        assert len(res) == 1, f"expected 1, got {len(res)} results"
        x = res[0]

        x["data"]["ext_question_id"] = x["property_code"]
        x["data"]["explanation_template"] = x["explanation_template"]
        x["data"]["explanation_fragment_template"] = x["explanation_fragment_template"]
        x["data"].pop("categories", None)
        return UpkQuestion.model_validate(x["data"])

    def filter_by_property(
        self, lookup: Collection[Tuple[str, str, str]]
    ) -> List[UpkQuestion]:
        """
        lookup is [(property_code, country_iso, language_iso)]
        """
        where_str = " OR ".join(
            "(property_code = %s AND country_iso = %s AND language_iso = %s)"
            for _ in lookup
        )
        query = f"""
        SELECT data, property_code, explanation_template, explanation_fragment_template
        FROM marketplace_question
        WHERE {where_str}
        """
        flat_params = [item for tup in lookup for item in tup]
        res: List[Dict[str, Any]] = self.pg_config.execute_sql_query(
            query=query, params=flat_params
        )
        for x in res:
            x["data"]["ext_question_id"] = x["property_code"]
            x["data"]["explanation_template"] = x["explanation_template"]
            x["data"]["explanation_fragment_template"] = x[
                "explanation_fragment_template"
            ]
            x["data"].pop("categories", None)
        res2 = []
        for x in res:
            try:
                res2.append(UpkQuestion.model_validate(x["data"]))
            except ValidationError as e:
                LOG.warning(e)
        return res2

    def update_question_explanation(self, q: UpkQuestion):
        # Assuming the question already exists in the db, and we're updating
        # the fields explanation_template and explanation_fragment_template
        assert q.id, "q.id must be set"
        query = """
        UPDATE marketplace_question
        SET explanation_template = %(explanation_template)s,
        explanation_fragment_template = %(explanation_fragment_template)s
        WHERE id = %(id)s;"""
        params = {
            "id": q.id,
            "explanation_template": q.explanation_template,
            "explanation_fragment_template": q.explanation_fragment_template,
        }
        with self.pg_config.make_connection() as conn:
            with conn.cursor() as c:
                c.execute(query, params)
                assert c.rowcount == 1
            conn.commit()
        return None
