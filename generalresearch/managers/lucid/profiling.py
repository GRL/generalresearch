import json
from typing import Collection, List, Optional, Tuple

from pydantic import ValidationError

from generalresearch.decorators import LOG
from generalresearch.models.lucid.question import LucidQuestion, LucidQuestionType
from generalresearch.sql_helper import SqlHelper


def get_profiling_library(
    sql_helper: SqlHelper,
    country_iso: Optional[str] = None,
    language_iso: Optional[str] = None,
    question_ids: Optional[Collection[str]] = None,
    pks: Optional[Collection[Tuple[str | int, str, str]]] = None,
) -> List[LucidQuestion]:
    """
    Accepts lots of optional filters.

    :param country_iso: filters on country_iso field
    :param language_iso: filters on language_iso field
    :param question_ids: filters on question_id field, accepts multiple values
    :param pks: The pk is (question_id, country_iso, language_iso). pks accepts a collection of
        len(3) tuples. e.g. [('123', 'us', 'eng'), ('123', 'us', 'spa')]
    :return:
    """

    filters = ["`q`.question_type != 'o'"]
    params = {}

    if country_iso:
        params["country_iso"] = country_iso
        filters.append("`q`.`country_iso` = %(country_iso)s")
    if language_iso:
        params["language_iso"] = language_iso
        filters.append("`q`.`language_iso` = %(language_iso)s")
    if question_ids:
        params["question_ids"] = question_ids
        filters.append("question_id IN %(question_ids)s")
    if pks:
        # In this table, the question_id is an int
        pks = [(int(x[0]), x[1], x[2]) for x in pks]
        params["pks"] = pks
        filters.append("(q.question_id, q.country_iso, q.language_iso) IN %(pks)s")

    filter_str = " AND ".join(filters)
    filter_str = "WHERE " + filter_str if filter_str else ""

    db_name = sql_helper.db_name
    res = sql_helper.execute_sql_query(
        query=f"""
            SELECT  q.question_id, q.question_type, q.question_text, 
                    q.country_iso, q.language_iso,
                    JSON_ARRAYAGG(
                        JSON_OBJECT('id', qo.precode, 'text', qo.option_text)
                    ) AS options
            FROM `{db_name}`.`lucid_question` q
            LEFT JOIN `{db_name}`.lucid_questionoption qo 
                ON q.question_id = qo.question_id
                    AND q.country_iso = qo.country_iso 
                    AND q.language_iso = qo.language_iso 
            {filter_str}
            GROUP BY q.question_id, q.country_iso, q.language_iso
        """,
        params=params,
    )
    for x in res:
        x["question_id"] = str(x["question_id"])
        x["options"] = json.loads(x["options"]) if x["options"] else None
        # the mysql JSON_ARRAYAGG returns this if there are no options
        x["options"] = (
            x["options"] if x["options"] != [{"id": None, "text": None}] else []
        )
        for n, y in enumerate(x["options"]):
            y["order"] = n
        # Special hack... These don't have options, but they should
        # (CBSA, MSA, DMA),
        if x["question_id"] in {"116", "120", "121"}:
            x["question_type"] = LucidQuestionType.TEXT_ENTRY
    qs = []

    for x in res:
        try:
            qs.append(LucidQuestion.from_db(x))
        except ValidationError as e:
            LOG.warning(f"{x['question_id']}: {e}")
            # print(x)
    return qs
