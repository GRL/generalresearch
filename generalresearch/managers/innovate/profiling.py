import json
from typing import List, Collection, Optional, Tuple

from generalresearch.models.innovate.question import InnovateQuestion
from generalresearch.sql_helper import SqlHelper


def get_profiling_library(
    sql_helper: SqlHelper,
    country_iso: Optional[str] = None,
    language_iso: Optional[str] = None,
    question_keys: Optional[Collection[str]] = None,
    max_options: Optional[int] = None,
    is_live: Optional[bool] = None,
    pks: Optional[Collection[Tuple[str, str, str]]] = None,
) -> List[InnovateQuestion]:
    """
    Accepts lots of optional filters.

    :param country_iso: filters on country_iso field
    :param language_iso: filters on language_iso field
    :param question_keys: filters on question_key field, accepts multiple values
    :param max_options: filters on max_options field
    :param is_live: filters on is_live field
    :param pks: The pk is (question_key, country_iso, language_iso). pks accepts a collection of
        len(3) tuples. e.g. [('CORE_AUTOMOTIVE_0002', 'us', 'eng'), ('AGE', 'us', 'spa')]
    :return:
    """
    filters = []
    params = {}
    if country_iso:
        params["country_iso"] = country_iso
        filters.append("`country_iso` = %(country_iso)s")
    if language_iso:
        params["language_iso"] = language_iso
        filters.append("`language_iso` = %(language_iso)s")
    if question_keys:
        params["question_keys"] = question_keys
        filters.append("question_key IN %(question_keys)s")
    if max_options is not None:
        params["max_options"] = max_options
        filters.append("COALESCE(num_options, 0) <= %(max_options)s")
    if is_live is not None:
        params["is_live"] = is_live
        filters.append("is_live = %(is_live)s")
    if pks:
        params["pks"] = pks
        filters.append("(question_key, country_iso, language_iso) IN %(pks)s")
    filter_str = " AND ".join(filters)
    filter_str = "WHERE " + filter_str if filter_str else ""
    res = sql_helper.execute_sql_query(
        f"""
    SELECT *
    FROM `{sql_helper.db}`.`innovate_question` q
    {filter_str}
    """,
        params,
    )
    for x in res:
        x["options"] = json.loads(x["options"]) if x["options"] else None
    qs = [InnovateQuestion.from_db(x) for x in res]
    return qs
