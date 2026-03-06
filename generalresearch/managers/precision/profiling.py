import json
from typing import List, Collection, Optional, Tuple

from generalresearch.models.precision.question import PrecisionQuestion
from generalresearch.sql_helper import SqlHelper


def get_profiling_library(
    sql_helper: SqlHelper,
    country_iso: Optional[str] = None,
    language_iso: Optional[str] = None,
    question_ids: Optional[Collection[str]] = None,
    max_options: Optional[int] = None,
    is_live: Optional[bool] = None,
    pks: Optional[Collection[Tuple[str, str, str]]] = None,
) -> List[PrecisionQuestion]:
    """
    Accepts lots of optional filters.

    :param country_iso: filters on country_iso field
    :param language_iso: filters on language_iso field
    :param question_ids: filters on question_id field, accepts multiple values
    :param max_options: filters on max_options field
    :param is_live: filters on is_live field
    :param pks: The pk is (question_id, country_iso, language_iso). pks accepts a collection of
        len(3) tuples. e.g. [('123', 'us', 'eng'), ('123', 'us', 'spa')]
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
    if question_ids:
        params["question_ids"] = question_ids
        filters.append("question_id IN %(question_ids)s")
    if max_options is not None:
        params["max_options"] = max_options
        filters.append("COALESCE(num_options, 0) <= %(max_options)s")
    if is_live is not None:
        params["is_live"] = is_live
        filters.append("is_live = %(is_live)s")
    if pks:
        params["pks"] = pks
        filters.append("(question_id, country_iso, language_iso) IN %(pks)s")
    filter_str = " AND ".join(filters)
    filter_str = "WHERE " + filter_str if filter_str else ""
    res = sql_helper.execute_sql_query(
        f"""
    SELECT *
    FROM `thl-precision`.`precision_question` q
    {filter_str}
    """,
        params,
    )
    for x in res:
        x["options"] = json.loads(x["options"]) if x["options"] else None
    qs = [PrecisionQuestion.from_db(x) for x in res]
    return qs
