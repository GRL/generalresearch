import logging
from typing import Any, Collection, Dict, List

import pandas as pd
from more_itertools import chunked
from pydantic import PositiveInt

from generalresearch.pg_helper import PostgresConfig

LOG = logging.getLogger("incite")


def annotate_product_id(
    df: pd.DataFrame, pg_config: PostgresConfig, chunksize: PositiveInt = 500
) -> pd.DataFrame:
    """
    Dask map_partitions is being called on a dask df. However, the function
    it applies to each partition is being passed a chunk of the dask
    df AS a pandas df.

    expects column 'user_id', adds column 'product_id'
    """
    LOG.warning(f"annotate_product_id.chunk: {df.shape}")
    assert "user_id" in df.columns, "must have a user_id column to join on"

    user_ids = df["user_id"].dropna()
    user_ids = set(user_ids)
    assert len(user_ids) >= 1, "must have user_ids"
    LOG.warning(f"annotate_product_id.len(user_ids): {len(user_ids)}")

    res: List[Dict[str, Any]] = []
    with pg_config.make_connection() as conn:
        for chunk in chunked(user_ids, chunksize):
            try:
                with conn.cursor() as c:
                    c.execute(
                        query="""
                            SELECT id as user_id, product_id
                            FROM thl_user
                            WHERE id = ANY(%s)""",
                        params=[list(chunk)],
                    )
                    res.extend(c.fetchall())

            except Exception:
                LOG.exception(f"annotate_product_id: {chunk}")
                raise

    dfu = pd.DataFrame(res, columns=["user_id", "product_id"])

    return df.merge(dfu, on="user_id", how="left")


def lookup_product_and_team_id(
    user_ids: Collection[int],
    pg_config: PostgresConfig,
) -> List[Dict[str, Any]]:

    user_ids = set(user_ids)
    LOG.info(f"lookup_product_and_team_id: {len(user_ids)}")
    LOG.info({type(x) for x in user_ids})

    assert all(type(x) is int for x in user_ids), "must pass all integers"
    assert len(user_ids) >= 1, "must have user_ids"
    assert len(user_ids) <= 1000, "you should chunk this bro"

    res: List[Dict[str, Any]] = []
    with pg_config.make_connection() as conn:
        try:
            with conn.cursor() as c:
                c.execute(
                    query="""
                        SELECT  u.id AS user_id,
                                u.product_id, 
                                bp.team_id
                        FROM thl_user u
                        INNER JOIN userprofile_brokerageproduct AS bp
                            ON bp.id = u.product_id
                        WHERE u.id = ANY(%s);
                    """,
                    params=[list(user_ids)],
                )
                res.extend(c.fetchall())

        except Exception as e:
            LOG.exception(f"lookup_product_and_team_id: {e}")
            raise

    return res


def annotate_product_and_team_id(
    df: pd.DataFrame, pg_config: PostgresConfig, chunksize: PositiveInt = 500
) -> pd.DataFrame:
    """
    Dask map_partitions is being called on a dask df. However, the function
    it applies to each partition is being passed a chunk of the dask
    df AS a pandas df.

    expects column 'user_id', adds column 'product_id' and team_id
    """

    LOG.info(f"annotate_product_and_team_id.chunk: {df.shape}")
    assert "user_id" in df.columns, "must have a user_id column to join on"

    user_ids = df["user_id"].dropna()
    user_ids = set(user_ids)
    assert len(user_ids) >= 1, "must have user_ids"
    LOG.warning(f"annotate_product_and_team_id.len(user_ids): {len(user_ids)}")

    res: List[Dict[str, Any]] = []
    with pg_config.make_connection() as conn:
        for chunk in chunked(user_ids, chunksize):
            try:
                with conn.cursor() as c:
                    c.execute(
                        query=f"""
                            SELECT  u.id AS user_id, u.product_id, 
                                    bp.team_id
                            FROM thl_user u
                            INNER JOIN userprofile_brokerageproduct AS bp
                                ON bp.id = u.product_id
                            WHERE u.id = ANY(%s);
                        """,
                        params=[list(chunk)],
                    )
                    res.extend(c.fetchall())

            except Exception:
                LOG.exception(f"annotate_product_and_team_id: {chunk}")
                raise

    dfu = pd.DataFrame(res, columns=["user_id", "product_id", "team_id"])

    return df.merge(dfu, on="user_id", how="left")


def annotate_product_user(
    df: pd.DataFrame, pg_config: PostgresConfig, chunksize: PositiveInt = 500
) -> pd.DataFrame:
    LOG.info(f"annotate_product_user.chunk: {df.shape}")
    assert "user_id" in df.columns, "must have a user_id column to join on"

    user_ids = df["user_id"].dropna()
    user_ids = set(user_ids)
    assert len(user_ids) >= 1, "must have user_ids"
    LOG.warning(f"annotate_product_user.len(user_ids): {len(user_ids)}")

    res: List[Dict[str, Any]] = []
    with pg_config.make_connection() as conn:
        for chunk in chunked(user_ids, chunksize):
            try:
                with conn.cursor() as c:
                    c.execute(
                        query="""
                            SELECT u.id AS user_id, u.product_user_id 
                            FROM thl_user u
                            WHERE u.id = ANY(%s);
                        """,
                        params=[list(chunk)],
                    )
                    res.extend(c.fetchall())

            except Exception:
                LOG.exception(f"annotate_product_user: {chunk}")
                raise

    dfu = pd.DataFrame(res, columns=["user_id", "product_user_id"])

    return df.merge(dfu, on="user_id", how="left")
