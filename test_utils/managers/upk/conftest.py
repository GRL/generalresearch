import os
import time
from typing import Optional
from uuid import UUID

import pandas as pd
import pytest

from generalresearch.pg_helper import PostgresConfig


def insert_data_from_csv(
    thl_web_rw: PostgresConfig,
    table_name: str,
    fp: Optional[str] = None,
    disable_fk_checks: bool = False,
    df: Optional[pd.DataFrame] = None,
):
    assert fp is not None or df is not None and not (fp is not None and df is not None)
    if fp:
        df = pd.read_csv(fp, dtype=str)
    df = df.where(pd.notnull(df), None)
    cols = list(df.columns)
    col_str = ", ".join(cols)
    values_str = ", ".join(["%s"] * len(cols))
    if "id" in df.columns and len(df["id"].iloc[0]) == 36:
        df["id"] = df["id"].map(lambda x: UUID(x).hex)
    args = df.to_dict("tight")["data"]

    with thl_web_rw.make_connection() as conn:
        with conn.cursor() as c:
            if disable_fk_checks:
                c.execute("SET CONSTRAINTS ALL DEFERRED")
            c.executemany(
                f"INSERT INTO {table_name} ({col_str}) VALUES ({values_str})",
                params_seq=args,
            )
        conn.commit()


@pytest.fixture(scope="session")
def category_data(thl_web_rw, category_manager) -> None:
    fp = os.path.join(os.path.dirname(__file__), "marketplace_category.csv.gz")
    insert_data_from_csv(
        thl_web_rw,
        fp=fp,
        table_name="marketplace_category",
        disable_fk_checks=True,
    )
    # Don't strictly need to do this, but probably we should
    category_manager.populate_caches()
    cats = category_manager.categories.values()
    path_id = {c.path: c.id for c in cats}
    data = [
        {"id": c.id, "parent_id": path_id[c.parent_path]} for c in cats if c.parent_path
    ]
    query = """
    UPDATE marketplace_category
    SET parent_id = %(parent_id)s
    WHERE id = %(id)s;
    """
    with thl_web_rw.make_connection() as conn:
        with conn.cursor() as c:
            c.executemany(query=query, params_seq=data)
        conn.commit()


@pytest.fixture(scope="session")
def property_data(thl_web_rw) -> None:
    fp = os.path.join(os.path.dirname(__file__), "marketplace_property.csv.gz")
    insert_data_from_csv(thl_web_rw, fp=fp, table_name="marketplace_property")


@pytest.fixture(scope="session")
def item_data(thl_web_rw) -> None:
    fp = os.path.join(os.path.dirname(__file__), "marketplace_item.csv.gz")
    insert_data_from_csv(thl_web_rw, fp=fp, table_name="marketplace_item")


@pytest.fixture(scope="session")
def propertycategoryassociation_data(
    thl_web_rw, category_data, property_data, category_manager
) -> None:
    table_name = "marketplace_propertycategoryassociation"
    fp = os.path.join(os.path.dirname(__file__), f"{table_name}.csv.gz")
    # Need to lookup category pk from uuid
    category_manager.populate_caches()
    df = pd.read_csv(fp, dtype=str)
    df["category_id"] = df["category_id"].map(
        lambda x: category_manager.categories[x].id
    )
    insert_data_from_csv(thl_web_rw, df=df, table_name=table_name)


@pytest.fixture(scope="session")
def propertycountry_data(thl_web_rw, property_data) -> None:
    fp = os.path.join(os.path.dirname(__file__), "marketplace_propertycountry.csv.gz")
    insert_data_from_csv(thl_web_rw, fp=fp, table_name="marketplace_propertycountry")


@pytest.fixture(scope="session")
def propertymarketplaceassociation_data(thl_web_rw, property_data) -> None:
    table_name = "marketplace_propertymarketplaceassociation"
    fp = os.path.join(os.path.dirname(__file__), f"{table_name}.csv.gz")
    insert_data_from_csv(thl_web_rw, fp=fp, table_name=table_name)


@pytest.fixture(scope="session")
def propertyitemrange_data(thl_web_rw, property_data, item_data) -> None:
    table_name = "marketplace_propertyitemrange"
    fp = os.path.join(os.path.dirname(__file__), f"{table_name}.csv.gz")
    insert_data_from_csv(thl_web_rw, fp=fp, table_name=table_name)


@pytest.fixture(scope="session")
def question_data(thl_web_rw) -> None:
    table_name = "marketplace_question"
    fp = os.path.join(os.path.dirname(__file__), f"{table_name}.csv.gz")
    insert_data_from_csv(
        thl_web_rw, fp=fp, table_name=table_name, disable_fk_checks=True
    )


@pytest.fixture(scope="session")
def clear_upk_tables(thl_web_rw):
    tables = [
        "marketplace_propertyitemrange",
        "marketplace_propertymarketplaceassociation",
        "marketplace_propertycategoryassociation",
        "marketplace_category",
        "marketplace_item",
        "marketplace_property",
        "marketplace_propertycountry",
        "marketplace_question",
    ]
    table_str = ", ".join(tables)

    with thl_web_rw.make_connection() as conn:
        with conn.cursor() as c:
            c.execute(f"TRUNCATE {table_str} RESTART IDENTITY CASCADE;")
        conn.commit()


@pytest.fixture(scope="session")
def upk_data(
    clear_upk_tables,
    category_data,
    property_data,
    item_data,
    propertycategoryassociation_data,
    propertycountry_data,
    propertymarketplaceassociation_data,
    propertyitemrange_data,
    question_data,
) -> None:
    # Wait a second to make sure the HarmonizerCache refresh loop pulls these in
    time.sleep(2)


def test_fixtures(upk_data):
    pass
