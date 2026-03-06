import logging

import time

import pytest
from datetime import datetime, timezone
from generalresearch.managers.spectrum.survey import (
    SpectrumSurveyManager,
    SpectrumCriteriaManager,
)
from generalresearch.models.spectrum.survey import SpectrumSurvey
from generalresearch.sql_helper import SqlHelper

from .surveys_json import SURVEYS_JSON, CONDITIONS


@pytest.fixture(scope="session")
def spectrum_rw(settings) -> SqlHelper:
    print(f"{settings.spectrum_rw_db=}")
    logging.info(f"{settings.spectrum_rw_db=}")
    assert "/unittest-" in settings.spectrum_rw_db.path
    return SqlHelper(
        dsn=settings.spectrum_rw_db,
        read_timeout=2,
        write_timeout=1,
        connect_timeout=2,
    )


@pytest.fixture(scope="session")
def spectrum_criteria_manager(spectrum_rw) -> SpectrumCriteriaManager:
    assert "/unittest-" in spectrum_rw.dsn.path
    return SpectrumCriteriaManager(spectrum_rw)


@pytest.fixture(scope="session")
def spectrum_survey_manager(spectrum_rw) -> SpectrumSurveyManager:
    assert "/unittest-" in spectrum_rw.dsn.path
    return SpectrumSurveyManager(spectrum_rw)


@pytest.fixture(scope="session")
def setup_spectrum_surveys(
    spectrum_rw, spectrum_survey_manager, spectrum_criteria_manager
):
    now = datetime.now(timezone.utc)
    # make sure these example surveys exist in db
    surveys = [SpectrumSurvey.model_validate_json(x) for x in SURVEYS_JSON]
    for s in surveys:
        s.modified_api = datetime.now(tz=timezone.utc)
    spectrum_survey_manager.create_or_update(surveys)
    spectrum_criteria_manager.update(CONDITIONS)

    # and make sure they have allocation for 687
    spectrum_rw.execute_sql_query(
        f"""
    INSERT IGNORE INTO `{spectrum_rw.db}`.spectrum_supplier
    (supplier_id, name, api_key, secret_key, username, password)
    VALUES (%s, %s, %s, %s, %s, %s)""",
        ["687", "GRL", "x", "x", "x", "x"],
        commit=True,
    )
    supplier687_pk = spectrum_rw.execute_sql_query(
        f"""
    select id from `{spectrum_rw.db}`.spectrum_supplier where supplier_id = '687'"""
    )[0]["id"]
    conn = spectrum_rw.make_connection()
    c = conn.cursor()
    c.executemany(
        f"""
    INSERT IGNORE INTO `{spectrum_rw.db}`.spectrum_surveysupplier
    (created, surveySig, supplier_id, survey_id)
    VALUES (%s, %s, %s, %s)
    """,
        [[now, "xxx", supplier687_pk, s.survey_id] for s in surveys],
    )
    conn.commit()
    # Wait a second to make sure the spectrum-grpc pulls these from the db into global-vars
    time.sleep(1)
