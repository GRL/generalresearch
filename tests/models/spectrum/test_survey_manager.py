from __future__ import annotations

import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from pymysql import IntegrityError

from generalresearch.config import is_debug

if TYPE_CHECKING:
    from generalresearch.managers.spectrum.survey import (
        SpectrumSurveyManager,
    )
    from generalresearch.sql_helper import SqlHelper

logger = logging.getLogger()


class TestSpectrumSurvey:

    def test_survey_create(
        self,
        spectrum_survey_manager: SpectrumSurveyManager,
        spectrum_rw: SqlHelper,
        spectrum_api_survey_json: dict[str, Any],
    ):
        from generalresearch.models.spectrum.survey import SpectrumSurvey

        assert is_debug(), "CRITICAL: Do not run this on production."

        now = datetime.now(tz=UTC)
        spectrum_rw.execute_sql_query(
            query=f"""
                DELETE FROM `{spectrum_rw.db}`.spectrum_survey
                WHERE survey_id = '29333264'""",
            commit=True,
        )

        s = SpectrumSurvey.from_api(spectrum_api_survey_json)
        assert isinstance(s, SpectrumSurvey)
        spectrum_survey_manager.create(s)

        surveys = spectrum_survey_manager.get_survey_library(updated_since=now)
        assert len(surveys) == 1
        assert "29333264" == surveys[0].survey_id
        assert s.is_unchanged(surveys[0])

        try:
            spectrum_survey_manager.create(s)
        except IntegrityError as e:
            print(e.args)

    def test_survey_update(
        self,
        spectrum_survey_manager: SpectrumSurveyManager,
        spectrum_rw: SqlHelper,
        spectrum_api_survey_json: dict[str, Any],
    ):
        from generalresearch.models.spectrum.survey import SpectrumSurvey

        assert is_debug(), "CRITICAL: Do not run this on production."

        now = datetime.now(tz=UTC)
        spectrum_rw.execute_sql_query(
            query=f"""
                DELETE FROM `{spectrum_rw.db}`.spectrum_survey
                WHERE survey_id = '29333264'
            """,
            commit=True,
        )
        s = SpectrumSurvey.from_api(spectrum_api_survey_json)
        assert isinstance(s, SpectrumSurvey)

        spectrum_survey_manager.create(s)
        s.cpi = Decimal("0.50")
        spectrum_survey_manager.update([s])
        surveys = spectrum_survey_manager.get_survey_library(updated_since=now)
        assert len(surveys) == 1
        assert "29333264" == surveys[0].survey_id
        assert Decimal("0.50") == surveys[0].cpi
        assert s.is_unchanged(surveys[0])

        #  --- Updating bid/overall/last block
        assert 600 == s.bid_loi
        assert s.overall_loi is None
        assert s.last_block_loi is None

        # now the last block is set
        s.bid_loi = None
        s.overall_loi = 1000
        s.last_block_loi = 1000
        spectrum_survey_manager.update([s])
        surveys = spectrum_survey_manager.get_survey_library(updated_since=now)
        assert 600 == surveys[0].bid_loi
        assert 1000 == surveys[0].overall_loi
        assert 1000 == surveys[0].last_block_loi
