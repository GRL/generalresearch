import copy
import logging
from datetime import timezone, datetime
from decimal import Decimal

from pymysql import IntegrityError


logger = logging.getLogger()

example_survey_api_response = {
    "survey_id": 29333264,
    "survey_name": "#29333264",
    "survey_status": 22,
    "field_end_date": datetime(2024, 5, 23, 18, 18, 31, tzinfo=timezone.utc),
    "category": "Exciting New",
    "category_code": 232,
    "crtd_on": datetime(2024, 5, 20, 17, 48, 13, tzinfo=timezone.utc),
    "mod_on": datetime(2024, 5, 20, 18, 18, 31, tzinfo=timezone.utc),
    "soft_launch": False,
    "click_balancing": 0,
    "price_type": 1,
    "pii": False,
    "buyer_message": "",
    "buyer_id": 4726,
    "incl_excl": 0,
    "cpi": Decimal("1.20"),
    "last_complete_date": None,
    "project_last_complete_date": None,
    "quotas": [
        {
            "quota_id": "c2bc961e-4f26-4223-b409-ebe9165cfdf5",
            "quantities": {"currently_open": 491, "remaining": 495, "achieved": 0},
            "criteria": [
                {
                    "qualification_code": 214,
                    "range_sets": [{"units": 311, "to": 64, "from": 18}],
                }
            ],
        }
    ],
    "qualifications": [
        {
            "range_sets": [{"units": 311, "to": 64, "from": 18}],
            "qualification_code": 212,
        },
        {"condition_codes": ["111", "117", "112"], "qualification_code": 1202},
    ],
    "country_iso": "fr",
    "language_iso": "fre",
    "bid_ir": 0.4,
    "bid_loi": 600,
    "overall_ir": None,
    "overall_loi": None,
    "last_block_ir": None,
    "last_block_loi": None,
    "survey_exclusions": set(),
    "exclusion_period": 0,
}


class TestSpectrumSurvey:

    def test_survey_create(self, settings, spectrum_manager, spectrum_rw):
        from generalresearch.models.spectrum.survey import SpectrumSurvey

        assert settings.debug, "CRITICAL: Do not run this on production."

        now = datetime.now(tz=timezone.utc)
        spectrum_rw.execute_sql_query(
            query=f"""
                DELETE FROM `{spectrum_rw.db}`.spectrum_survey
                WHERE survey_id = '29333264'""",
            commit=True,
        )

        d = example_survey_api_response.copy()
        s = SpectrumSurvey.from_api(d)
        spectrum_manager.create(s)

        surveys = spectrum_manager.get_survey_library(updated_since=now)
        assert len(surveys) == 1
        assert "29333264" == surveys[0].survey_id
        assert s.is_unchanged(surveys[0])

        try:
            spectrum_manager.create(s)
        except IntegrityError as e:
            print(e.args)

    def test_survey_update(self, settings, spectrum_manager, spectrum_rw):
        from generalresearch.models.spectrum.survey import SpectrumSurvey

        assert settings.debug, "CRITICAL: Do not run this on production."

        now = datetime.now(tz=timezone.utc)
        spectrum_rw.execute_sql_query(
            query=f"""
                DELETE FROM `{spectrum_rw.db}`.spectrum_survey
                WHERE survey_id = '29333264'
            """,
            commit=True,
        )
        d = copy.deepcopy(example_survey_api_response)
        s = SpectrumSurvey.from_api(d)
        print(s)

        spectrum_manager.create(s)
        s.cpi = Decimal("0.50")
        spectrum_manager.update([s])
        surveys = spectrum_manager.get_survey_library(updated_since=now)
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
        spectrum_manager.update([s])
        surveys = spectrum_manager.get_survey_library(updated_since=now)
        assert 600 == surveys[0].bid_loi
        assert 1000 == surveys[0].overall_loi
        assert 1000 == surveys[0].last_block_loi
