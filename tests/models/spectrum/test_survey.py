from datetime import timezone, datetime
from decimal import Decimal


class TestSpectrumCondition:

    def test_condition_create(self):
        from generalresearch.models import LogicalOperator
        from generalresearch.models.spectrum.survey import (
            SpectrumCondition,
        )
        from generalresearch.models.thl.survey.condition import ConditionValueType

        c = SpectrumCondition.from_api(
            {
                "qualification_code": 212,
                "range_sets": [
                    {"units": 311, "to": 28, "from": 25},
                    {"units": 311, "to": 42, "from": 40},
                ],
            }
        )
        assert (
            SpectrumCondition(
                question_id="212",
                values=["25-28", "40-42"],
                value_type=ConditionValueType.RANGE,
                negate=False,
                logical_operator=LogicalOperator.OR,
            )
            == c
        )

        # These equal each other b/c age ranges get automatically converted
        assert (
            SpectrumCondition(
                question_id="212",
                values=["25", "26", "27", "28", "40", "41", "42"],
                value_type=ConditionValueType.LIST,
                negate=False,
                logical_operator=LogicalOperator.OR,
            )
            == c
        )

        c = SpectrumCondition.from_api(
            {
                "condition_codes": ["111", "117", "112", "113", "118"],
                "qualification_code": 1202,
            }
        )
        assert (
            SpectrumCondition(
                question_id="1202",
                values=["111", "112", "113", "117", "118"],
                value_type=ConditionValueType.LIST,
                negate=False,
                logical_operator=LogicalOperator.OR,
            )
            == c
        )


class TestSpectrumQuota:

    def test_quota_create(self):
        from generalresearch.models.spectrum.survey import (
            SpectrumCondition,
            SpectrumQuota,
        )

        d = {
            "quota_id": "a846b545-4449-4d76-93a2-f8ebdf6e711e",
            "quantities": {"currently_open": 57, "remaining": 57, "achieved": 0},
            "criteria": [{"qualification_code": 211, "condition_codes": ["111"]}],
            "crtd_on": 1716227282077,
            "mod_on": 1716227284146,
            "last_complete_date": None,
        }
        criteria = [SpectrumCondition.from_api(q) for q in d["criteria"]]
        d["condition_hashes"] = [x.criterion_hash for x in criteria]
        q = SpectrumQuota.from_api(d)
        assert SpectrumQuota(remaining_count=57, condition_hashes=["c23c0b9"]) == q
        assert q.is_open

    def test_quota_passes(self):
        from generalresearch.models.spectrum.survey import (
            SpectrumQuota,
        )

        q = SpectrumQuota(remaining_count=57, condition_hashes=["a"])
        assert q.passes({"a": True})
        assert not q.passes({"a": False})
        assert not q.passes({})

        # We have to match all
        q = SpectrumQuota(remaining_count=57, condition_hashes=["a", "b", "c"])
        assert not q.passes({"a": True, "b": False})
        assert q.passes({"a": True, "b": True, "c": True})

        # Quota must be open, even if we match
        q = SpectrumQuota(remaining_count=0, condition_hashes=["a"])
        assert not q.passes({"a": True})

    def test_quota_passes_soft(self):
        from generalresearch.models.spectrum.survey import (
            SpectrumQuota,
        )

        q = SpectrumQuota(remaining_count=57, condition_hashes=["a", "b", "c"])
        # Pass if we match all
        assert (True, set()) == q.matches_soft({"a": True, "b": True, "c": True})
        # Fail if we don't match any
        assert (False, set()) == q.matches_soft({"a": True, "b": False, "c": None})
        # Unknown if any are unknown AND we don't fail any
        assert (None, {"c", "b"}) == q.matches_soft({"a": True, "b": None, "c": None})
        assert (None, {"a", "c", "b"}) == q.matches_soft(
            {"a": None, "b": None, "c": None}
        )
        assert (False, set()) == q.matches_soft({"a": None, "b": False, "c": None})


class TestSpectrumSurvey:
    def test_survey_create(self):
        from generalresearch.models import (
            LogicalOperator,
            Source,
            TaskCalculationType,
        )
        from generalresearch.models.spectrum import SpectrumStatus
        from generalresearch.models.spectrum.survey import (
            SpectrumCondition,
            SpectrumQuota,
            SpectrumSurvey,
        )
        from generalresearch.models.thl.survey.condition import ConditionValueType

        # Note: d is the raw response after calling SpectrumAPI.preprocess_survey() on it!
        d = {
            "survey_id": 29333264,
            "survey_name": "Exciting New Survey #29333264",
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
            "cpi": Decimal("1.20000"),
            "last_complete_date": None,
            "project_last_complete_date": None,
            "survey_performance": {
                "overall": {"ir": 40, "loi": 10},
                "last_block": {"ir": None, "loi": None},
            },
            "supplier_completes": {
                "needed": 495,
                "achieved": 0,
                "remaining": 495,
                "guaranteed_allocation": 0,
                "guaranteed_allocation_remaining": 0,
            },
            "pds": {"enabled": False, "buyer_name": None},
            "quotas": [
                {
                    "quota_id": "c2bc961e-4f26-4223-b409-ebe9165cfdf5",
                    "quantities": {
                        "currently_open": 491,
                        "remaining": 495,
                        "achieved": 0,
                    },
                    "criteria": [
                        {
                            "qualification_code": 212,
                            "range_sets": [{"units": 311, "to": 64, "from": 18}],
                        }
                    ],
                    "crtd_on": 1716227293496,
                    "mod_on": 1716229289847,
                    "last_complete_date": None,
                }
            ],
            "qualifications": [
                {
                    "range_sets": [{"units": 311, "to": 64, "from": 18}],
                    "qualification_code": 212,
                }
            ],
            "country_iso": "fr",
            "language_iso": "fre",
            "bid_ir": 0.4,
            "bid_loi": 600,
            "last_block_ir": None,
            "last_block_loi": None,
            "survey_exclusions": set(),
            "exclusion_period": 0,
        }
        s = SpectrumSurvey.from_api(d)
        expected_survey = SpectrumSurvey(
            cpi=Decimal("1.20000"),
            country_isos=["fr"],
            language_isos=["fre"],
            buyer_id="4726",
            source=Source.SPECTRUM,
            used_question_ids={"212"},
            survey_id="29333264",
            survey_name="Exciting New Survey #29333264",
            status=SpectrumStatus.LIVE,
            field_end_date=datetime(2024, 5, 23, 18, 18, 31, tzinfo=timezone.utc),
            category_code="232",
            calculation_type=TaskCalculationType.COMPLETES,
            requires_pii=False,
            survey_exclusions=set(),
            exclusion_period=0,
            bid_ir=0.40,
            bid_loi=600,
            last_block_loi=None,
            last_block_ir=None,
            overall_loi=None,
            overall_ir=None,
            project_last_complete_date=None,
            country_iso="fr",
            language_iso="fre",
            include_psids=None,
            exclude_psids=None,
            qualifications=["77f493d"],
            quotas=[SpectrumQuota(remaining_count=491, condition_hashes=["77f493d"])],
            conditions={
                "77f493d": SpectrumCondition(
                    logical_operator=LogicalOperator.OR,
                    value_type=ConditionValueType.RANGE,
                    negate=False,
                    question_id="212",
                    values=["18-64"],
                )
            },
            created_api=datetime(2024, 5, 20, 17, 48, 13, tzinfo=timezone.utc),
            modified_api=datetime(2024, 5, 20, 18, 18, 31, tzinfo=timezone.utc),
            updated=None,
        )
        assert expected_survey.model_dump_json() == s.model_dump_json()

    def test_survey_properties(self):
        from generalresearch.models.spectrum.survey import (
            SpectrumSurvey,
        )

        d = {
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
            "cpi": Decimal("1.20000"),
            "last_complete_date": None,
            "project_last_complete_date": None,
            "quotas": [
                {
                    "quota_id": "c2bc961e-4f26-4223-b409-ebe9165cfdf5",
                    "quantities": {
                        "currently_open": 491,
                        "remaining": 495,
                        "achieved": 0,
                    },
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
            "overall_ir": 0.4,
            "overall_loi": 600,
            "last_block_ir": None,
            "last_block_loi": None,
            "survey_exclusions": set(),
            "exclusion_period": 0,
        }
        s = SpectrumSurvey.from_api(d)
        assert {"212", "1202", "214"} == s.used_question_ids
        assert s.is_live
        assert s.is_open
        assert {"38cea5e", "83955ef", "77f493d"} == s.all_hashes

    def test_survey_eligibility(self):
        from generalresearch.models.spectrum.survey import (
            SpectrumQuota,
            SpectrumSurvey,
        )

        d = {
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
            "cpi": Decimal("1.20000"),
            "last_complete_date": None,
            "project_last_complete_date": None,
            "quotas": [],
            "qualifications": [],
            "country_iso": "fr",
            "language_iso": "fre",
            "overall_ir": 0.4,
            "overall_loi": 600,
            "last_block_ir": None,
            "last_block_loi": None,
            "survey_exclusions": set(),
            "exclusion_period": 0,
        }
        s = SpectrumSurvey.from_api(d)
        s.qualifications = ["a", "b", "c"]
        s.quotas = [
            SpectrumQuota(remaining_count=10, condition_hashes=["a", "b"]),
            SpectrumQuota(remaining_count=0, condition_hashes=["d"]),
            SpectrumQuota(remaining_count=10, condition_hashes=["e"]),
        ]

        assert s.passes_qualifications({"a": True, "b": True, "c": True})
        assert not s.passes_qualifications({"a": True, "b": True, "c": False})

        # we do NOT match a full quota, so we pass
        assert s.passes_quotas({"a": True, "b": True, "d": False})
        # We dont pass any
        assert not s.passes_quotas({})
        # we only pass a full quota
        assert not s.passes_quotas({"d": True})
        # we only dont pass a full quota, but we haven't passed any open
        assert not s.passes_quotas({"d": False})
        # we pass a quota, but also pass a full quota, so fail
        assert not s.passes_quotas({"e": True, "d": True})
        # we pass a quota, but are unknown in a full quota, so fail
        assert not s.passes_quotas({"e": True})

        # # Soft Pair
        assert (True, set()) == s.passes_qualifications_soft(
            {"a": True, "b": True, "c": True}
        )
        assert (False, set()) == s.passes_qualifications_soft(
            {"a": True, "b": True, "c": False}
        )
        assert (None, set("c")) == s.passes_qualifications_soft(
            {"a": True, "b": True, "c": None}
        )

        # we do NOT match a full quota, so we pass
        assert (True, set()) == s.passes_quotas_soft({"a": True, "b": True, "d": False})
        # We dont pass any
        assert (None, {"a", "b", "d", "e"}) == s.passes_quotas_soft({})
        # we only pass a full quota
        assert (False, set()) == s.passes_quotas_soft({"d": True})
        # we only dont pass a full quota, but we haven't passed any open
        assert (None, {"a", "b", "e"}) == s.passes_quotas_soft({"d": False})
        # we pass a quota, but also pass a full quota, so fail
        assert (False, set()) == s.passes_quotas_soft({"e": True, "d": True})
        # we pass a quota, but are unknown in a full quota, so fail
        assert (None, {"d"}) == s.passes_quotas_soft({"e": True})

        assert s.determine_eligibility({"a": True, "b": True, "c": True, "d": False})
        assert not s.determine_eligibility(
            {"a": True, "b": True, "c": False, "d": False}
        )
        assert not s.determine_eligibility(
            {"a": True, "b": True, "c": None, "d": False}
        )
        assert (True, set()) == s.determine_eligibility_soft(
            {"a": True, "b": True, "c": True, "d": False}
        )
        assert (False, set()) == s.determine_eligibility_soft(
            {"a": True, "b": True, "c": False, "d": False}
        )
        assert (None, set("c")) == s.determine_eligibility_soft(
            {"a": True, "b": True, "c": None, "d": False}
        )
        assert (None, {"c", "d"}) == s.determine_eligibility_soft(
            {"a": True, "b": True, "c": None, "d": None}
        )
