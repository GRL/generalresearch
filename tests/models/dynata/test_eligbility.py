from datetime import datetime, timezone


class TestEligibility:

    def test_evaluate_task_criteria(self):
        from generalresearch.models.dynata.survey import (
            DynataQuotaGroup,
            DynataFilterGroup,
            DynataSurvey,
            DynataRequirements,
        )

        filters = [[["a", "b"], ["c", "d"]], [["e"], ["f"]]]
        filters = [DynataFilterGroup.model_validate(f) for f in filters]
        criteria_evaluation = {
            "a": True,
            "b": True,
            "c": True,
            "d": False,
            "e": True,
            "f": True,
        }
        quotas = [
            DynataQuotaGroup.model_validate(
                [{"count": 100, "condition_hashes": [], "status": "OPEN"}]
            )
        ]
        task = DynataSurvey.model_validate(
            {
                "survey_id": "1",
                "filters": filters,
                "quotas": quotas,
                "allowed_devices": set("1"),
                "calculation_type": "COMPLETES",
                "client_id": "",
                "country_iso": "us",
                "language_iso": "eng",
                "group_id": "g1",
                "project_id": "p1",
                "status": "OPEN",
                "project_exclusions": set(),
                "created": datetime.now(tz=timezone.utc),
                "category_exclusions": set(),
                "category_ids": set(),
                "cpi": 1,
                "days_in_field": 0,
                "expected_count": 0,
                "order_number": "",
                "live_link": "",
                "bid_ir": 0.5,
                "bid_loi": 500,
                "requirements": DynataRequirements(),
            }
        )
        assert task.determine_eligibility(criteria_evaluation)

        # task status
        task.status = "CLOSED"
        assert not task.determine_eligibility(criteria_evaluation)
        task.status = "OPEN"

        # one quota with no space left (count = 0)
        quotas = [
            DynataQuotaGroup.model_validate(
                [{"count": 0, "condition_hashes": [], "status": "OPEN"}]
            )
        ]
        task.quotas = quotas
        assert not task.determine_eligibility(criteria_evaluation)

        # we pass 'a' and 'b'
        quotas = [
            DynataQuotaGroup.model_validate(
                [{"count": 100, "condition_hashes": ["a", "b"], "status": "OPEN"}]
            )
        ]
        task.quotas = quotas
        assert task.determine_eligibility(criteria_evaluation)

        # make 'f' false, we still pass the 2nd filtergroup b/c 'e' is True
        criteria_evaluation = {
            "a": True,
            "b": True,
            "c": True,
            "d": False,
            "e": True,
            "f": False,
        }
        assert task.determine_eligibility(criteria_evaluation)

        # make 'e' false, we don't pass the 2nd filtergroup
        criteria_evaluation = {
            "a": True,
            "b": True,
            "c": True,
            "d": False,
            "e": False,
            "f": False,
        }
        assert not task.determine_eligibility(criteria_evaluation)

        # We fail quota 'c','d', but we pass 'a','b', so we pass the first quota group
        criteria_evaluation = {
            "a": True,
            "b": True,
            "c": True,
            "d": False,
            "e": True,
            "f": True,
        }
        quotas = [
            DynataQuotaGroup.model_validate(
                [
                    {"count": 100, "condition_hashes": ["a", "b"], "status": "OPEN"},
                    {"count": 100, "condition_hashes": ["c", "d"], "status": "CLOSED"},
                ]
            )
        ]
        task.quotas = quotas
        assert task.determine_eligibility(criteria_evaluation)

        # we pass the first qg, but then fall into a full 2nd qg
        quotas = [
            DynataQuotaGroup.model_validate(
                [
                    {"count": 100, "condition_hashes": ["a", "b"], "status": "OPEN"},
                    {"count": 100, "condition_hashes": ["c", "d"], "status": "CLOSED"},
                ]
            ),
            DynataQuotaGroup.model_validate(
                [{"count": 100, "condition_hashes": ["f"], "status": "CLOSED"}]
            ),
        ]
        task.quotas = quotas
        assert not task.determine_eligibility(criteria_evaluation)

    def test_soft_pair(self):
        from generalresearch.models.dynata.survey import (
            DynataQuotaGroup,
            DynataFilterGroup,
            DynataSurvey,
            DynataRequirements,
        )

        filters = [[["a", "b"], ["c", "d"]], [["e"], ["f"]]]
        filters = [DynataFilterGroup.model_validate(f) for f in filters]
        criteria_evaluation = {
            "a": True,
            "b": True,
            "c": True,
            "d": False,
            "e": True,
            "f": True,
        }
        quotas = [
            DynataQuotaGroup.model_validate(
                [{"count": 100, "condition_hashes": [], "status": "OPEN"}]
            )
        ]
        task = DynataSurvey.model_validate(
            {
                "survey_id": "1",
                "filters": filters,
                "quotas": quotas,
                "allowed_devices": set("1"),
                "calculation_type": "COMPLETES",
                "client_id": "",
                "country_iso": "us",
                "language_iso": "eng",
                "group_id": "g1",
                "project_id": "p1",
                "status": "OPEN",
                "project_exclusions": set(),
                "created": datetime.now(tz=timezone.utc),
                "category_exclusions": set(),
                "category_ids": set(),
                "cpi": 1,
                "days_in_field": 0,
                "expected_count": 0,
                "order_number": "",
                "live_link": "",
                "bid_ir": 0.5,
                "bid_loi": 500,
                "requirements": DynataRequirements(),
            }
        )
        assert task.passes_filters(criteria_evaluation)
        passes, condition_hashes = task.passes_filters_soft(criteria_evaluation)
        assert passes

        # make 'e' & 'f' None, we don't pass the 2nd filtergroup
        criteria_evaluation = {
            "a": True,
            "b": True,
            "c": True,
            "d": False,
            "e": None,
            "f": None,
        }
        assert not task.passes_filters(criteria_evaluation)
        passes, conditional_hashes = task.passes_filters_soft(criteria_evaluation)
        assert passes is None
        assert {"e", "f"} == conditional_hashes

        # 1st filtergroup unknown
        criteria_evaluation = {
            "a": True,
            "b": None,
            "c": None,
            "d": None,
            "e": None,
            "f": None,
        }
        assert not task.passes_filters(criteria_evaluation)
        passes, conditional_hashes = task.passes_filters_soft(criteria_evaluation)
        assert passes is None
        assert {"b", "c", "d", "e", "f"} == conditional_hashes

        # 1st filtergroup unknown, 2nd cell False
        criteria_evaluation = {
            "a": True,
            "b": None,
            "c": None,
            "d": False,
            "e": None,
            "f": None,
        }
        assert not task.passes_filters(criteria_evaluation)
        passes, conditional_hashes = task.passes_filters_soft(criteria_evaluation)
        assert passes is None
        assert {"b", "e", "f"} == conditional_hashes

        # we pass the first qg, unknown 2nd
        criteria_evaluation = {
            "a": True,
            "b": True,
            "c": None,
            "d": False,
            "e": None,
            "f": None,
        }
        quotas = [
            DynataQuotaGroup.model_validate(
                [
                    {"count": 100, "condition_hashes": ["a", "b"], "status": "OPEN"},
                    {"count": 100, "condition_hashes": ["c", "d"], "status": "CLOSED"},
                ]
            ),
            DynataQuotaGroup.model_validate(
                [{"count": 100, "condition_hashes": ["f"], "status": "OPEN"}]
            ),
        ]
        task.quotas = quotas
        passes, conditional_hashes = task.passes_quotas_soft(criteria_evaluation)
        assert passes is None
        assert {"f"} == conditional_hashes

        # both quota groups unknown
        criteria_evaluation = {
            "a": True,
            "b": None,
            "c": None,
            "d": False,
            "e": None,
            "g": None,
        }
        quotas = [
            DynataQuotaGroup.model_validate(
                [
                    {"count": 100, "condition_hashes": ["a", "b"], "status": "OPEN"},
                    {"count": 100, "condition_hashes": ["c", "d"], "status": "CLOSED"},
                ]
            ),
            DynataQuotaGroup.model_validate(
                [{"count": 100, "condition_hashes": ["g"], "status": "OPEN"}]
            ),
        ]
        task.quotas = quotas
        passes, conditional_hashes = task.passes_quotas_soft(criteria_evaluation)
        assert passes is None
        assert {"b", "g"} == conditional_hashes

        passes, conditional_hashes = task.determine_eligibility_soft(
            criteria_evaluation
        )
        assert passes is None
        assert {"b", "e", "f", "g"} == conditional_hashes

    # def x(self):
    #     # ----
    #     c1 = DynataCondition(question_id='gender', values=['male'], value_type=ConditionValueType.LIST)  # 718f759
    #     c2 = DynataCondition(question_id='age', values=['18-24'], value_type=ConditionValueType.RANGE)  # 7a7b290
    #     obj1 = DynataFilterObject(cells=[c1.criterion_hash, c2.criterion_hash])
    #
    #     c3 = DynataCondition(question_id='gender', values=['female'], value_type=ConditionValueType.LIST)  # 38fa4e1
    #     c4 = DynataCondition(question_id='age', values=['35-45'], value_type=ConditionValueType.RANGE)  # e4f06fa
    #     obj2 = DynataFilterObject(cells=[c3.criterion_hash, c4.criterion_hash])
    #
    #     grp1 = DynataFilterGroup(objects=[obj1, obj2])
    #
    #     # -----
    #     c5 = DynataCondition(question_id='ethnicity', values=['white'], value_type=ConditionValueType.LIST)  # eb9b9a4
    #     obj3 = DynataFilterObject(cells=[c5.criterion_hash])
    #
    #     c6 = DynataCondition(question_id='ethnicity', values=['black'], value_type=ConditionValueType.LIST)  # 039fe2d
    #     obj4 = DynataFilterObject(cells=[c6.criterion_hash])
    #
    #     grp2 = DynataFilterGroup(objects=[obj3, obj4])
    #     # -----
    #     q1 = DynataQuota(count=5, status=DynataStatus.OPEN,
    #                      condition_hashes=[c1.criterion_hash, c2.criterion_hash])
    #     q2 = DynataQuota(count=10, status=DynataStatus.CLOSED,
    #                      condition_hashes=[c3.criterion_hash, c4.criterion_hash])
    #     qg1 = DynataQuotaGroup(cells=[q1, q2])
    #     # ----
    #
    #     s = DynataSurvey(survey_id='123', status=DynataStatus.OPEN, country_iso='us',
    #                      language_iso='eng', group_id='123', client_id='123', project_id='12',
    #                      filters=[grp1, grp2],
    #                      quotas=[qg1])
    #     ce = {'718f759': True, '7a7b290': True, 'eb9b9a4': True}
    #     s.passes_filters(ce)
    #     s.passes_quotas(ce)
