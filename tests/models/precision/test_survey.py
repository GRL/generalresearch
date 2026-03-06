class TestPrecisionQuota:

    def test_quota_passes(self):
        from generalresearch.models.precision.survey import PrecisionSurvey
        from tests.models.precision import survey_json

        s = PrecisionSurvey.model_validate(survey_json)
        q = s.quotas[0]
        ce = {k: True for k in ["b41e1a3", "bc89ee8", "4124366", "9f32c61"]}
        assert q.matches(ce)

        ce["b41e1a3"] = False
        assert not q.matches(ce)

        ce.pop("b41e1a3")
        assert not q.matches(ce)
        assert not q.matches({})

    def test_quota_passes_closed(self):
        from generalresearch.models.precision import PrecisionStatus
        from generalresearch.models.precision.survey import PrecisionSurvey
        from tests.models.precision import survey_json

        s = PrecisionSurvey.model_validate(survey_json)
        q = s.quotas[0]
        q.status = PrecisionStatus.CLOSED
        ce = {k: True for k in ["b41e1a3", "bc89ee8", "4124366", "9f32c61"]}
        # We still match, but the quota is not open
        assert q.matches(ce)
        assert not q.is_open


class TestPrecisionSurvey:

    def test_passes(self):
        from generalresearch.models.precision.survey import PrecisionSurvey
        from tests.models.precision import survey_json

        s = PrecisionSurvey.model_validate(survey_json)
        ce = {k: True for k in ["b41e1a3", "bc89ee8", "4124366", "9f32c61"]}
        assert s.determine_eligibility(ce)

    def test_elig_closed_quota(self):
        from generalresearch.models.precision import PrecisionStatus
        from generalresearch.models.precision.survey import PrecisionSurvey
        from tests.models.precision import survey_json

        s = PrecisionSurvey.model_validate(survey_json)
        ce = {k: True for k in ["b41e1a3", "bc89ee8", "4124366", "9f32c61"]}
        q = s.quotas[0]
        q.status = PrecisionStatus.CLOSED
        # We match a closed quota
        assert not s.determine_eligibility(ce)

        s.quotas[0].status = PrecisionStatus.OPEN
        s.quotas[1].status = PrecisionStatus.CLOSED
        # Now me match an open quota and dont match the closed quota, so we should be eligible
        assert s.determine_eligibility(ce)

    def test_passes_sp(self):
        from generalresearch.models.precision import PrecisionStatus
        from generalresearch.models.precision.survey import PrecisionSurvey
        from tests.models.precision import survey_json

        s = PrecisionSurvey.model_validate(survey_json)
        ce = {k: True for k in ["b41e1a3", "bc89ee8", "4124366", "9f32c61"]}
        passes, hashes = s.determine_eligibility_soft(ce)

        # We don't know if we match the 2nd quota, but it is open so it doesn't matter
        assert passes
        assert (True, []) == s.quotas[0].matches_soft(ce)
        assert (None, ["0cdc304", "500af2c"]) == s.quotas[1].matches_soft(ce)

        # Now don't know if we match either
        ce.pop("9f32c61")  # age
        passes, hashes = s.determine_eligibility_soft(ce)
        assert passes is None
        assert {"500af2c", "9f32c61", "0cdc304"} == hashes

        ce["9f32c61"] = False
        ce["0cdc304"] = False
        # We know we don't match either
        assert (False, set()) == s.determine_eligibility_soft(ce)

        # We pass 1st quota, 2nd is unknown but closed, so we don't know for sure we pass
        ce = {k: True for k in ["b41e1a3", "bc89ee8", "4124366", "9f32c61"]}
        s.quotas[1].status = PrecisionStatus.CLOSED
        assert (None, {"0cdc304", "500af2c"}) == s.determine_eligibility_soft(ce)
