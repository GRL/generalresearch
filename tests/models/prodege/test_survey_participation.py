from datetime import timezone, datetime, timedelta


class TestProdegeParticipation:

    def test_exclude(self):
        from generalresearch.models.prodege import ProdegePastParticipationType
        from generalresearch.models.prodege.survey import (
            ProdegePastParticipation,
            ProdegeUserPastParticipation,
        )

        now = datetime.now(tz=timezone.utc)
        pp = ProdegePastParticipation.from_api(
            {
                "participation_project_ids": [152677146, 152803285],
                "filter_type": "exclude",
                "in_past_days": 7,
                "participation_types": ["complete"],
            }
        )
        # User has no history, so is eligible
        assert pp.is_eligible([])

        # user abandoned. its a click, not complete, so he's eligible
        upps = [
            ProdegeUserPastParticipation(
                started=now - timedelta(hours=69), survey_id="152677146"
            )
        ]
        assert pp.is_eligible(upps)

        # user completes. ineligible
        upps = [
            ProdegeUserPastParticipation(
                started=now - timedelta(hours=69),
                survey_id="152677146",
                ext_status_code_1="1",
            )
        ]
        assert not pp.is_eligible(upps)

        # user completed. but too long ago
        upps = [
            ProdegeUserPastParticipation(
                started=now - timedelta(days=100),
                survey_id="152677146",
                ext_status_code_1="1",
            )
        ]
        assert pp.is_eligible(upps)

        # remove day filter, should be ineligble again
        pp = ProdegePastParticipation.from_api(
            {
                "participation_project_ids": [152677146, 152803285],
                "filter_type": "exclude",
                "in_past_days": 0,
                "participation_types": ["complete"],
            }
        )
        assert not pp.is_eligible(upps)

        # I almost forgot this.... a "complete" IS ALSO A "click"!!!
        pp = ProdegePastParticipation.from_api(
            {
                "participation_project_ids": [152677146, 152803285],
                "filter_type": "exclude",
                "in_past_days": 0,
                "participation_types": ["click"],
            }
        )
        upps = [
            ProdegeUserPastParticipation(
                started=now - timedelta(hours=69),
                survey_id="152677146",
                ext_status_code_1="1",
            )
        ]
        assert {
            ProdegePastParticipationType.COMPLETE,
            ProdegePastParticipationType.CLICK,
        } == upps[0].participation_types
        assert not pp.is_eligible(upps)

    def test_include(self):
        from generalresearch.models.prodege.survey import (
            ProdegePastParticipation,
            ProdegeUserPastParticipation,
        )

        now = datetime.now(tz=timezone.utc)
        pp = ProdegePastParticipation.from_api(
            {
                "participation_project_ids": [152677146, 152803285],
                "filter_type": "include",
                "in_past_days": 7,
                "participation_types": ["complete"],
            }
        )
        # User has no history, so is IN-eligible
        assert not pp.is_eligible([])

        # user abandoned. its a click, not complete, so he's INeligible
        upps = [
            ProdegeUserPastParticipation(
                started=now - timedelta(hours=69), survey_id="152677146"
            )
        ]
        assert not pp.is_eligible(upps)

        # user completes, eligible
        upps = [
            ProdegeUserPastParticipation(
                started=now - timedelta(hours=69),
                survey_id="152677146",
                ext_status_code_1="1",
            )
        ]
        assert pp.is_eligible(upps)
