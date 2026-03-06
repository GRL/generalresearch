import uuid

import pytest
from cachetools.keys import _HashedTuple

from generalresearch.models import Source
from generalresearch.models.thl.survey.penalty import (
    BPSurveyPenalty,
    TeamSurveyPenalty,
)


@pytest.fixture
def product_uuid() -> str:
    # Nothing touches the db here, we don't need actual products or teams
    return uuid.uuid4().hex


@pytest.fixture
def team_uuid() -> str:
    # Nothing touches the db here, we don't need actual products or teams
    return uuid.uuid4().hex


@pytest.fixture
def penalties(product_uuid, team_uuid):
    return [
        BPSurveyPenalty(
            source=Source.TESTING, survey_id="a", penalty=0.1, product_id=product_uuid
        ),
        BPSurveyPenalty(
            source=Source.TESTING, survey_id="b", penalty=0.2, product_id=product_uuid
        ),
        TeamSurveyPenalty(
            source=Source.TESTING, survey_id="a", penalty=1, team_id=team_uuid
        ),
        TeamSurveyPenalty(
            source=Source.TESTING, survey_id="c", penalty=1, team_id=team_uuid
        ),
        # Source.TESTING:a is different from Source.TESTING2:a !
        BPSurveyPenalty(
            source=Source.TESTING2, survey_id="a", penalty=0.5, product_id=product_uuid
        ),
        # For a random BP, should not do anything
        BPSurveyPenalty(
            source=Source.TESTING, survey_id="b", penalty=1, product_id=uuid.uuid4().hex
        ),
    ]


class TestSurveyPenalty:
    def test(self, surveypenalty_manager, penalties, product_uuid, team_uuid):
        surveypenalty_manager.set_penalties(penalties)

        res = surveypenalty_manager.get_penalties_for(
            product_id=product_uuid, team_id=team_uuid
        )
        assert res == {"t:a": 1.0, "t:b": 0.2, "t:c": 1, "u:a": 0.5}

        # We can update penalties for a marketplace and not erase them for another.
        # But remember, marketplace is batched, so it'll overwrite the previous
        #   values for that marketplace
        penalties = [
            BPSurveyPenalty(
                source=Source.TESTING2,
                survey_id="b",
                penalty=0.1,
                product_id=product_uuid,
            )
        ]
        surveypenalty_manager.set_penalties(penalties)
        res = surveypenalty_manager.get_penalties_for(
            product_id=product_uuid, team_id=team_uuid
        )
        assert res == {"t:a": 1.0, "t:b": 0.2, "t:c": 1, "u:b": 0.1}

        # Team id doesn't exist, so it should return the product's penalties
        team_id_random = uuid.uuid4().hex
        surveypenalty_manager.cache.clear()
        res = surveypenalty_manager.get_penalties_for(
            product_id=product_uuid, team_id=team_id_random
        )
        assert res == {"t:a": 0.1, "t:b": 0.2, "u:b": 0.1}

        # Assert it is cached (no redis lookup needed)
        assert surveypenalty_manager.cache.currsize == 1
        res = surveypenalty_manager.get_penalties_for(
            product_id=product_uuid, team_id=team_id_random
        )
        assert res == {"t:a": 0.1, "t:b": 0.2, "u:b": 0.1}
        assert surveypenalty_manager.cache.currsize == 1
        cached_key = tuple(list(list(surveypenalty_manager.cache.keys())[0])[1:])
        assert cached_key == tuple(
            ["product_id", product_uuid, "team_id", team_id_random]
        )

        # Both don't exist, return nothing
        res = surveypenalty_manager.get_penalties_for(
            product_id=uuid.uuid4().hex, team_id=uuid.uuid4().hex
        )
        assert res == {}
