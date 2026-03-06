import json

from generalresearch.models import Source
from generalresearch.models.legacy.bucket import (
    TopNPlusBucket,
    SurveyEligibilityCriterion,
    DurationSummary,
    PayoutSummary,
    BucketTask,
)


class TestOfferwallTopNAndStarwall:
    def test_45b7228a7(self):
        from generalresearch.models.legacy.offerwall import (
            TopNOfferWall,
            TopNOfferWallResponse,
            StarwallOfferWallResponse,
        )
        from tests.models.legacy.data import (
            RESPONSE_45b7228a7,
        )

        res = json.loads(RESPONSE_45b7228a7)
        assert TopNOfferWallResponse.model_validate(res)
        offerwall = TopNOfferWall.model_validate(res["offerwall"])
        assert offerwall
        offerwall.censor()
        # Format is identical to starwall
        assert StarwallOfferWallResponse.model_validate(res)

    def test_b145b803(self):
        from generalresearch.models.legacy.offerwall import (
            TopNPlusOfferWallResponse,
            StarwallPlusOfferWallResponse,
        )
        from tests.models.legacy.data import (
            RESPONSE_b145b803,
        )

        res = json.loads(RESPONSE_b145b803)
        assert TopNPlusOfferWallResponse.model_validate(res)
        assert StarwallPlusOfferWallResponse.model_validate(res)

    def test_d48cce47(self):
        from generalresearch.models.legacy.offerwall import (
            TopNPlusBlockOfferWallResponse,
            StarwallPlusBlockOfferWallResponse,
        )
        from tests.models.legacy.data import (
            RESPONSE_b145b803,
            RESPONSE_d48cce47,
        )

        res = json.loads(RESPONSE_d48cce47)  # this is a blocked user's response
        assert TopNPlusBlockOfferWallResponse.model_validate(res)
        assert StarwallPlusBlockOfferWallResponse.model_validate(res)
        # otherwise it is identical to the plus's response
        res = json.loads(RESPONSE_b145b803)
        assert TopNPlusBlockOfferWallResponse.model_validate(res)
        assert StarwallPlusBlockOfferWallResponse.model_validate(res)

    def test_1e5f0af8(self):
        from generalresearch.models.legacy.offerwall import (
            TopNPlusBlockRecontactOfferWallResponse,
            StarwallPlusBlockRecontactOfferWallResponse,
        )
        from tests.models.legacy.data import (
            RESPONSE_d48cce47,
            RESPONSE_1e5f0af8,
        )

        res = json.loads(RESPONSE_1e5f0af8)
        assert TopNPlusBlockRecontactOfferWallResponse.model_validate(res)
        assert StarwallPlusBlockRecontactOfferWallResponse.model_validate(res)

        res = json.loads(RESPONSE_d48cce47)  # this is a blocked user's response
        assert TopNPlusBlockRecontactOfferWallResponse.model_validate(res)
        assert StarwallPlusBlockRecontactOfferWallResponse.model_validate(res)

    def test_eligibility_criteria(self):
        b = TopNPlusBucket(
            id="c82cf98c578a43218334544ab376b00e",
            contents=[
                BucketTask(
                    id="12345",
                    payout=10,
                    source=Source.TESTING,
                    id_code="t:12345",
                    loi=120,
                )
            ],
            duration=DurationSummary(max=1, min=1, q1=1, q2=1, q3=1),
            quality_score=1,
            payout=PayoutSummary(max=1, min=1, q1=1, q2=1, q3=1),
            uri="https://task.generalresearch.com/api/v1/52d3f63b2709/00ff1d9b71b94bf4b20d22cd56774120/?i=2a4a897a76464af2b85703b72a125da0&b=379fb74f-05b2-42dc-b283-47e1c8678b04&66482fb=82fe142",
            eligibility_criteria=(
                SurveyEligibilityCriterion(
                    question_id="71a367fb71b243dc89f0012e0ec91749",
                    question_text="what is something",
                    qualifying_answer=("1",),
                    qualifying_answer_label=("abc",),
                    property_code="t:123",
                ),
                SurveyEligibilityCriterion(
                    question_id="81a367fb71b243dc89f0012e0ec91749",
                    question_text="what is something 2",
                    qualifying_answer=("2",),
                    qualifying_answer_label=("ddd",),
                    property_code="t:124",
                ),
            ),
        )
        assert b.eligibility_criteria[0].rank == 0
        assert b.eligibility_criteria[1].rank == 1
        print(b.model_dump_json())
        b.censor()
        print(b.model_dump_json())


class TestOfferwallSingle:
    def test_5fl8bpv5(self):
        from generalresearch.models.legacy.offerwall import (
            SingleEntryOfferWallResponse,
        )
        from tests.models.legacy.data import (
            RESPONSE_5fl8bpv5,
        )

        res = json.loads(RESPONSE_5fl8bpv5)
        assert SingleEntryOfferWallResponse.model_validate(res)


class TestOfferwallSoftPair:
    def test_37d1da64(self):
        from generalresearch.models.legacy.offerwall import (
            SoftPairOfferwallResponse,
        )
        from tests.models.legacy.data import (
            RESPONSE_37d1da64,
        )

        res = json.loads(RESPONSE_37d1da64)
        assert SoftPairOfferwallResponse.model_validate(res)


class TestMarketplace:
    def test_5fa23085(self):
        from generalresearch.models.legacy.offerwall import (
            MarketplaceOfferwallResponse,
        )

        from tests.models.legacy.data import (
            RESPONSE_5fa23085,
        )

        res = json.loads(RESPONSE_5fa23085)
        assert MarketplaceOfferwallResponse.model_validate(res)


class TestTimebucks:
    def test_1705e4f8(self):
        from generalresearch.models.legacy.offerwall import (
            TimeBucksOfferwallResponse,
        )
        from tests.models.legacy.data import (
            RESPONSE_1705e4f8,
        )

        res = json.loads(RESPONSE_1705e4f8)
        assert TimeBucksOfferwallResponse.model_validate(res)

    def test_0af0f7ec(self):
        from generalresearch.models.legacy.offerwall import (
            TimeBucksBlockOfferwallResponse,
        )
        from tests.models.legacy.data import (
            RESPONSE_1705e4f8,
            RESPONSE_0af0f7ec,
        )

        res = json.loads(RESPONSE_0af0f7ec)
        assert TimeBucksBlockOfferwallResponse.model_validate(res)

        res = json.loads(RESPONSE_1705e4f8)
        assert TimeBucksBlockOfferwallResponse.model_validate(res)
