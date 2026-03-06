from __future__ import annotations

from typing import List, Dict

from pydantic import BaseModel, Field, ConfigDict, NonNegativeInt

from generalresearch.models.custom_types import UUIDStr
from generalresearch.models.legacy.bucket import (
    BucketBase,
    SoftPairBucket,
    TopNBucket,
    TimeBucksBucket,
    MarketplaceBucket,
    TopNPlusBucket,
    SingleEntryBucket,
    WXETOfferwallBucket,
    OneShotOfferwallBucket,
    OneShotSoftPairOfferwallBucket,
    TopNPlusRecontactBucket,
)
from generalresearch.models.legacy.definitions import OfferwallReason
from generalresearch.models.thl.payout_format import (
    PayoutFormatField,
    PayoutFormatType,
)
from generalresearch.models.thl.profiling.upk_question import UpkQuestion

"""
Not Done:
8531fee24712: jeopardy
"""


class OfferWallInfo(BaseModel):
    success: bool = Field()


class OfferWallResponse(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    info: OfferWallInfo = Field()
    offerwall: OfferWall = Field()


class OfferWall(BaseModel):
    model_config = ConfigDict(extra="forbid", arbitrary_types_allowed=True)

    id: UUIDStr = Field(
        description="Unique identifier to reference a generated offerwall",
        examples=["7dc1d3aeb4844a6fab17ecd370b8bf1e"],
    )

    availability_count: NonNegativeInt = Field(
        description="Total opportunities available for specific bpuid "
        "respondent and parameters. This value changes frequently "
        "and can be used to determine if a respondent has potential "
        "tasks available, regardless of the offerwall type being "
        "requested. If the value is 0, no buckets will be generated.",
        examples=[42],
    )

    attempted_live_eligible_count: NonNegativeInt = Field(
        description=(
            "Number of currently live opportunities for which the respondent "
            "meets all eligibility criteria but is excluded due to a prior attempt. "
            "Only includes surveys that are still live and otherwise eligible; "
            "does not include previously attempted surveys that are no longer available."
        ),
        examples=[7],
        default=0,
    )

    buckets: List[BucketBase] = Field(default_factory=list)

    offerwall_reasons: List[OfferwallReason] = Field(
        default_factory=list,
        description=(
            "Explanations describing why so many or few opportunities are available."
        ),
        examples=[[OfferwallReason.USER_BLOCKED, OfferwallReason.UNDER_MINIMUM_AGE]],
    )

    def censor(self):
        for bucket in self.buckets:
            bucket.censor()


class SingleEntryOfferWall(OfferWall):
    """Only returns a single bucket with the top scoring tasks.

    Offerwall code: `5fl8bpv5`
    """

    payout_format: PayoutFormatType = PayoutFormatField
    buckets: List[SingleEntryBucket] = Field(default_factory=list, max_length=1)


class TopNOfferWall(OfferWall):
    """An offerwall with buckets that are clustered by the `split_by` argument
    using KMeans clustering.

    Offerwall code: `45b7228a7`
    """

    buckets: List[TopNBucket] = Field(default_factory=list)
    payout_format: PayoutFormatType = PayoutFormatField


class StarwallOfferWall(OfferWall):
    """An offerwall with buckets that are clustered by setting as seeds the
    highest scoring surveys for each bin, then the rest are distributed
    according to their Euclidean distance using the bucket's features.

    Offerwall code: `b59a2d2b`
    """

    buckets: List[TopNBucket] = Field(default_factory=list)
    payout_format: PayoutFormatType = PayoutFormatField


class TopNPlusOfferWall(OfferWall):
    """Same as the TopNOfferWall, but the buckets include contents.

    Offerwall code: `b145b803`
    """

    buckets: List[TopNPlusBucket] = Field(default_factory=list)


class TopNPlusBlockOfferWall(OfferWall):
    """Same as the TopNOfferWall, but the buckets include contents and no
    buckets are returned if the user is blocked.

    Offerwall code: `d48cce47`
    """

    buckets: List[TopNPlusBucket] = Field(default_factory=list)

    # This incorrectly gets returned only when the user is blocked. It
    #   shouldn't get returned at all
    payout_format: str = Field(exclude=True, default="")


class TopNPlusBlockRecontactOfferWall(OfferWall):
    """Same as the TopNOfferWall, but the buckets include contents, no buckets
    are returned if the user is blocked, and each bucket includes a
    `is_recontact` key.

    Offerwall code: `1e5f0af8`
    """

    buckets: List[TopNPlusRecontactBucket] = Field(default_factory=list)

    # This incorrectly gets returned only when the user is blocked. It
    #   shouldn't get returned at all
    payout_format: str = Field(exclude=True, default="")


class StarwallPlusOfferWall(OfferWall):
    """Same as the StarwallOfferWall, but the buckets include contents.

    Offerwall code: `5481f322`
    """

    buckets: List[TopNPlusBucket] = Field(default_factory=list)


class StarwallPlusBlockOfferWall(OfferWall):
    """Same as the StarwallOfferWall, but the buckets include contents and no
    buckets are returned if the user is blocked.

    Offerwall code: `7fa1b3f4`
    """

    buckets: List[TopNPlusBucket] = Field(default_factory=list)

    # This incorrectly gets returned only when the user is blocked. It
    #   shouldn't get returned at all
    payout_format: str = Field(exclude=True, default="")


class StarwallPlusBlockRecontactOfferWall(OfferWall):
    """Same as the StarwallOfferWall, but the buckets include contents, no
    buckets are returned if the user is blocked, and each bucket includes
    a recontact key.

    Offerwall code: `630db2a4`
    """

    buckets: List[TopNPlusRecontactBucket] = Field(default_factory=list)

    # This incorrectly gets returned only when the user is blocked. It
    #   shouldn't get returned at all
    payout_format: str = Field(exclude=True, default="")


class SoftPairOfferwall(OfferWall):
    """This offerwall contains tasks for which the user has a conditional
    eligibility. The questions that a user must answer to determine the
    eligibility are included within each bucket. Additionally, the question
    definitions are included for convenience.

    Offerwall code: `37d1da64`
    """

    buckets: List[SoftPairBucket] = Field(default_factory=list)

    question_info: Dict[str, "UpkQuestion"] = Field(
        default_factory=dict,
        examples=[
            # {
            #     UpkQuestion.model_config["json_schema_extra"]["example"][
            #         "question_id"
            #     ]: UpkQuestion.model_config["json_schema_extra"]["example"]
            # }
        ],
    )

    # This incorrectly gets returned only when the user is blocked. It
    #   shouldn't get returned at all
    payout_format: str = Field(exclude=True, default="")


class MarketplaceOfferwall(OfferWall):
    """Returns buckets grouped by marketplace, one per marketplace, with the
    tasks ordered by quality.

    Offerwall code: `5fa23085`
    """

    buckets: List[MarketplaceBucket] = Field(default_factory=list)


class TimeBucksOfferwall(OfferWall):
    """A modification of the TopNOfferwall:
    1) topN split by payout with 10 buckets
    2) remove buckets min_payout > $4 (distribute those surveys to the
        other buckets)
    3) duplicate each bucket 3x, with loi and payout jitter. no contents
        key, no IQR, just return loi = q1_duration, payout = min_payout

    Offerwall code: `1705e4f8`
    """

    buckets: List[TimeBucksBucket] = Field(default_factory=list)


class TimeBucksBlockOfferwall(OfferWall):
    """Same as the TimeBucksOfferwall, but no buckets are returned if the
    user is blocked.

    Offerwall code: `0af0f7ec`
    """

    buckets: List[TimeBucksBucket] = Field(default_factory=list)
    # This incorrectly gets returned only when the user is blocked. It shouldn't get returned at all
    payout_format: str = Field(exclude=True, default="")


class OneShotOfferwall(OfferWall):
    """Each bucket has only 1 single task, and only basic info is returned
        about each bucket.

    Offerwall code: `6f27b1ae`
    """

    buckets: List[OneShotOfferwallBucket] = Field(default_factory=list)


class OneShotSoftPairOfferwall(SoftPairOfferwall):
    """Each bucket has only 1 single task, and only basic info is returned
        about each bucket. Supports soft pair

    Offerwall code: `18347426`
    """

    buckets: List[OneShotSoftPairOfferwallBucket] = Field(default_factory=list)


class WXETOfferwall(OfferWall):
    """Returns buckets from WXET as single tasks
    Offerwall code: `55a4e1a9`
    """

    buckets: List[WXETOfferwallBucket] = Field(default_factory=list)


class SingleEntryOfferWallResponse(OfferWallResponse):
    offerwall: SingleEntryOfferWall = Field()


class TopNOfferWallResponse(OfferWallResponse):
    offerwall: TopNOfferWall = Field()


class TopNPlusOfferWallResponse(OfferWallResponse):
    offerwall: TopNPlusOfferWall = Field()


class TopNPlusBlockOfferWallResponse(OfferWallResponse):
    offerwall: TopNPlusBlockOfferWall = Field()


class TopNPlusBlockRecontactOfferWallResponse(OfferWallResponse):
    offerwall: TopNPlusBlockRecontactOfferWall = Field()


class StarwallOfferWallResponse(OfferWallResponse):
    offerwall: StarwallOfferWall = Field()


class StarwallPlusOfferWallResponse(OfferWallResponse):
    offerwall: StarwallPlusOfferWall = Field()


class StarwallPlusBlockOfferWallResponse(OfferWallResponse):
    offerwall: StarwallPlusBlockOfferWall = Field()


class StarwallPlusBlockRecontactOfferWallResponse(OfferWallResponse):
    offerwall: StarwallPlusBlockRecontactOfferWall = Field()


class SoftPairOfferwallResponse(OfferWallResponse):
    offerwall: SoftPairOfferwall = Field()


class MarketplaceOfferwallResponse(OfferWallResponse):
    offerwall: MarketplaceOfferwall = Field()


class TimeBucksOfferwallResponse(OfferWallResponse):
    offerwall: TimeBucksOfferwall = Field()


class TimeBucksBlockOfferwallResponse(OfferWallResponse):
    offerwall: TimeBucksBlockOfferwall = Field()


class OneShotOfferwallResponse(OfferWallResponse):
    offerwall: OneShotOfferwall = Field()


class OneShotSoftPairOfferwallResponse(OfferWallResponse):
    offerwall: OneShotSoftPairOfferwall = Field()


class WXETOfferwallResponse(OfferWallResponse):
    offerwall: WXETOfferwall = Field()
