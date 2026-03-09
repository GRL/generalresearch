from __future__ import annotations

import hashlib
import json
from decimal import Decimal
from enum import Enum
from typing import Any, Dict, Literal, Optional, Set

from pydantic import (
    BaseModel,
    Field,
    PositiveInt,
    computed_field,
    model_validator,
)
from typing_extensions import Self

from generalresearch.models import Source
from generalresearch.models.custom_types import IPvAnyAddressStr
from generalresearch.models.thl.locales import (
    CountryISO,
    LanguageISO,
    locale_helper,
)
from generalresearch.models.thl.offerwall.behavior import (
    OfferWallBehaviorsType,
)
from generalresearch.models.thl.product import (
    OfferWallCategoryRequest,
    OfferWallRequestYieldmanParams,
)
from generalresearch.models.thl.user import User


class OfferWallType(str, Enum):
    """
    The specific offerwall type
    """

    TOPN_PLUS = "b145b803"
    TOPN_PLUS_BLOCK = "d48cce47"
    TOPN_PLUS_BLOCK_RECONTACT = "1e5f0af8"
    STARWALL_PLUS = "5481f322"
    STARWALL_PLUS_BLOCK = "7fa1b3f4"
    STARWALL_PLUS_BLOCK_RECONTACT = "630db2a4"
    MARKETPLACE = "5fa23085"
    TIMEBUCKS = "1705e4f8"
    TIMEBUCKS_BLOCK = "0af0f7ec"
    SOFTPAIR = "37d1da64"
    SOFTPAIR_BLOCK = "7a89dcdb"
    ONESHOT = "6f27b1ae"
    ONESHOT_SOFTPAIR = "18347426"
    WXET = "55a4e1a9"
    # LEGACY
    SINGLE = "5fl8bpv5"
    TOPN = "45b7228a7"
    STARWALL = "b59a2d2b"


class OfferWallTypeClass(str, Enum):
    """
    A higher level "class" to organize similar offerwall types.
    For e.g. STARWALL_PLUS_BLOCK, STARWALL_PLUS, STARWALL all use the same
        bucket-generation algorithm, and have the same API response, except
        for maybe including extra keys or have specific customizations.
    """

    TOPN = "TOPN"
    STARWALL = "STARWALL"
    MARKETPLACE = "MARKETPLACE"
    SOFTPAIR = "SOFTPAIR"
    SINGLE = "SINGLE"


OFFERWALL_TYPE_CLASS = {
    OfferWallType.TOPN: OfferWallTypeClass.TOPN,
    OfferWallType.TOPN_PLUS: OfferWallTypeClass.TOPN,
    OfferWallType.TOPN_PLUS_BLOCK: OfferWallTypeClass.TOPN,
    OfferWallType.TOPN_PLUS_BLOCK_RECONTACT: OfferWallTypeClass.TOPN,
    OfferWallType.TIMEBUCKS: OfferWallTypeClass.TOPN,
    OfferWallType.TIMEBUCKS_BLOCK: OfferWallTypeClass.TOPN,
    OfferWallType.ONESHOT: OfferWallTypeClass.STARWALL,
    OfferWallType.WXET: OfferWallTypeClass.STARWALL,
    OfferWallType.STARWALL: OfferWallTypeClass.STARWALL,
    OfferWallType.STARWALL_PLUS: OfferWallTypeClass.STARWALL,
    OfferWallType.STARWALL_PLUS_BLOCK: OfferWallTypeClass.STARWALL,
    OfferWallType.STARWALL_PLUS_BLOCK_RECONTACT: OfferWallTypeClass.STARWALL,
    OfferWallType.MARKETPLACE: OfferWallTypeClass.MARKETPLACE,
    OfferWallType.SOFTPAIR: OfferWallTypeClass.SOFTPAIR,
    OfferWallType.SOFTPAIR_BLOCK: OfferWallTypeClass.SOFTPAIR,
    OfferWallType.ONESHOT_SOFTPAIR: OfferWallTypeClass.SOFTPAIR,
    OfferWallType.SINGLE: OfferWallTypeClass.SINGLE,
}

# TODO: We could have a class for each offerwalltype, and each has attributes,
# but this is the only attribute I can think of, so just doing this
USER_BLOCK_OFFERWALLS = {
    OfferWallType.TOPN_PLUS_BLOCK,
    OfferWallType.STARWALL_PLUS_BLOCK,
    OfferWallType.TOPN_PLUS_BLOCK_RECONTACT,
    OfferWallType.STARWALL_PLUS_BLOCK_RECONTACT,
    OfferWallType.TIMEBUCKS_BLOCK,
    OfferWallType.ONESHOT,
    OfferWallType.ONESHOT_SOFTPAIR,
    OfferWallType.SOFTPAIR_BLOCK,
    OfferWallType.WXET,
}


class OfferWallRequest(BaseModel):
    offerwall_type: OfferWallType = Field()
    user: User = Field()

    ip: Optional[IPvAnyAddressStr] = Field(
        default=None,
        description="Respondent's IP address (IPv4 or IPv6). Either 'ip' must be "
        "provided, or 'country_iso' must be provided if 'ip' is "
        "not provided.",
    )

    country_iso: CountryISO = Field(
        description="Respondent's country code (ISO 3166-1 alpha-2, lowercase)"
    )
    language_isos: Set[LanguageISO] = Field(
        description="Respondent's desired language (ISO 639-2/B, lowercase)",
    )

    behavior: Optional[OfferWallBehaviorsType] = Field(
        default=None,
        max_length=12,
        description="Allows using custom scoring functions. Please "
        "discuss directly with GRL.",
    )

    min_payout: Optional[Decimal] = Field(
        default=None,
        description="Decimal representation of the minimum amount of USD that "
        "any of the tasks will pay",
        examples=["1.23"],
    )

    duration: Optional[int] = Field(
        default=60 * 90,
        description="Maximum length of desired task (in seconds).",
        gt=0,
    )

    n_bins: Optional[int] = Field(
        default=None,
        description="Number of bins requested in the offerwall.",
        le=100,
        gt=0,
    )

    min_bin_size: Optional[int] = Field(
        default=None,
        description="Minimum number of tasks that need to be in a bucket",
        gt=0,
        le=100,
    )

    dynamic_min_bin_size: bool = Field(
        default=True,
        description="Allows the bin size to drop below the min bin size when not enough tasks "
        "are available.",
    )

    split_by: Literal["payout", "duration"] = Field(
        default="payout", description="Cluster tasks by payout or duration"
    )

    passthrough_kwargs: Dict[str, str] = Field(
        default_factory=dict,
        description="These are pulled from the url params. They are any 'extra' url params "
        "in the getofferwall request. They'll be available through the task_status "
        "endpoint. These used to be in the wallsessionmetadata.",
    )

    # Only for soft pair (offerwall_id, max_options, max_questions)
    offerwall_id: Optional[str] = Field(default=None)
    max_options: Optional[int] = Field(
        default=None,
        description="Max number of options an allowed question can have (allowed to be asked)",
    )
    max_questions: Optional[int] = Field(
        default=None,
        description="Max number of missing questions on a single bin",
    )

    category_request: OfferWallCategoryRequest = Field(
        default_factory=OfferWallCategoryRequest
    )

    yieldman_kwargs: OfferWallRequestYieldmanParams = Field(
        default_factory=OfferWallRequestYieldmanParams,
        description="These get passed into the scoring function to adjust how filter/score eligible"
        "tasks that are used to build an offerwall. There are not setable directly from the "
        "url, instead a behavior can be set, which may translate into things here. Or"
        "these may be set in the bpc table globally for a BP.",
    )

    marketplaces: Optional[Set[Source]] = Field(
        default=None,
        description="If set, restrict tasks to those from these marketplaces only.",
    )

    grpc_method: Literal["GetOpportunityIDs", "GetOpportunitiesSoftPairing"] = Field(
        description="Which grpc method should be hit for this offerwall",
        default="GetOpportunityIDs",
    )

    @model_validator(mode="after")
    def check_grpc_method(self) -> Self:
        if self.offerwall_type_class == OfferWallTypeClass.SOFTPAIR:
            assert self.grpc_method == "GetOpportunitiesSoftPairing", "grpc_method"
        else:
            assert self.grpc_method == "GetOpportunityIDs", "grpc_method"
        return self

    @model_validator(mode="after")
    def set_language_isos(self) -> Self:
        # Special hook for languages. If no languages were passed, we set the
        #   lang_codes to 'eng' and the default lang for their country.
        if len(self.language_isos) == 0:
            self.language_isos = {
                "eng",
                locale_helper.get_default_lang_from_country(self.country_iso),
            } - {None}
        return self

    @model_validator(mode="after")
    def set_offerwall_defaults(self) -> Self:
        # Set specific defaults depending on the offerwall type
        if self.offerwall_type_class == OfferWallTypeClass.SOFTPAIR:
            self.max_options = self.max_options if self.max_options is not None else 40
            self.max_questions = (
                self.max_questions if self.max_questions is not None else 3
            )
            self.n_bins = self.n_bins if self.n_bins is not None else 12
            self.min_bin_size = (
                self.min_bin_size if self.min_bin_size is not None else 3
            )
        if self.offerwall_type_class == OfferWallTypeClass.MARKETPLACE:
            self.min_bin_size = (
                self.min_bin_size if self.min_bin_size is not None else 3
            )
        if self.offerwall_type_class in {
            OfferWallTypeClass.STARWALL,
            OfferWallTypeClass.TOPN,
        }:
            self.n_bins = self.n_bins if self.n_bins is not None else 1
            self.min_bin_size = (
                self.min_bin_size if self.min_bin_size is not None else 1
            )
        return self

    @computed_field
    def offerwall_type_class(self) -> OfferWallTypeClass:
        return OFFERWALL_TYPE_CLASS[self.offerwall_type]

    @property
    def request_id(self) -> str:
        return hashlib.md5(
            json.dumps(self.model_dump(mode="json"), sort_keys=True).encode("utf-8")
        ).hexdigest()[:7]

    def to_grpc_request(self) -> Dict[str, Any]:
        # We need this so thl-core can refresh an offerwall in order to continue
        #   a session
        d = self.model_dump(mode="json")
        kwargs = dict()
        keys = [
            "n_bins",
            "min_bin_size",
            "max_options",
            "max_questions",
            "behavior",
            "min_payout",
            "duration",
            "dynamic_min_bin_size",
            "split_by",
        ]
        for k in keys:
            if getattr(self, k) is not None:
                kwargs[k] = str(getattr(self, k))
        d["offerwall_kwargs"] = kwargs
        d["start_task"] = {
            "product_id": self.user.product_id,
            "bp_user_id": self.user.product_user_id,
            "req_duration": self.duration,
            "country_iso": self.country_iso,
            "languages": [{"iso_code": x} for x in self.language_isos],
            "kwargs": kwargs,
        }
        # We can't import protos in here, so the caller has to actually
        #   cast this dict as a generalresearch_pb2.OfferwallRequest
        return {
            "start_task": d["start_task"],
            "offerwall_type": d["offerwall_type"],
            "offerwall_kwargs": d["offerwall_kwargs"],
        }

    @property
    def product_id(self) -> Optional[str]:
        return self.user.product_id

    @property
    def product_user_id(self) -> Optional[str]:
        return self.user.product_user_id

    @property
    def bpuid(self) -> Optional[str]:
        return self.user.product_user_id

    @property
    def min_opp_count(self) -> PositiveInt:
        # This is the min number of surveys we need available to show any
        # offerwall at all.
        if self.min_bin_size is not None:
            return self.min_bin_size
        return 3
