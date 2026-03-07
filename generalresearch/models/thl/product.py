from __future__ import annotations

import copy
import inspect
import json
import math
import warnings
from collections import defaultdict
from decimal import Decimal
from enum import Enum
from functools import cached_property, partial
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    List,
    Literal,
    Optional,
    Set,
)
from urllib.parse import parse_qs, urlencode, urlsplit, urlunsplit
from uuid import uuid4

import pandas as pd
from dask.distributed import Client
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    NonNegativeFloat,
    NonNegativeInt,
    PositiveFloat,
    PositiveInt,
    computed_field,
    field_serializer,
    field_validator,
    model_validator,
)
from pydantic.json_schema import SkipJsonSchema
from typing_extensions import Self

from generalresearch.currency import USDCent
from generalresearch.decorators import LOG
from generalresearch.models import Source
from generalresearch.models.custom_types import (
    AwareDatetimeISO,
    CountryISOLike,
    HttpsUrlStr,
    UUIDStr,
)
from generalresearch.models.thl.ledger import LedgerAccount
from generalresearch.models.thl.payout_format import (
    PayoutFormatType,
    format_payout_format,
)
from generalresearch.models.thl.payout_format import (
    description as payout_format_description,
)
from generalresearch.models.thl.payout_format import (
    examples as payout_format_examples,
)
from generalresearch.models.thl.supplier_tag import SupplierTag
from generalresearch.models.thl.wallet import PayoutType
from generalresearch.models.utils import decimal_to_usd_cents
from generalresearch.redis_helper import RedisConfig

if TYPE_CHECKING:
    from generalresearch.incite.base import GRLDatasets
    from generalresearch.incite.mergers.pop_ledger import PopLedgerMerge
    from generalresearch.managers.thl.ledger_manager.thl_ledger import (
        ThlLedgerManager,
    )
    from generalresearch.managers.thl.payout import (
        BrokerageProductPayoutEventManager,
    )
    from generalresearch.models.thl.finance import (
        POPFinancial,
        ProductBalances,
    )
    from generalresearch.models.thl.payout import (
        BrokerageProductPayoutEvent,
    )
    from generalresearch.models.thl.user import User


# fmt: off
GRS_SKINS = [
    "mmfwcl.com", "profile.generalresearch.com",
    "eureka.generalresearch.com", "opinioncapital.generalresearch.com",
    "freeskins.generalresearch.com", "surveys.freeskins.com",
    "cheddar.generalresearch.com", "drop.generalresearch.com",
    "mobrog.generalresearch.com", "freecash.generalresearch.com",
    "inbrain.generalresearch.com", "just.generalresearch.com",
    "300large.generalresearch.com", "samplicious.generalresearch.com",
    "l.generalresearch.com", "surveys2skins.generalresearch.com",
    "surveyjunkie.generalresearch.com", "opinionhero.generalresearch.com",
    "ozone.generalresearch.com", "adbloom.generalresearch.com",
    "prime.generalresearch.com", "rakuten.generalresearch.com",
    "pch.generalresearch.com", "solipay.generalresearch.com",
    "widget.generalresearch.com", "inventory.adbloom.co",
    "voooice.generalresearch.com", "kashkick.generalresearch.com",
    "splendid.generalresearch.com", "monlix.generalresearch.com",
    "freeward.generalresearch.com", "surveys.mnlx.me",
    "bananabucks.generalresearch.com", "cashcamel.generalresearch.com",
    "surveypop.generalresearch.com", "surveymagic.generalresearch.com",
    "surveyspin.generalresearch.com", "cube.generalresearch.com",
    "innovate.generalresearch.com", "timebucks.generalresearch.com",
    "kaching.generalresearch.com", "precision.generalresearch.com",
    "bitburst.generalresearch.com", "talk.generalresearch.com",
    "theorem.generalresearch.com", "surveys.timewallresearch.com",
    "gmo.generalresearch.com", "pinchme.generalresearch.com"
]


# fmt: on


class OfferwallConfig(BaseModel):
    pass


class ProfilingConfig(BaseModel):
    # called "harmonizer_config" in the old bpc version

    enabled: bool = Field(
        default=True,
        description="If False, the harmonizer/profiling system is not used at all. This should "
        "never be False unless special circumstances",
    )

    grs_enabled: bool = Field(
        default=True,
        description="""If grs_enabled is False, and is_grs is passed in the profiling-questions call, 
        then don't actually return any questions. This allows a client to hit the endpoint with no limit 
        and still get questions. In effect, this means that we'll redirect the user through the GRS
        system but won't present them any questions.""",
    )

    n_questions: Optional[PositiveInt] = Field(
        default=None,
        description="Use to hard code the number of questions to ask. None means use default algorithm.",
    )

    max_questions: PositiveInt = Field(
        default=10,
        description="The max number of questions we would ask in a session",
    )

    avg_question_count: PositiveFloat = Field(
        default=5,
        description="The average number of questions to ask in a session",
    )

    # Don't set this to 0, use enabled
    task_injection_freq_mult: PositiveFloat = Field(
        default=1,
        description="Scale how frequently we inject profiling questions, relative to the default."
        "1 is default, 2 is twice as often. 10 means always. 0.5 half as often",
    )

    non_us_mult: PositiveFloat = Field(
        default=2,
        description="Non-us multiplier, used to increase freq and length of profilers in all non-us countries."
        "This value is multiplied by task_injection_freq_mult and avg_question_count.",
    )

    hidden_questions_expiration_hours: PositiveInt = Field(
        default=7 * 24,
        description="How frequently we should refresh hidden questions",
    )

    # todo: nothing uses this
    # consent: Dict = Field()
    #     # Used to configure consent questions
    #     "consent": {
    #         "enabled": False,
    #         "property_code": ""  # gr:consent_v1
    #     }
    # }


class UserHealthConfig(BaseModel):
    # Users in these countries are "blocked". Blocked in quotes because
    #   the user doesn't actually get blocked, they just are treated like they
    #   are blocked.
    banned_countries: List[CountryISOLike] = Field(default_factory=list)

    # Decide if a user can be blocked for IP-related triggers such as sharing IPs
    #   and location history. This should eventually be deprecated and replaced
    #   with something with more specificity.
    allow_ban_iphist: bool = Field(default=True)

    # These are only checked by ym-user-predict, which I'm not sure even works properly.
    # To be deprecated ... don't even use them.
    userprofit_cutoff: Optional[Decimal] = Field(default=None, exclude=True)
    recon_cutoff: Optional[float] = Field(default=None, exclude=True)
    droprate_cutoff: Optional[float] = Field(default=None, exclude=True)
    conversion_cutoff: Optional[float] = Field(default=None, exclude=True)

    @field_validator("banned_countries", mode="after")
    def sort_values(cls, values: List[str]):
        return sorted(values)


class OfferWallRequestYieldmanParams(BaseModel):
    # model_config = ConfigDict(extra='forbid')
    # keys: use_stats, use_harmonizer, allow_pii, add_default_lang_eng, first_n_completes_easier_per_day are
    # ignored/deprecated
    # allow_pii: bool = Field(default=True, description="Allow tasks that request PII. This actually does nothing.")

    # see thl-grpc:yield_management.scoring.score() for more info
    conversion_factor_adj: float = Field(
        default=0.0,
        description="Centered around 0. Higher results in higher weight given to conversion (in the scoring function)",
    )

    dropoffrate_factor_adj: float = Field(
        default=0.0,
        description="Centered around 0. Higher results in higher penalty given to dropoffs (in the scoring function)",
    )

    longfail_factor_adj: float = Field(
        default=0.0,
        description="Centered around 0. Higher results in higher penalty given to long fail (in the scoring function)",
    )

    recon_factor_adj: float = Field(
        default=0.0,
        description="Centered around 0. Higher results in higher penalty given to recons (in the scoring function)",
    )

    recon_likelihood_max: float = Field(
        default=0.8, description="Tolerance for recon likelihood (0 to 1)"
    )

    def update(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)


class OfferWallCategoryRequest(BaseModel):
    # Only include these categories
    adwords_category: Optional[List[str]] = Field(default=None, examples=[["45", "65"]])
    category: Optional[List[str]] = Field(
        default=None, examples=[["98c137e4e90a4d92ac6c00e523eb1b50"]]
    )
    # Exclude these categories
    exclude_adwords_category: Optional[List[str]] = Field(
        default=None, examples=[["1558"]]
    )
    exclude_category: Optional[List[str]] = Field(
        default=None,
        examples=[
            [
                "21536f160f784189be6194ca894f3a65",
                "7aa8bf4e71a84dc3b2035f93f9f9c77e",
            ]
        ],
    )

    @property
    def any(self):
        return bool(
            self.adwords_category
            or self.category
            or self.exclude_adwords_category
            or self.exclude_category
        )


class YieldManConfig(BaseModel):
    category_request: OfferWallCategoryRequest = Field(
        default_factory=OfferWallCategoryRequest
    )
    scoring_params: OfferWallRequestYieldmanParams = Field(
        default_factory=OfferWallRequestYieldmanParams
    )


class SourcesConfig(BaseModel):
    """Describes the marketplaces or sources that a BP can access and their
    respective configs,
    aka 'BP:Marketplace Configs'
    """

    model_config = ConfigDict(frozen=True)

    user_defined: List[SourceConfig] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_user_defined(self):
        cs = [c.name for c in self.user_defined]
        assert len(cs) == len(set(cs)), "Can only have one SourceConfig per Source!"
        return self

    @cached_property
    def default_sources(self) -> List[SourceConfig]:
        return [SourceConfig.model_validate({"name": s}) for s in Source]

    @cached_property
    def sources(self) -> List[SourceConfig]:
        # If a BP has no user_defined SourceConfigs, we use the default. Any
        # defined in user_defined will replace the default for that
        # SourceConfig.name
        # This can be a cached_property because the class is frozen. If we ever
        # change that, we should make this a property instead.
        user_defined = {x.name: x for x in self.user_defined}
        default = {x.name: x for x in self.default_sources}
        default.update(user_defined)
        return list(default.values())


class PayoutConfig(BaseModel):
    """Store configuration related to payouts, payout transformation, and user
    payout formatting."""

    payout_format: Optional[PayoutFormatType] = Field(
        default=None,
        description=payout_format_description,
        examples=payout_format_examples,
    )

    payout_transformation: Optional[PayoutTransformation] = Field(
        default=None,
        description="How the BP's payout is converted to the User's payout",
    )

    @model_validator(mode="before")
    @classmethod
    def payout_format_default(cls, data: Any):
        # If the BP's user payout_transformation is None, the payout_format
        # should also be None. If payout_transformation is set, and
        # payout_format is none, use $XX.YY
        if data.get("payout_transformation") is None:
            # Don't assert this b/c it'll fail b/c a lot of BPC in the db has
            # this set for no reason
            data["payout_format"] = None
        else:
            if data.get("payout_format") is None:
                data["payout_format"] = "${payout/100:.2f}"
        return data


class SessionConfig(BaseModel):
    """Stores configuration related to the Session, a session being a users
    experience attempting to do work.
    """

    max_session_len: int = Field(
        default=600,
        ge=60,
        le=90 * 60,
        description="The amount of time (in seconds) that a respondent may spend "
        "attempting to get into a survey within a session.If NULL, "
        "there is no limit.",
    )

    max_session_hard_retry: int = Field(
        default=5,
        ge=0,
        description="The number of surveys that a respondent may attempt within a "
        "session before the session is terminated.",
    )

    min_payout: Decimal = Field(
        default=Decimal("0.14"),
        description="""The minimum amount the user should be paid for a complete. If 
        no payout transformation is defined, the value is based on the BP's payout. 
        If a payout transformation is defined, the min_payout is applied on the 
        user's payout. Note, this is separate and distinct from the payout 
        transformation's min payout. The payout transformation's min_payout does not 
        care what the task's actual payout was. This min_payout will prevent
        the user from being show any tasks that would pay below this amount.""",
        examples=[Decimal("0.50")],
    )


class UserCreateConfig(BaseModel):
    """Stores configuration for the user creation experience.

    The user creation limit is determined dynamically based on the median
        daily completion rate. min_hourly_create_limit &
        max_hourly_create_limit can be used to constrain the dynamically
        determined rate limit within set values.
    """

    min_hourly_create_limit: NonNegativeInt = Field(
        default=0,
        description="The smallest allowed value for the hourly user create limit.",
    )

    max_hourly_create_limit: Optional[NonNegativeInt] = Field(
        default=None,
        description="The largest allowed value for the hourly user create "
        "limit. If None, the hourly create limit is unconstrained.",
    )

    def clip_hourly_create_limit(self, limit: int) -> int:
        limit = max(self.min_hourly_create_limit, limit)
        if self.max_hourly_create_limit is not None:
            limit = min(limit, self.max_hourly_create_limit)
        return limit


class UserWalletConfig(BaseModel):
    """
    Stores configuration for the user wallet handling
    """

    enabled: bool = Field(
        default=False, description="If enabled, the users' wallets are managed."
    )

    # This field could go in supported_payout_types ---v
    amt: bool = Field(default=False, description="Uses Amazon Mechanical Turk")

    supported_payout_types: Set["PayoutType"] = Field(
        default={PayoutType.CASH_IN_MAIL, PayoutType.TANGO, PayoutType.PAYPAL}
    )

    min_cashout: Optional[Decimal] = Field(
        default=None,
        gt=0,
        description="Minimum cashout amount. If enabled is True and no min_cashout is "
        "set, will default to $0.01.",
        examples=[Decimal("10.00")],
    )

    @field_serializer("supported_payout_types", when_used="json")
    def serialize_supported_payout_types_in_order(
        self, supported_payout_types: Set["PayoutType"]
    ) -> Set["PayoutType"]:
        return set(sorted(supported_payout_types))

    @field_validator("min_cashout", mode="after")
    @classmethod
    def check_payout_decimal_places(cls, v: Decimal) -> Decimal:
        if v is not None:
            assert (
                v.as_tuple().exponent >= -2
            ), "Must have 2 or fewer decimal places ('XXX.YY')"
            # explicitly make sure it is 2 decimal places, after checking that it is
            # already 2 or less.
            v = v.quantize(Decimal("0.00"))
        return v

    @model_validator(mode="after")
    def check_enabled(self):
        if self.enabled is False:
            assert self.amt is False, "amt can't be set if enabled is False"
            assert (
                self.min_cashout is None
            ), "min_cashout can't be set if enabled is False"
        else:
            if self.min_cashout is None:
                self.min_cashout = Decimal("0.01")
        return self


class PayoutTransformationPercentArgs(BaseModel):
    pct: NonNegativeFloat = Field(
        le=1.0,
        description="The percentage of the bp_payout to pay the user",
        examples=[0.5],
    )

    min_payout: Optional[Decimal] = Field(
        default=None,
        description="The minimum amount paid for a complete. Note: This does not "
        "check that the actual payout was at least this amount.",
        examples=[Decimal("0.50")],
    )

    max_payout: Optional[Decimal] = Field(
        default=None,
        description="The maximum amount paid for a complete",
        examples=[Decimal("5.00")],
    )

    @field_validator("min_payout", "max_payout", mode="after")
    @classmethod
    def check_payout_decimal_places(cls, v: Decimal) -> Decimal:
        if v is not None:
            assert (
                v.as_tuple().exponent >= -2
            ), "Must have 2 or fewer decimal places ('XXX.YY')"
            # explicitly make sure it is 2 decimal places, after checking that it is
            # already 2 or less.
            v = v.quantize(Decimal("0.00"))
        return v

    @field_validator("pct", mode="after")
    def validate_payout_transformation(cls, pct: float) -> float:
        if pct >= 0.95:
            warnings.warn("Are you sure you want to pay respondents >95% of CPI?")

        if pct == 0:
            raise ValueError("Disable payout transformation if payout percentage is 0%")

        return pct


class PayoutTransformation(BaseModel):
    """This model describe how the bp_payout is converted to the user_payout.
    If None, the user_payout is None.

    If the user_wallet_enabled is `False`, the user_payout is used to
        1) know how to transform the expected payouts for offerwall buckets
            (if min_payout is requested, this is based on the user_payout)
        2) show the user (using the payout_format) how much they made (in
            the Task Status Response).

    If the user_wallet_enabled is `True`, then in addition to the above, the
        user_payout is the amount actually paid to the user's wallet.
    """

    f: Literal["payout_transformation_percent", "payout_transformation_amt"] = Field(
        description="The name of the transformation function to use."
    )

    kwargs: Optional[PayoutTransformationPercentArgs] = Field(
        description="The kwargs to pass to the transformation function.",
        examples=[{"pct": 0.50, "max_payout": "5.00"}],
        default=None,
    )

    def get_payout_transformation_func(self) -> Callable:
        """Returns a callable which transforms the bp_payout to the
        user_payout.
        """
        assert self.f in {
            "payout_transformation_percent",
            "payout_transformation_amt",
        }, f"unsupported f: {self.f}"
        if self.f == "payout_transformation_amt":
            return self.payout_transformation_amt
        else:
            return partial(
                self.payout_transformation_percent, **self.kwargs.model_dump()
            )

    def payout_transformation_percent(
        self,
        payout: Decimal,
        pct: Decimal = 1,
        min_payout: Decimal = 0,
        max_payout: Optional[Decimal] = None,
    ) -> Decimal:
        """Payout transformation for user displayed values"""
        if min_payout is None:
            min_payout = Decimal(0)
        pct = Decimal(pct)

        payout = Decimal(payout)
        min_payout = Decimal(min_payout)
        max_payout = Decimal(max_payout) if max_payout else None

        payout: Decimal = payout * pct
        payout: Decimal = max([payout, min_payout])
        payout: Decimal = min([payout, max_payout]) if max_payout else payout
        return payout

    def payout_transformation_amt(
        self, payout: Decimal, user_wallet_balance: Optional[Decimal] = None
    ) -> Decimal:
        """Payout transformation for user displayed values"""
        # If user_wallet_balance isn't passed, we are re-calculating this
        # (display, adjustment) so ignore the 7-cent rounding.
        if user_wallet_balance is None:
            return self.payout_transformation_percent(payout=payout, pct=Decimal(".95"))
        payout = Decimal(payout)

        payout: Decimal = payout * Decimal("0.95")
        new_balance = payout + user_wallet_balance
        # If the new_balance is <0, we aren't paying anything, so use the
        # full amount
        if new_balance < 0:
            return payout

        amt = (5 * math.floor((int(new_balance * 100) - 2) / 5)) + 2
        rounded_new_balance = Decimal(amt / 100).quantize(Decimal("0.00"))
        payout = rounded_new_balance - user_wallet_balance
        if payout < Decimal(0):
            return Decimal(0)

        return payout


class SourceConfig(BaseModel):
    """
    This could also be named "BP:Marketplace Config", as it describes the
    config for a BP on a single marketplace.
    """

    name: Source = Field()
    active: bool = Field(default=True)
    banned_countries: List[CountryISOLike] = Field(default_factory=list)
    allow_mobile_ip: bool = Field(default=True)

    allow_pii_only_buyers: bool = Field(
        default=False,
        description="Allow Tasks from Buyers that want traffic that comes from "
        "Suppliers that can identify their users. Only supported on "
        "Pure Spectrum.",
    )

    allow_unhashed_buyers: bool = Field(
        default=False,
        description="Return Tasks from Buyers that don't have URL hashing "
        "enabled. Only supported on Pure Spectrum.",
    )

    withhold_profiling: bool = Field(
        default=False,
        description="For some Products, we may have privacy agreements "
        "prohibiting us from sharing information with the inventory"
        "Source. If True, don't add MRPQ (Market Research Profiling"
        "Question) onto the entry link.",
    )

    # Allows marketplace to return survey as eligible if there are unknown
    # question where the user can answer any possible answer and still
    # be eligible
    pass_unconditional_eligible_unknowns: bool = Field(
        default=True, description="Not used at the moment"
    )


class Scope(str, Enum):
    GLOBAL = "global"
    TEAM = "team"
    PRODUCT = "product"


class IntegrationMode(str, Enum):
    # We handle integration, get paid
    PLATFORM = "platform"
    # "external" credentials, we do not get paid for this activity
    PASS_THROUGH = "pass_through"


class SupplyConfig(BaseModel):
    """Describes the set of policies for how GRL can interact with marketplaces.
    This is only used on the special "global product"."""

    model_config = ConfigDict(frozen=False, validate_assignment=True)

    policies: List[SupplyPolicy] = Field(default_factory=list)

    @property
    def configs(self):
        return self.policies

    @model_validator(mode="after")
    def validate_scope_global(self):
        gcs = [c.name for c in self.policies if c.scope == Scope.GLOBAL]
        assert len(gcs) == len(set(gcs)), "Can only have one GLOBAL policy per Source"
        return self

    @model_validator(mode="after")
    def validate_scope_team(self):
        team_names = [
            (c.name, team_id)
            for c in self.policies
            if c.scope == Scope.TEAM
            for team_id in c.team_ids
        ]
        assert len(team_names) == len(
            set(team_names)
        ), "Can only have one TEAM policy per Source per Team"
        return self

    @model_validator(mode="after")
    def validate_scope_bp(self):
        bp_names = [
            (c.name, product_id)
            for c in self.policies
            if c.scope == Scope.PRODUCT
            for product_id in c.product_ids
        ]
        assert len(bp_names) == len(
            set(bp_names)
        ), "Can only have one PRODUCT policy per Source per BP"
        return self

    @property
    def global_scoped_policies(self):
        return [c for c in self.policies if c.scope == Scope.GLOBAL]

    @property
    def team_scoped_policies(self):
        return [c for c in self.policies if c.scope == Scope.TEAM]

    @property
    def product_scoped_policies(self):
        return [c for c in self.policies if c.scope == Scope.PRODUCT]

    @property
    def global_scoped_policies_dict(self) -> Dict[Source, SupplyPolicy]:
        return {c.name: c for c in self.policies if c.scope == Scope.GLOBAL}

    @property
    def team_scoped_policies_dict(
        self,
    ) -> Dict[str, Dict[Source, SupplyPolicy]]:
        # str in top-level dict is the team_id
        d = defaultdict(dict)
        for c in self.team_scoped_policies:
            for team_id in c.team_ids:
                d[team_id][c.name] = c
        return d

    @property
    def product_scoped_policies_dict(
        self,
    ) -> Dict[str, Dict[Source, SupplyPolicy]]:
        # str in top-level dict is the product_id
        d = defaultdict(dict)
        for c in self.product_scoped_policies:
            for product_id in c.product_ids:
                d[product_id][c.name] = c
        return d

    def get_policies_for(
        self, product_id: str, team_id: str
    ) -> Dict[Source, SupplyPolicy]:
        """
        Is there a config scoped to this product? If not,
            Is there a config scoped to this team? If not,
            Use global config.
        """
        d = self.global_scoped_policies_dict.copy()
        d.update(self.team_scoped_policies_dict.get(team_id, dict()))
        d.update(self.product_scoped_policies_dict.get(product_id, dict()))
        return d

    def get_config_for_product(self, product: Product) -> MergedSupplyConfig:
        product_id = product.id
        team_id = product.team_id
        policy_dict = copy.deepcopy(
            self.get_policies_for(product_id=product_id, team_id=team_id)
        )
        # 'supply_dict' is the config GRL is allowed to use for this product/team.
        # The specific product's SourcesConfig can still override some things.
        sources_dict = {s.name: s for s in product.sources_config.sources}

        return MergedSupplyConfig(
            policies=[
                SupplyPolicy.merge_source_config(
                    supply_policy=policy_dict[source],
                    source_config=sources_dict[source],
                )
                for source in policy_dict.keys()
            ]
        )


class SupplyPolicy(SourceConfig):
    """
    One policy describing how GRL can interact with a marketplaces in a
    certain way. This is only used on the special "global product", and then
    internally in grpc logic.
    """

    address: List[str] = Field(description="address for the grpc GetOpps call")

    allow_vpn: bool = Field(default=False)

    distribute_harmonizer_active: bool = Field(default=True)

    supplier_id: Optional[str] = Field(
        default=None,
        description="For some inventory Sources, we may partition traffic using "
        "different supplier accounts instead",
    )

    team_ids: Optional[List[UUIDStr]] = Field(default=None)
    product_ids: Optional[List[UUIDStr]] = Field(default=None)

    integration_mode: IntegrationMode = Field(default=IntegrationMode.PLATFORM)

    @computed_field(
        description="There must be only 1 GLOBAL config per Source. We can have more than "
        "one TEAM/PRODUCT config per Source."
    )
    @property
    def scope(self) -> Scope:
        if self.team_ids is not None:
            return Scope.TEAM
        if self.product_ids is not None:
            return Scope.PRODUCT
        return Scope.GLOBAL

    @classmethod
    def merge_source_config(
        cls, supply_policy: SupplyPolicy, source_config: SourceConfig
    ) -> Self:
        # This function could also be called "apply_bp_overrides".
        # We have a SupplyConfig (which describes how GRL is allowed to
        # interact with a marketplace). and we retrieved a BP's source_config
        # (for this same Source), which can override certain properties.
        # Do that here.

        assert supply_policy.name == source_config.name, "Must operate on same Source"
        out_config = supply_policy.model_copy()
        out_config.active = supply_policy.active and source_config.active
        out_config.banned_countries = sorted(
            set(supply_policy.banned_countries + source_config.banned_countries)
        )
        out_config.allow_mobile_ip = source_config.allow_mobile_ip
        out_config.allow_unhashed_buyers = source_config.allow_unhashed_buyers
        out_config.allow_pii_only_buyers = source_config.allow_pii_only_buyers
        out_config.withhold_profiling = source_config.withhold_profiling
        out_config.pass_unconditional_eligible_unknowns = (
            source_config.pass_unconditional_eligible_unknowns
        )
        return out_config


class MergedSupplyConfig(SupplyConfig):
    """
    This is a supply config after it has been merged/harmonized/reconciled with
    the Brokerage Product's SourcesConfig. This is what is used to do the
    getOpps work.
    """

    # At this point, there will be one single policy per Source
    #   (vs in the global config, which lists possibly many policies per source (that
    #       are applied to different scopes))
    @model_validator(mode="after")
    def validate_single_policy(self):
        sources = [c.name for c in self.policies]
        assert len(sources) == len(set(sources)), "Can only have one policy per Source"
        return self


class Product(BaseModel, validate_assignment=True):
    id: UUIDStr = Field(
        default_factory=lambda: uuid4().hex,
        description="Unique identifier of the Brokerage Product",
        examples=["1108d053e4fa47c5b0dbdcd03a7981e7"],
    )

    id_int: SkipJsonSchema[Optional[PositiveInt]] = Field(default=None)

    name: str = Field(
        min_length=3,
        max_length=255,
        description="Name of the Brokerage Product. Must be unique within a Team",
        examples=["Website ABC"],
    )

    enabled: bool = Field(
        default=True,
        description="This is only used to hard block a Product in order to "
        "immediately & safely protect against fraud entrances.",
    )

    payments_enabled: bool = Field(
        default=True,
        description="This is only to determine if ACH or Wire payments should "
        "be made to the Produce.",
    )

    created: Optional[AwareDatetimeISO] = Field(
        # TODO: make this non-nullable
        default=None,
        description="When the Product was created, this does necessarily mean "
        "it started to retrieve traffic at that time.",
    )

    team_id: Optional[UUIDStr] = Field(
        # TODO: make this non-nullable
        default=None,
        examples=["b96c1209cf4a4baaa27d38082421a039"],
        description="The organization (group of generalresearch.com admin "
        "accounts) that is allowed to modify and manage this"
        "Product",
    )

    business_id: Optional[UUIDStr] = Field(
        default=None,
        examples=[uuid4().hex],
        description="The legal business entity or individual that is "
        "responsible for this account, and that receive Supplier"
        "Payments for this Product's activity.",
    )

    tags: Set["SupplierTag"] = Field(
        default_factory=set,
        description="Tags which are used to annotate supplier traffic",
    )

    commission_pct: Decimal = Field(
        default=Decimal("0.05"), decimal_places=5, max_digits=6, le=1, ge=0
    )

    redirect_url: HttpsUrlStr = Field(
        description="Where to redirect the user after finishing a session. When a "
        "user get redirected back to the supplier, a query param will be "
        "added with "
        "the name 'tsid', and the value of the TSID for the session. For "
        "example: "
        "callback_uri: 'https://www.example.com/test/?a=1&b=2' "
        "might result in the user getting redirected to: "
        "'https://www.example.com/grl-callback/?a=1&b=2&tsid"
        "=c6ab6ba1e75b44e2bf5aab00fc68e3b7'.",
        examples=["https://www.example.com/grl-callback/?a=1&b=2"],
    )

    # This is called grs_domain in the BP table
    harmonizer_domain: HttpsUrlStr = Field(
        default="https://profile.generalresearch.com/",
        description="This is the domain that is used for the GRS (General "
        "Research Survey) platform. This is a simple javascript "
        "application which may profile the respondent for any"
        "profiling questions, along with collecting any browser"
        "based security information. The value is a scheme+domain "
        "only (no path).",
    )

    # We can do this b/c SourcesConfig & SupplyConfigs have different top-level keys,
    #   so it'll try to model validate with each in order.
    sources_config: SourcesConfig | SupplyConfig = Field(default_factory=SourcesConfig)

    session_config: SessionConfig = Field(default_factory=SessionConfig)

    payout_config: PayoutConfig = Field(default_factory=PayoutConfig)

    user_wallet_config: UserWalletConfig = Field(default_factory=UserWalletConfig)

    user_create_config: UserCreateConfig = Field(default_factory=UserCreateConfig)

    # these are just empty placeholders
    offerwall_config: OfferwallConfig = Field(default_factory=OfferwallConfig)
    profiling_config: ProfilingConfig = Field(default_factory=ProfilingConfig)
    user_health_config: UserHealthConfig = Field(default_factory=UserHealthConfig)
    yield_man_config: YieldManConfig = Field(default_factory=YieldManConfig)

    # Initialization is deferred until unless it's called
    # (see .prebuild_***())
    balance: Optional["ProductBalances"] = Field(
        default=None, description="Product Balance"
    )

    payouts_total_str: Optional[str] = Field(default=None)
    payouts_total: Optional[USDCent] = Field(default=None)
    payouts: Optional[List["BrokerageProductPayoutEvent"]] = Field(
        default=None,
        description="Product Payouts. These are the ACH or Wire payments that were sent to the"
        "Business on behalf of this specific Product",
    )

    pop_financial: Optional[List["POPFinancial"]] = Field(default=None)
    bp_account: Optional[LedgerAccount] = Field(default=None)

    # --- Validators ---
    @field_validator("harmonizer_domain", mode="before")
    def harmonizer_domain_https(cls, s: Optional[str]):
        # in the db, this has no scheme. accept both with a default of https://
        if s is not None and not (s.startswith("https://") or s.startswith("http://")):
            s = f"https://{s}"
        return s

    @field_validator("harmonizer_domain", mode="after")
    def validate_harmonizer_domain(cls, v: str):
        if urlsplit(v).netloc not in GRS_SKINS:
            raise ValueError("Unsupported harmonizer_domain")
        return v

    @field_validator("harmonizer_domain", mode="after")
    def harmonizer_domain_only(cls, s: str):
        # maks sure there is no path
        url_split = urlsplit(s)
        assert (
            url_split.path == "/"
        ), f"harmonizer_domain should be a schema+domain only: {url_split.path}"
        assert (
            url_split.query == ""
        ), f"harmonizer_domain should be a schema+domain only: {url_split.query}"
        assert (
            url_split.fragment == ""
        ), f"harmonizer_domain should be a schema+domain only: {url_split.fragment}"
        return s

    @field_validator("redirect_url", mode="after")
    def validate_redirect_url(cls, s: str) -> str:
        url_split = urlsplit(s)
        query_dict = parse_qs(url_split.query)
        assert "tsid" not in query_dict, "URL should not contain a query param 'tsid'"
        return s

    # --- Properties ---
    @property
    def commission(self) -> Decimal:
        return self.commission_pct

    @property
    def uuid(self) -> UUIDStr:
        return self.id

    @property
    def business_uuid(self) -> UUIDStr:
        return self.business_id

    @property
    def team_uuid(self) -> UUIDStr:
        return self.team_id

    @property
    def callback_uri(self):
        return self.redirect_url

    @property
    def sources(self):
        return self.sources_config.sources

    @property
    def sources_dict(self) -> Dict[Source, SourceConfig]:
        # This stores the same info as sources but with the keys as a Source
        return {x.name: x for x in self.sources}

    # Should make sure nothing uses this and remove it
    # @property
    # def routers(self):
    #     return self.sources

    @computed_field
    def user_wallet(self) -> UserWalletConfig:
        return self.user_wallet_config

    @property
    def user_wallet_enabled(self) -> bool:
        return self.user_wallet_config.enabled

    @property
    def user_wallet_amt(self) -> bool:
        # Controls whether AMT-related cashout methods and ledger transactions are
        # allowed.
        return self.user_wallet_config.amt

    @property
    def cache_key(self) -> str:
        return f"product:{self.uuid}"

    @property
    def file_key(self) -> str:
        return f"product-{self.uuid}"

    # --- Prefetch ---
    def prefetch_bp_account(self, thl_lm: "ThlLedgerManager"):
        account = thl_lm.get_account_or_create_bp_wallet(product=self)
        self.bp_account = account

        return None

    # --- Prebuild ---

    def prebuild_balance(
        self,
        thl_lm: "ThlLedgerManager",
        ds: "GRLDatasets",
        client: Client,
        pop_ledger: Optional["PopLedgerMerge"] = None,
    ) -> None:
        """
        This returns the Product's Balances that are calculated across
        all time. They are inclusive of every transaction that has ever
        occurred in relation to this particular Product.

        GRL does not use a Net30 or other time or Monthly styling billing
        practice. All financials are calculated in real time and immediately
        available based off the real-time calculated Smart Retainer balance.

        Smart Retainer:
        GRL's fully automated smart retainer system incorporates the real-time
        recon risk exposure on the BPID account. The retainer amount is prone
        to change every few hours based off real time traffic characteristics.
        The intention is to provide protection against an account immediately
        stopping traffic and having up to 2 months worth of reconciliations
        continue to roll in. Using the Smart Retainer amount will allow the
        most amount of an accounts balance to be deposited into the owner's
        account at any frequency without being tied to monthly invoicing. The
        goal is to be as aggressive as possible and not hold funds longer than
        absolutely required, Smart Retainer accounts are supported for any
        volume levels.
        """
        LOG.debug(f"Product.prebuild_balance({self.uuid=})")

        from generalresearch.incite.schemas.mergers.pop_ledger import (
            numerical_col_names,
        )
        from generalresearch.models.thl.ledger import LedgerAccount

        account: LedgerAccount = thl_lm.get_account_or_create_bp_wallet(product=self)
        assert self.id == account.reference_uuid

        if pop_ledger is None:
            from generalresearch.incite.defaults import pop_ledger as plm

            pop_ledger = plm(ds=ds)

        ddf = pop_ledger.ddf(
            force_rr_latest=False,
            include_partial=True,
            columns=numerical_col_names + ["time_idx"],
            filters=[
                ("account_id", "==", account.uuid),
            ],
        )

        if ddf is None:
            raise AssertionError("Cannot build Product Balance")

        df = client.compute(collections=ddf, sync=True)

        if df.empty:
            # A Product may not have any ledger transactional events. Don't
            #   attempt to build a balance, leave it as None rather than
            #   all zeros
            LOG.warning(f"Product({self.uuid=}).prebuild_balance empty dataframe")
            assert thl_lm.get_account_balance_timerange(account=account) == 0, (
                "If the df is empty, we can also assume that there should be no "
                "transactions in the ledger."
            )
            return None

        df = df.set_index("time_idx")
        from generalresearch.models.thl.finance import ProductBalances

        balance = ProductBalances.from_pandas(df)
        balance.product_id = self.uuid

        bal: int = thl_lm.get_account_balance_timerange(
            account=account, time_end=balance.last_event
        )
        assert bal == balance.balance, "Sql and Parquet Balance inconsistent"

        self.balance = balance
        return None

    def prebuild_pop_financial(
        self,
        thl_lm: "ThlLedgerManager",
        ds: "GRLDatasets",
        client: Client,
        pop_ledger: Optional["PopLedgerMerge"] = None,
    ) -> None:
        """This is very similar to the Product POP Financial endpoint; however,
        it returns more than one item for a single time interval. This is
        because more than a single account will have likely had any
        financial activity within that time window.
        """
        if self.bp_account is None:
            self.prefetch_bp_account(thl_lm=thl_lm)

        from generalresearch.incite.schemas.mergers.pop_ledger import (
            numerical_col_names,
        )
        from generalresearch.models.admin.request import (
            ReportRequest,
            ReportType,
        )

        rr = ReportRequest(report_type=ReportType.POP_LEDGER, interval="5min")

        if pop_ledger is None:
            from generalresearch.incite.defaults import pop_ledger as plm

            pop_ledger = plm(ds=ds)

        ddf = pop_ledger.ddf(
            force_rr_latest=False,
            include_partial=True,
            columns=numerical_col_names + ["time_idx", "account_id"],
            filters=[
                ("account_id", "==", self.bp_account.uuid),
                ("time_idx", ">=", pop_ledger.start),
            ],
        )
        if ddf is None:
            self.pop_financial = []
            return None

        df = client.compute(collections=ddf, sync=True)

        if df.empty:
            self.pop_financial = []
            return None

        df = df.groupby(
            [pd.Grouper(key="time_idx", freq=rr.interval), "account_id"]
        ).sum()

        from generalresearch.models.thl.finance import POPFinancial

        self.pop_financial = POPFinancial.list_from_pandas(
            input_data=df, accounts=[self.bp_account]
        )

        return None

    def prebuild_payouts(
        self,
        thl_lm: "ThlLedgerManager",
        bp_pem: "BrokerageProductPayoutEventManager",
    ) -> None:
        LOG.debug(f"Product.prebuild_payouts({self.uuid=})")
        from generalresearch.models.thl.ledger import OrderBy

        self.payouts = bp_pem.get_bp_bp_payout_events_for_products(
            thl_ledger_manager=thl_lm,
            product_uuids=[self.uuid],
            order_by=OrderBy.DESC,
        )

        self.prebuild_payouts_total()

    def prebuild_payouts_total(self) -> None:
        assert self.payouts is not None

        self.payouts_total = USDCent(sum([po.amount for po in self.payouts]))
        self.payouts_total_str = self.payouts_total.to_usd_str()

        return None

    # def prebuild_pop(self):
    #     account = LM.get_account(qualified_name=f"{LM.currency.value}:bp_wallet:{product.id}")
    #
    #     from main import data
    #
    #     gv: GlobalVar = data["gv"]
    #
    #     ddf = gv.pop_ledger.ddf(
    #         force_rr_latest=False,
    #         include_partial=True,
    #         columns=numerical_col_names + ["time_idx"],
    #         filters=[
    #             ("account_id", "==", account.uuid),
    #             ("time_idx", ">=", rr.start),
    #         ],
    #     )
    #
    #     df = gv.dask_client.compute(collections=ddf, sync=True)
    #     df = df.set_index("time_idx").resample(rr.freq).sum()
    #
    #     res = []
    #     for index, row in df.iterrows():
    #         index: pd.Timestamp
    #         row: pd.DataFrame
    #
    #         dt = index.to_pydatetime().replace(tzinfo=None)
    #         instance = ProductBalances.from_pandas(row)
    #
    #         res.append(
    #             {
    #                 "time": dt,
    #                 "payout": instance.payout / 100,
    #                 "adjustment": instance.adjustment / 100,
    #                 "expense": instance.expense / 100,
    #                 "net": (instance.payout + instance.adjustment + instance.expense) / 100,
    #             }
    #         )
    #
    #     df = pd.DataFrame.from_records(res)

    # def financial(
    #         product: Product = Depends(product_from_path),
    #         rr: ReportRequest = Depends(rr_from_query),
    # ) -> Any:
    #     account = LM.get_account(qualified_name=f"{LM.currency.value}:bp_wallet:{product.id}")
    #
    #     from main import data
    #
    #     gv: GlobalVar = data["gv"]
    #
    #     ddf = gv.pop_ledger.ddf(
    #         force_rr_latest=False,
    #         include_partial=True,
    #         columns=numerical_col_names + ["time_idx", "account_id"],
    #         filters=[("account_id", "==", account.uuid), ("time_idx", ">=", rr.start)],
    #     )
    #
    #     df = gv.dask_client.compute(collections=ddf, sync=True)
    #
    #     # We only do it this way so it's consistent with the Business.financial view
    #     df = df.groupby([pd.Grouper(key="time_idx", freq=rr.interval), "account_id"]).sum()
    #     return POPFinancial.list_from_pandas(df, accounts=[account])

    # def payments(self):
    #     """Payments are the amount of money that General Research has sent
    #     the owner of this Product.
    #
    #     These are typically ACH or Wire payments to company bank accounts.
    #     These are not respondent payments for Products where
    #
    #     This is Provided in a standard list without any POP Grouping to show
    #     the exact time and amount of any Issued Payments.
    #     """
    #
    #     account = LM.get_account(qualified_name=f"{LM.currency.value}:bp_wallet:{product.id}")
    #
    #     from main import data
    #
    #     gv: GlobalVar = data["gv"]
    #     ddf = gv.pop_ledger.ddf(
    #         force_rr_latest=False,
    #         include_partial=True,
    #         columns=numerical_col_names + ["time_idx", "account_id"],
    #         filters=[("account_id", "==", account.uuid)],
    #     )
    #
    #     df = gv.dask_client.compute(collections=ddf, sync=True)

    # --- Methods ---
    def set_cache(
        self,
        thl_lm: "ThlLedgerManager",
        ds: "GRLDatasets",
        client: Client,
        bp_pem: "BrokerageProductPayoutEventManager",
        redis_config: RedisConfig,
        pop_ledger: Optional[PopLedgerMerge] = None,
    ) -> None:
        LOG.debug(f"Product.set_cache({self.uuid=})")

        ex_secs = 60 * 60 * 24 * 3  # 3 days

        self.prefetch_bp_account(thl_lm=thl_lm)

        self.prebuild_balance(
            thl_lm=thl_lm, ds=ds, client=client, pop_ledger=pop_ledger
        )
        self.prebuild_payouts(thl_lm=thl_lm, bp_pem=bp_pem)
        self.prebuild_pop_financial(
            thl_lm=thl_lm, ds=ds, client=client, pop_ledger=pop_ledger
        )

        # Validation steps. Don't save into redis until we confirm against
        #   the ledger. This allows parquet + db ledger balance checks
        #   The balance check needs to stop when the last parquet file was
        #   built, otherwise they'll appear unequal when it's really just
        #   a delay in the incite merge file not being built yet.
        # bal = thl_lm.get_account_balance_timerange(time_end=)

        rc = redis_config.create_redis_client()
        rc.set(name=self.cache_key, value=self.model_dump_json(), ex=ex_secs)

        return None

    def determine_bp_payment(self, thl_net: Decimal) -> Decimal:
        """
        How much should we pay the BP?
        """
        # How much we should get paid by the MPs for all completes in this session (
        # usually 0 or 1 completes)
        commission_amount = self.determine_bp_commission(thl_net)
        payout = thl_net - commission_amount
        payout = payout.quantize(Decimal("0.01"))
        return payout

    def determine_bp_commission(self, thl_net: Decimal) -> Decimal:
        return (thl_net * self.commission_pct).quantize(Decimal("0.01"))

    def get_payout_transformation_func(self) -> Callable:
        """ """
        if self.payout_config.payout_transformation is None:
            return lambda x: x
        else:
            return (
                self.payout_config.payout_transformation.get_payout_transformation_func()
            )

    def calculate_user_payment(
        self, bp_payout: Decimal, user_wallet_balance: Optional[Decimal] = None
    ) -> Optional[Decimal]:
        """
        :param bp_payout: This is the amount we paid to the brokerage product
        :return: The amount that should be paid to the user
        """
        if self.payout_config.payout_transformation is None:
            return None
        payout_xform_func = self.get_payout_transformation_func()
        kwargs = dict()
        if "user_wallet_balance" in inspect.signature(payout_xform_func).parameters:
            kwargs["user_wallet_balance"] = user_wallet_balance
        user_payout: Decimal = payout_xform_func(bp_payout, **kwargs)
        user_payout = user_payout.quantize(Decimal("0.00"))
        return user_payout

    def generate_bp_redirect(self, tsid: str):
        url_split = urlsplit(self.redirect_url)
        query_dict = parse_qs(url_split.query)
        query_dict["tsid"] = [tsid]
        url_split = list(url_split)
        url_split[3] = urlencode(query_dict, doseq=True)
        url = urlunsplit(url_split)
        return url

    def format_payout_format(self, payout: Decimal) -> Optional[str]:
        assert isinstance(payout, Decimal), "payout should be a Decimal"
        if self.payout_config.payout_format is None:
            return None
        payout_int = decimal_to_usd_cents(payout)
        return format_payout_format(self.payout_config.payout_format, payout_int)

    # --- ORM ---

    def model_dump_mysql(self, *args, **kwargs) -> Dict[str, Any]:
        d = self.model_dump(mode="json", *args, **kwargs)

        if "created" in d:
            d["created"] = self.created.replace(tzinfo=None)

        # JSONify these various configuration objects
        for k in [
            "user_create_config",
            "payout_config",
            "session_config",
            "sources_config",
            "user_wallet_config",
            "offerwall_config",
            "profiling_config",
            "user_health_config",
            "yield_man_config",
        ]:
            if k in d:
                d[k] = json.dumps(d[k])
        return d
