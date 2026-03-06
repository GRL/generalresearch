from __future__ import annotations

import binascii
import json
import os
from datetime import datetime, timezone
from typing import Optional, List, TYPE_CHECKING, Dict, Union

from pydantic import AnyHttpUrl
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    PositiveInt,
    field_validator,
    NonNegativeInt,
)
from typing_extensions import Self

from generalresearch.decorators import LOG
from generalresearch.models.custom_types import AwareDatetimeISO, UUIDStr
from generalresearch.pg_helper import PostgresConfig
from generalresearch.redis_helper import RedisConfig

if TYPE_CHECKING:
    from generalresearch.models.gr.business import Business
    from generalresearch.models.gr.team import Team
    from generalresearch.models.thl.product import Product


class Claims(BaseModel):
    iss: Optional[str] = Field(
        default=None,
        description="Issuer: https://www.rfc-editor.org/rfc/rfc7519.html#section-4.1.1",
    )

    sub: Optional[str] = Field(
        default=None,
        description="Subject: https://www.rfc-editor.org/rfc/rfc7519.html#section-4.1.2",
    )

    aud: Optional[str] = Field(
        default=None,
        description="Audience: https://www.rfc-editor.org/rfc/rfc7519.html#section-4.1.3",
    )

    exp: Optional[NonNegativeInt] = Field(
        default=None,
        description="Expiration time: https://www.rfc-editor.org/rfc/rfc7519.html#section-4.1.4",
    )

    iat: Optional[NonNegativeInt] = Field(
        default=None,
        description="Issued at: https://www.rfc-editor.org/rfc/rfc7519.html#section-4.1.6",
    )

    auth_time: Optional[NonNegativeInt] = Field(
        default=None,
        description="When authentication occured: https://openid.net/specs/openid-connect-core-1_0.html#IDToken",
    )

    acr: Optional[str] = Field(
        default=None,
        description="Authentication Context Class Reference: https://openid.net/specs/openid-connect-core-1_0.html#IDToken",
    )

    amr: Optional[List[str]] = Field(
        default=None,
        description="Authentication Methods References: https://openid.net/specs/openid-connect-core-1_0.html#IDToken",
    )

    c_hash: Optional[str] = Field(
        default=None,
        description="Code hash value: http://openid.net/specs/openid-connect-core-1_0.html",
    )

    nonce: Optional[str] = Field(
        default=None,
        description="Value used to associate a Client session with an ID Token: http://openid.net/specs/openid-connect-core-1_0.html",
    )

    at_hash: Optional[str] = Field(
        default=None,
        description="Access Token hash value: http://openid.net/specs/openid-connect-core-1_0.html",
    )

    sid: Optional[str] = Field(
        default=None,
        description="Session ID: https://openid.net/specs/openid-connect-frontchannel-1_0.html#ClaimsContents",
    )

    # --- Properties ---

    @property
    def subject(self):
        return self.sub


class GRUser(BaseModel):
    model_config = ConfigDict(
        # extra="forbid",
        # from_attributes=True,
        arbitrary_types_allowed=True
    )

    id: Optional[PositiveInt] = Field(default=None)
    sub: Optional[str] = Field(max_length=200)
    is_superuser: bool = Field(default=False)

    date_joined: AwareDatetimeISO = Field(
        description="When the GR User account signed up."
    )

    # prefetch attributes
    businesses: Optional[List["Business"]] = Field(default=None)
    teams: Optional[List["Team"]] = Field(default=None)
    products: Optional[List["Product"]] = Field(default=None)
    token: Optional["GRToken"] = Field(default=None)
    claims: Optional["Claims"] = Field(default=None)

    def prefetch_claims(
        self, token: str, key: Dict, audience: str, issuer: AnyHttpUrl
    ) -> None:
        from jose import jwt

        payload = jwt.decode(
            token=token,
            key=key,
            algorithms=["RS256"],
            audience=audience,
            issuer=issuer,
        )
        self.claims = Claims.model_validate(payload)

    def prefetch_businesses(
        self, pg_config: PostgresConfig, redis_config: RedisConfig
    ) -> None:
        from generalresearch.managers.gr.business import BusinessManager

        bm = BusinessManager(pg_config=pg_config, redis_config=redis_config)

        if self.is_superuser:
            self.businesses = bm.get_all()
        else:
            self.businesses = bm.get_by_user_id(user_id=self.id)

    def prefetch_teams(
        self, pg_config: PostgresConfig, redis_config: RedisConfig
    ) -> None:
        from generalresearch.managers.gr.team import TeamManager

        tm = TeamManager(pg_config=pg_config, redis_config=redis_config)

        if self.is_superuser:
            self.teams = tm.get_all()
        else:
            self.teams = tm.get_by_user(gr_user=self)

    def prefetch_products(
        self,
        pg_config: PostgresConfig,
        thl_pg_config: PostgresConfig,
        redis_config: RedisConfig,
    ) -> None:

        self.prefetch_businesses(pg_config=pg_config, redis_config=redis_config)
        self.prefetch_teams(pg_config=pg_config, redis_config=redis_config)
        business_uuids = self.business_uuids
        team_uuids = self.team_uuids

        if len(business_uuids + team_uuids) == 0:
            self.products = []
            return None

        from generalresearch.managers.thl.product import ProductManager

        pm = ProductManager(pg_config=thl_pg_config)

        business_products = (
            pm.fetch_uuids(business_uuids=business_uuids) if business_uuids else []
        )
        team_products = pm.fetch_uuids(team_uuids=team_uuids) if team_uuids else []
        products = {p.id: p for p in business_products + team_products}

        self.products = sorted(products.values(), key=lambda x: getattr(x, "created"))

    def prefetch_token(self, pg_config: PostgresConfig):
        from generalresearch.managers.gr.authentication import (
            GRTokenManager,
        )

        tm = GRTokenManager(pg_config=pg_config)
        self.token = tm.get_by_user_id(user_id=self.id)

    def __eq__(self, other: "GRUser") -> bool:
        return self.id == other.id

    # --- Validations ---
    @field_validator("date_joined")
    @classmethod
    def date_joined_utc(cls, v: datetime) -> datetime:
        return v.replace(tzinfo=timezone.utc)

    # --- Properties ---
    @property
    def cache_key(self) -> str:
        return f"gr_user:{self.id}"

    @property
    def business_uuids(self) -> Optional[List[UUIDStr]]:
        if self.businesses is None:
            LOG.warning("prefetch not run")
            return None

        return [b.uuid for b in self.businesses]

    @property
    def business_ids(self) -> Optional[List[PositiveInt]]:
        if self.businesses is None:
            LOG.warning("prefetch not run")
            return None

        return [b.id for b in self.businesses]

    @property
    def team_uuids(self) -> Optional[List[UUIDStr]]:
        if self.teams is None:
            LOG.warning("prefetch not run")
            return None

        return [t.uuid for t in self.teams]

    @property
    def team_ids(self) -> Optional[List[PositiveInt]]:
        if self.teams is None:
            LOG.warning("prefetch not run")
            return None

        return [t.id for t in self.teams]

    @property
    def product_uuids(self) -> Optional[List[UUIDStr]]:
        if self.products is None:
            LOG.warning("prefetch not run")
            return None

        return [p.uuid for p in self.products]

    # --- Methods ---

    def set_cache(
        self,
        pg_config: PostgresConfig,
        thl_web_rr: PostgresConfig,
        redis_config: RedisConfig,
    ) -> None:
        ex_secs = 60 * 60 * 24 * 3  # 3 days

        self.prefetch_teams(pg_config=pg_config, redis_config=redis_config)
        self.prefetch_businesses(pg_config=pg_config, redis_config=redis_config)
        self.prefetch_products(
            pg_config=pg_config,
            thl_pg_config=thl_web_rr,
            redis_config=redis_config,
        )
        self.prefetch_token(pg_config=pg_config)

        rc = redis_config.create_redis_client()

        rc.set(name=self.cache_key, value=self.to_redis(), ex=ex_secs)
        rc.set(
            name=f"{self.cache_key}:team_uuids",
            value=json.dumps(self.team_uuids),
            ex=ex_secs,
        )
        rc.set(
            name=f"{self.cache_key}:business_uuids",
            value=json.dumps(self.business_uuids),
            ex=ex_secs,
        )
        rc.set(
            name=f"{self.cache_key}:product_uuids",
            value=json.dumps(self.product_uuids),
            ex=ex_secs,
        )

        return None

    # --- ORM ---

    @classmethod
    def from_postgresql(cls, d: dict) -> Self:
        d["date_joined"] = d["date_joined"].replace(tzinfo=timezone.utc)
        return GRUser.model_validate(d)

    @classmethod
    def from_redis(cls, d: Union[str, Dict]) -> Self:
        if isinstance(d, str):
            d = json.loads(d)
        assert isinstance(d, dict)

        d["date_joined"] = datetime.fromisoformat(d["date_joined"])

        if d.get("token"):
            d["token"] = GRToken.from_redis(d["token"])

        return GRUser.model_validate(d)

    def to_redis(self) -> str:
        d = self.model_dump(mode="json", exclude={"businesses", "teams", "products"})
        d["business_uuids"] = self.business_uuids
        d["team_uuids"] = self.team_uuids
        d["product_uuids"] = self.product_uuids

        return json.dumps(d)


class GRToken(BaseModel):
    key: str = Field(
        min_length=32,
        max_length=2_000,
        # rest_framework.authtoken.models.py:37 generate_key()
        examples=[binascii.hexlify(os.urandom(20)).decode()],
    )

    created: AwareDatetimeISO = Field()
    user_id: PositiveInt = Field()

    # --- prefetch field ---
    user: Optional["GRUser"] = Field(default=None)

    @property
    def sso(self) -> bool:
        return GRToken.is_sso(api_key=self.key)

    @staticmethod
    def is_sso(api_key: str) -> bool:
        return len(api_key) > 255

    def prefetch_user(
        self, pg_config: PostgresConfig, redis_config: RedisConfig
    ) -> None:
        from generalresearch.managers.gr.authentication import (
            GRUserManager,
        )

        gr_um = GRUserManager(pg_config=pg_config, redis_config=redis_config)

        self.user = gr_um.get_by_id(gr_user_id=self.user_id)

    def __eq__(self, other: "GRToken") -> bool:
        return self.key == other.key

    @field_validator("created", mode="before")
    @classmethod
    def created_utc(cls, v: datetime) -> datetime:
        return v.replace(tzinfo=timezone.utc)

    # --- Properties ---

    @property
    def auth_header(self, key_name="Authorization") -> Dict:
        return {key_name: self.key}

    # --- ORM ---

    @classmethod
    def from_redis(cls, d: Union[str, Dict]) -> Self:
        if isinstance(d, str):
            d = json.loads(d)
        assert isinstance(d, dict)

        d["created"] = datetime.fromisoformat(d["created"])

        return GRToken.model_validate(d)
