import json
import os
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Optional, Union, List, TYPE_CHECKING
from uuid import uuid4

import pandas as pd
from dask.distributed import Client
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    PositiveInt,
    field_validator,
)
from pydantic.json_schema import SkipJsonSchema
from typing_extensions import Self

from generalresearch.decorators import LOG
from generalresearch.incite.mergers.foundations.enriched_session import (
    EnrichedSessionMerge,
)
from generalresearch.incite.mergers.foundations.enriched_wall import (
    EnrichedWallMerge,
)
from generalresearch.utils.enum import ReprEnumMeta
from generalresearch.models.admin.request import ReportRequest, ReportType
from generalresearch.models.custom_types import (
    AwareDatetimeISO,
    UUIDStr,
    UUIDStrCoerce,
)
from generalresearch.pg_helper import PostgresConfig
from generalresearch.redis_helper import RedisConfig

if TYPE_CHECKING:
    from generalresearch.incite.base import GRLDatasets
    from generalresearch.models.gr.business import Business
    from generalresearch.models.gr.authentication import GRUser
    from generalresearch.models.thl.product import Product


class MembershipPrivilege(Enum, metaclass=ReprEnumMeta):
    ADMIN = 0
    MAINTAIN = 1
    READ = 2


class Membership(BaseModel):
    """A Membership is the relationship between a GR User and a Team.

    GRUsers do not have direct connections to Businesses or Products,
    they're all connected through a Team and a GRUser's relationship to
    a Team can have various levels of permissions and rights.
    """

    model_config = ConfigDict(use_enum_values=True)

    id: SkipJsonSchema[Optional[PositiveInt]] = Field(
        default=None,
    )
    uuid: UUIDStrCoerce = Field(examples=[uuid4().hex])

    privilege: MembershipPrivilege = Field(
        default=MembershipPrivilege.MAINTAIN,
        examples=[MembershipPrivilege.READ.value],
        description=MembershipPrivilege.as_openapi(),
    )

    owner: bool = Field(default=False, examples=[True])

    created: AwareDatetimeISO = Field(
        description="This is when the User was added to the Team, it's when"
        "the Membership was created and not when the GR User "
        "account was created."
    )

    user_id: SkipJsonSchema[PositiveInt] = Field(default=None)

    team_id: SkipJsonSchema[PositiveInt] = Field()

    # prefetch attributes
    team: SkipJsonSchema[Optional["Team"]] = Field(default=None)

    # --- Validators ---

    @field_validator("created", mode="before")
    @classmethod
    def created_utc(cls, v: Union[datetime, str]) -> Union[datetime, str]:
        if isinstance(v, datetime):
            return v.replace(tzinfo=timezone.utc)
        return v

    # --- prefetch methods ---

    def prefetch_team(
        self, pg_config: PostgresConfig, redis_config: RedisConfig
    ) -> None:
        from generalresearch.managers.gr.team import TeamManager

        tm = TeamManager(pg_config=pg_config, redis_config=redis_config)
        self.team = tm.get_by_id(team_id=self.team_id)


class Team(BaseModel):
    id: SkipJsonSchema[Optional[PositiveInt]] = Field(default=None)
    uuid: UUIDStrCoerce = Field(examples=[uuid4().hex])
    name: str = Field(max_length=255, examples=["Team ABC"])

    # prefetch attributes
    memberships: SkipJsonSchema[Optional[List["Membership"]]] = Field(default=None)
    gr_users: SkipJsonSchema[Optional[List["GRUser"]]] = Field(default=None)
    businesses: SkipJsonSchema[Optional[List["Business"]]] = Field(default=None)
    products: SkipJsonSchema[Optional[List["Product"]]] = Field(default=None)

    # --- Prefetch Methods ---

    def prefetch_memberships(self, pg_config: PostgresConfig) -> None:
        from generalresearch.managers.gr.team import MembershipManager

        mm = MembershipManager(pg_config=pg_config)
        self.memberships = mm.get_by_team_id(team_id=self.id)

    def prefetch_gr_users(
        self, pg_config: PostgresConfig, redis_config: RedisConfig
    ) -> None:
        from generalresearch.managers.gr.authentication import (
            GRUserManager,
        )

        gr_um = GRUserManager(pg_config=pg_config, redis_config=redis_config)

        self.gr_users = gr_um.get_by_team(team_id=self.id)

    def prefetch_businesses(
        self, pg_config: PostgresConfig, redis_config: RedisConfig
    ) -> None:
        from generalresearch.managers.gr.business import BusinessManager

        bm = BusinessManager(pg_config=pg_config, redis_config=redis_config)
        self.businesses = bm.get_by_team(team_id=self.id)

    def prefetch_products(self, thl_pg_config: PostgresConfig) -> None:
        from generalresearch.managers.thl.product import ProductManager

        pm = ProductManager(pg_config=thl_pg_config)
        self.products = pm.fetch_uuids(team_uuids=[self.uuid])

    # --- Prebuild Methods ---

    def prebuild_enriched_session_parquet(
        self,
        thl_pg_config: PostgresConfig,
        ds: "GRLDatasets",
        client: Client,
        mnt_gr_api: Path,
        enriched_session: Optional["EnrichedSessionMerge"] = None,
    ) -> None:
        self.prefetch_products(thl_pg_config=thl_pg_config)

        if enriched_session is None:
            from generalresearch.incite.defaults import (
                enriched_session as es,
            )

            enriched_session = es(ds=ds)

        rr = ReportRequest.model_validate(
            {
                "start": enriched_session.start,
                "interval": "5min",
                "type": ReportType.POP_SESSION,
            }
        )
        df = enriched_session.to_admin_response(
            product_ids=self.product_uuids, rr=rr, client=client
        )

        path = Path(
            os.path.join(mnt_gr_api, rr.report_type.value, f"{self.file_key}.parquet")
        )

        df.to_parquet(
            path=path,
            engine="pyarrow",
            compression="brotli",
        )

        try:
            test = pd.read_parquet(path, engine="pyarrow")
        except Exception as e:
            raise IOError(f"Parquet verification failed: {e}")

        return None

    def prebuild_enriched_wall_parquet(
        self,
        thl_pg_config: PostgresConfig,
        ds: "GRLDatasets",
        client: Client,
        mnt_gr_api: Path,
        enriched_wall: Optional["EnrichedWallMerge"] = None,
    ) -> None:
        self.prefetch_products(thl_pg_config=thl_pg_config)

        if enriched_wall is None:
            from generalresearch.incite.defaults import (
                enriched_wall as ew,
            )

            enriched_wall = ew(ds=ds)

        rr = ReportRequest.model_validate(
            {
                "start": enriched_wall.start,
                "interval": "5min",
                "report_type": ReportType.POP_EVENT,
            }
        )
        df = enriched_wall.to_admin_response(
            product_ids=self.product_uuids, rr=rr, client=client
        )

        path = Path(
            os.path.join(mnt_gr_api, rr.report_type.value, f"{self.file_key}.parquet")
        )

        df.to_parquet(
            path=path,
            engine="pyarrow",
            compression="brotli",
        )

        try:
            test = pd.read_parquet(path, engine="pyarrow")
        except Exception as e:
            raise IOError(f"Parquet verification failed: {e}")

        return None

    @classmethod
    def required_fields(cls) -> List[str]:
        return [
            field_name
            for field_name, field_info in cls.model_fields.items()
            if field_info.is_required()
        ]

    # --- Properties ---
    @property
    def cache_key(self) -> str:
        return f"team:{self.uuid}"

    @property
    def file_key(self) -> str:
        return f"team-{self.uuid}"

    @property
    def product_ids(self) -> Optional[List[UUIDStr]]:
        if self.products is None:
            LOG.warning("prefetch not run")
            return None

        return [p.uuid for p in self.products]

    @property
    def product_uuids(self) -> Optional[List[UUIDStr]]:
        return self.product_ids

    # --- Methods ---

    def set_cache(
        self,
        pg_config: PostgresConfig,
        thl_web_rr: PostgresConfig,
        redis_config: RedisConfig,
        client: "Client",
        ds: "GRLDatasets",
        mnt_gr_api: Union[Path, str],
        enriched_session: Optional["EnrichedSessionMerge"] = None,
        enriched_wall: Optional["EnrichedWallMerge"] = None,
    ) -> None:
        ex_secs = 60 * 60 * 24 * 3  # 3 days

        self.prefetch_products(thl_pg_config=thl_web_rr)
        self.prefetch_gr_users(pg_config=pg_config, redis_config=redis_config)
        self.prefetch_businesses(pg_config=pg_config, redis_config=redis_config)
        self.prefetch_memberships(pg_config=pg_config)

        rc = redis_config.create_redis_client()
        mapping = self.model_dump(mode="json")
        for key in mapping:
            mapping[key] = json.dumps(mapping[key])
        rc.hset(name=self.cache_key, mapping=mapping)

        # -- Saves Parquet files
        if enriched_session is None:
            from generalresearch.incite.defaults import (
                enriched_session as es,
            )

            enriched_session = es(ds=ds)

        self.prebuild_enriched_session_parquet(
            thl_pg_config=thl_web_rr,
            client=client,
            ds=ds,
            mnt_gr_api=mnt_gr_api,
            enriched_session=enriched_session,
        )

        if enriched_wall is None:
            from generalresearch.incite.defaults import enriched_wall as ew

            enriched_wall = ew(ds=ds)

        self.prebuild_enriched_wall_parquet(
            thl_pg_config=thl_web_rr,
            client=client,
            ds=ds,
            mnt_gr_api=mnt_gr_api,
            enriched_wall=enriched_wall,
        )

        return None

    # --- ORM ---

    @classmethod
    def from_redis(
        cls,
        uuid: UUIDStr,
        fields: List[str],
        gr_redis_config: RedisConfig,
    ) -> Optional[Self]:
        keys: List = Team.required_fields() + fields
        rc = gr_redis_config.create_redis_client()

        try:
            res: List = rc.hmget(name=f"team:{uuid}", keys=keys)
            d = {val: json.loads(res[idx]) for idx, val in enumerate(keys)}
            return Team.model_validate(d)
        except (Exception,) as e:
            return None
