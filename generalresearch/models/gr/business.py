from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, List, Optional, Union
from uuid import uuid4

import pandas as pd
from dask.distributed import Client
from psycopg.cursor import Cursor
from psycopg.rows import dict_row
from pydantic import BaseModel, ConfigDict, Field, PositiveInt
from pydantic.json_schema import SkipJsonSchema
from pydantic_extra_types.phone_numbers import PhoneNumber
from typing_extensions import Self

from generalresearch.currency import USDCent
from generalresearch.decorators import LOG
from generalresearch.incite.mergers.pop_ledger import PopLedgerMerge
from generalresearch.incite.schemas.mergers.pop_ledger import (
    numerical_col_names,
)
from generalresearch.models.admin.request import ReportRequest, ReportType
from generalresearch.models.custom_types import (
    AwareDatetime,
    UUIDStr,
    UUIDStrCoerce,
)
from generalresearch.models.thl.finance import POPFinancial
from generalresearch.models.thl.ledger import LedgerAccount, OrderBy
from generalresearch.models.thl.payout import BusinessPayoutEvent
from generalresearch.pg_helper import PostgresConfig
from generalresearch.redis_helper import RedisConfig
from generalresearch.utils.aggregation import group_by_year
from generalresearch.utils.enum import ReprEnumMeta

if TYPE_CHECKING:
    from generalresearch.incite.base import GRLDatasets
    from generalresearch.incite.mergers.foundations.enriched_session import (
        EnrichedSessionMerge,
    )
    from generalresearch.incite.mergers.foundations.enriched_wall import (
        EnrichedWallMerge,
    )
    from generalresearch.managers.thl.ledger_manager.ledger import (
        LedgerManager,
    )
    from generalresearch.managers.thl.ledger_manager.thl_ledger import (
        ThlLedgerManager,
    )
    from generalresearch.managers.thl.payout import (
        BusinessPayoutEventManager,
    )
    from generalresearch.models.gr.team import Team
    from generalresearch.models.thl.finance import BusinessBalances
    from generalresearch.models.thl.product import Product


class TransferMethod(Enum, metaclass=ReprEnumMeta):
    ACH = 0
    WIRE = 1


class BusinessType(str, Enum, metaclass=ReprEnumMeta):
    INDIVIDUAL = "i"
    COMPANY = "c"


class BusinessBankAccount(BaseModel):
    model_config = ConfigDict(
        use_enum_values=True,
        json_encoders={TransferMethod: lambda tm: tm.value},
    )

    id: SkipJsonSchema[Optional[PositiveInt]] = Field(default=None)
    uuid: UUIDStrCoerce = Field(examples=[uuid4().hex])

    business_id: PositiveInt = Field()

    # 'business' is a Class with values that are fetched from the DB.
    #   Initialization is deferred until it is actually needed
    #   (see .prefetch_business())
    business: SkipJsonSchema[Optional["Business"]] = Field(default=None)

    transfer_method: TransferMethod = Field(
        description=TransferMethod.as_openapi(),
        examples=[TransferMethod.ACH.value],
    )

    # ACH requirements
    account_number: Optional[str] = Field(
        default=None,
        max_length=16,
        description="ACH requirements",
        examples=[f"{'*' * 9}1234"],
    )

    routing_number: Optional[str] = Field(
        default=None,
        max_length=9,
        description="ACH requirements",
        examples=[f"{'*' * 5}1234"],
    )

    # Wire requirements
    iban: Optional[str] = Field(
        default=None,
        max_length=50,
        description="Wire requirements",
        examples=[None],
    )
    swift: Optional[str] = Field(
        default=None,
        max_length=50,
        description="Wire requirements",
        examples=[None],
    )

    def prefetch_business(
        self, pg_config: PostgresConfig, redis_config: RedisConfig
    ) -> None:
        from generalresearch.managers.gr.business import BusinessManager

        if self.business is None:
            bm = BusinessManager(pg_config=pg_config, redis_config=redis_config)
            self.business = bm.get_by_id(business_id=self.business_id)


class BusinessAddress(BaseModel):
    model_config = ConfigDict(extra="ignore")

    id: SkipJsonSchema[Optional[PositiveInt]] = Field(default=None)
    uuid: UUIDStrCoerce = Field(examples=[uuid4().hex])

    line_1: Optional[str] = Field(
        default=None, max_length=255, examples=["540 Mariposa"]
    )

    line_2: Optional[str] = Field(default=None, max_length=255, examples=[None])

    city: Optional[str] = Field(
        default=None, max_length=255, examples=["Mountain View"]
    )

    state: Optional[str] = Field(
        default=None,
        max_length=255,
        description="This can only be more than len=2 if it's a state or"
        "providence out of the United States",
        examples=["CA"],
    )

    postal_code: Optional[str] = Field(default=None, max_length=12, examples=["94041"])

    phone_number: Optional[PhoneNumber] = Field(default=None)

    country: Optional[str] = Field(default=None, max_length=2, examples=["US"])

    business_id: PositiveInt = Field()


class BusinessContact(BaseModel):
    model_config = ConfigDict(extra="ignore")

    name: Optional[str] = Field(default=None)
    email: Optional[str] = Field(default=None)

    phone_number: Optional[str] = Field(
        default=None,
        min_length=10,
        max_length=31,
        examples=["+1 (888) 888-8888"],
    )


class Business(BaseModel):
    """This is the Base model to represent a Business,"""

    model_config = ConfigDict(extra="ignore")

    id: SkipJsonSchema[Optional[PositiveInt]] = Field(default=None)
    uuid: UUIDStrCoerce = Field(examples=[uuid4().hex])

    name: str = Field(
        min_length=3,
        max_length=255,
        examples=["General Research Laboratories, LLC"],
    )

    kind: str = Field(
        max_length=1,
        description=BusinessType.as_openapi(),
        examples=[BusinessType.COMPANY.value],
    )

    tax_number: Optional[str] = Field(default=None, max_length=20)
    contact: Optional["BusinessContact"] = Field(default=None)

    # Initialization is deferred until it is actually needed
    # (see .prefetch_***())
    addresses: Optional[List["BusinessAddress"]] = Field(default=None)
    teams: Optional[List["Team"]] = Field(default=None)
    products: Optional[List["Product"]] = Field(default=None)
    bank_accounts: Optional[List["BusinessBankAccount"]] = Field(default=None)

    # Initialization is deferred until unless it's called
    # (see .prebuild_***())
    balance: Optional["BusinessBalances"] = Field(default=None, name="Business Balance")

    payouts_total_str: Optional[str] = Field(default=None)
    payouts_total: Optional[USDCent] = Field(default=None)
    payouts: Optional[List[BusinessPayoutEvent]] = Field(
        default=None,
        name="Business Payouts",
        description="These are the ACH or Wire payments that were sent to the"
        "Business as a single amount, summed for all the Business"
        "child Products",
    )

    pop_financial: Optional[List[POPFinancial]] = Field(default=None)
    bp_accounts: Optional[List[LedgerAccount]] = Field(default=None)

    def __str__(self) -> str:
        return (
            f"Name: {self.name} ({self.uuid})\n"
            f"Products: {len(self.products) if self.products else 'Not Loaded'}\n"
            f"Ledger Accounts: {len(self.bp_accounts) if self.bp_accounts else 'Not Loaded'}\n"
            f"Addresses: {len(self.addresses) if self.addresses else 'Not Loaded'}\n"
            f"Teams: {len(self.teams) if self.teams else 'Not Loaded'}\n"
            f"Bank Accounts: {len(self.bank_accounts) if self.bank_accounts else 'Not Loaded'}\n"
            f"–––\n"
            f"Payouts: {len(self.payouts) if self.payouts else 'Not Loaded'}\n"
            f"Available Balance: {self.balance.available_balance if self.balance else 'Not Loaded'}\n"
        )

    def __repr__(self):
        return f"<Business: {self.name} ({self.uuid}) >"

        # --- Prefetch ---

    def prefetch_addresses(self, pg_config: PostgresConfig) -> None:
        with pg_config.make_connection() as conn:
            with conn.cursor(row_factory=dict_row) as c:
                c.execute(
                    query="""
                        SELECT *
                        FROM common_businessaddress AS ba
                        WHERE ba.business_id = %s
                        LIMIT 1
                    """,
                    params=[self.id],
                )
                res = c.fetchall()

        if len(res) == 0:
            self.addresses = []

        self.addresses = [BusinessAddress.model_validate(i) for i in res]

    def prefetch_teams(self, pg_config: PostgresConfig) -> None:
        from generalresearch.models.gr.team import Team

        with pg_config.make_connection() as conn:
            with conn.cursor(row_factory=dict_row) as c:
                c: Cursor

                c.execute(
                    query="""
                    SELECT t.* 
                    FROM common_team AS t
                    INNER JOIN common_team_businesses AS tb
                        ON tb.team_id = t.id
                    WHERE tb.business_id = %s
                """,
                    params=(self.id,),
                )

                res = c.fetchall()

        if len(res) == 0:
            self.teams = []

        self.teams = [Team.model_validate(i) for i in res]

    def prefetch_products(self, thl_pg_config: PostgresConfig) -> None:
        """
        :return: All the Products for this Business
        """
        from generalresearch.managers.thl.product import ProductManager

        pm = ProductManager(pg_config=thl_pg_config)
        self.products = pm.fetch_uuids(business_uuids=[self.uuid])

    def prefetch_bank_accounts(self, pg_config: PostgresConfig) -> None:
        from generalresearch.managers.gr.business import (
            BusinessBankAccountManager,
        )

        bam = BusinessBankAccountManager(pg_config=pg_config)
        self.bank_accounts = bam.get_by_business_id(business_id=self.id)

    def prefetch_bp_accounts(self, lm: LedgerManager, thl_pg_config: PostgresConfig):
        # We need to prefetch the Products everytime because there is no way
        #   of knowing if a new Product has been added since the last time it
        #   ran.
        self.prefetch_products(thl_pg_config=thl_pg_config)

        accounts = lm.get_accounts_if_exists(
            qualified_names=[
                f"{lm.currency.value}:bp_wallet:{bpid}" for bpid in self.product_uuids
            ]
        )

        assert len(accounts) == len(self.product_uuids)

        self.bp_accounts = accounts

    # --- Prebuild ---

    def prebuild_balance(
        self,
        thl_pg_config: PostgresConfig,
        lm: "LedgerManager",
        ds: "GRLDatasets",
        client: Client,
        pop_ledger: Optional["PopLedgerMerge"] = None,
        at_timestamp: Optional[AwareDatetime] = None,
    ) -> None:
        """
        This returns the Business's Balances that are calculated across
        all time. They are inclusive of every transaction that has ever
        occurred in relation to any of the Products for this Business

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
        LOG.debug(f"Business.prebuild_balance({self.uuid=})")

        self.prefetch_products(thl_pg_config=thl_pg_config)

        accounts: List[LedgerAccount] = lm.get_accounts_if_exists(
            qualified_names=(
                [f"{lm.currency.value}:bp_wallet:{bpid}" for bpid in self.product_uuids]
                if self.product_uuids
                else []
            )
        )

        if len(accounts) != len(self.products):
            raise ValueError("Inconsistent BP Wallet Accounts for Business: ")

        if pop_ledger is None:
            from generalresearch.incite.defaults import pop_ledger as plm

            pop_ledger = plm(ds=ds)

        if at_timestamp is None:
            at_timestamp = datetime.now(tz=timezone.utc)
        assert at_timestamp.tzinfo == timezone.utc

        ddf = pop_ledger.ddf(
            force_rr_latest=False,
            include_partial=True,
            columns=numerical_col_names + ["account_id"],
            filters=[
                ("account_id", "in", [a.uuid for a in accounts]),
                ("time_idx", "<=", at_timestamp),
            ],
        )

        if ddf is None:
            raise AssertionError("Cannot build Business Balance")

        # This is so stupid. Something goes wrong when trying to groupby directly
        #   on the ddf (says there is a datetime), so drop the small speed
        #   improvement and simply build the full df, and then group by on
        #   a pandas df instead of a dask dataframe
        # https://g-r-l.slack.com/archives/G8ULA6CV8/p1755898636685149?thread_ts=1755868251.296459&cid=G8ULA6CV8
        # ddf = ddf.groupby("account_id").sum()
        df: pd.DataFrame = client.compute(collections=ddf, sync=True)

        if df.empty:
            # A Business can have multiple Products. However, none of those
            #   Products need to have had any ledger transactional events and
            #   that is still valid. Don't attempt to build a balance, leave it
            #   as None rather than all zeros
            LOG.warning(f"Business({self.uuid=}).prebuild_balance empty dataframe")
            return None

        LOG.debug(f"Business.prebuild_balance.groupby() {df.head()}")
        df = df.groupby("account_id").sum()

        from generalresearch.models.thl.finance import BusinessBalances

        self.balance = BusinessBalances.from_pandas(
            input_data=df, accounts=accounts, thl_pg_config=thl_pg_config
        )

        return None

    def prebuild_payouts(
        self,
        thl_pg_config: PostgresConfig,
        thl_lm: "ThlLedgerManager",
        bpem: BusinessPayoutEventManager,
    ) -> None:
        LOG.debug(f"Business.prebuild_payouts({self.uuid=})")

        self.prefetch_products(thl_pg_config=thl_pg_config)

        self.payouts = bpem.get_business_payout_events_for_products(
            thl_ledger_manager=thl_lm,
            product_uuids=self.product_uuids,
            order_by=OrderBy.DESC,
        )

        self.prebuild_payouts_total()

    def prebuild_payouts_total(self):
        assert self.payouts is not None
        self.payouts_total = USDCent(sum([po.amount for po in self.payouts]))
        self.payouts_total_str = self.payouts_total.to_usd_str()

        return None

    def prebuild_pop_financial(
        self,
        thl_pg_config: PostgresConfig,
        lm: "LedgerManager",
        ds: "GRLDatasets",
        client: Client,
        pop_ledger: Optional["PopLedgerMerge"] = None,
    ) -> None:
        """This is very similar to the Product POP Financial endpoint; however,
        it returns more than one item for a single time interval. This is
        because more than a single account will have likely had any
        financial activity within that time window.
        """
        if self.bp_accounts is None:
            self.prefetch_bp_accounts(lm=lm, thl_pg_config=thl_pg_config)

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
                ("account_id", "in", [a.uuid for a in self.bp_accounts]),
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

        self.pop_financial = POPFinancial.list_from_pandas(
            input_data=df, accounts=self.bp_accounts
        )

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
    def product_uuids(self) -> Optional[List[UUIDStr]]:
        if self.products is None:
            LOG.warning("prefetch not run")
            return None

        return [p.uuid for p in self.products]

    @property
    def cache_key(self) -> str:
        return f"business:{self.uuid}"

    @property
    def file_key(self) -> str:
        return f"business-{self.uuid}"

    # --- Methods ---

    def set_cache(
        self,
        pg_config: PostgresConfig,
        thl_web_rr: PostgresConfig,
        redis_config: RedisConfig,
        client: "Client",
        ds: "GRLDatasets",
        lm: "LedgerManager",
        thl_lm: "ThlLedgerManager",
        bpem: "BusinessPayoutEventManager",
        mnt_gr_api: Union[Path, str],
        pop_ledger: Optional["PopLedgerMerge"] = None,
        enriched_session: Optional["EnrichedSessionMerge"] = None,
        enriched_wall: Optional["EnrichedWallMerge"] = None,
    ) -> None:
        LOG.debug(f"Business.set_cache({self.uuid=})")

        ex_secs = 60 * 60 * 24 * 3  # 3 days

        self.prefetch_addresses(pg_config=pg_config)
        self.prefetch_teams(pg_config=pg_config)
        self.prefetch_products(thl_pg_config=thl_web_rr)
        self.prefetch_bank_accounts(pg_config=pg_config)
        self.prefetch_bp_accounts(lm=lm, thl_pg_config=thl_web_rr)

        self.prebuild_balance(
            thl_pg_config=thl_web_rr,
            lm=lm,
            ds=ds,
            client=client,
            pop_ledger=pop_ledger,
        )
        self.prebuild_payouts(thl_pg_config=thl_web_rr, thl_lm=thl_lm, bpem=bpem)
        self.prebuild_pop_financial(
            thl_pg_config=thl_web_rr,
            lm=lm,
            ds=ds,
            client=client,
            pop_ledger=pop_ledger,
        )

        rc = redis_config.create_redis_client()
        mapping = self.model_dump(mode="json")

        # For POP Financial data, we want to also break that out by year
        res = {
            f"pop_financial:{key}": value
            for key, value in group_by_year(
                records=mapping["pop_financial"], datetime_field="time"
            ).items()
        }
        mapping = mapping | res

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
        keys: List[str] = Business.required_fields() + fields

        if "pop_financial" in keys:
            # We should explicitly pass the pop_financial years we want. By default,
            #   at least get this year.
            year = datetime.now(tz=timezone.utc).year
            keys = list(set(keys) | {f"pop_financial:{year}"})
        rc = gr_redis_config.create_redis_client()

        try:
            res: List = rc.hmget(name=f"business:{uuid}", keys=keys)
            d = {
                val: json.loads(res[idx]) if res[idx] is not None else None
                for idx, val in enumerate(keys)
            }

            # Extract all pop_financial records
            pop_financial = [
                record
                for key, value in d.items()
                if key.startswith("pop_financial:") and value is not None
                for record in value
            ]

            result = {k: v for k, v in d.items() if not k.startswith("pop_financial:")}
            result["pop_financial"] = pop_financial

            return Business.model_validate(result)
        except Exception as e:
            logging.exception(e)
            return None
