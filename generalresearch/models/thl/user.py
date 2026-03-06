from __future__ import annotations

import json
import logging
import re
from datetime import timezone, datetime
from typing import Optional, Dict, List, TYPE_CHECKING
from uuid import uuid4, UUID

from pydantic import (
    AwareDatetime,
    Field,
    BaseModel,
    field_validator,
    model_validator,
    PositiveInt,
    ConfigDict,
    StringConstraints,
    AfterValidator,
)
from sentry_sdk import set_tag, set_user
from typing_extensions import Annotated, Self

from generalresearch.models import MAX_INT32
from generalresearch.models.custom_types import AwareDatetimeISO, UUIDStr
from generalresearch.models.thl.ipinfo import GeoIPInformation
from generalresearch.models.thl.ledger import LedgerTransaction
from generalresearch.models.thl.product import Product
from generalresearch.models.thl.userhealth import AuditLog
from generalresearch.pg_helper import PostgresConfig

if TYPE_CHECKING:
    from generalresearch.managers.thl.userhealth import AuditLogManager
    from generalresearch.managers.thl.ledger_manager.thl_ledger import (
        ThlLedgerManager,
    )

    # from generalresearch.managers.thl.userhealth import UserIpHistoryManager

logger = logging.getLogger()

BPUID_ALLOWED = r"0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ!#$%&()*+,-.:;<=>?@[\]^_{|}~"


class User(BaseModel):
    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    user_id: Optional[PositiveInt] = Field(
        default=None, lt=MAX_INT32, serialization_alias="id"
    )

    uuid: Optional[UUIDStr] = Field(default=None, examples=[uuid4().hex])

    # 'product' is a Class with values that are fetched from the DB.
    #   Initialization is deferred until it is actually needed
    #   (see .prefetch_product())
    product: Optional[Product] = Field(default=None)

    product_id: Optional[UUIDStr] = Field(
        default=None, examples=["4fe381fb7186416cb443a38fa66c6557"]
    )

    product_user_id: Optional[BPUIDStr] = Field(
        default=None,
        examples=["app-user-9329ebd"],
        description="A unique identifier for each user, which is set by the "
        "Supplier. It should not contain any sensitive information"
        "like email or names, and should avoid using any"
        "incrementing values.",
    )

    # TODO: Is it possible to protect these from ever being initialized?
    #  - Would need to be allowed with .from_json but not User constructor directly
    #  - Would need to allow private setters for setting from DB values
    blocked: Optional[bool] = Field(default=False, strict=True)

    created: Optional[AwareDatetimeISO] = Field(
        default=None,
        description="When the user was created on the GRL platform.",
    )

    # Note: due to cacheing, last_seen might be up to a day out of date!
    last_seen: Optional[AwareDatetimeISO] = Field(
        default=None,
        description="When the user was last seen on, or acting on any"
        "part of the GRL platform.",
    )

    # --- Prefetch Fields ---
    audit_log: Optional[List[AuditLog]] = Field(default=None)
    transactions: Optional[List["LedgerTransaction"]] = Field(default=None)
    location_history: Optional[List["GeoIPInformation"]] = Field(default=None)

    # --- Prebuild Fields ---
    # session: Optional[List] = Field(default=None)
    # wall: Optional[List] = Field(default=None)

    def __eq__(self, other: "User"):
        return (
            self.product_id == other.product_id
            and self.product_user_id == other.product_user_id
            and self.user_id == other.user_id
            and self.uuid == other.uuid
        )

    # --- Validation ---
    @field_validator("product_user_id")
    def check_product_user_id(cls, v: str) -> str:
        if v is not None:
            if " " in v:
                raise ValueError("String cannot contain spaces")
            if "\\" in v:
                raise ValueError("String cannot contain backslash")
            if "/" in v:
                raise ValueError("String cannot contain slash")
            # I think the * on the regex messes up value matches that are
            # the same length as the
            rex = re.fullmatch("[" + BPUID_ALLOWED + "]*", v)
            if not bool(rex):
                raise ValueError("String is not valid regex")
        return v

    # noinspection PyNestedDecoratorsk
    @field_validator("created", "last_seen")
    @classmethod
    def check_not_in_future(cls, v: AwareDatetime) -> AwareDatetime:
        if v is not None:
            try:
                assert v < datetime.now(tz=timezone.utc)
            except Exception:
                raise ValueError("Input is in the future")
        return v

    # noinspection PyNestedDecorators
    @field_validator("created", "last_seen")
    @classmethod
    def check_after_anno_domini(cls, v: AwareDatetime) -> AwareDatetime:
        if v is not None:
            try:
                assert v > datetime(year=2016, month=7, day=13, tzinfo=timezone.utc)
            except Exception:
                raise ValueError("Input is before Anno Domini")
        return v

    @model_validator(mode="after")
    def check_identifiable(self) -> "User":
        if not self.is_identifiable:
            raise ValueError("User is not identifiable")

        return self

    @model_validator(mode="after")
    def check_created_first(self) -> "User":
        # TODO: require the created value comes before, or is equal to the
        #   last_seen
        created = self.created
        last_seen = self.last_seen
        if created is not None and last_seen is not None and created > last_seen:
            raise ValueError("User created time invalid")
        return self

    # --- Properties ---
    @property
    def is_identifiable(self) -> bool:
        return bool(
            self.user_id is not None
            or self.uuid is not None
            or (self.product_id is not None and self.product_user_id)
        )

    @classmethod
    def is_valid_ubp(cls, *, product_id, product_user_id) -> bool:
        # Attempt to create common_struct solely for validation purposes,
        # using the product_id and product_user_id
        try:
            cls.check_bpuid_is_not_bpid(product_id, product_user_id)
            cls(
                user_id=None,
                product_id=product_id,
                product_user_id=product_user_id,
            )
        except Exception as e:
            logger.info(e)
            return False
        else:
            return True

    # --- Methods ---
    @staticmethod
    def check_bpuid_is_not_bpid(product_id, product_user_id):
        """Unfortunately users were already created failing this constraint,
        so only check for new users!
        """
        if (
            product_id is not None
            and product_user_id is not None
            and product_id == product_user_id
        ):
            raise ValueError("product_user_id must not equal the product_id")
        return True

    def to_dict(self) -> Dict:
        return self.model_dump(mode="python", exclude={"product"})

    def to_json(self) -> str:
        d = self.model_dump(mode="json", exclude={"product"})
        d["user_id"] = self.user_id
        return json.dumps(d)

    def set_sentry_user(self):
        # https://docs.sentry.io/platforms/python/enriching-events/identify-user/
        set_user(
            {
                "id": self.user_id,
                "product_id": self.product_id,
                "product_user_id": self.product_user_id,
            }
        )
        set_tag(key="bpid", value=self.product_id)
        set_tag(key="bpuid", value=self.product_user_id)

    def delete_profiling_history(self, thl_sql_rw: PostgresConfig) -> bool:
        """This is how we remove any profiling data on a user from our system.

        (1) Delete from thl-web tables
        (2) Delete from thl-marketplace tables

        # Possible future steps:
        #  - Notify Marketplaces of deletion requests
        #  - FullCircle: DanaH@ilovefullcircle.com
        """

        self.set_sentry_user()

        # Delete from db.300large-web
        for table in [
            "marketplace_userprofileknowledgeitem",
            "marketplace_userprofileknowledgenumerical",
            "marketplace_userprofileknowledgetext",
            "marketplace_userquestionanswer",
            "userprofile_useriphistory",
        ]:
            thl_sql_rw.execute_write(
                query=f"""
                    DELETE FROM {table}
                    WHERE user_id = %s;
                """,
                params=[self.user_id],
            )

        # # Delete from db.thl-marketplaces
        # We need DELETE credentials for all these...
        # from generalresearch.models import Source
        # mp_db_table = {
        #     Source.SPECTRUM: "`thl-spectrum`.`spectrum_marketresearchprofilequestion`",
        #     Source.INNOVATE: "`thl-innovate`.`innovate_marketresearchprofilequestion`",
        #     Source.DYNATA: "`thl-dynata`.`dynata_rexmarketresearchprofilequestion`",
        #     Source.SAGO: "`thl-schlesinger`.`sago_marketresearchprofilequestion`",
        #     Source.PRODEGE: "`thl-prodege`.`prodege_marketresearchprofilequestion`",
        #     Source.POLLFISH: "`thl-pollfish`.`pollfish_marketresearchprofilequestion`",
        #     Source.PRECISION: "`thl-precision`.`precision_marketresearchprofilequestion`",
        #     Source.MORNING_CONSULT: "`thl-morning`.`morning_marketresearchprofilequestion`",
        #     # Source.FULL_CIRCLE: "`300large-fullcircle`.`fullcircle_marketresearchprofilequestion`"
        # }
        #     for source in PRIVACY_MP_MYSQLC.keys():
        #         PRIVACY_MP_MYSQLC[source].execute_sql_query(f"""
        #         DELETE FROM {MP_DB_TABLE[source]}
        #         WHERE user_id = %s;""", [user.user_id], commit=True)
        #

        return True

    # --- Prefetch ---

    def prefetch_product(self, pg_config: PostgresConfig) -> None:
        from generalresearch.managers.thl.product import ProductManager

        if self.product is None:
            pm = ProductManager(pg_config=pg_config)
            self.product = pm.get_by_uuid(product_uuid=self.product_id)

        return None

    def prefetch_audit_log(self, audit_log_manager: "AuditLogManager") -> None:
        self.audit_log = audit_log_manager.filter_by_user_id(user_id=self.user_id)
        return None

    def prefetch_transactions(self, thl_lm: "ThlLedgerManager") -> None:
        account = thl_lm.get_account_or_create_user_wallet(user=self)
        self.transactions = thl_lm.get_tx_filtered_by_account(account_uuid=account.uuid)
        return None

    # def prefetch_location_history(self, user_ip_history_manager: "UserIpHistoryManager") -> None:
    #     return user_ip_history_manager.get_user_ip_history(user_id=self.user_id)

    # --- Prebuild ---

    @classmethod
    def from_db(cls, res) -> Self:
        if res["created"]:
            res["created"] = res["created"].replace(tzinfo=timezone.utc)
        if res["last_seen"]:
            res["last_seen"] = res["last_seen"].replace(tzinfo=timezone.utc)
        res["product_id"] = UUID(res["product_id"]).hex
        res["uuid"] = UUID(res["uuid"]).hex
        return cls(
            user_id=res["user_id"],
            product_id=res["product_id"],
            product_user_id=res["product_user_id"],
            uuid=res["uuid"],
            blocked=bool(res["blocked"]),
            created=res["created"],
            last_seen=res["last_seen"],
        )


# Used in other places where the bpuid is part of a model that's used in
# the API (separate from a User)
BPUIDStr = Annotated[
    str,
    StringConstraints(min_length=3, max_length=128),
    AfterValidator(User.check_product_user_id),
]
