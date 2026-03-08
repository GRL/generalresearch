import ipaddress
from datetime import datetime, timedelta, timezone
from itertools import zip_longest
from random import choice as rchoice
from random import random
from typing import Any, Collection, Dict, List, Optional, Tuple

import faker
from pydantic import NonNegativeInt, PositiveInt

from generalresearch.decorators import LOG
from generalresearch.managers.base import (
    Permission,
    PostgresManager,
    PostgresManagerWithRedis,
)
from generalresearch.managers.thl.ipinfo import GeoIpInfoManager
from generalresearch.models.custom_types import IPvAnyAddressStr
from generalresearch.models.thl.product import Product
from generalresearch.models.thl.user import User
from generalresearch.models.thl.user_iphistory import (
    IPRecord,
    UserIPHistory,
    UserIPRecord,
)
from generalresearch.models.thl.userhealth import AuditLog, AuditLogLevel
from generalresearch.pg_helper import PostgresConfig
from generalresearch.redis_helper import RedisConfig

fake = faker.Faker()


class UserIpHistoryManager(PostgresManagerWithRedis):
    def __init__(
        self,
        pg_config: PostgresConfig,
        redis_config: RedisConfig,
        permissions: Collection[Permission] = None,
        cache_prefix: Optional[str] = None,
    ):
        super().__init__(
            pg_config=pg_config,
            redis_config=redis_config,
            permissions=permissions,
            cache_prefix=cache_prefix,
        )
        self.geoipinfo_manager = GeoIpInfoManager(
            pg_config=pg_config,
            redis_config=redis_config,
            cache_prefix=cache_prefix,
        )

    def get_redis_key(self, user_id: int) -> str:
        return f"py-utils:user-ip-history:{user_id}"

    def get_user_ip_records_sql(self, user_id: int) -> List[UserIPRecord]:
        # The IP metadata is ONLY for the 'ip', NOT for any forwarded ips.
        # This might get called immediately after a write, so use the non-rr
        res = self.pg_config.execute_sql_query(
            query="""
                SELECT iph.ip, iph.created, iph.user_id,
                    geo.subdivision_1_iso,
                    ipinfo.country_iso,
                    ipinfo.is_anonymous
                FROM userhealth_iphistory iph
                LEFT JOIN thl_ipinformation AS ipinfo
                    ON iph.ip = ipinfo.ip
                LEFT JOIN thl_geoname AS geo
                    ON ipinfo.geoname_id = geo.geoname_id
                WHERE iph.user_id = %s
                AND created > NOW() - INTERVAL '28 days'
                ORDER BY iph.created DESC 
                LIMIT 100
            """,
            params=[user_id],
        )

        res = [UserIPRecord.model_validate(x) for x in res]
        return res

    def get_user_ip_history_cache(self, user_id: int) -> Optional[UserIPHistory]:
        res = self.redis_client.get(self.get_redis_key(user_id))
        if res:
            return UserIPHistory.model_validate_json(res)
        return None

    def delete_user_ip_history_cache(self, user_id: int) -> None:
        self.redis_client.delete(self.get_redis_key(user_id))
        return None

    def set_user_ip_history_cache(self, user_id: int, iph: UserIPHistory) -> None:
        value = iph.model_dump_json()
        self.redis_client.set(self.get_redis_key(user_id), value, ex=3 * 24 * 3600)
        return None

    def recreate_user_ip_history_cache(self, user_id: int) -> None:
        self.delete_user_ip_history_cache(user_id=user_id)
        records = self.get_user_ip_records_sql(user_id=user_id)
        # todo: we may get dns records from somewhere else here ...
        iph = UserIPHistory(user_id=user_id, ips=records)
        self.set_user_ip_history_cache(user_id=user_id, iph=iph)
        return None

    def get_user_ip_history(self, user_id: int) -> UserIPHistory:
        assert isinstance(user_id, int)
        iph = self.get_user_ip_history_cache(user_id=user_id)
        if iph:
            LOG.debug(f"get_user_ip_history got in cache: {iph.model_dump_json()}")

        else:
            LOG.debug("get_user_ip_history cache not found, using mysql")
            records = self.get_user_ip_records_sql(user_id=user_id)
            # todo: we may get dns records from somewhere else here ...
            iph = UserIPHistory(user_id=user_id, ips=records)
            self.set_user_ip_history_cache(user_id=user_id, iph=iph)

        iph.enrich_ips(pg_config=self.pg_config, redis_config=self.redis_config)
        return iph

    def get_user_latest_ip(
        self, user: User, exclude_anon: bool = False
    ) -> Optional[str]:
        record = self.get_user_latest_ip_record(user=user, exclude_anon=exclude_anon)
        if record:
            return record.ip
        return None

    def get_user_latest_ip_record(
        self, user: User, exclude_anon: bool = False
    ) -> Optional[UserIPRecord]:
        iphistory = self.get_user_ip_history(user_id=user.user_id)

        if iphistory.ips:
            if exclude_anon:
                return next(
                    filter(
                        lambda x: not x.information.is_anonymous,
                        iphistory.ips[::-1],
                    ),
                    None,
                )
            else:
                return iphistory.ips[-1]

        return None

    def get_user_latest_country(
        self, user: User, exclude_anon: bool = False
    ) -> Optional[str]:
        """Get the country the user is in, based off their latest ip."""
        ipr = self.get_user_latest_ip_record(user, exclude_anon=exclude_anon)
        # The ipr.information should exist, but it is possible the user has
        #   no IP history at all, so the record is None
        return ipr.country_iso if ipr is not None else None

    def is_user_anonymous(self, user: User) -> Optional[bool]:
        # Get the user's latest ip. is it marked as anonymous?
        # Note: it is possible we only did a "basic" lookup of this IP so
        #   we don't know if they are anonymous. Default to False
        # Return None if the user has no IP history at all
        ipr = self.get_user_latest_ip_record(user)
        if ipr:
            return ipr.is_anonymous if ipr.is_anonymous is not None else False
        return None


class IPRecordManager(PostgresManagerWithRedis):

    def __init__(
        self,
        pg_config: PostgresConfig,
        redis_config: RedisConfig,
        permissions: Collection[Permission] = None,
        cache_prefix: Optional[str] = None,
    ):
        super().__init__(
            pg_config=pg_config,
            redis_config=redis_config,
            permissions=permissions,
            cache_prefix=cache_prefix,
        )
        self.user_ip_history_manager = UserIpHistoryManager(
            pg_config=self.pg_config,
            redis_config=self.redis_config,
            cache_prefix=self.cache_prefix,
            permissions=self.permissions,
        )

    def create_dummy(
        self,
        user_id: PositiveInt,
        ip: Optional[IPvAnyAddressStr] = None,
        forwarded_ip1: Optional[IPvAnyAddressStr] = None,
        forwarded_ip2: Optional[IPvAnyAddressStr] = None,
        forwarded_ip3: Optional[IPvAnyAddressStr] = None,
        forwarded_ip4: Optional[IPvAnyAddressStr] = None,
        forwarded_ip5: Optional[IPvAnyAddressStr] = None,
        forwarded_ip6: Optional[IPvAnyAddressStr] = None,
    ) -> IPRecord:
        return self.create(
            user_id=user_id,
            ip=ip or fake.ipv4_public(),
            forwarded_ip1=(forwarded_ip1 or fake.ipv4_public()),
            forwarded_ip2=(forwarded_ip2 or fake.ipv6() if random() < 0.5 else None),
            forwarded_ip3=(
                forwarded_ip3 or fake.ipv4_public() if random() < 0.25 else None
            ),
            forwarded_ip4=forwarded_ip4,
            forwarded_ip5=forwarded_ip5,
            forwarded_ip6=forwarded_ip6,
        )

    def create_unpack(
        self,
        user_id: PositiveInt,
        ip: IPvAnyAddressStr,
        forwarded_ips: List[str],
    ) -> IPRecord:
        if len(forwarded_ips) > 6:
            raise ValueError("A maximum of 6 forwarded IPs is allowed.")

        padded = list(forwarded_ips) + [None] * (6 - len(forwarded_ips))

        return self.create(user_id, ip, *padded)

    def create(
        self,
        user_id: PositiveInt,
        ip: IPvAnyAddressStr,
        forwarded_ip1: IPvAnyAddressStr,
        forwarded_ip2: IPvAnyAddressStr,
        forwarded_ip3: IPvAnyAddressStr,
        forwarded_ip4: IPvAnyAddressStr,
        forwarded_ip5: IPvAnyAddressStr,
        forwarded_ip6: IPvAnyAddressStr,
    ) -> IPRecord:

        data = {
            "user_id": user_id,
            "ip": ipaddress.ip_address(ip).exploded,
            "created": datetime.now(tz=timezone.utc),
        }

        fips_cols = [
            "forwarded_ip1",
            "forwarded_ip2",
            "forwarded_ip3",
            "forwarded_ip4",
            "forwarded_ip5",
            "forwarded_ip6",
        ]
        for col, ip in zip_longest(
            fips_cols,
            [
                forwarded_ip1,
                forwarded_ip2,
                forwarded_ip3,
                forwarded_ip4,
                forwarded_ip5,
                forwarded_ip6,
            ],
            fillvalue=None,
        ):
            data[col] = ipaddress.ip_address(ip).exploded if ip else ip

        self.pg_config.execute_write(
            query="""
                INSERT INTO userhealth_iphistory (
                    user_id, ip, created,
                    forwarded_ip1, forwarded_ip2, forwarded_ip3,
                    forwarded_ip4, forwarded_ip5, forwarded_ip6
                )
                VALUES (
                    %(user_id)s, %(ip)s, %(created)s,
                    %(forwarded_ip1)s, %(forwarded_ip2)s, %(forwarded_ip3)s,
                    %(forwarded_ip4)s, %(forwarded_ip5)s, %(forwarded_ip6)s
                );
            """,
            params=data,
        )
        self.recreate_user_ip_history_cache(user_id=user_id)

        return IPRecord.from_mysql(data)

    def get_user_latest_ip_record(self, user: User) -> Optional[IPRecord]:
        res = self.filter_ip_records(user_ids=[user.user_id], limit=1)
        if res:
            return res[0]
        return None

    def filter_ip_records(
        self,
        filter_ips: Optional[List[IPvAnyAddressStr]] = None,
        user_ids: Optional[List[PositiveInt]] = None,
        created_from: Optional[datetime] = None,
        limit: Optional[int] = None,
    ) -> List[IPRecord]:

        assert any([filter_ips, user_ids, created_from]), "Must provide filter criteria"

        if filter_ips is not None and not filter_ips:
            raise AssertionError("Must provide valid filter_ips filter lists")

        if user_ids is not None and not user_ids:
            raise AssertionError("Must provide valid user_id filter lists")

        filters = []
        params = {}
        if filter_ips:
            params["filter_ips"] = filter_ips
            filters.append("ip = ANY(%(filter_ips)s)")

        if user_ids:
            params["user_ids"] = user_ids
            filters.append("user_id = ANY(%(user_ids)s)")

        if created_from:
            params["created_from"] = created_from
            filters.append("created >= %(created_from)s")

        filter_str = " AND ".join(filters)
        filter_str = "WHERE " + filter_str if filter_str else ""
        if limit is not None:
            assert type(limit) is int
            assert 0 <= limit <= 1000
        limit_str = f"LIMIT {limit}" if limit is not None else ""

        res = self.pg_config.execute_sql_query(
            query=f"""
                SELECT  i.ip, i.user_id,
                        i.forwarded_ip1, i.forwarded_ip2,
                        i.forwarded_ip3, i.forwarded_ip4, 
                        i.forwarded_ip5, i.forwarded_ip6,
                        i.created 
                FROM userhealth_iphistory AS i
                {filter_str} 
                ORDER BY created DESC
                {limit_str}
            """,
            params=params,
        )

        return [IPRecord.from_mysql(i) for i in res]

    def recreate_user_ip_history_cache(self, user_id: int):
        return self.user_ip_history_manager.recreate_user_ip_history_cache(
            user_id=user_id
        )


class AuditLogManager(PostgresManager):

    def create_dummy(
        self,
        user_id: PositiveInt,
        level: Optional[AuditLogLevel] = None,
        event_type: Optional[str] = None,
        event_msg: Optional[str] = None,
        event_value: Optional[float] = None,
    ) -> AuditLog:

        event_types = {
            "offerwall-enter.blocked",
            "offerwall-enter.rate-limited",
            "offerwall-enter.url-modified",
        }

        return self.create(
            user_id=user_id,
            level=level or rchoice(list(AuditLogLevel)),
            event_type=event_type or rchoice(list(event_types)),
            event_msg=event_msg,
            event_value=event_value,
        )

    def create(
        self,
        user_id: PositiveInt,
        level: AuditLogLevel,
        event_type: str,
        event_msg: Optional[str] = None,
        event_value: Optional[float] = None,
    ) -> AuditLog:
        """AuditLogs may exist with the same event_type, and with different levels"""

        al = AuditLog.model_validate(
            {
                "user_id": user_id,
                "created": datetime.now(tz=timezone.utc),
                "level": level,
                "event_type": event_type,
                "event_msg": event_msg,
                "event_value": event_value,
            }
        )

        with self.pg_config.make_connection() as conn:
            with conn.cursor() as c:
                c.execute(
                    query="""
                        INSERT INTO userhealth_auditlog
                        (user_id, created, level, 
                         event_type, event_msg, event_value)
                        VALUES ( %(user_id)s , %(created)s, %(level)s, 
                                 %(event_type)s, %(event_msg)s, %(event_value)s)
                        RETURNING id;
                    """,
                    params=al.model_dump_mysql(),
                )
                pk = c.fetchone()["id"]  # type: ignore
                conn.commit()

        al.id = pk
        return al

    def get_by_id(self, auditlog_id: PositiveInt) -> AuditLog:

        res = self.pg_config.execute_sql_query(
            query="""
                SELECT al.*
                FROM userhealth_auditlog AS al
                WHERE al.id = %s
                LIMIT 2;
            """,
            params=(auditlog_id,),
        )

        if len(res) == 0:
            raise Exception(f"No AuditLog with id of '{auditlog_id}'")

        if len(res) > 1:
            raise Exception(f"Too many AuditLog found with id of '{auditlog_id}'")

        return AuditLog.from_mysql(res[0])

    def filter_by_product(self, product: Product) -> List[AuditLog]:

        res = self.pg_config.execute_sql_query(
            query="""
                SELECT al.* 
                FROM userhealth_auditlog AS al
                INNER JOIN thl_user AS u
                    ON u.id = al.user_id
                WHERE u.product_id = %s 
                ORDER BY al.created DESC
                LIMIT 2500;
            """,
            params=(product.uuid,),
        )

        return [AuditLog.from_mysql(i) for i in res]

    def filter_by_user_id(self, user_id: PositiveInt) -> List[AuditLog]:
        res = self.pg_config.execute_sql_query(
            query="""
                SELECT * 
                FROM userhealth_auditlog AS al
                WHERE al.user_id = %s
                ORDER BY al.created DESC
                LIMIT 2500;
            """,
            params=(user_id,),
        )

        return [AuditLog.from_mysql(i) for i in res]

    def filter(
        self,
        user_ids: Collection[int],
        level: Optional[int] = None,
        level_ge: Optional[int] = None,
        event_type: Optional[str] = None,
        event_type_like: Optional[str] = None,
        event_msg: Optional[str] = None,
        created_after: Optional[datetime] = None,
    ) -> List[AuditLog]:

        filter_str, args = self.make_filter_str(
            user_ids=user_ids,
            level=level,
            level_ge=level_ge,
            event_type=event_type,
            event_type_like=event_type_like,
            event_msg=event_msg,
            created_after=created_after,
        )

        res = self.pg_config.execute_sql_query(
            query=f"""
                SELECT  user_id, created, level, event_type, 
                        event_msg, event_value
                FROM userhealth_auditlog
                WHERE {filter_str}
            """,
            params=args,
        )

        return [AuditLog.from_mysql(i) for i in res]

    def filter_count(
        self,
        user_ids: Collection[int],
        level: Optional[int] = None,
        level_ge: Optional[int] = None,
        event_type: Optional[str] = None,
        event_type_like: Optional[str] = None,
        event_msg: Optional[str] = None,
        created_after: Optional[datetime] = None,
    ) -> NonNegativeInt:

        filter_str, args = self.make_filter_str(
            user_ids=user_ids,
            level=level,
            level_ge=level_ge,
            event_type=event_type,
            event_type_like=event_type_like,
            event_msg=event_msg,
            created_after=created_after,
        )

        res = self.pg_config.execute_sql_query(
            query=f"""
                SELECT COUNT(1) as c 
                FROM userhealth_auditlog
                WHERE {filter_str}
            """,
            params=args,
        )

        assert len(res) == 1

        return int(res[0]["c"])  # type: ignore

    @staticmethod
    def make_filter_str(
        user_ids: Collection[int],
        level: Optional[int] = None,
        level_ge: Optional[int] = None,
        event_type: Optional[str] = None,
        event_type_like: Optional[str] = None,
        event_msg: Optional[str] = None,
        created_after: Optional[datetime] = None,
    ) -> Tuple[str, Dict[str, Any]]:
        assert user_ids, "must pass at least 1 user_id"
        assert all(
            [isinstance(uid, int) for uid in user_ids]
        ), "must pass user_id as int"

        if created_after is None:
            created_after = datetime.now(tz=timezone.utc) - timedelta(days=7)

        filters = [
            "user_id = ANY(%(user_ids)s)",
            "created >= %(created_after)s",
        ]
        args = {"user_ids": user_ids, "created_after": created_after}

        if level:
            assert level_ge is None
            filters.append("level = %(level)s")
            args["level"] = level
        if level_ge:
            assert level is None
            filters.append("level >= %(level_ge)s")
            args["level_ge"] = level_ge

        if event_type:
            assert event_type_like is None
            filters.append("event_type = %(event_type)s")
            args["event_type"] = event_type
        if event_type_like:
            assert event_type is None
            filters.append("event_type LIKE %(event_type_like)s")
            args["event_type_like"] = event_type_like

        if event_msg:
            filters.append("event_msg = %(event_msg)s")
            args["event_msg"] = event_msg

        filter_str = " AND ".join(filters)
        return filter_str, args
