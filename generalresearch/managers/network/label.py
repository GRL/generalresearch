from datetime import datetime, timezone, timedelta
from typing import Collection, Optional, List

from psycopg import sql
from pydantic import TypeAdapter, IPvAnyNetwork

from generalresearch.managers.base import PostgresManager
from generalresearch.models.custom_types import (
    AwareDatetimeISO,
    IPvAnyAddressStr,
    IPvAnyNetworkStr,
)
from generalresearch.models.network.label import IPLabel, IPLabelKind, IPLabelSource


class IPLabelManager(PostgresManager):
    def create(self, ip_label: IPLabel) -> IPLabel:
        query = sql.SQL(
            """
        INSERT INTO network_iplabel (
            ip, labeled_at, created_at,
            label_kind, source, confidence,
            provider, metadata
        ) VALUES (
            %(ip)s, %(labeled_at)s, %(created_at)s,
            %(label_kind)s, %(source)s, %(confidence)s,
            %(provider)s, %(metadata)s
        ) RETURNING id;"""
        )
        params = ip_label.model_dump_postgres()
        with self.pg_config.make_connection() as conn:
            with conn.cursor() as c:
                c.execute(query, params)
                pk = c.fetchone()["id"]
        return ip_label

    def make_filter_str(
        self,
        ips: Optional[Collection[IPvAnyNetworkStr]] = None,
        ip_in_network: Optional[IPvAnyAddressStr] = None,
        label_kind: Optional[IPLabelKind] = None,
        source: Optional[IPLabelSource] = None,
        labeled_at: Optional[AwareDatetimeISO] = None,
        labeled_after: Optional[AwareDatetimeISO] = None,
        labeled_before: Optional[AwareDatetimeISO] = None,
        provider: Optional[str] = None,
    ):
        filters = []
        params = {}
        if labeled_after or labeled_before:
            time_end = labeled_before or datetime.now(tz=timezone.utc)
            time_start = labeled_after or datetime(2017, 1, 1, tzinfo=timezone.utc)
            assert time_start.tzinfo.utcoffset(time_start) == timedelta(), "must be UTC"
            assert time_end.tzinfo.utcoffset(time_end) == timedelta(), "must be UTC"
            filters.append("labeled_at BETWEEN %(time_start)s AND %(time_end)s")
            params["time_start"] = time_start
            params["time_end"] = time_end
        if labeled_at:
            assert labeled_at.tzinfo.utcoffset(labeled_at) == timedelta(), "must be UTC"
            filters.append("labeled_at == %(labeled_at)s")
            params["labeled_at"] = labeled_at
        if label_kind:
            filters.append("label_kind = %(label_kind)s")
            params["label_kind"] = label_kind.value
        if source:
            filters.append("source = %(source)s")
            params["source"] = source.value
        if provider:
            filters.append("provider = %(provider)s")
            params["provider"] = provider
        if ips is not None:
            filters.append("ip = ANY(%(ips)s)")
            params["ips"] = list(ips)
        if ip_in_network:
            """
            Return matching networks.
            e.g. ip = '13f9:c462:e039:a38c::1', might return rows
            where ip = '13f9:c462:e039::/48' or '13f9:c462:e039:a38c::/64'
            """
            filters.append("ip >>= %(ip_in_network)s")
            params["ip_in_network"] = ip_in_network

        filter_str = "WHERE " + " AND ".join(filters) if filters else ""
        return filter_str, params

    def filter(
        self,
        ips: Optional[Collection[IPvAnyNetworkStr]] = None,
        ip_in_network: Optional[IPvAnyAddressStr] = None,
        label_kind: Optional[IPLabelKind] = None,
        source: Optional[IPLabelSource] = None,
        labeled_at: Optional[AwareDatetimeISO] = None,
        labeled_after: Optional[AwareDatetimeISO] = None,
        labeled_before: Optional[AwareDatetimeISO] = None,
        provider: Optional[str] = None,
    ) -> List[IPLabel]:
        filter_str, params = self.make_filter_str(
            ips=ips,
            ip_in_network=ip_in_network,
            label_kind=label_kind,
            source=source,
            labeled_at=labeled_at,
            labeled_after=labeled_after,
            labeled_before=labeled_before,
            provider=provider,
        )
        query = f"""
        SELECT 
            ip, labeled_at, created_at,
            label_kind, source, confidence,
            provider, metadata
        FROM network_iplabel
        {filter_str}
        """
        res = self.pg_config.execute_sql_query(query, params)
        return [IPLabel.model_validate(rec) for rec in res]

    def get_most_specific_matching_network(self, ip: IPvAnyAddressStr) -> IPvAnyNetwork:
        """
        e.g. ip = 'b5f4:dc2:f136:70d5:5b6e:9a85:c7d4:3517', might return
        'b5f4:dc2:f136:70d5::/64'
        """
        ip = TypeAdapter(IPvAnyAddressStr).validate_python(ip)

        query = """
        SELECT ip
        FROM network_iplabel
        WHERE ip >>= %(ip)s
        ORDER BY masklen(ip) DESC
        LIMIT 1;"""
        res = self.pg_config.execute_sql_query(query, {"ip": ip})
        if res:
            return IPvAnyNetwork(res[0]["ip"])

    def test_join(self, ip):
        query = """
        SELECT
            to_jsonb(i) AS ipinfo,
            to_jsonb(l) AS iplabel
        FROM thl_ipinformation i
        LEFT JOIN network_iplabel l
          ON l.ip >>= i.ip
        WHERE i.ip = %(ip)s
        ORDER BY masklen(l.ip) DESC;"""
        params = {"ip": ip}
        res = self.pg_config.execute_sql_query(query, params)
        return res
