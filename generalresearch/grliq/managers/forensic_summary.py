from __future__ import annotations

import statistics
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from typing import List, Dict

import numpy as np

from generalresearch.grliq.managers.forensic_data import GrlIqDataManager
from generalresearch.grliq.managers.forensic_events import (
    GrlIqEventManager,
)
from generalresearch.grliq.models.forensic_result import (
    GrlIqForensicCategoryResult,
    GrlIqCheckerResults,
)
from generalresearch.grliq.models.forensic_summary import (
    GrlIqForensicCategorySummary,
    GrlIqCheckerResultsSummary,
    UserForensicSummary,
    CountryRTTDistribution,
    TimingDataCountrySummary,
)
from generalresearch.models.thl.user import User
from generalresearch.redis_helper import RedisConfig


def calculate_category_summary(
    res: List[GrlIqForensicCategoryResult],
) -> GrlIqForensicCategorySummary:
    totals = defaultdict(int)
    is_complete_count = 0
    is_attempt_allowed_count = 0
    n = len(res)
    fields = GrlIqForensicCategoryResult.model_score_fields()
    fraud_score = 0

    for r in res:
        fraud_score += r.fraud_score
        if r.is_complete:
            is_complete_count += 1
        if r.is_attempt_allowed():
            is_attempt_allowed_count += 1
        for field in fields:
            totals[field] += getattr(r, field)

    return GrlIqForensicCategorySummary(
        attempt_count=n,
        is_attempt_allowed_count=is_attempt_allowed_count,
        is_complete_rate=is_complete_count / n if n else 0.0,
        fraud_score_avg=fraud_score / n if n else None,
        **{f"{field}_avg": totals[field] / n if n else 0.0 for field in totals},
    )


def calculate_checker_summary(
    res: List[GrlIqCheckerResults],
) -> GrlIqCheckerResultsSummary:
    totals = defaultdict(list)
    none_totals = defaultdict(int)
    n = len(res)
    fields = [f for f in GrlIqCheckerResults.model_fields if f.startswith("check_")]

    for r in res:
        for field in fields:
            value = getattr(r, field)
            if value is None:
                none_totals[field] += 1
            else:
                totals[field].append(value.score)
    field_avg = {f"{k}_avg": statistics.mean(v) if v else 0 for k, v in totals.items()}
    field_pct_none = {
        f"{k}_pct_none": v / n if n else 0 for k, v in none_totals.items()
    }
    field_avg.update(
        {
            k.replace("_pct_none", "_avg"): None
            for k, v in field_pct_none.items()
            if v == 1
        }
    )

    return GrlIqCheckerResultsSummary(**field_avg, **field_pct_none)


def calculate_timing_summary(
    redis_config: RedisConfig, timing_res
) -> Dict[str, TimingDataCountrySummary]:
    country_median_rtts = defaultdict(list)
    for x in timing_res:
        s = x["timing_data"].summarize
        if s:
            country_median_rtts[x["country_iso"]].append(
                float(np.exp(s.median_log_rtt))
            )
    country_isos = list(country_median_rtts.keys())

    rc = redis_config.create_redis_client()
    country_distributions = dict(
        zip(
            country_isos,
            rc.hmget("grl-iq:country_rtt_distributions", *country_isos),
        )
    )
    country_distributions = {
        k: CountryRTTDistribution.model_validate_json(v)
        for k, v in country_distributions.items()
    }

    out = dict()
    for country_iso, median_rtts in country_median_rtts.items():
        country_stats = country_distributions[country_iso]
        z_scores = [
            (np.log(x) - country_stats.rtt_log_mean) / country_stats.rtt_log_std
            for x in median_rtts
        ]
        out[country_iso] = TimingDataCountrySummary(
            country_iso=country_iso,
            rtt_min=min(median_rtts),
            rtt_max=max(median_rtts),
            rtt_mean=statistics.mean(median_rtts),
            rtt_median=statistics.median(median_rtts),
            rtt_q25=float(np.quantile(median_rtts, 0.25)),
            rtt_q75=float(np.quantile(median_rtts, 0.75)),
            expected_rtt_range=country_distributions[country_iso].expected_rtt_range,
            mean_z_score=statistics.mean(z_scores),
        )
    return out


def run_user_forensic_summary(
    iq_dm: GrlIqDataManager,
    iq_em: GrlIqEventManager,
    redis_config: RedisConfig,
    user: User,
) -> UserForensicSummary:
    now = datetime.now(tz=timezone.utc)
    created_between = (now - timedelta(days=90), now)
    select_str = "id, session_uuid, product_id, product_user_id, created_at, result_data, category_result"
    res = iq_dm.filter(
        select_str=select_str,
        user=user,
        created_between=created_between,
        limit=500,
        order_by="created_at DESC",
    )
    period_start = min([x["created_at"] for x in res]) if res else None
    period_end = max([x["created_at"] for x in res]) if res else None

    category_result_summary = (
        calculate_category_summary([x["category_result"] for x in res]) if res else None
    )
    checker_result_summary = (
        calculate_checker_summary([x["result_data"] for x in res]) if res else None
    )

    session_uuids = {x["session_uuid"] for x in res}
    timing_res: List[Dict] = iq_em.filter_distinct_timing(session_uuids=session_uuids)

    country_timing_data_summary = (
        calculate_timing_summary(redis_config=redis_config, timing_res=timing_res)
        if timing_res
        else dict()
    )

    s = UserForensicSummary(
        period_start=period_start,
        period_end=period_end,
        category_result_summary=category_result_summary,
        checker_result_summary=checker_result_summary,
        country_timing_data_summary=country_timing_data_summary,
        ip_timing_data_summary={},
    )
    return s
