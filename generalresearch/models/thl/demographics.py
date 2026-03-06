from __future__ import annotations

import copy
from collections import defaultdict, Counter
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Literal, List, Dict

import numpy as np

from generalresearch.models.thl.locales import CountryISO

if TYPE_CHECKING:
    from generalresearch.models.thl.survey import MarketplaceTask


@dataclass(frozen=True)
class DemographicTarget:
    country: CountryISO | Literal["*"]
    gender: Gender | Literal["*"]
    age_group: AgeGroup | Literal["*"]

    def __post_init__(self):
        assert self.country == self.country.lower(), "country must be lower"

    def to_tags(self):
        gender = self.gender.value if isinstance(self.gender, Gender) else "*"
        age_group = (
            self.age_group.value if isinstance(self.age_group, AgeGroup) else "*"
        )
        return {
            "country": self.country,
            "gender": gender,
            "age_group": age_group,
        }


class Gender(str, Enum):
    """
    The respondent's gender
    """

    MALE = "male"
    FEMALE = "female"
    OTHER = "other"


class AgeGroup(Enum):
    """
    The respondent's age.
    """

    AGE_UNDER_18 = (0, 17, "<18")
    AGE_18_TO_35 = (18, 35, "18-35")
    AGE_36_TO_55 = (36, 55, "36-55")
    AGE_56_TO_75 = (56, 75, "56-75")
    AGE_OVER_75 = (76, 120, ">75")

    def __init__(self, low: int, high: int, label: str):
        # [inclusive,
        self._low = low
        # exclusive)
        self._high = high
        self.label = label

    @property
    def low(self):
        return self._low

    @property
    def high(self):
        return self._high

    @property
    def value(self):
        return self.label


def calculate_demographic_metrics(opps: List[MarketplaceTask]):
    """
    Measurement: marketplace_survey_demographics
    tags: source (marketplace)
        : (all combinations of): country, gender, age_groups
    values/fields: cost (aka cpi) (min, p25, p50, mean, p75, p90, p95, p99, max)
                 : count, open_count
    """
    source = {opp.source for opp in opps}
    assert len(source) == 1
    source = list(source)[0]
    survey_cpi = defaultdict(list)
    target_open = defaultdict(int)
    for opp in opps:
        is_open = opp.is_open
        cpi = float(opp.cpi)
        tgs = opp.demographic_targets
        for t in tgs:
            survey_cpi[t].append(cpi)
            if is_open:
                target_open[t] += 1
    survey_counter = {k: len(v) for k, v in survey_cpi.items()}
    survey_counter = {k: {"count": v} for k, v in survey_counter.items() if v}

    grp_stats = dict()
    for grp, costs in survey_cpi.items():
        stats = {
            "cost_min": np.min(costs),
            "cost_p25": np.percentile(costs, 25),
            "cost_p50": np.median(costs),
            "cost_mean": np.mean(costs),
            "cost_p75": np.percentile(costs, 75),
            "cost_p90": np.percentile(costs, 90),
            "cost_p95": np.percentile(costs, 95),
            "cost_p99": np.percentile(costs, 99),
            "cost_max": np.max(costs),
            "open_count": target_open[grp],
        }
        grp_stats[grp] = stats
        survey_counter[grp].update(stats)

    # fmt: off
    TOP_COUNTRIES = [
        'us', 'cn', 'au', 'gb', 'kr', 'de', 'at', 'fr', 'es', 'jp',
        'ca', 'ie', 'br', 'mx', 'nl', 'ar', 'nz', 'in', 'sg', 'it',
        'be', 'hk', 'ch', 'co', 'my'
    ]
    # fmt: on
    survey_counter = {
        k: v
        for k, v in survey_counter.items()
        if k.country == "*" or k.country in TOP_COUNTRIES
    }

    base = {
        "measurement": "marketplace_survey_demographics",
        "tags": {"source": source.value},
        "fields": {},
    }
    points = []
    for k, v in survey_counter.items():
        d = copy.deepcopy(base)
        d["tags"].update(k.to_tags())
        d["fields"].update(v)
        points.append(d)
    return points


def calculate_used_question_metrics(
    opps: List[MarketplaceTask], qid_label: Dict[str, str]
):
    """
    Measurement: marketplace_survey_targeting
    tags: source (marketplace), "type", country (all and individual)
    values/fields: {question_label: count}
    """
    source = {opp.source for opp in opps}
    assert len(source) == 1
    source = list(source)[0]
    country_q_counter = defaultdict(Counter)
    for opp in opps:
        for q in opp.used_question_ids:
            if q not in qid_label:
                continue
            label = qid_label[q]
            country_q_counter["*"][label] += 1
            country_q_counter[opp.country_iso][label] += 1

    points = []
    for country, q in country_q_counter.items():
        points.append(
            {
                "measurement": "marketplace_survey_targeting",
                "tags": {
                    "source": source.value,
                    "type": "question_label",
                    "country": country,
                },
                "fields": dict(q),
            }
        )
    return points
