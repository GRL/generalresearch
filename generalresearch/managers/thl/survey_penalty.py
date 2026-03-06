import json
import threading
from collections import defaultdict
from datetime import timedelta
from typing import Optional, List, Tuple, Dict

from generalresearch.decorators import LOG
from generalresearch.managers.base import RedisManager
from generalresearch.models.custom_types import (
    UUIDStr,
)
from generalresearch.models.thl.survey.penalty import (
    BPSurveyPenalty,
    TeamSurveyPenalty,
    PenaltyListAdapter,
    Penalty,
)
from generalresearch.redis_helper import RedisConfig
from cachetools import cachedmethod, TTLCache


class SurveyPenaltyManager(RedisManager):
    """
    Penalties are stored in redis with keys index by the product_id or team_id.
    So getting the penalties for a BP will return all surveys that have
        penalties for that BP.
    The redis object is a hash, where the key is "survey-penalty-{bp/team}",
        and each has fields per source. The field value is a list of
        json-dumped SurveyPenalty objects.
    Since we calculate the penalties batched by marketplace, when we set
        this field it *replaces* all previous penalties for that
        BP/team - marketplace.
    """

    def __init__(
        self,
        redis_config: RedisConfig,
        cache_prefix: Optional[str] = None,
        **kwargs,
    ):
        super().__init__(redis_config=redis_config, cache_prefix=cache_prefix, **kwargs)
        self.redis_prefix = (
            f"{self.cache_prefix}:survey-penalty"
            if self.cache_prefix
            else "survey-penalty"
        )
        self.cache = TTLCache(maxsize=128, ttl=60)
        self.cache_lock = threading.Lock()

    def get_redis_key(self, penalty: Penalty) -> str:
        if penalty.kind == "team":
            return f"{self.redis_prefix}:{penalty.team_id}"
        elif penalty.kind == "bp":
            return f"{self.redis_prefix}:{penalty.product_id}"
        else:
            raise AssertionError("unreachable")

    def get_redis_key_for_id(self, uuid_id: UUIDStr):
        return f"{self.redis_prefix}:{uuid_id}"

    def set_penalties(self, penalties: List[Penalty]):
        """ """
        if len(penalties) > 1000:
            LOG.warning("SurveyPenaltyManager.set_penalties batch me!")
        assert len(penalties) < 10_000, "something is surely wrong"
        self.cache.clear()
        d = defaultdict(lambda: defaultdict(list))
        for p in penalties:
            d[self.get_redis_key(p)][p.source.value].append(p.model_dump(mode="json"))

        pipe = self.redis_client.pipeline(transaction=False)
        for key, mapping in d.items():
            mapping = {
                k: json.dumps(v, separators=(",", ":")) for k, v in mapping.items()
            }
            pipe.hmset(key, mapping=mapping)
            pipe.expire(key, timedelta(days=1))
            pipe.hexpire(key, timedelta(days=1), *mapping.keys())
        pipe.execute()

    def _load_penalties(
        self, product_id: UUIDStr, team_id: UUIDStr
    ) -> Tuple[List[BPSurveyPenalty], List[TeamSurveyPenalty]]:
        pipe = self.redis_client.pipeline(transaction=False)
        bp_res, team_res = (
            pipe.hgetall(self.get_redis_key_for_id(product_id))
            .hgetall(self.get_redis_key_for_id(team_id))
            .execute()
        )
        bp_penalties = []
        for v in bp_res.values():
            bp_penalties.extend(PenaltyListAdapter.validate_python(json.loads(v)))
        team_penalties = []
        for v in team_res.values():
            team_penalties.extend(PenaltyListAdapter.validate_python(json.loads(v)))
        return bp_penalties, team_penalties

    @cachedmethod(lambda self: self.cache, lock=lambda self: self.cache_lock)
    def get_penalties_for(
        self, product_id: UUIDStr, team_id: UUIDStr
    ) -> Dict[str, float]:
        """
        Returns a dict with keys survey sids ({source}:{survey_id}) and values penalties.
        e.g. {'s:1234': 0.8}
        """
        bp_penalties, team_penalties = self._load_penalties(
            product_id=product_id, team_id=team_id
        )
        penalties: dict[str, float] = {}
        for p in (*bp_penalties, *team_penalties):
            penalties[p.sid] = max(p.penalty, penalties.get(p.sid, 0.0))
        return penalties
