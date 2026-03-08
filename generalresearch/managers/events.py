import logging
import math
import socket
import threading
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import TYPE_CHECKING, Dict, List, Optional, Set, Union

from redis.client import PubSub, Redis

from generalresearch.managers.base import RedisManager
from generalresearch.models import Source
from generalresearch.models.custom_types import UUIDStr
from generalresearch.models.events import (
    AggregateBySource,
    EventEnvelope,
    EventMessage,
    EventType,
    MaxGaugeBySource,
    ServerToClientMessage,
    ServerToClientMessageAdapter,
    SessionEnterPayload,
    SessionFinishPayload,
    StatsMessage,
    TaskEnterPayload,
    TaskFinishPayload,
    TaskStatsSnapshot,
)
from generalresearch.models.thl.definitions import Status
from generalresearch.models.thl.session import Session, Wall
from generalresearch.models.thl.user import User

if TYPE_CHECKING:
    from influxdb import InfluxDBClient
else:
    InfluxDBClient = object

# Sums all the values in a single hashmap
SUM_HASH_LUA_SCRIPT = """
local vals = redis.call("HVALS", KEYS[1])
local sum = 0
for i = 1, #vals do
    sum = sum + tonumber(vals[i])
end
return sum
"""

# Returns the max of all the values in a single hashmap
MAX_HASH_LUA_SCRIPT = """
local vals = redis.call("HVALS", KEYS[1])
local max = nil
for i = 1, #vals do
    local v = tonumber(vals[i])
    if v then
        if not max or v > max then
            max = v
        end
    end
end
return max
"""


class UserStatsManager(RedisManager):
    """
    We store a hashmap for each of last 1hr, last 24hr, for
    global counts and per BP. The hashmap's key is the user,
    and it expires N hours after it gets set.
    To calculate the active user count, we simply get
    the number of keys in the hashmap.
    """

    def handle_user(self, user: User):
        self.mark_user_active(user=user)
        self.handle_user_signup(user=user)

    def get_user_stats(self, product_id: UUIDStr):
        r = self.redis_client
        pipe = r.pipeline(transaction=False)

        keys = [
            f"active_users_last_1h:{product_id}",
            f"active_users_last_24h:{product_id}",
            f"signups_last_24h:{product_id}",
            f"in_progress_users:{product_id}",
        ]
        keys_out = [
            "active_users_last_1h",
            "active_users_last_24h",
            "signups_last_24h",
            "in_progress_users",
        ]

        for k in keys:
            pipe.hlen(k)

        return {k: v for k, v in zip(keys_out, pipe.execute())}

    def get_global_user_stats(self):
        r = self.redis_client
        pipe = r.pipeline(transaction=False)

        keys = [
            "active_users_last_1h",
            "active_users_last_24h",
            "signups_last_24h",
            "in_progress_users",
        ]
        for k in keys:
            pipe.hlen(k)

        return {k: v for k, v in zip(keys, pipe.execute())}

    def handle_user_signup(self, user: User) -> None:
        # Use the user.created timestamp instead of "now". This allows
        #   us to test this function also, and we also can avoid
        #   having the caller do any sort of logic, this just always gets called.
        # The key is the user's ID so this can be called multiple times
        #   without side effects.
        if user.created is None:
            return None
        now = round(time.time())
        sec_24hr = round(timedelta(hours=24).total_seconds())

        minute = int(user.created.timestamp() // 60) * 60
        expires_at = minute + sec_24hr
        ttl = expires_at - now
        if ttl <= 0:
            return None

        pipe = self.redis_client.pipeline()
        name = "signups_last_24h"
        pipe.hset(name, user.uuid, now)
        pipe.hexpire(name, ttl, user.uuid)
        name = f"signups_last_24h:{user.product_id}"
        pipe.hset(name, user.product_user_id, now)
        pipe.hexpire(name, ttl, user.product_user_id)
        pipe.execute()
        return None

    def mark_user_active(self, user: User) -> None:
        now = datetime.now(tz=timezone.utc).isoformat()
        r = self.redis_client

        pipe = r.pipeline(transaction=False)

        name_1h_bpid = f"active_users_last_1h:{user.product_id}"
        key = user.product_user_id  # I could use user.uuid here also
        # store last-seen timestamp (value (now) is informational, we could just store "1")
        pipe.hset(name_1h_bpid, key, now)
        # refresh TTL for this user only
        pipe.hexpire(name_1h_bpid, timedelta(hours=1), key)

        name_1h_global = "active_users_last_1h"
        key = user.uuid  # must use user.uuid here b/c user.bpuid might not be unique
        pipe.hset(name_1h_global, key, now)
        pipe.hexpire(name_1h_global, timedelta(hours=1), key)

        name_24h_bpid = f"active_users_last_24h:{user.product_id}"
        key = user.product_user_id
        pipe.hset(name_24h_bpid, key, now)
        pipe.hexpire(name_24h_bpid, timedelta(hours=24), key)

        name_24h_global = "active_users_last_24h"
        key = user.uuid
        pipe.hset(name_24h_global, key, now)
        pipe.hexpire(name_24h_global, timedelta(hours=24), key)

        pipe.execute()

    def mark_user_inprogress(self, user: User) -> None:
        # Call when a user enters a Session
        # This call is idempotent; it can be called multiple times (for the
        # same user) and won't falsely increase a counter; it will just
        # reset the expiration for this user (times out after 60 min)
        now = datetime.now(tz=timezone.utc).isoformat()
        r = self.redis_client
        pipe = r.pipeline(transaction=False)

        name = f"in_progress_users:{user.product_id}"
        key = user.product_user_id  # I could use user.uuid here also
        # store last-seen timestamp (value (now) is informational, we could just store "1")
        pipe.hset(name, key, now)
        # Expire after 1 hr
        pipe.hexpire(name, timedelta(hours=1), key)

        name = "in_progress_users"
        key = user.uuid  # must use user.uuid here b/c user.bpuid might not be unique
        pipe.hset(name, key, now)
        pipe.hexpire(name, timedelta(hours=1), key)
        pipe.execute()
        return None

    def unmark_user_inprogress(self, user: User):
        # Call when a user exits a Session
        # This call is idempotent; it can be called multiple times (for the same user)
        #   and won't falsely decrease a counter.
        r = self.redis_client
        pipe = r.pipeline(transaction=False)

        name = f"in_progress_users:{user.product_id}"
        # Delete the key, whether it exists or not
        pipe.hdel(name, user.product_user_id)
        name = "in_progress_users"
        pipe.hdel(name, user.uuid)
        pipe.execute()
        return None

    def clear_global_user_stats(self) -> None:
        # For testing
        r = self.redis_client
        r.delete("active_users_last_1h")
        r.delete("active_users_last_24h")
        r.delete("signups_last_24h")
        r.delete("in_progress_users")
        return None


class TaskStatsManager(RedisManager):
    task_stats = [
        "task_created_count_last_1h",
        "task_created_count_last_24h",
        "live_task_count",
        "live_tasks_max_payout",
        "TaskStatsManager:latest",
    ]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.SUM_HASH_LUA = self.redis_client.register_script(SUM_HASH_LUA_SCRIPT)
        self.MAX_HASH_LUA = self.redis_client.register_script(MAX_HASH_LUA_SCRIPT)

    def set_source_task_stats(
        self,
        source: Source,
        live_task_count: int,
        live_tasks_max_payout: Decimal,
        created_count: int = 0,
    ):
        self._incr_task_created_count(source=source, created_count=created_count)
        self._set_live_task_stats(
            source=source,
            live_task_count=live_task_count,
            live_tasks_max_payout=live_tasks_max_payout,
        )
        self.refresh_latest_task_stats()

    def refresh_latest_task_stats(self):
        res = TaskStatsSnapshot.model_validate(self.get_task_stats_raw())
        self.redis_client.set(
            "TaskStatsManager:latest", res.model_dump_json(), ex=timedelta(hours=24)
        )

    def get_latest_task_stats(self) -> Optional[TaskStatsSnapshot]:
        res = self.redis_client.get("TaskStatsManager:latest")
        if res is not None:
            return TaskStatsSnapshot.model_validate_json(res)
        return None

    def _incr_task_created_count(self, source: Source, created_count: int) -> None:
        now = round(time.time())
        minute = int(now // 60) * 60
        key = str(minute)
        sec_24hr = round(timedelta(hours=24).total_seconds())
        sec_1hr = round(timedelta(hours=1).total_seconds())

        expires_at_24hr = minute + sec_24hr
        ttl_24hr = expires_at_24hr - now
        expires_at_1hr = minute + sec_1hr
        ttl_1hr = expires_at_1hr - now

        pipe = self.redis_client.pipeline(transaction=False)
        name_1h_all = "task_created_count_last_1h"
        pipe.hincrby(name_1h_all, key, created_count)
        pipe.hexpire(name_1h_all, ttl_1hr, key)
        name_24h_all = "task_created_count_last_24h"
        pipe.hincrby(name_24h_all, key, created_count)
        pipe.hexpire(name_24h_all, ttl_24hr, key)

        name_1h_source = f"task_created_count_last_1h:{source.value}"
        pipe.hincrby(name_1h_source, key, created_count)
        pipe.hexpire(name_1h_source, ttl_1hr, key)
        name_24h_source = f"task_created_count_last_24h:{source.value}"
        pipe.hincrby(name_24h_source, key, created_count)
        pipe.hexpire(name_24h_source, ttl_24hr, key)

        pipe.execute()
        return None

    def _set_live_task_stats(
        self, source: Source, live_task_count: int, live_tasks_max_payout: Decimal
    ):
        # Keep the live stats per source. The total is the sum across all sources
        pipe = self.redis_client.pipeline(transaction=False)

        # keys are source, value is the live task count
        name = "live_task_count"
        pipe.hset(name, source.value, live_task_count)
        pipe.hexpire(name, timedelta(hours=24), source.value)

        # keys are source, value is the max_payout
        name = "live_tasks_max_payout"
        pipe.hset(name, source.value, round(live_tasks_max_payout * 100))
        pipe.hexpire(name, timedelta(hours=24), source.value)

        pipe.execute()

    def get_active_sources(self) -> List[Source]:
        return [Source(x) for x in self.redis_client.hkeys("live_task_count")]

    def get_task_stats_raw(
        self,
    ) -> Dict[str, Union[AggregateBySource, MaxGaugeBySource]]:
        sources = self.get_active_sources()

        pipe = self.redis_client.pipeline(transaction=False)
        pipe.hgetall("live_task_count")
        pipe.hgetall("live_tasks_max_payout")
        for source in sources:
            self.SUM_HASH_LUA(
                keys=[f"task_created_count_last_1h:{source.value}"],
                client=pipe,
            )
            self.SUM_HASH_LUA(
                keys=[f"task_created_count_last_24h:{source.value}"],
                client=pipe,
            )
        pipe_res = pipe.execute()
        live_task_count_raw = pipe_res.pop(0)
        live_tasks_max_payout_raw = pipe_res.pop(0)

        live_task_count_by_source = {
            Source(k): int(v) for k, v in live_task_count_raw.items()
        }
        live_task_count = AggregateBySource(
            total=sum(live_task_count_by_source.values()),
            by_source=live_task_count_by_source,
        )

        live_tasks_max_payout_by_source = {
            Source(k): int(v) for k, v in live_tasks_max_payout_raw.items()
        }
        live_tasks_max_payout = MaxGaugeBySource(
            value=max(live_tasks_max_payout_by_source.values(), default=None),
            by_source=live_tasks_max_payout_by_source,
        )

        task_created_count_last_1h = dict()
        task_created_count_last_24h = dict()
        for source in sources:
            task_created_count_last_1h[source] = pipe_res.pop(0)
            task_created_count_last_24h[source] = pipe_res.pop(0)
        task_created_count_last_1h = AggregateBySource(
            total=sum(task_created_count_last_1h.values()),
            by_source=task_created_count_last_1h,
        )
        task_created_count_last_24h = AggregateBySource(
            total=sum(task_created_count_last_24h.values()),
            by_source=task_created_count_last_24h,
        )

        return {
            "task_created_count_last_1h": task_created_count_last_1h,
            "task_created_count_last_24h": task_created_count_last_24h,
            "live_task_count": live_task_count,
            "live_tasks_max_payout": live_tasks_max_payout,
        }

    def clear_task_stats(self) -> None:
        keys = self.task_stats.copy()
        keys.extend([f"task_created_count_last_1h:{source.value}" for source in Source])
        keys.extend(
            [f"task_created_count_last_24h:{source.value}" for source in Source]
        )
        self.redis_client.delete(*keys)

        return None


class SessionStatsManager(RedisManager):
    """
    Each hashmap name stores keys where each key is a unix epoch minute.
    The key expires in now - bucket's time period seconds. The value
    is a counter. To get the sum, we just sum all the values. Any key
    older than 1 hr (in the 1 hr bucket) will expire.
    """

    # Must be ordered. Don't change this
    global_keys = [
        "session_enters_last_1h",
        "session_enters_last_24h",
        "session_fails_last_1h",
        "session_fails_last_24h",
        "session_completes_last_1h",
        "session_completes_last_24h",
        "sum_payouts_last_1h",
        "sum_payouts_last_24h",
        "sum_user_payouts_last_1h",
        "sum_user_payouts_last_24h",
        # "session_fail_loi_sum_last_1h",
        "session_fail_loi_sum_last_24h",
        # "session_complete_loi_sum_last_1h",
        "session_complete_loi_sum_last_24h",
    ]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.SUM_HASH_LUA = self.redis_client.register_script(SUM_HASH_LUA_SCRIPT)

    def session_on_finish(self, session: Session, user: User):
        if session.status == Status.COMPLETE:
            self.session_on_complete(session=session, user=user)
        else:
            self.session_on_fail(session=session, user=user)
        return None

    def session_on_fail(self, session: Session, user: User):
        r = self.redis_client

        now = int(time.time())
        assert session.status != Status.COMPLETE
        assert session.finished

        ts = int(session.finished.timestamp())
        bucket_ts = (ts // 60) * 60  # minute-aligned epoch
        key = str(bucket_ts)

        pipe = r.pipeline(transaction=True)

        for name_postfix, window in [
            ("last_1h", timedelta(hours=1).total_seconds()),
            ("last_24h", timedelta(hours=24).total_seconds()),
        ]:
            ttl = round(window - (now - bucket_ts))
            if ttl <= 0:
                continue

            name = "session_fails_" + name_postfix
            # Global tracker
            pipe.hincrby(name, key, 1)
            pipe.hexpire(name, ttl, key, nx=True)
            # BP-specific tracker
            pipe.hincrby(name + ":" + user.product_id, key, 1)
            pipe.hexpire(name + ":" + user.product_id, ttl, key, nx=True)

            # We're not returning this, but keep the sums, so we can
            # calculate the avg
            name = "session_fail_loi_sum_" + name_postfix
            value = round(session.elapsed.total_seconds())
            pipe.hincrby(name, key, value)
            pipe.hexpire(name, ttl, key, nx=True)
            pipe.hincrby(name + ":" + user.product_id, key, value)
            pipe.hexpire(name + ":" + user.product_id, ttl, key, nx=True)

        pipe.execute()

    def session_on_complete(self, session: Session, user: User):
        r = self.redis_client

        now = int(time.time())
        assert session.status == Status.COMPLETE
        assert session.finished
        assert session.payout is not None

        ts = int(session.finished.timestamp())
        bucket_ts = (ts // 60) * 60  # minute-aligned epoch
        key = str(bucket_ts)

        pipe = r.pipeline(transaction=True)

        for name_postfix, window in [
            ("last_1h", timedelta(hours=1).total_seconds()),
            ("last_24h", timedelta(hours=24).total_seconds()),
        ]:
            ttl = round(window - (now - bucket_ts))
            if ttl <= 0:
                continue

            name = "session_completes_" + name_postfix
            # Global tracker
            pipe.hincrby(name, key, 1)
            pipe.hexpire(name, ttl, key, nx=True)
            # BP-specific tracker
            pipe.hincrby(name + ":" + user.product_id, key, 1)
            pipe.hexpire(name + ":" + user.product_id, ttl, key, nx=True)

            name = "sum_payouts_" + name_postfix
            amount = round(session.payout * 100)
            pipe.hincrby(name, key, amount)
            pipe.hexpire(name, ttl, key, nx=True)
            pipe.hincrby(name + ":" + user.product_id, key, amount)
            pipe.hexpire(name + ":" + user.product_id, ttl, key, nx=True)

            if session.user_payout:
                name = "sum_user_payouts_" + name_postfix
                amount = round(session.user_payout * 100)
                pipe.hincrby(name, key, amount)
                pipe.hexpire(name, ttl, key, nx=True)
                pipe.hincrby(name + ":" + user.product_id, key, amount)
                pipe.hexpire(name + ":" + user.product_id, ttl, key, nx=True)

            # We're not returning this, but keep the sums, so we can calculate the avg
            name = "session_complete_loi_sum_" + name_postfix
            value = round(session.elapsed.total_seconds())
            pipe.hincrby(name, key, value)
            pipe.hexpire(name, ttl, key, nx=True)
            pipe.hincrby(name + ":" + user.product_id, key, value)
            pipe.hexpire(name + ":" + user.product_id, ttl, key, nx=True)

        pipe.execute()

    def session_on_enter(self, session: Session, user: User):
        r = self.redis_client

        now = int(time.time())
        assert session.status is None

        ts = int(session.started.timestamp())
        bucket_ts = (ts // 60) * 60  # minute-aligned epoch
        key = str(bucket_ts)

        pipe = r.pipeline(transaction=True)

        for name_postfix, window in [
            ("last_1h", timedelta(hours=1).total_seconds()),
            ("last_24h", timedelta(hours=24).total_seconds()),
        ]:
            ttl = round(window - (now - bucket_ts))
            if ttl <= 0:
                continue

            name = "session_enters_" + name_postfix
            # Global tracker
            pipe.hincrby(name, key, 1)
            pipe.hexpire(name, ttl, key, nx=True)
            # BP-specific tracker
            pipe.hincrby(name + ":" + user.product_id, key, 1)
            pipe.hexpire(name + ":" + user.product_id, ttl, key, nx=True)

        pipe.execute()

    def get_session_stats(self, product_id: UUIDStr):
        r = self.redis_client
        key_map = {k: k + ":" + product_id for k in self.global_keys}
        keys = list(key_map.values())

        pipe = r.pipeline(transaction=False)
        for k in keys:
            self.SUM_HASH_LUA(keys=[k], client=pipe)

        res = {k: v for k, v in zip(list(key_map.keys()), pipe.execute())}
        self.calculate_avg_stats(res)
        return res

    def get_global_session_stats(self):
        r = self.redis_client

        pipe = r.pipeline(transaction=False)
        for k in self.global_keys:
            self.SUM_HASH_LUA(keys=[k], client=pipe)

        res = {k: v for k, v in zip(self.global_keys, pipe.execute())}
        self.calculate_avg_stats(res)
        return res

    def calculate_avg_stats(self, res: Dict[str, Optional[float | int]]):
        res["session_avg_payout_last_24h"] = None
        res["session_avg_user_payout_last_24h"] = None
        res["session_complete_avg_loi_last_24h"] = None
        res["session_fail_avg_loi_last_24h"] = None
        if res["session_completes_last_24h"]:
            res["session_avg_payout_last_24h"] = math.ceil(
                res["sum_payouts_last_24h"] / res["session_completes_last_24h"]
            )
            res["session_avg_user_payout_last_24h"] = math.ceil(
                res["sum_user_payouts_last_24h"] / res["session_completes_last_24h"]
            )
            res["session_complete_avg_loi_last_24h"] = round(
                res["session_complete_loi_sum_last_24h"]
                / res["session_completes_last_24h"]
            )
        if res["session_fails_last_24h"]:
            res["session_fail_avg_loi_last_24h"] = round(
                res["session_fail_loi_sum_last_24h"] / res["session_fails_last_24h"]
            )
        res.pop("session_complete_loi_sum_last_24h")
        res.pop("session_fail_loi_sum_last_24h")
        return res

    def clear_global_session_stats(self):
        # For testing
        r = self.redis_client
        for k in self.global_keys:
            r.delete(k)


class StatsManager(UserStatsManager, SessionStatsManager, TaskStatsManager):

    def get_stats_message(self, product_id: UUIDStr) -> StatsMessage:
        res = self.get_session_stats(product_id=product_id)
        res.update(self.get_user_stats(product_id=product_id))
        ts = self.get_latest_task_stats()
        if ts is not None:
            res.update(ts.model_dump())
        return StatsMessage.model_validate({"data": res})

    def clear_stats(self):
        self.clear_task_stats()
        self.clear_global_user_stats()
        self.clear_global_session_stats()


class EventManager(StatsManager):
    CACHE_PREFIX = "EventManager"

    def __init__(self, *args, influx_client: InfluxDBClient = None, **kwargs):
        super().__init__(*args, **kwargs)
        self.influx_client = influx_client
        self.stats_worker_thread = None
        # Don't bother starting this thread if the influx_client is not set
        if self.influx_client is not None:
            self.stats_worker_thread = threading.Thread(
                target=self.stats_worker, daemon=True
            )
            self.stats_worker_thread.start()

    def get_channel_name(self, product_id: UUIDStr):
        return f"{self.cache_prefix}:event-channel:{product_id}"

    def get_replay_channel_name(self, product_id: UUIDStr):
        return f"{self.cache_prefix}:event-channel-replay:{product_id}"

    def get_last_stats_key(self, product_id: UUIDStr):
        return f"{self.cache_prefix}:last_stats:{product_id}"

    def get_active_subscribers(self) -> Set[UUIDStr]:
        res = self.redis_client.pubsub_channels(f"{self.cache_prefix}:event-channel:*")
        product_ids = {x.rsplit(":", 1)[-1] for x in res}
        return product_ids

    def stats_worker(self):
        while True:
            try:
                self.stats_worker_task()
            except Exception as e:
                logging.exception(e)
            finally:
                time.sleep(60)

    def stats_worker_task(self) -> None:
        """
        Only a single worker will be running. It'll be responsible
        for periodic publication of summary/stats messages.

        active_subscribers are product_ids that have a pubsub subscription
        """
        # Make sure only whoever grabs the lock first runs
        now = time.monotonic()
        lock_key = f"{self.cache_prefix}:event-channel-lock"
        res = self.redis_client.set(lock_key, 1, ex=120, nx=True)
        if not res:
            logging.debug("failed to acquire stats_worker_task lock")
            return None
        logging.info("Acquired stats_worker_task lock")

        for product_id in self.get_active_subscribers():
            if time.monotonic() - now > 120:
                logging.exception("stats_worker_task is taking too long")
                break
            channel = self.get_channel_name(product_id)
            msg = self.get_stats_message(product_id=product_id)
            self.redis_client.publish(channel, msg.model_dump_json())
            self.redis_client.set(
                self.get_last_stats_key(product_id),
                msg.model_dump_json(),
                ex=timedelta(hours=24),
            )
            if self.influx_client:
                _, numsub = self.redis_client.pubsub_numsub(channel)[0]
                point = self.make_influx_point(channel, numsub)
                self.influx_client.write_points([point])

        self.redis_client.delete(lock_key)

        return None

    def make_influx_point(self, channel: str, numsub: int):
        return {
            "measurement": "redis_pubsub_subscribers",
            "tags": {"hostname": socket.gethostname(), "channel": channel},
            "fields": {
                "subscribers": float(numsub),
            },
        }

    def publish_event(self, msg: EventMessage, product_id: UUIDStr):
        channel = self.get_channel_name(product_id)
        replay = self.get_replay_channel_name(product_id)
        print(f"publish: {self.get_channel_name(product_id)} {msg.kind=}")

        msg_json = msg.model_dump_json()
        pipe = self.redis_client.pipeline()
        pipe.publish(channel, msg_json)

        # replay buffer
        pipe.lpush(replay, msg_json)
        pipe.ltrim(replay, 0, 9)  # keep last 10
        pipe.expire(replay, 86400)  # 24h since last message

        # Last stats

        pipe.execute()

    def handle_task_enter(self, wall: Wall, session: Session, user: User):
        self.handle_user(user=user)

        msg = EventMessage(
            data=EventEnvelope(
                event_type=EventType.TASK_ENTER,
                timestamp=wall.started,
                product_id=user.product_id,
                product_user_id=user.product_user_id,
                payload=TaskEnterPayload(
                    source=wall.source,
                    survey_id=wall.req_survey_id,
                    country_iso=session.country_iso,
                ),
            )
        )
        self.publish_event(msg, product_id=user.product_id)
        return None

    def handle_task_finish(self, wall: Wall, session: Session, user: User):
        self.mark_user_active(user=user)

        msg = EventMessage(
            data=EventEnvelope(
                event_type=EventType.TASK_FINISH,
                timestamp=wall.finished,
                product_id=user.product_id,
                product_user_id=user.product_user_id,
                payload=TaskFinishPayload(
                    source=wall.source,
                    survey_id=wall.req_survey_id,
                    country_iso=session.country_iso,
                    duration_sec=wall.elapsed.total_seconds(),
                    status=wall.status,
                    status_code_1=wall.status_code_1,
                    status_code_2=wall.status_code_2,
                    cpi=round(wall.cpi * 100),
                ),
            )
        )
        self.publish_event(msg, product_id=user.product_id)

    def handle_session_enter(self, session: Session, user: User):
        self.handle_user(user=user)
        self.mark_user_inprogress(user=user)
        self.session_on_enter(session=session, user=user)

        msg = EventMessage(
            data=EventEnvelope(
                event_type=EventType.SESSION_ENTER,
                timestamp=session.started,
                product_id=user.product_id,
                product_user_id=user.product_user_id,
                payload=SessionEnterPayload(
                    country_iso=session.country_iso,
                ),
            )
        )
        self.publish_event(msg, product_id=user.product_id)

    def handle_session_finish(self, session: Session, user: User):
        self.mark_user_active(user=user)
        self.unmark_user_inprogress(user=user)
        self.session_on_finish(session=session, user=user)

        msg = EventMessage(
            data=EventEnvelope(
                event_type=EventType.SESSION_FINISH,
                timestamp=session.finished,
                product_id=user.product_id,
                product_user_id=user.product_user_id,
                payload=SessionFinishPayload(
                    country_iso=session.country_iso,
                    duration_sec=session.elapsed.total_seconds(),
                    status=session.status,
                    status_code_1=session.status_code_1,
                    status_code_2=session.status_code_2,
                    user_payout=(
                        round(session.user_payout * 100)
                        if session.user_payout
                        else None
                    ),
                ),
            )
        )
        self.publish_event(msg, product_id=user.product_id)


class EventSubscriber(RedisManager):
    """
    Initialize this class once per websocket connection. This subscribes that client
    to a redis PubSub and handles any filtering and parsing of the messages.
    """

    CACHE_PREFIX = "EventManager"

    def __init__(self, *args, product_id: UUIDStr, **kwargs):
        super().__init__(*args, **kwargs)
        self.product_id = product_id
        self.pubsub_client: Optional[Redis] = None
        self.pubsub: Optional[PubSub] = None
        self._subscribe()

    def _subscribe(self):
        if self.pubsub is not None:
            raise ValueError("Already subscribed")
        r = self.redis_config.create_redis_client()
        p = r.pubsub(ignore_subscribe_messages=True)
        p.subscribe(self.get_channel_name())
        self.pubsub_client = r
        self.pubsub = p
        return None

    def get_channel_name(self):
        return f"{self.cache_prefix}:event-channel:{self.product_id}"

    def get_replay_channel_name(self):
        return f"{self.cache_prefix}:event-channel-replay:{self.product_id}"

    def get_last_stats_key(self):
        return f"{self.cache_prefix}:last_stats:{self.product_id}"

    def get_last_stats_msg(self) -> Optional[StatsMessage]:
        raw = self.redis_client.get(self.get_last_stats_key())
        if raw is not None:
            return StatsMessage.model_validate_json(raw)
        return None

    def get_replay_messages(self) -> list[ServerToClientMessage]:
        key = self.get_replay_channel_name()
        raw = self.redis_client.lrange(key, 0, -1)
        # messages are newest -> oldest; reverse for playback
        raw.reverse()
        return [ServerToClientMessageAdapter.validate_json(x) for x in raw]

    def poll_message(self) -> Optional[ServerToClientMessage]:
        res = self.pubsub.get_message(ignore_subscribe_messages=True)
        if res is None:
            return None
        return ServerToClientMessageAdapter.validate_json(res["data"])

    def get_next_message(self) -> ServerToClientMessage:
        while True:
            res = self.poll_message()
            if res is None:
                time.sleep(0.1)
                continue
            return res

    def clear_replay_messages(self):
        # For testing
        self.redis_client.delete(self.get_replay_channel_name())
