import logging
from datetime import datetime, timedelta, timezone
from typing import Collection, List, Optional

from generalresearch.managers.base import PostgresManagerWithRedis
from generalresearch.models.thl.profiling.user_question_answer import (
    DUMMY_UQA,
    UserQuestionAnswer,
)
from generalresearch.models.thl.user import User

logger = logging.getLogger()


class UQAManager(PostgresManagerWithRedis):

    CACHE_PREFIX = "thl-grpc:uqa-cache-v2"

    def redis_key(self, user: User) -> str:
        return f"{self.CACHE_PREFIX}:{user.user_id}"

    def redis_lock_key(self, user: User) -> str:
        return self.redis_key(user) + ":lock"

    def update_cache(
        self,
        user: User,
        uqas: List[UserQuestionAnswer],
    ):
        """
        Adds new answers to the redis cache for this user. If the cache
        doesn't exist, does a db query to populate the cache.
        """
        REDIS = self.redis_client
        redis_key = self.redis_key(user)
        redis_lock = self.redis_lock_key(user)
        json_answers = [uqa.model_dump_json() for uqa in uqas]
        with REDIS.lock(redis_lock, timeout=2):
            # Append the new answers into the cache.
            res = REDIS.rpushx(redis_key, *json_answers)
            if res == 0:
                # If the cache doesn't exist, we need to make it from scratch
                uqas = self.get_from_db(user)
                all_json_answers = {uqa.model_dump_json() for uqa in uqas}
                # And then make sure thew new answers are in it (b/c we query from the RR)
                all_json_answers.update(set(json_answers))
                REDIS.rpush(redis_key, *all_json_answers)
                REDIS.expire(redis_key, 3600)
        self.clear_user_demographic_cache(user)
        return res

    def clear_cache(self, user: User) -> None:
        self.redis_client.delete(self.redis_key(user))

    def recreate_cache(self, user: User) -> Collection[UserQuestionAnswer]:
        REDIS = self.redis_client
        redis_key = self.redis_key(user)
        redis_lock = self.redis_lock_key(user)
        with REDIS.lock(redis_lock, timeout=2):
            # once we've acquired the lock, we need to check again if someone else just made it
            if REDIS.exists(redis_key):
                values = REDIS.lrange(redis_key, 0, -1)
                uqas = [UserQuestionAnswer.model_validate_json(x) for x in values]
            else:
                uqas = self.get_from_db(user)
                if not uqas:
                    # We can't set this to an empty list in redis (no
                    # difference between None and []).
                    #
                    #   I don't know what is the best thing to do here, so I'm
                    #   gonna push in a "dummy" UQA, just to indicate this has
                    #   been set and we shouldn't query the db anymore unless
                    #   something changes...
                    REDIS.rpush(redis_key, DUMMY_UQA.model_dump_json())
                else:
                    REDIS.rpush(redis_key, *[uqa.model_dump_json() for uqa in uqas])
                REDIS.expire(redis_key, 3600 * 3 * 24)

        uqas = self._dedupe_and_clean_uqas(uqas)
        self.clear_user_demographic_cache(user)
        return uqas

    def _dedupe_and_clean_uqas(
        self,
        uqas: List[UserQuestionAnswer],
    ) -> List[UserQuestionAnswer]:
        # Remove anything older than 30 days
        uqas = [uqa for uqa in uqas if not uqa.is_stale()]

        # Dedupe, latest answer per question
        new_uqas = set()
        seen_question_ids = set()
        uqas = sorted(uqas, key=lambda x: x.timestamp, reverse=True)
        for uqa in uqas:
            if uqa.question_id not in seen_question_ids:
                seen_question_ids.add(uqa.question_id)
                new_uqas.add(uqa)

        return sorted(new_uqas, key=lambda x: x.timestamp, reverse=True)

    def get(self, user: User) -> List[UserQuestionAnswer]:
        uqas = self.get_from_cache(user=user)

        if uqas is None:
            uqas = self.recreate_cache(user)
        return self._dedupe_and_clean_uqas(uqas)

    def get_from_cache(self, user: User) -> Optional[List[UserQuestionAnswer]]:
        redis_key = self.redis_key(user)

        # Do the exists check and the list retrieval in a single transaction
        with self.redis_client.pipeline() as pipe:
            exists = pipe.exists(redis_key)
            if exists:
                pipe.lrange(redis_key, 0, -1)
            result = pipe.execute()
        exists = result[0]
        values = result[1]
        if not exists:
            logger.info(f"{redis_key} doesn't exist")
            return None
        uqas = [UserQuestionAnswer.model_validate_json(x) for x in values]
        logger.info(f"{redis_key} exists")
        return uqas

    def get_from_db(self, user: User) -> List[UserQuestionAnswer]:
        logger.info(f"get_uqa_from_db: {user.user_id}")
        # Only store the latest row per question_id. We don't need it multiple times.
        since = datetime.now(tz=timezone.utc) - timedelta(days=30)

        # We CAN use the RR, b/c either
        #   1) the cache expired and the user hasn't sent an answer recently
        #   or 2) The user just sent an answer, so we'll make sure it gets put into the results
        #       after this query runs.
        query = """
        WITH ranked AS (
            SELECT
                uqa.*,
                ROW_NUMBER() OVER (
                    PARTITION BY question_id
                    ORDER BY created DESC
                ) AS rn
            FROM marketplace_userquestionanswer uqa
            WHERE uqa.user_id = %(user_id)s
              AND uqa.created > %(since)s
        )
        SELECT
            r.question_id::uuid,
            r.created::timestamptz AS timestamp,
            r.calc_answer::jsonb AS calc_answers,
            r.answer::jsonb,
            r.user_id,
            mq.property_code,
            mq.country_iso,
            mq.language_iso
        FROM ranked r
        JOIN marketplace_question mq
          ON r.question_id = mq.id
        WHERE rn = 1
        ORDER BY r.created;
        """
        res = self.pg_config.execute_sql_query(
            query=query,
            params={"user_id": user.user_id, "since": since},
        )
        uqas = [UserQuestionAnswer.model_validate(x) for x in res]
        return uqas

    def clear_user_demographic_cache(
        self,
        user: User,
    ) -> None:
        # this will get regenerated by thl-grpc when an offerwall call is made
        redis_key = f"thl-grpc:user-demographics:{user.user_id}"
        self.redis_client.delete(redis_key)

        return None

    def create(
        self,
        user: User,
        uqas: List[UserQuestionAnswer],
        session_id: Optional[str] = None,
    ):
        for uqa in uqas:
            if uqa.user_id is None:
                uqa.user_id = user.user_id
            else:
                assert uqa.user_id == user.user_id
        self.create_in_db(uqas=uqas, session_id=session_id)
        self.update_cache(user=user, uqas=uqas)
        return None

    def create_in_db(
        self, uqas: List[UserQuestionAnswer], session_id: Optional[str] = None
    ):
        values = [uqa.model_dump_mysql(session_id=session_id) for uqa in uqas]
        query = """
        INSERT INTO marketplace_userquestionanswer
        (created, session_id, answer, question_id, user_id, calc_answer)
        VALUES (
            %(created)s, %(session_id)s, %(answer)s,
            %(question_id)s, %(user_id)s, %(calc_answer)s
        );
        """

        with self.pg_config.make_connection() as conn:
            with conn.cursor() as c:
                c.executemany(query=query, params_seq=values)
            conn.commit()
        return None
