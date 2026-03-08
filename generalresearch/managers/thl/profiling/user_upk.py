import json
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any, Collection, Dict, List, Optional, Set, Tuple, Union
from uuid import UUID

from psycopg import Cursor
from pydantic import PositiveInt

from generalresearch.managers.base import (
    Permission,
    PostgresManagerWithRedis,
)
from generalresearch.managers.thl.profiling.schema import UpkSchemaManager
from generalresearch.models.thl.profiling.upk_property import (
    Cardinality,
    PropertyType,
    UpkProperty,
)
from generalresearch.models.thl.profiling.upk_question_answer import (
    UpkQuestionAnswer,
)
from generalresearch.pg_helper import PostgresConfig
from generalresearch.redis_helper import RedisConfig


class UserUpkManager(PostgresManagerWithRedis):
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
        self.upk_schema_manager = UpkSchemaManager(pg_config=pg_config)

    def clear_upk_cache(self, user_id: int) -> None:
        self.redis_client.delete(f"thl-grpc:user-upk:{user_id}")
        return None

    def get_user_upk(self, user_id: int) -> List[UpkQuestionAnswer]:
        res = self.redis_client.get(f"thl-grpc:user-upk:{user_id}")
        if res:
            return [UpkQuestionAnswer.model_validate(x) for x in json.loads(res)]
        res = self.get_user_upk_mysql(user_id)
        value = json.dumps([x.model_dump(mode="json") for x in res])
        self.redis_client.set(f"thl-grpc:user-upk:{user_id}", value, ex=60 * 60 * 24)
        return res

    def get_user_upk_mysql(self, user_id: int) -> List[UpkQuestionAnswer]:
        since = datetime.now(tz=timezone.utc) - timedelta(days=89)

        query = """
        SELECT 
           x.property_id,
           mp.label AS property_label,
           mi.id    AS item_id,
           mi.label AS item_label,
           mp.prop_type,
           mp.cardinality,
           x.created,
           x.value_num,
           x.value_text,
           x.country_iso
        FROM (
            SELECT
                property_id,
                value::uuid AS item_id,
                NULL::numeric AS value_num,
                NULL::text AS value_text,
                created,
                country_iso
            FROM marketplace_userprofileknowledgeitem AS upki
            WHERE user_id = %(user_id)s AND created > %(since)s
            
            UNION ALL
            
            SELECT
                property_id,
                NULL::uuid AS item_id,
                value::numeric AS value_num,
                NULL::text AS value_text,
                created,
                country_iso
            FROM marketplace_userprofileknowledgenumerical
            WHERE user_id = %(user_id)s AND created > %(since)s
            
            UNION ALL
            
            SELECT
                property_id,
                NULL::uuid AS item_id,
                NULL::numeric AS value_num,
                value::text AS value_text,
                created,
                country_iso
            FROM marketplace_userprofileknowledgetext
            WHERE user_id = %(user_id)s AND created > %(since)s
        ) x
        JOIN marketplace_property mp
            ON x.property_id = mp.id
        LEFT JOIN marketplace_item mi
            ON x.item_id = mi.id::uuid;
        """
        params = {"user_id": user_id, "since": since}
        res = self.pg_config.execute_sql_query(query, params=params)
        for x in res:
            x["user_id"] = user_id
            x["property_id"] = UUID(x["property_id"]).hex
            if x["item_id"]:
                x["item_id"] = UUID(x["item_id"]).hex
        return [UpkQuestionAnswer.model_validate(x) for x in res]

    def get_user_upk_simple(
        self, user_id: PositiveInt, country_iso: str = "us"
    ) -> Dict[str, Union[Set[str], str, float]]:

        res = self.get_user_upk(user_id=user_id)
        res = [x for x in res if x.country_iso == country_iso]
        d: Dict[str, Union[Set[str], str, float]] = defaultdict(set)
        for x in res:
            if x.cardinality == Cardinality.ZERO_OR_ONE:
                d[x.property_label] = x.value
            else:
                d[x.property_label].add(x.value)

        return dict(d)

    def get_age_gender(
        self, user_id: PositiveInt, country_iso: str = "us"
    ) -> Tuple[Optional[int], Optional[str]]:

        # Returns an integer year for age, and {'male', 'female', 'other_gender'}
        d = self.get_user_upk_simple(user_id, country_iso)
        age = d.get("age_in_years")
        if age is not None:
            age = int(age)

        gender = d.get("gender")
        return age, gender

    def get_upk_schema(self, country_iso: str) -> List[UpkProperty]:
        return self.upk_schema_manager.get_props_info_for_country(
            country_iso=country_iso
        )

    def populate_user_upk_from_dict(
        self, upk_ans_dict: List[Dict[str, Any]]
    ) -> List[UpkQuestionAnswer]:

        country_isos = {x["country_iso"] for x in upk_ans_dict}
        assert len(country_isos) == 1
        country_iso = list(country_isos)[0]
        for x in upk_ans_dict:
            x["pred"] = x["pred"].replace("gr:", "")
            x["obj"] = x["obj"].replace("gr:", "")
        prop_labels = {x["pred"] for x in upk_ans_dict}

        props = self.get_upk_schema(country_iso=country_iso)
        props = [
            x
            for x in props
            if x.property_label in prop_labels or x.property_id in prop_labels
        ]
        label_to_prop = {x.property_label: x for x in props}
        id_to_prop = {x.property_id: x for x in props}

        for x in upk_ans_dict:
            prop = label_to_prop.get(x["pred"]) or id_to_prop[x["pred"]]
            x["property_id"] = prop.property_id
            x["property_label"] = prop.property_label
            x["prop_type"] = prop.prop_type
            x["cardinality"] = prop.cardinality
            x["created"] = x["timestamp"]
            if prop.prop_type == PropertyType.UPK_ITEM:
                if x["obj"] in prop.allowed_items_by_id:
                    x["item_label"] = prop.allowed_items_by_id[x["obj"]].label
                    x["item_id"] = x["obj"]
                else:
                    x["item_label"] = x["obj"]
                    x["item_id"] = prop.allowed_items_by_label[x["obj"]].id
            elif prop.prop_type == PropertyType.UPK_TEXT:
                x["value_text"] = x["obj"]
            elif prop.prop_type == PropertyType.UPK_NUMERICAL:
                x["value_num"] = x["obj"]

        upk_ans = [UpkQuestionAnswer.model_validate(x) for x in upk_ans_dict]
        return upk_ans

    def upsert_user_profile_knowledge(self, c: Cursor, row: UpkQuestionAnswer):
        prop_type_table = {
            PropertyType.UPK_ITEM: "marketplace_userprofileknowledgeitem",
            PropertyType.UPK_NUMERICAL: "marketplace_userprofileknowledgenumerical",
            PropertyType.UPK_TEXT: "marketplace_userprofileknowledgetext",
        }
        prop_type_value = {
            PropertyType.UPK_ITEM: "item_id",
            PropertyType.UPK_NUMERICAL: "value_num",
            PropertyType.UPK_TEXT: "value_text",
        }
        table = prop_type_table[row.prop_type]
        value = prop_type_value[row.prop_type]
        args = row.model_dump_mysql()

        c.execute(
            f"""
        SELECT id FROM {table}
        WHERE user_id = %(user_id)s AND property_id = %(property_id)s AND country_iso = %(country_iso)s
        LIMIT 1""",
            args,
        )
        existing = c.fetchone()

        if existing:
            c.execute(
                f"""
                UPDATE {table}
                SET value = %({value})s,
                    created = %(created)s,
                    question_id = %(question_id)s,
                    session_id = %(session_id)s
                WHERE user_id = %(user_id)s AND 
                    property_id = %(property_id)s AND 
                    country_iso = %(country_iso)s
            """,
                args,
            )
        else:
            c.execute(
                f"""
                INSERT INTO {table}
                (property_id, value, created, country_iso, question_id, user_id, session_id)
                VALUES (%(property_id)s, %({value})s, %(created)s, %(country_iso)s,
                %(question_id)s, %(user_id)s, %(session_id)s)
            """,
                args,
            )

    def upsert_user_profile_knowledge_multi_item(
        self, c: Cursor, row: UpkQuestionAnswer
    ):
        args = row.model_dump_mysql()

        c.execute(
            """
        SELECT id FROM marketplace_userprofileknowledgeitem
        WHERE user_id = %(user_id)s AND
            property_id = %(property_id)s AND
            country_iso = %(country_iso)s AND
            value = %(item_id)s
        LIMIT 1""",
            args,
        )
        existing = c.fetchone()

        if existing:
            c.execute(
                """
                UPDATE marketplace_userprofileknowledgeitem
                SET created = %(created)s,
                    question_id = %(question_id)s,
                    session_id = %(session_id)s
                WHERE user_id = %(user_id)s AND 
                    property_id = %(property_id)s AND 
                    country_iso = %(country_iso)s AND
                    value = %(item_id)s
            """,
                args,
            )
        else:
            c.execute(
                """
                INSERT INTO marketplace_userprofileknowledgeitem
                (property_id, value, created, country_iso, question_id, user_id, session_id)
                VALUES (%(property_id)s, %(item_id)s, %(created)s, %(country_iso)s,
                %(question_id)s, %(user_id)s, %(session_id)s)
            """,
                args,
            )

    def delete_user_profile_knowledge_multi_item(
        self, c: Cursor, row: UpkQuestionAnswer
    ) -> None:
        args = row.model_dump_mysql()
        c.execute(
            """
            DELETE FROM marketplace_userprofileknowledgeitem
            WHERE user_id = %(user_id)s AND 
                property_id = %(property_id)s AND 
                country_iso = %(country_iso)s AND
                value = %(item_id)s
            """,
            args,
        )

        return None

    def set_user_upk(self, upk_ans: List[UpkQuestionAnswer]):
        user_id = {x.user_id for x in upk_ans}
        assert len(user_id) == 1, "only run for 1 user at a time"
        user_id = list(user_id)[0]

        curr_upk = self.get_user_upk(user_id=user_id)
        curr_upk_simple = self.get_user_upk_simple(user_id=user_id)

        new_upk_simple = defaultdict(set)
        delete_items = set()
        upk_multi = list()
        delete_upk_multi = list()
        for x in upk_ans:
            # For zero or more (multiple values) We want all values to equal these.
            #   Might involve deleting values if they exist and are not in upk_ans
            if (
                x.cardinality == Cardinality.ZERO_OR_MORE
                and x.prop_type != PropertyType.UPK_ITEM
            ):
                raise ValueError("unsupported")
            if (
                x.cardinality == Cardinality.ZERO_OR_MORE
                and x.prop_type == PropertyType.UPK_ITEM
            ):
                new_upk_simple[x.property_label].add(x.item_label)
                upk_multi.append(x)
        for k, v in new_upk_simple.items():
            prop_delete_labels = curr_upk_simple.get(k, set()) - v
            for x in prop_delete_labels:
                delete_items.add((k, x))
        if delete_items:
            for x in curr_upk:
                if (x.property_label, x.item_label) in delete_items:
                    delete_upk_multi.append(x)

        with self.pg_config.make_connection() as conn:
            with conn.cursor() as c:
                for x in upk_multi:
                    self.upsert_user_profile_knowledge_multi_item(c, row=x)
                for x in delete_upk_multi:
                    self.delete_user_profile_knowledge_multi_item(c, row=x)
                for x in upk_ans:
                    if x.cardinality == Cardinality.ZERO_OR_ONE:
                        # If the cardinality is 0 or 1, we're inserting or updating the answer
                        self.upsert_user_profile_knowledge(c, x)
            conn.commit()
        self.clear_upk_cache(user_id=user_id)
