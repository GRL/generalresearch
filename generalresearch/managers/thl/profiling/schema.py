from threading import RLock
from typing import List
from uuid import UUID

from cachetools import TTLCache, cached

from generalresearch.managers.base import PostgresManager
from generalresearch.models.thl.profiling.upk_property import (
    UpkProperty,
)


class UpkSchemaManager(PostgresManager):

    @cached(cache=TTLCache(maxsize=1, ttl=18 * 60), lock=RLock())
    def get_props_info(self) -> List[UpkProperty]:
        query = """
        SELECT
            p.id AS property_id,
            p.label AS property_label,
            p.cardinality,
            p.prop_type,
            pc.country_iso,
            pc.gold_standard,
            allowed_items.allowed_items,
            cats.categories
        FROM marketplace_property p
        JOIN marketplace_propertycountry pc
            ON p.id = pc.property_id
        -- allowed_items: all items for this property + country
        LEFT JOIN LATERAL (
            SELECT jsonb_agg(
                       jsonb_build_object(
                           'id', mi.id,
                           'label', mi.label,
                           'description', mi.description
                       ) ORDER BY mi.label
                   ) AS allowed_items
            FROM marketplace_propertyitemrange pir
            JOIN marketplace_item mi
                ON pir.item_id = mi.id
            WHERE pir.property_id = p.id
              AND pir.country_iso = pc.country_iso
        ) allowed_items ON TRUE
        
        -- categories: all categories for this property
        LEFT JOIN LATERAL (
            SELECT
                jsonb_agg(
                    jsonb_build_object(
                        'uuid', cat.uuid,
                        'label', cat.label,
                        'path', cat.path,
                        'adwords_vertical_id', cat.adwords_vertical_id
                    )
                ) AS categories
                FROM marketplace_propertycategoryassociation pcat
                JOIN marketplace_category cat ON pcat.category_id = cat.id
                WHERE pcat.property_id = p.id
        ) AS cats ON TRUE;
        """
        res = self.pg_config.execute_sql_query(query)
        for x in res:
            for c in x["categories"]:
                c["uuid"] = UUID(c["uuid"]).hex
            if x["allowed_items"]:
                for c in x["allowed_items"]:
                    c["id"] = UUID(c["id"]).hex
        return [UpkProperty.model_validate(x) for x in res]

    def get_props_info_for_country(self, country_iso: str) -> List[UpkProperty]:
        assert country_iso.lower() == country_iso
        res = self.get_props_info()
        res = [x for x in res if x.country_iso == country_iso].copy()
        return res
