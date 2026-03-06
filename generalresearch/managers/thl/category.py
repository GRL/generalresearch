from typing import Collection, Dict

from generalresearch.managers.base import Permission, PostgresManager
from generalresearch.models.custom_types import UUIDStr
from generalresearch.models.thl.category import Category
from generalresearch.pg_helper import PostgresConfig


class CategoryManager(PostgresManager):
    categories = dict()
    category_label_map = dict()

    def __init__(
        self,
        pg_config: PostgresConfig,
        permissions: Collection[Permission] = None,
    ):
        super().__init__(pg_config=pg_config, permissions=permissions)
        self.categories: Dict[UUIDStr, Category] = dict()
        self.category_label_map: Dict[str, Category] = dict()
        self.populate_caches()

    def populate_caches(self):
        query = """
        SELECT
            c.id, c.uuid, c.adwords_vertical_id, c.label, c.path, c.parent_id,
            p.uuid AS parent_uuid
        FROM marketplace_category AS c
        LEFT JOIN marketplace_category AS p
            ON p.id = c.parent_id;"""
        res = self.pg_config.execute_sql_query(query)
        self.categories = {d["uuid"]: Category.model_validate(d) for d in res}
        self.category_label_map = {c.label: c for c in self.categories.values()}

    def get_by_label(self, label: str) -> Category:
        return self.category_label_map[label]

    def get_top_level(self, category: Category) -> Category:
        return self.category_label_map[category.root_label]

    def get_category_root(self, category: Category) -> Category:
        # These are the categories we'd display. Almost all are just the top-level
        #   of all paths, but we have a couple we pull out separately
        # Alcoholic Beverages, Tobacco Use, Mature Content, Social Research, Demographic, Politics
        custom_root = {
            "4fd8381d5a1c4409ab007ca254ced084",
            "90f92a5d192848ad9a230587c219b82c",
            "21536f160f784189be6194ca894f3a65",
            "7aa8bf4e71a84dc3b2035f93f9f9c77e",
            "c82cf98c578a43218334544ab376b00e",
            "87b6d819f3ca4815bf1f135b1e829cc6",
        }
        if category.uuid in custom_root:
            return category
        else:
            return self.get_top_level(category)
