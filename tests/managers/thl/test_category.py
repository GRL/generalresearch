import pytest

from generalresearch.models.thl.category import Category


class TestCategory:

    @pytest.fixture
    def beauty_fitness(self, thl_web_rw):

        return Category(
            uuid="12c1e96be82c4642a07a12a90ce6f59e",
            adwords_vertical_id="44",
            label="Beauty & Fitness",
            path="/Beauty & Fitness",
        )

    @pytest.fixture
    def hair_care(self, beauty_fitness):

        return Category(
            uuid="dd76c4b565d34f198dad3687326503d6",
            adwords_vertical_id="146",
            label="Hair Care",
            path="/Beauty & Fitness/Hair Care",
        )

    @pytest.fixture
    def hair_loss(self, hair_care):

        return Category(
            uuid="aacff523c8e246888215611ec3b823c0",
            adwords_vertical_id="235",
            label="Hair Loss",
            path="/Beauty & Fitness/Hair Care/Hair Loss",
        )

    @pytest.fixture
    def category_data(
        self, category_manager, thl_web_rw, beauty_fitness, hair_care, hair_loss
    ):
        cats = [beauty_fitness, hair_care, hair_loss]
        data = [x.model_dump(mode="json") for x in cats]
        # We need the parent pk's to set the parent_id. So insert all without a parent,
        #   then pull back all pks and map to the parents as parsed by the parent_path
        query = """
        INSERT INTO marketplace_category
            (uuid, adwords_vertical_id, label, path)
        VALUES
            (%(uuid)s, %(adwords_vertical_id)s, %(label)s, %(path)s)
        ON CONFLICT (uuid) DO NOTHING;
        """
        with thl_web_rw.make_connection() as conn:
            with conn.cursor() as c:
                c.executemany(query=query, params_seq=data)
            conn.commit()

        res = thl_web_rw.execute_sql_query("SELECT id, path FROM marketplace_category")
        path_id = {x["path"]: x["id"] for x in res}
        data = [
            {"id": path_id[c.path], "parent_id": path_id[c.parent_path]}
            for c in cats
            if c.parent_path
        ]
        query = """
        UPDATE marketplace_category
        SET parent_id = %(parent_id)s
        WHERE id = %(id)s;
        """
        with thl_web_rw.make_connection() as conn:
            with conn.cursor() as c:
                c.executemany(query=query, params_seq=data)
            conn.commit()

        category_manager.populate_caches()

    def test(
        self,
        category_data,
        category_manager,
        beauty_fitness,
        hair_care,
        hair_loss,
    ):
        # category_manager on init caches the category info. This rarely/never changes so this is fine,
        #   but now that tests get run on a new db each time, the category_manager is inited before
        #   the fixtures run. so category_manager's cache needs to be rerun

        # path='/Beauty & Fitness/Hair Care/Hair Loss'
        c: Category = category_manager.get_by_label("Hair Loss")
        # Beauty & Fitness
        assert beauty_fitness.uuid == category_manager.get_top_level(c).uuid

        c: Category = category_manager.categories[beauty_fitness.uuid]
        # The root is itself
        assert c == category_manager.get_category_root(c)

        # The root is Beauty & Fitness
        c: Category = category_manager.get_by_label("Hair Loss")
        assert beauty_fitness.uuid == category_manager.get_category_root(c).uuid
