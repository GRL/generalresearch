from uuid import uuid4

import pytest

from generalresearch.models import Source
from generalresearch.models.thl.product import (
    Product,
    SourceConfig,
    UserCreateConfig,
    SourcesConfig,
    UserHealthConfig,
    ProfilingConfig,
    SupplyPolicy,
    SupplyConfig,
)
from test_utils.models.conftest import product_factory


class TestProductManagerGetMethods:
    def test_get_by_uuid(self, product_manager):
        product: Product = product_manager.create_dummy(
            product_id=uuid4().hex,
            team_id=uuid4().hex,
            name=f"Test Product ID #{uuid4().hex[:6]}",
        )

        instance = product_manager.get_by_uuid(product_uuid=product.id)
        assert isinstance(instance, Product)
        # self.assertEqual(instance.model_dump(mode="json"), product.model_dump(mode="json"))
        assert instance.id == product.id

        with pytest.raises(AssertionError) as cm:
            product_manager.get_by_uuid(product_uuid="abc123")
        assert "invalid uuid" in str(cm.value)

        with pytest.raises(AssertionError) as cm:
            product_manager.get_by_uuid(product_uuid=uuid4().hex)
        assert "product not found" in str(cm.value)

    def test_get_by_uuids(self, product_manager):
        cnt = 5

        product_uuids = [uuid4().hex for idx in range(cnt)]
        for product_id in product_uuids:
            product_manager.create_dummy(
                product_id=product_id,
                team_id=uuid4().hex,
                name=f"Test Product ID #{uuid4().hex[:6]}",
            )

        res = product_manager.get_by_uuids(product_uuids=product_uuids)
        assert isinstance(res, list)
        assert cnt == len(res)
        for instance in res:
            assert isinstance(instance, Product)

        with pytest.raises(AssertionError) as cm:
            product_manager.get_by_uuids(product_uuids=product_uuids + [uuid4().hex])
        assert "incomplete product response" in str(cm.value)

        with pytest.raises(AssertionError) as cm:
            product_manager.get_by_uuids(product_uuids=product_uuids + ["abc123"])
        assert "invalid uuid" in str(cm.value)

    def test_get_by_uuid_if_exists(self, product_manager):
        product: Product = product_manager.create_dummy(
            product_id=uuid4().hex,
            team_id=uuid4().hex,
            name=f"Test Product ID #{uuid4().hex[:6]}",
        )
        instance = product_manager.get_by_uuid_if_exists(product_uuid=product.id)
        assert isinstance(instance, Product)

        instance = product_manager.get_by_uuid_if_exists(product_uuid="abc123")
        assert instance == None

    def test_get_by_uuids_if_exists(self, product_manager):
        product_uuids = [uuid4().hex for _ in range(2)]
        for product_id in product_uuids:
            product_manager.create_dummy(
                product_id=product_id,
                team_id=uuid4().hex,
                name=f"Test Product ID #{uuid4().hex[:6]}",
            )

        res = product_manager.get_by_uuids_if_exists(product_uuids=product_uuids)
        assert isinstance(res, list)
        assert 2 == len(res)
        for instance in res:
            assert isinstance(instance, Product)

        res = product_manager.get_by_uuids_if_exists(
            product_uuids=product_uuids + [uuid4().hex]
        )
        assert isinstance(res, list)
        assert 2 == len(res)
        for instance in res:
            assert isinstance(instance, Product)

        # # This will raise an error b/c abc123 isn't a uuid.
        # res = product_manager.get_by_uuids_if_exists(
        #     product_uuids=product_uuids + ["abc123"]
        # )
        # assert isinstance(res, list)
        # assert 2 == len(res)
        # for instance in res:
        #     assert isinstance(instance, Product)

    def test_get_by_business_ids(self, product_manager):
        business_ids = [uuid4().hex for i in range(5)]

        product_manager.fetch_uuids(business_uuids=business_ids)

        for business_id in business_ids:
            product_manager.create(
                product_id=uuid4().hex,
                team_id=None,
                business_id=business_id,
                redirect_url="https://www.example.com",
                name=f"Test Product ID #{uuid4().hex[:6]}",
                user_create_config=None,
            )


class TestProductManagerCreation:

    def test_base(self, product_manager):
        instance = product_manager.create_dummy(
            product_id=uuid4().hex,
            team_id=uuid4().hex,
            name=f"New Test Product {uuid4().hex[:6]}",
        )

        assert isinstance(instance, Product)


class TestProductManagerCreate:

    def test_create_simple(self, product_manager):
        # Always required: product_id, team_id, name, redirect_url
        # Required internally - if not passed use default: harmonizer_domain,
        #   commission_pct, sources
        product_id = uuid4().hex
        team_id = uuid4().hex
        business_id = uuid4().hex

        product_manager.create(
            product_id=product_id,
            team_id=team_id,
            business_id=business_id,
            name=f"Test Product ID #{uuid4().hex[:6]}",
            redirect_url="https://www.example.com",
        )

        instance = product_manager.get_by_uuid(product_uuid=product_id)

        assert team_id == instance.team_id
        assert business_id == instance.business_id


class TestProductManager:
    sources = [
        SourceConfig.model_validate(x)
        for x in [
            {"name": "d", "active": True},
            {
                "name": "f",
                "active": False,
                "banned_countries": ["ps", "in", "in"],
            },
            {
                "name": "s",
                "active": True,
                "supplier_id": "3488",
                "allow_pii_only_buyers": True,
                "allow_unhashed_buyers": True,
            },
            {"name": "e", "active": True, "withhold_profiling": True},
        ]
    ]

    def test_get_by_uuid1(self, product_manager, team, product, product_factory):
        p1 = product_factory(team=team)
        instance = product_manager.get_by_uuid(product_uuid=p1.uuid)
        assert instance.id == p1.id

        # No Team and no user_create_config
        assert instance.team_id == team.uuid

        # user_create_config can't be None, so ensure the default was set.
        assert isinstance(instance.user_create_config, UserCreateConfig)
        assert 0 == instance.user_create_config.min_hourly_create_limit
        assert instance.user_create_config.max_hourly_create_limit is None

    def test_get_by_uuid2(self, product_manager, product_factory):
        p2 = product_factory()
        instance = product_manager.get_by_uuid(p2.id)
        assert instance.id, p2.id

        # Team and default user_create_config
        assert instance.team_id is not None
        assert instance.team_id == p2.team_id

        assert 0 == instance.user_create_config.min_hourly_create_limit
        assert instance.user_create_config.max_hourly_create_limit is None

    def test_get_by_uuid3(self, product_manager, product_factory):
        p3 = product_factory()
        instance = product_manager.get_by_uuid(p3.id)
        assert instance.id == p3.id

        # Team and default user_create_config
        assert instance.team_id is not None
        assert instance.team_id == p3.team_id

        assert (
            p3.user_create_config.min_hourly_create_limit
            == instance.user_create_config.min_hourly_create_limit
        )
        assert instance.user_create_config.max_hourly_create_limit is None
        assert not instance.user_wallet_config.enabled

    def test_sources(self, product_manager):
        user_defined = [SourceConfig(name=Source.DYNATA, active=False)]
        sources_config = SourcesConfig(user_defined=user_defined)
        p = product_manager.create_dummy(sources_config=sources_config)

        p2 = product_manager.get_by_uuid(p.id)

        assert p == p2
        assert p2.sources_config.user_defined == user_defined

        # Assert d is off and everything else is on
        dynata = p2.sources_dict[Source.DYNATA]
        assert not dynata.active
        assert all(x.active is True for x in p2.sources if x.name != Source.DYNATA)

    def test_global_sources(self, product_manager):
        sources_config = SupplyConfig(
            policies=[
                SupplyPolicy(
                    name=Source.DYNATA,
                    active=True,
                    address=["https://www.example.com"],
                    distribute_harmonizer_active=True,
                )
            ]
        )
        p1 = product_manager.create_dummy(sources_config=sources_config)
        p2 = product_manager.get_by_uuid(p1.id)
        assert p1 == p2

        p1.sources_config.policies.append(
            SupplyPolicy(
                name=Source.CINT,
                active=True,
                address=["https://www.example.com"],
                distribute_harmonizer_active=True,
            )
        )
        product_manager.update(p1)
        p2 = product_manager.get_by_uuid(p1.id)
        assert p1 == p2

    def test_user_health_config(self, product_manager):
        p = product_manager.create_dummy(
            user_health_config=UserHealthConfig(banned_countries=["ng", "in"])
        )

        p2 = product_manager.get_by_uuid(p.id)

        assert p == p2
        assert p2.user_health_config.banned_countries == ["in", "ng"]
        assert p2.user_health_config.allow_ban_iphist

    def test_profiling_config(self, product_manager):
        p = product_manager.create_dummy(
            profiling_config=ProfilingConfig(max_questions=1)
        )
        p2 = product_manager.get_by_uuid(p.id)

        assert p == p2
        assert p2.profiling_config.max_questions == 1

    # def test_user_create_config(self):
    #     # -- Product 1 ---
    #     instance1 = PM.get_by_uuid(self.product_id1)
    #     self.assertEqual(instance1.user_create_config.min_hourly_create_limit, 0)
    #
    #     self.assertEqual(instance1.user_create_config.max_hourly_create_limit, None)
    #     self.assertEqual(60, instance1.user_create_config.clip_hourly_create_limit(60))
    #
    #     # -- Product 2 ---
    #     instance2 = PM.get_by_uuid(self.product2.id)
    #     self.assertEqual(instance2.user_create_config.min_hourly_create_limit, 200)
    #     self.assertEqual(instance2.user_create_config.max_hourly_create_limit, None)
    #
    #     self.assertEqual(200, instance2.user_create_config.clip_hourly_create_limit(60))
    #     self.assertEqual(300, instance2.user_create_config.clip_hourly_create_limit(300))
    #
    # def test_create_and_cache(self):
    #     PM.uuid_cache.clear()
    #
    #     # Hit it once, should hit mysql
    #     with self.assertLogs(level='INFO') as cm:
    #         p = PM.get_by_uuid(product_id3)
    #     self.assertEqual(cm.output, [f"INFO:root:Product.get_by_uuid:{product_id3}"])
    #     self.assertIsNotNone(p)
    #     self.assertEqual(p.id, product_id3)
    #
    #     # Hit it again, should be pulled from cachetools cache
    #     with self.assertLogs(level='INFO') as cm:
    #         logger.info("nothing")
    #         p = PM.get_by_uuid(product_id3)
    #     self.assertEqual(cm.output, [f"INFO:root:nothing"])
    #     self.assertEqual(len(cm.output), 1)
    #     self.assertIsNotNone(p)
    #     self.assertEqual(p.id, product_id3)


class TestProductManagerUpdate:

    def test_update(self, product_manager):
        p = product_manager.create_dummy()
        p.name = "new name"
        p.enabled = False
        p.user_create_config = UserCreateConfig(min_hourly_create_limit=200)
        p.sources_config = SourcesConfig(
            user_defined=[SourceConfig(name=Source.DYNATA, active=False)]
        )
        product_manager.update(new_product=p)
        # We cleared the cache in the update
        # PM.id_cache.clear()
        p2 = product_manager.get_by_uuid(p.id)

        assert p2.name == "new name"
        assert not p2.enabled
        assert p2.user_create_config.min_hourly_create_limit == 200
        assert not p2.sources_dict[Source.DYNATA].active


class TestProductManagerCacheClear:

    def test_cache_clear(self, product_manager):
        p = product_manager.create_dummy()
        product_manager.get_by_uuid(product_uuid=p.id)
        product_manager.get_by_uuid(product_uuid=p.id)
        product_manager.pg_config.execute_write(
            query="""
        UPDATE userprofile_brokerageproduct
        SET name = 'test-6d9a5ddfd'
        WHERE id = %s""",
            params=[p.id],
        )

        product_manager.cache_clear(p.id)

        # Calling this with or without kwargs hits different internal keys in the cache!
        p2 = product_manager.get_by_uuid(product_uuid=p.id)
        assert p2.name == "test-6d9a5ddfd"
        p2 = product_manager.get_by_uuid(product_uuid=p.id)
        assert p2.name == "test-6d9a5ddfd"
