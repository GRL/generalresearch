import logging
from uuid import uuid4

import pytest

from generalresearch.models.thl.product import Product
from test_utils.models.conftest import product_factory

logger = logging.getLogger()


class TestProductManagerGetMethods:

    def test_get_by_uuid(self, product_manager, product_factory):
        # Just test that we load properly
        for p in [product_factory(), product_factory(), product_factory()]:
            instance = product_manager.get_by_uuid(product_uuid=p.id)
            assert isinstance(instance, Product)
            assert instance.id == p.id
            assert instance.uuid == p.uuid

        with pytest.raises(expected_exception=AssertionError) as cm:
            product_manager.get_by_uuid(product_uuid=uuid4().hex)
        assert "product not found" in str(cm.value)

    def test_get_by_uuids(self, product_manager, product_factory):
        products = [product_factory(), product_factory(), product_factory()]
        cnt = len(products)
        res = product_manager.get_by_uuids(product_uuids=[p.id for p in products])
        assert isinstance(res, list)
        assert cnt == len(res)
        for instance in res:
            assert isinstance(instance, Product)

        with pytest.raises(expected_exception=AssertionError) as cm:
            product_manager.get_by_uuids(
                product_uuids=[p.id for p in products] + [uuid4().hex]
            )
        assert "incomplete product response" in str(cm.value)
        with pytest.raises(expected_exception=AssertionError) as cm:
            product_manager.get_by_uuids(
                product_uuids=[p.id for p in products] + ["abc123"]
            )
        assert "invalid uuid passed" in str(cm.value)

    def test_get_by_uuid_if_exists(self, product_factory, product_manager):
        products = [product_factory(), product_factory(), product_factory()]

        instance = product_manager.get_by_uuid_if_exists(product_uuid=products[0].id)
        assert isinstance(instance, Product)

        instance = product_manager.get_by_uuid_if_exists(product_uuid="abc123")
        assert instance is None

    def test_get_by_uuids_if_exists(self, product_manager, product_factory):
        products = [product_factory(), product_factory(), product_factory()]

        res = product_manager.get_by_uuids_if_exists(
            product_uuids=[p.id for p in products[:2]]
        )
        assert isinstance(res, list)
        assert 2 == len(res)
        for instance in res:
            assert isinstance(instance, Product)

        res = product_manager.get_by_uuids_if_exists(
            product_uuids=[p.id for p in products[:2]] + [uuid4().hex]
        )
        assert isinstance(res, list)
        assert 2 == len(res)
        for instance in res:
            assert isinstance(instance, Product)


class TestProductManagerGetAll:

    @pytest.mark.skip(reason="TODO")
    def test_get_ALL_by_ids(self, product_manager):
        products = product_manager.get_all(rand_limit=50)
        logger.info(f"Fetching {len(products)} product uuids")
        # todo: once timebucks stops spamming broken accounts, fetch more
        pass
