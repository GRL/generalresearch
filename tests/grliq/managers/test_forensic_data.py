from datetime import timedelta
from typing import TYPE_CHECKING
from uuid import uuid4

import pytest

if TYPE_CHECKING:
    from generalresearch.grliq.managers.forensic_data import (
        GrlIqDataManager,
        GrlIqEventManager,
    )
    from generalresearch.grliq.models.events import MouseEvent, TimingData
    from generalresearch.grliq.models.forensic_data import GrlIqData
    from generalresearch.grliq.models.forensic_result import (
        GrlIqCheckerResults,
        GrlIqForensicCategoryResult,
    )
    from generalresearch.models.thl.product import Product

try:
    from psycopg.errors import UniqueViolation
except ImportError:
    pass


class TestGrlIqDataManager:

    def test_create_dummy(self, grliq_dm: "GrlIqDataManager"):
        from generalresearch.grliq.models.forensic_data import GrlIqData

        gd1: GrlIqData = grliq_dm.create_dummy(is_attempt_allowed=True)

        assert isinstance(gd1, GrlIqData)
        assert isinstance(gd1.results, GrlIqCheckerResults)
        assert isinstance(gd1.category_result, GrlIqForensicCategoryResult)

    def test_create(self, grliq_data: "GrlIqData", grliq_dm: "GrlIqDataManager"):
        grliq_dm.create(grliq_data)
        assert grliq_data.id is not None

        with pytest.raises(UniqueViolation):
            grliq_dm.create(grliq_data)

    @pytest.mark.skip(reason="todo")
    def test_set_result(self):
        pass

    @pytest.mark.skip(reason="todo")
    def test_update_fingerprint(self):
        pass

    @pytest.mark.skip(reason="todo")
    def test_update_data(self):
        pass

    def test_get_id(self, grliq_data: "GrlIqData", grliq_dm: "GrlIqDataManager"):
        grliq_dm.create(grliq_data)

        res = grliq_dm.get_data(forensic_id=grliq_data.id)
        assert res == grliq_data

    def test_get_uuid(self, grliq_data: "GrlIqData", grliq_dm: "GrlIqDataManager"):
        grliq_dm.create(grliq_data)

        res = grliq_dm.get_data(forensic_uuid=grliq_data.uuid)
        assert res == grliq_data

    @pytest.mark.skip(reason="todo")
    def test_filter_timing_data(self):
        pass

    @pytest.mark.skip(reason="todo")
    def test_get_unique_user_count_by_fingerprint(self):
        pass

    def test_filter_data(self, grliq_data: "GrlIqData", grliq_dm: "GrlIqDataManager"):
        grliq_dm.create(grliq_data)
        res = grliq_dm.filter_data(uuids=[grliq_data.uuid])[0]
        assert res == grliq_data
        now = res.created_at
        res = grliq_dm.filter_data(
            created_after=now, created_before=now + timedelta(minutes=1)
        )
        assert len(res) == 1
        res = grliq_dm.filter_data(
            created_after=now + timedelta(seconds=1),
            created_before=now + timedelta(minutes=1),
        )
        assert len(res) == 0

    @pytest.mark.skip(reason="todo")
    def test_filter_results(self):
        pass

    @pytest.mark.skip(reason="todo")
    def test_filter_category_results(self):
        pass

    @pytest.mark.skip(reason="todo")
    def test_make_filter_str(self):
        pass

    def test_filter_count(self, grliq_dm: "GrlIqDataManager", product: "Product"):
        res = grliq_dm.filter_count(product_id=product.uuid)

        assert isinstance(res, int)

    @pytest.mark.skip(reason="todo")
    def test_filter(self):
        pass

    @pytest.mark.skip(reason="todo")
    def test_temporary_add_missing_fields(self):
        pass


class TestForensicDataGetAndFilter:

    def test_events(self, grliq_dm: "GrlIqDataManager"):
        """If load_events=True, the events and mouse_events attributes should
        be an array no matter what. An empty array means that the events were
        loaded, but there were no events available.

        If loaded_eventsFalse, the events and mouse_events attributes should
        be None
        """
        # Load Events == False
        forensic_uuid = uuid4().hex
        grliq_dm.create_dummy(is_attempt_allowed=True, uuid=forensic_uuid)

        instance = grliq_dm.filter_data(uuids=[forensic_uuid])[0]
        assert isinstance(instance, GrlIqData)

        assert instance.events is None
        assert instance.mouse_events is None

        # Load Events == True
        instance = grliq_dm.get_data(forensic_uuid=forensic_uuid, load_events=True)
        assert isinstance(instance, GrlIqData)
        # This one doesn't have any events though
        assert len(instance.events) == 0
        assert len(instance.mouse_events) == 0

    def test_timing(self, grliq_dm: "GrlIqDataManager", grliq_em: "GrlIqEventManager"):
        forensic_uuid = uuid4().hex
        grliq_dm.create_dummy(is_attempt_allowed=True, uuid=forensic_uuid)

        instance = grliq_dm.filter_data(uuids=[forensic_uuid])[0]

        grliq_em.update_or_create_timing(
            session_uuid=instance.mid,
            timing_data=TimingData(
                client_rtts=[100, 200, 150], server_rtts=[150, 120, 120]
            ),
        )

        instance = grliq_dm.get_data(forensic_uuid=forensic_uuid, load_events=True)
        assert isinstance(instance, GrlIqData)
        assert isinstance(instance.events, list)
        assert isinstance(instance.mouse_events, list)
        assert isinstance(instance.timing_data, TimingData)

    def test_events_events(
        self, grliq_dm: "GrlIqDataManager", grliq_em: "GrlIqEventManager"
    ):
        forensic_uuid = uuid4().hex
        grliq_dm.create_dummy(is_attempt_allowed=True, uuid=forensic_uuid)

        instance = grliq_dm.filter_data(uuids=[forensic_uuid])[0]

        grliq_em.update_or_create_events(
            session_uuid=instance.mid,
            events=[{"a": "b"}],
            mouse_events=[],
            event_start=instance.created_at,
            event_end=instance.created_at + timedelta(minutes=1),
        )
        instance = grliq_dm.get_data(forensic_uuid=forensic_uuid, load_events=True)
        assert isinstance(instance, GrlIqData)
        assert isinstance(instance.events, list)
        assert isinstance(instance.mouse_events, list)
        assert instance.timing_data is None
        assert instance.events == [{"a": "b"}]
        assert len(instance.mouse_events) == 0
        assert len(instance.pointer_move_events) == 0
        assert len(instance.keyboard_events) == 0

    def test_events_click(
        self, grliq_dm: "GrlIqDataManager", grliq_em: "GrlIqEventManager"
    ):
        forensic_uuid = uuid4().hex
        grliq_dm.create_dummy(is_attempt_allowed=True, uuid=forensic_uuid)
        instance = grliq_dm.get_data(forensic_uuid=forensic_uuid, load_events=True)

        click_event = {
            "type": "click",
            "pageX": 0,
            "pageY": 0,
            "timeStamp": 123,
            "pointerType": "mouse",
        }
        me = MouseEvent.from_dict(click_event)
        grliq_em.update_or_create_events(
            session_uuid=instance.mid,
            events=[click_event],
            mouse_events=[],
            event_start=instance.created_at,
            event_end=instance.created_at + timedelta(minutes=1),
        )
        instance = grliq_dm.get_data(forensic_uuid=forensic_uuid, load_events=True)
        assert isinstance(instance, GrlIqData)
        assert isinstance(instance.events, list)
        assert isinstance(instance.mouse_events, list)
        assert instance.timing_data is None
        assert instance.events == [click_event]
        assert instance.mouse_events == [me]
        assert len(instance.pointer_move_events) == 0
        assert len(instance.keyboard_events) == 0
