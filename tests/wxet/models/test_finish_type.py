import pytest

from generalresearch.wxet.models.definitions import WXETStatus, WXETStatusCode1
from generalresearch.wxet.models.finish_type import FinishType, is_a_finish


class TestFinishType:

    def test_init_entrance(self):
        instance = FinishType.ENTRANCE
        finish_statuses = instance.finish_statuses
        assert isinstance(finish_statuses, set)
        assert 5 == len(finish_statuses)

    def test_init_complete(self):
        instance = FinishType.COMPLETE
        finish_statuses = instance.finish_statuses
        assert isinstance(finish_statuses, set)
        assert 1 == len(finish_statuses)

    def test_init_fail_or_complete(self):
        instance = FinishType.FAIL_OR_COMPLETE
        finish_statuses = instance.finish_statuses
        assert isinstance(finish_statuses, set)
        assert 2 == len(finish_statuses)

    def test_init_fail(self):
        instance = FinishType.FAIL
        finish_statuses = instance.finish_statuses
        assert isinstance(finish_statuses, set)
        assert 1 == len(finish_statuses)


class TestFunctionIsAFinish:

    def test_init_ft_entrance(self):
        assert is_a_finish(
            status=None,
            status_code_1=None,
            finish_type=FinishType.ENTRANCE,
        )

        assert is_a_finish(
            status=WXETStatus.ABANDON,
            status_code_1=None,
            finish_type=FinishType.ENTRANCE,
        )

        assert is_a_finish(
            status=WXETStatus.ABANDON,
            status_code_1=WXETStatusCode1.BUYER_ABANDON,
            finish_type=FinishType.ENTRANCE,
        )

        # If it's a WXET Abandon, they ever entered the Task so don't
        # consider it a Finish
        assert not is_a_finish(
            status=WXETStatus.ABANDON,
            status_code_1=WXETStatusCode1.WXET_ABANDON,
            finish_type=FinishType.ENTRANCE,
        )

    def test_init_ft_complete(self):
        assert is_a_finish(
            status=WXETStatus.COMPLETE,
            status_code_1=None,
            finish_type=FinishType.COMPLETE,
        )

        assert is_a_finish(
            status=WXETStatus.COMPLETE,
            status_code_1=WXETStatusCode1.COMPLETE,
            finish_type=FinishType.COMPLETE,
        )

    def test_init_ft_fail_or_complete(self):
        assert is_a_finish(
            status=WXETStatus.FAIL,
            status_code_1=None,
            finish_type=FinishType.FAIL_OR_COMPLETE,
        )

        assert is_a_finish(
            status=WXETStatus.FAIL,
            status_code_1=WXETStatusCode1.BUYER_FAIL,
            finish_type=FinishType.FAIL_OR_COMPLETE,
        )

        # If it's a WXET Fail, the Worker never made it into a WXET Task
        #   experience, so it should not be considered a Finish
        assert not is_a_finish(
            status=WXETStatus.FAIL,
            status_code_1=WXETStatusCode1.WXET_FAIL,
            finish_type=FinishType.FAIL_OR_COMPLETE,
        )

        assert is_a_finish(
            status=WXETStatus.COMPLETE,
            status_code_1=WXETStatusCode1.COMPLETE,
            finish_type=FinishType.FAIL_OR_COMPLETE,
        )

    def test_init_ft_fail(self):
        assert is_a_finish(
            status=WXETStatus.FAIL,
            status_code_1=None,
            finish_type=FinishType.FAIL,
        )

        assert is_a_finish(
            status=WXETStatus.FAIL,
            status_code_1=WXETStatusCode1.BUYER_FAIL,
            finish_type=FinishType.FAIL,
        )

    def test_invalid_status_code_1(self):
        for ft in FinishType:
            for s in WXETStatus:
                with pytest.raises(expected_exception=AssertionError) as cm:
                    is_a_finish(
                        status=s,
                        status_code_1=WXETStatus.COMPLETE,
                        finish_type=ft,
                    )
                assert "Invalid status_code_1" == str(cm.value)

    def test_invalid_none_status(self):
        for ft in FinishType:
            for sc1 in WXETStatusCode1:
                with pytest.raises(expected_exception=AssertionError) as cm:
                    is_a_finish(
                        status=None,
                        status_code_1=sc1,
                        finish_type=ft,
                    )
                assert "Cannot provide status_code_1 without a status" == str(cm.value)
