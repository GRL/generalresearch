import pytest


class TestWXETStatusCode1:

    def test_is_pre_task_entry_fail_pre(self):
        from generalresearch.wxet.models.definitions import (
            WXETStatusCode1,
        )

        assert WXETStatusCode1.UNKNOWN.is_pre_task_entry_fail
        assert WXETStatusCode1.WXET_FAIL.is_pre_task_entry_fail
        assert WXETStatusCode1.WXET_ABANDON.is_pre_task_entry_fail

    def test_is_pre_task_entry_fail_post(self):
        from generalresearch.wxet.models.definitions import (
            WXETStatusCode1,
        )

        assert not WXETStatusCode1.BUYER_OVER_QUOTA.is_pre_task_entry_fail
        assert not WXETStatusCode1.BUYER_DUPLICATE.is_pre_task_entry_fail
        assert not WXETStatusCode1.BUYER_TASK_NOT_AVAILABLE.is_pre_task_entry_fail

        assert not WXETStatusCode1.BUYER_ABANDON.is_pre_task_entry_fail
        assert not WXETStatusCode1.BUYER_FAIL.is_pre_task_entry_fail
        assert not WXETStatusCode1.BUYER_QUALITY_FAIL.is_pre_task_entry_fail
        assert not WXETStatusCode1.BUYER_POSTBACK_NOT_RECEIVED.is_pre_task_entry_fail
        assert not WXETStatusCode1.COMPLETE.is_pre_task_entry_fail


class TestCheckWXETStatusConsistent:

    def test_completes(self):

        from generalresearch.wxet.models.definitions import (
            WXETStatus,
            WXETStatusCode1,
            check_wxet_status_consistent,
        )

        with pytest.raises(AssertionError) as cm:
            check_wxet_status_consistent(
                status=WXETStatus.COMPLETE,
                status_code_1=WXETStatusCode1.UNKNOWN,
                status_code_2=None,
            )

        assert (
            "Invalid StatusCode1 when Status=COMPLETE. Use WXETStatusCode1.COMPLETE"
            == str(cm.value)
        )

    def test_abandon(self):

        from generalresearch.wxet.models.definitions import (
            WXETStatus,
            WXETStatusCode1,
            check_wxet_status_consistent,
        )

        with pytest.raises(AssertionError) as cm:
            check_wxet_status_consistent(
                status=WXETStatus.ABANDON,
                status_code_1=WXETStatusCode1.COMPLETE,
                status_code_2=None,
            )
        assert (
            "Invalid StatusCode1 when Status=ABANDON. Use WXET_ABANDON or BUYER_ABANDON"
            == str(cm.value)
        )

    def test_fail(self):

        from generalresearch.wxet.models.definitions import (
            WXETStatus,
            WXETStatusCode1,
            check_wxet_status_consistent,
        )

        for sc1 in [
            WXETStatusCode1.COMPLETE,
            WXETStatusCode1.WXET_ABANDON,
            WXETStatusCode1.WXET_ABANDON,
        ]:
            with pytest.raises(AssertionError) as cm:
                check_wxet_status_consistent(
                    status=WXETStatus.FAIL,
                    status_code_1=sc1,
                    status_code_2=None,
                )
            assert "Invalid StatusCode1 when Status=FAIL." == str(cm.value)

    def test_status_code_2(self):
        """Any StatusCode2 should fail if the StatusCode1 isn't
        StatusCode1.WXET_FAIL
        """

        from generalresearch.wxet.models.definitions import (
            WXETStatus,
            WXETStatusCode1,
            WXETStatusCode2,
            check_wxet_status_consistent,
        )

        for sc2 in WXETStatusCode2:
            with pytest.raises(AssertionError) as cm:
                check_wxet_status_consistent(
                    status=WXETStatus.FAIL,
                    status_code_1=WXETStatusCode1.COMPLETE,
                    status_code_2=sc2,
                )

            assert "Invalid StatusCode1 when Status=FAIL." == str(cm.value)
