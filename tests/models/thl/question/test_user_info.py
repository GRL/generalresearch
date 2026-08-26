from __future__ import annotations

from generalresearch.models.thl.profiling.user_info import UserInfo


class TestUserInfo:

    def test_init(self, profiling_user_info_json: str):

        instance = UserInfo.model_validate_json(profiling_user_info_json)
        assert isinstance(instance, UserInfo)
