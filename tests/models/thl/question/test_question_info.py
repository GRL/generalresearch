from __future__ import annotations

from generalresearch.models.thl.profiling.upk_property import (
    ProfilingInfo,
    UpkProperty,
)


class TestQuestionInfo:

    def test_init(self, profiling_info_json: str):

        instance_list = ProfilingInfo.validate_json(profiling_info_json)

        assert isinstance(instance_list, list)
        for i in instance_list:
            assert isinstance(i, UpkProperty)
