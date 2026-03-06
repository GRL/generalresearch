from typing import Optional
from uuid import uuid4

import pytest
from pydantic import BaseModel, ValidationError, Field

from generalresearch.models.custom_types import UUIDStr


class UUIDStrModel(BaseModel):
    uuid_optional: Optional[UUIDStr] = Field(default_factory=lambda: uuid4().hex)
    uuid: UUIDStr


class TestUUIDStr:
    def test_str(self):
        v = "58889cd67f9f4c699b25437112dce638"

        t = UUIDStrModel(uuid=v, uuid_optional=v)
        UUIDStrModel.model_validate_json(t.model_dump_json())

        t = UUIDStrModel(uuid=v, uuid_optional=None)
        t2 = UUIDStrModel.model_validate_json(t.model_dump_json())

        assert t2.uuid_optional is None
        assert t2.uuid == v

    def test_uuid(self):
        v = uuid4()

        with pytest.raises(ValidationError) as cm:
            UUIDStrModel(uuid=v, uuid_optional=None)
        assert "Input should be a valid string" in str(cm.value)

        with pytest.raises(ValidationError) as cm:
            UUIDStrModel(uuid="58889cd67f9f4c699b25437112dce638", uuid_optional=v)
        assert "Input should be a valid string" in str(cm.value)

    def test_invalid_format(self):
        v = "x"
        with pytest.raises(ValidationError):
            UUIDStrModel(uuid=v, uuid_optional=None)

        with pytest.raises(ValidationError):
            UUIDStrModel(uuid="58889cd67f9f4c699b25437112dce638", uuid_optional=v)

    def test_required(self):
        v = "58889cd67f9f4c699b25437112dce638"

        with pytest.raises(ValidationError):
            UUIDStrModel(uuid=None, uuid_optional=v)
