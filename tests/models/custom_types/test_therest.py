import json
from uuid import UUID

import pytest
from pydantic import TypeAdapter, ValidationError


class TestAll:

    def test_comma_sep_str(self):
        from generalresearch.models.custom_types import AlphaNumStrSet

        t = TypeAdapter(AlphaNumStrSet)
        assert {"a", "b", "c"} == t.validate_python(["a", "b", "c"])
        assert '"a,b,c"' == t.dump_json({"c", "b", "a"}).decode()
        assert '""' == t.dump_json(set()).decode()
        assert {"a", "b", "c"} == t.validate_json('"c,b,a"')
        assert set() == t.validate_json('""')

        with pytest.raises(ValidationError):
            t.validate_python({"", "b", "a"})

        with pytest.raises(ValidationError):
            t.validate_python({""})

        with pytest.raises(ValidationError):
            t.validate_json('",b,a"')

    def test_UUIDStrCoerce(self):
        from generalresearch.models.custom_types import UUIDStrCoerce

        t = TypeAdapter(UUIDStrCoerce)
        uuid_str = "18e70590176e49c693b07682f3c112be"
        assert uuid_str == t.validate_python("18e70590-176e-49c6-93b0-7682f3c112be")
        assert uuid_str == t.validate_python(
            UUID("18e70590-176e-49c6-93b0-7682f3c112be")
        )
        assert (
            json.dumps(uuid_str)
            == t.dump_json("18e70590176e49c693b07682f3c112be").decode()
        )
        assert uuid_str == t.validate_json('"18e70590-176e-49c6-93b0-7682f3c112be"')
