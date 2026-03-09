from typing import TYPE_CHECKING

import pytest
from pydantic import ValidationError

if TYPE_CHECKING:
    from generalresearch.grliq.models.forensic_data import GrlIqData


class TestGrlIqData:

    def test_supported_fonts(self, grliq_data: "GrlIqData"):
        s = grliq_data.supported_fonts_binary
        assert len(s) == 1043
        assert "Ubuntu" in grliq_data.supported_fonts

    def test_battery(self, grliq_data: "GrlIqData"):
        assert not grliq_data.battery_charging
        assert grliq_data.battery_level == 0.41

    def test_base(self, grliq_data: "GrlIqData"):
        from generalresearch.grliq.models.forensic_data import Platform

        assert grliq_data.timezone == "America/Los_Angeles"
        assert grliq_data.platform == Platform.LINUX_X86_64
        assert grliq_data.webgl_extensions
        # ... more

        assert grliq_data.results is None
        assert grliq_data.category_result is None

        s = grliq_data.model_dump_json()
        from generalresearch.grliq.models.forensic_data import GrlIqData, Platform

        g2: GrlIqData = GrlIqData.model_validate_json(s)

        assert g2.results is None
        assert g2.category_result is None

        assert grliq_data == g2

    # Testing things that will cause a validation error, should only be
    # because something is "corrupt", not b/c the user is a baddie
    def test_corrupt(self, grliq_data: "GrlIqData"):
        """Test for timestamp and timezone offset mismatch validation."""
        from generalresearch.grliq.models.forensic_data import GrlIqData

        d = grliq_data.model_dump(mode="json")
        d.update(
            {
                "timezone": "America/XXX",
            }
        )
        with pytest.raises(ValidationError) as e:
            GrlIqData.model_validate(d)

        assert "Invalid timezone name" in str(e.value)
