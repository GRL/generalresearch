import pytest
from pydantic import ValidationError

from generalresearch.grliq.models.forensic_data import GrlIqData, Platform


class TestGrlIqData:

    def test_supported_fonts(self, grliq_data):
        s = grliq_data.supported_fonts_binary
        assert len(s) == 1043
        assert "Ubuntu" in grliq_data.supported_fonts

    def test_battery(self, grliq_data):
        assert not grliq_data.battery_charging
        assert grliq_data.battery_level == 0.41

    def test_base(self, grliq_data):
        g: GrlIqData = grliq_data
        assert g.timezone == "America/Los_Angeles"
        assert g.platform == Platform.LINUX_X86_64
        assert g.webgl_extensions
        # ... more

        assert g.results is None
        assert g.category_result is None

        s = g.model_dump_json()
        g2: GrlIqData = GrlIqData.model_validate_json(s)

        assert g2.results is None
        assert g2.category_result is None

        assert g == g2

    # Testing things that will cause a validation error, should only be
    # because something is "corrupt", not b/c the user is a baddie
    def test_corrupt(self, grliq_data):
        """Test for timestamp and timezone offset mismatch validation."""
        d = grliq_data.model_dump(mode="json")
        d.update(
            {
                "timezone": "America/XXX",
            }
        )
        with pytest.raises(ValidationError) as e:
            GrlIqData.model_validate(d)

        assert "Invalid timezone name" in str(e.value)
