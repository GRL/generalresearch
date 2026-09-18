from generalresearch.managers.thl.ipinfo import GeoIpInfoManager
from generalresearch.models.thl.ipinfo import GeoIPInformation


class TestGeoIpInfoManager:
    def test_get(self, geoip_info_manager: GeoIpInfoManager):
        result = geoip_info_manager.get("8.8.8.8")

        assert result == GeoIPInformation(
            ip="8.8.8.8",
            country_iso="us",
            is_anonymous=False,
            autonomous_system_number=15169,
            autonomous_system_organization="Google",
            access_type=None,
        )
        geoip_info_manager.grip_mmdb.lookup.assert_called_once_with("8.8.8.8")

    def test_get_multi(self, geoip_info_manager: GeoIpInfoManager):
        result = geoip_info_manager.get_multi(["8.8.8.8", "1.1.1.1", "8.8.8.8"])

        assert result == {
            "8.8.8.8": GeoIPInformation(
                ip="8.8.8.8",
                country_iso="us",
                is_anonymous=False,
                autonomous_system_number=15169,
                autonomous_system_organization="Google",
                access_type=None,
            ),
            "1.1.1.1": GeoIPInformation(
                ip="1.1.1.1",
                country_iso="au",
                is_anonymous=True,
                autonomous_system_number=13335,
                autonomous_system_organization="Cloudflare",
                access_type=None,
            ),
        }
        assert geoip_info_manager.grip_mmdb.lookup.call_count == 2
        assert {
            call.args[0] for call in geoip_info_manager.grip_mmdb.lookup.call_args_list
        } == {"8.8.8.8", "1.1.1.1"}

    def test_get_multi_empty(self, geoip_info_manager: GeoIpInfoManager):
        assert geoip_info_manager.get_multi([]) == {}
        geoip_info_manager.grip_mmdb.lookup.assert_not_called()
