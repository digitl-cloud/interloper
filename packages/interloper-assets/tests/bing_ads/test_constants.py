"""Tests for the Bing Ads constants the sources share."""

from __future__ import annotations

import re
from importlib import resources
from zoneinfo import ZoneInfo

from interloper_assets.bing_ads import constants


def _wsdl_time_zones() -> list[str]:
    wsdl = resources.files("bingads") / "v13" / "proxies" / "production" / "customermanagement_service.xml"
    [enumeration] = re.findall(r'<xs:simpleType name="TimeZoneType">(.*?)</xs:simpleType>', wsdl.read_text(), re.DOTALL)
    return re.findall(r'<xs:enumeration value="(\w+)"', enumeration)


class TestTimeZones:
    def test_every_bing_time_zone_has_an_iana_zone(self):
        assert sorted(constants.TIME_ZONES) == sorted(_wsdl_time_zones())

    def test_every_iana_zone_exists(self):
        for bing, iana in constants.TIME_ZONES.items():
            assert ZoneInfo(iana), bing
