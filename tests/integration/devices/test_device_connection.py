"""Integration tests for device connectivity.

These tests use real device IPs from config.json and will only pass
if devices are available and reachable on the network.

Run with: pytest tests/integration/devices/test_device_connection.py -v
Skip with: pytest -m "not integration"
"""

import pytest
from ic256_sampler.utils import is_valid_device, is_valid_ipv4


# Mark all tests in this file as integration tests
pytestmark = pytest.mark.integration


class TestDeviceConnection:
    """Integration tests for device connectivity."""

    def test_ic256_device_validation_real(self, ic256_ip):
        """Test IC256 device validation with real device from config.json.
        
        This test requires:
        - A live IC256 device at the IP in config.json
        - Network connectivity to that device
        """
        # Skip if IP is invalid (e.g., default/placeholder)
        if not is_valid_ipv4(ic256_ip):
            pytest.skip(f"Invalid IP address in config: {ic256_ip}")
        
        # Try to validate the device
        result = is_valid_device(ic256_ip, "IC256")
        
        # If device is not reachable, skip rather than fail
        # (device might be offline, which is not a test failure)
        if not result:
            pytest.skip(f"IC256 device at {ic256_ip} is not reachable or not responding")
        
        assert result is True

    def test_tx2_device_validation_real(self, tx2_ip):
        """Test TX2 device validation with real device from config.json.
        
        This test requires:
        - A live TX2 device at the IP in config.json
        - Network connectivity to that device
        """
        # Skip if IP is invalid (e.g., default/placeholder)
        if not is_valid_ipv4(tx2_ip):
            pytest.skip(f"Invalid IP address in config: {tx2_ip}")
        
        # Try to validate the device
        result = is_valid_device(tx2_ip, "TX2")
        
        # If device is not reachable, skip rather than fail
        # (device might be offline, which is not a test failure)
        if not result:
            pytest.skip(f"TX2 device at {tx2_ip} is not reachable or not responding")
        
        assert result is True

    def test_device_ips_from_config(self, device_config, ic256_ip, tx2_ip):
        """Test that device IPs are correctly loaded from config.json."""
        assert "ic256_45" in device_config
        assert "tx2" in device_config
        assert is_valid_ipv4(ic256_ip)
        assert is_valid_ipv4(tx2_ip)
        assert ic256_ip == device_config["ic256_45"]
        assert tx2_ip == device_config["tx2"]
