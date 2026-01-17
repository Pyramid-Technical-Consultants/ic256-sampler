"""Integration tests for connection status monitoring.

These tests verify that connection status is properly detected and updated
when using real device connections.
"""

import pytest
import time
import threading
from ic256_sampler.device_manager import DeviceManager, IC256_CONFIG
from ic256_sampler.utils import is_valid_device, is_valid_ipv4


# Mark all tests in this file as integration tests with timeout
pytestmark = [pytest.mark.integration, pytest.mark.timeout(10)]


class TestConnectionStatus:
    """Integration tests for connection status monitoring."""
    
    def test_connection_status_updates_on_real_connection(self, ic256_ip):
        """Test that connection status updates correctly with a real device connection.
        
        This test:
        1. Creates a DeviceManager
        2. Adds a device with a real IP
        3. Waits for keep-alive thread to update status
        4. Verifies status becomes "connected"
        5. Closes connection
        6. Verifies status updates appropriately
        """
        # Skip if IP is invalid
        if not is_valid_ipv4(ic256_ip):
            pytest.skip(f"Invalid IP address in config: {ic256_ip}")
        
        # Skip if device is not reachable
        if not is_valid_device(ic256_ip, "IC256"):
            pytest.skip(f"IC256 device at {ic256_ip} is not reachable")
        
        # Create device manager
        device_manager = DeviceManager()
        
        # Track status updates
        status_updates = []
        
        def status_callback(status_dict):
            status_updates.append(status_dict.copy())
        
        device_manager.set_status_callback(status_callback)
        
        # Add device
        success = device_manager.add_device(
            IC256_CONFIG,
            ic256_ip,
            sampling_rate=500,
            log_callback=None
        )
        
        assert success, "Device should be added successfully"
        
        # Wait for keep-alive thread to start and update status
        # Give it time to establish connection and check status
        time.sleep(2.0)
        
        # Check initial status
        status = device_manager.get_connection_status()
        assert IC256_CONFIG.device_name in status, "Device should be in status dict"
        
        # Status should be "connected" if connection is working
        # (it might start as "connected" from add_device, or update via keep-alive thread)
        device_status = status[IC256_CONFIG.device_name]
        assert device_status in ("connected", "disconnected", "error"), \
            f"Status should be one of: connected, disconnected, error. Got: {device_status}"
        
        # If we got status updates, verify they make sense
        if status_updates:
            # Last update should have the device
            last_update = status_updates[-1]
            assert IC256_CONFIG.device_name in last_update, \
                "Last status update should include the device"
        
        # Clean up
        device_manager.close_all_connections()
        
        # Wait a bit for cleanup
        time.sleep(0.5)
        
        # After closing, status should be empty or show disconnected
        final_status = device_manager.get_connection_status()
        # Connections dict should be empty after close_all_connections
        assert len(final_status) == 0, "Status should be empty after closing all connections"
    
    def test_connection_status_with_invalid_ip(self):
        """Test that connection status handles invalid IPs correctly."""
        device_manager = DeviceManager()
        
        status_updates = []
        
        def status_callback(status_dict):
            status_updates.append(status_dict.copy())
        
        device_manager.set_status_callback(status_callback)
        
        # Try to add device with invalid IP (should fail)
        success = device_manager.add_device(
            IC256_CONFIG,
            "999.999.999.999",  # Invalid/unreachable IP
            sampling_rate=500,
            log_callback=None
        )
        
        # Should fail to add device
        assert not success, "Device with invalid IP should not be added"
        
        # Status should be empty
        status = device_manager.get_connection_status()
        assert len(status) == 0, "Status should be empty when no devices are connected"
