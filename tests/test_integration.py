"""Integration tests that require live device connections.

These tests use real device IPs from config.json and will only pass
if devices are available and reachable on the network.

Run with: pytest tests/test_integration.py -v
Skip with: pytest -m "not integration"
"""

import pytest
import time
import threading
import csv
from pathlib import Path
from datetime import datetime
from ic256_sampler.utils import is_valid_device, is_valid_ipv4
from ic256_sampler.igx_client import IGXWebsocketClient
from ic256_sampler.device_manager import setup_device_thread, DeviceConfig, IC256_CONFIG
from ic256_sampler.data_collection import collect_data


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


class TestDataCollectionRate:
    """Integration tests for data collection rate verification."""

    def test_ic256_sampling_rate_3000hz(self, ic256_ip, tmp_path):
        """Test that IC256 data collection writes rows at 3000 Hz.
        
        This test:
        1. Connects to a real IC256 device
        2. Collects data for 5 seconds at 3000 Hz
        3. Verifies that approximately 15,000 rows (5s * 3000 Hz) are written
        4. Checks that the row count is within acceptable tolerance (90-110%)
        
        This test requires:
        - A live IC256 device at the IP in config.json
        - Network connectivity to that device
        """
        # Skip if IP is invalid
        if not is_valid_ipv4(ic256_ip):
            pytest.skip(f"Invalid IP address in config: {ic256_ip}")
        
        # Verify device is reachable
        if not is_valid_device(ic256_ip, "IC256"):
            pytest.skip(f"IC256 device at {ic256_ip} is not reachable or not responding")
        
        # Test parameters
        sampling_rate = 3000  # Hz
        collection_duration = 5.0  # seconds
        expected_rows = int(sampling_rate * collection_duration)  # 15,000 rows
        tolerance = 0.10  # 10% tolerance (accounting for startup/shutdown delays)
        
        # Create temporary file for data collection
        test_file = tmp_path / f"test_collection_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        
        # Create stop event for data collection
        stop_event = threading.Event()
        
        # Statistics dictionary to track rows
        statistics = {"rows": 0, "file_size": 0}
        
        # Start data collection in a thread
        def collect_data_thread():
            try:
                # Create client and channels using IC256_CONFIG
                client = IGXWebsocketClient(ic256_ip)
                channels = IC256_CONFIG.channel_creator(client)
                env_channels = IC256_CONFIG.env_channel_creator(client) if IC256_CONFIG.env_channel_creator else None
                
                collect_data(
                    device_client=client,
                    channels=channels,
                    env_channels=env_channels,
                    file_name=test_file.name,
                    device_name="ic256_45",
                    note="Integration test",
                    save_folder=str(test_file.parent),
                    stop_event=stop_event,
                    sampling_rate=sampling_rate,
                    statistics=statistics,
                )
            except Exception as e:
                print(f"Data collection error: {e}")
                import traceback
                traceback.print_exc()
        
        collection_thread = threading.Thread(target=collect_data_thread, daemon=True)
        collection_thread.start()
        
        # Wait for collection duration
        time.sleep(collection_duration)
        
        # Stop collection
        stop_event.set()
        
        # Wait for thread to finish (with timeout)
        collection_thread.join(timeout=10.0)
        
        # Verify file was created
        assert test_file.exists(), f"Data file was not created: {test_file}"
        
        # Count rows in CSV file
        row_count = 0
        with open(test_file, 'r', newline='') as f:
            reader = csv.reader(f)
            # Skip header row
            next(reader, None)
            for row in reader:
                if row:  # Skip empty rows
                    row_count += 1
        
        # Verify row count
        min_expected = int(expected_rows * (1 - tolerance))  # 13,500 rows (90%)
        max_expected = int(expected_rows * (1 + tolerance))  # 16,500 rows (110%)
        
        print(f"\nCollection Results:")
        print(f"  Duration: {collection_duration} seconds")
        print(f"  Sampling Rate: {sampling_rate} Hz")
        print(f"  Expected Rows: {expected_rows}")
        print(f"  Actual Rows: {row_count}")
        print(f"  Statistics Rows: {statistics.get('rows', 0)}")
        print(f"  File Size: {test_file.stat().st_size} bytes")
        print(f"  Expected Range: {min_expected} - {max_expected} rows")
        
        assert row_count >= min_expected, (
            f"Row count {row_count} is below minimum expected {min_expected} "
            f"(expected ~{expected_rows} rows for {collection_duration}s at {sampling_rate} Hz)"
        )
        
        assert row_count <= max_expected, (
            f"Row count {row_count} is above maximum expected {max_expected} "
            f"(expected ~{expected_rows} rows for {collection_duration}s at {sampling_rate} Hz)"
        )
        
        # Verify statistics match file count (within small tolerance for timing)
        stats_rows = statistics.get('rows', 0)
        assert abs(stats_rows - row_count) <= 100, (
            f"Statistics row count ({stats_rows}) doesn't match file row count ({row_count})"
        )
        
        # Calculate actual sampling rate
        actual_rate = row_count / collection_duration
        print(f"  Actual Sampling Rate: {actual_rate:.2f} Hz")
        
        # Verify actual rate is close to target (within 10%)
        assert abs(actual_rate - sampling_rate) / sampling_rate <= tolerance, (
            f"Actual sampling rate {actual_rate:.2f} Hz is not close to target {sampling_rate} Hz"
        )
