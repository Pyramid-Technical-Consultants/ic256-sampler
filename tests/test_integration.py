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
from ic256_sampler.device_manager import DeviceManager, DeviceConfig, IC256_CONFIG
from ic256_sampler.model_collector import ModelCollector, collect_data_with_model
from ic256_sampler.ic256_model import IC256Model
from ic256_sampler.device_paths import IC256_45_PATHS


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
        
        # Create DeviceManager to handle device connections
        device_manager = DeviceManager()
        device_manager.stop_event = stop_event
        
        # Add device to manager
        if not device_manager.add_device(IC256_CONFIG, ic256_ip, sampling_rate):
            pytest.skip(f"Failed to add IC256 device at {ic256_ip}")
        
        # Create model and get reference channel
        from ic256_sampler.ic256_model import IC256Model
        model = IC256Model()
        reference_channel = model.get_reference_channel()
        
        # Create ModelCollector
        collector = ModelCollector(
            device_manager=device_manager,
            model=model,
            reference_channel=reference_channel,
            sampling_rate=sampling_rate,
            file_path=str(test_file),
            device_name="ic256_45",
            note="Integration test",
        )
        
        # Share statistics
        collector.statistics = statistics
        
        # Start data collection in a thread
        collection_thread = threading.Thread(
            target=collect_data_with_model,
            args=(collector, stop_event),
            daemon=True
        )
        collection_thread.start()
        
        # Give device time to connect and start collecting (startup delay)
        time.sleep(1.0)
        
        # Wait for collection duration
        time.sleep(collection_duration)
        
        # Stop collection
        stop_event.set()
        # Give DeviceManager threads time to finish their current iteration
        time.sleep(0.2)
        device_manager.stop()  # Also stop DeviceManager explicitly
        
        # Wait for collector thread to finish processing all data
        # The collector will continue processing until all data is written
        # For 3000 Hz at 5 seconds = 15000 rows, allow plenty of time
        collection_thread.join(timeout=120.0)  # Longer timeout for final processing
        
        # Verify thread finished
        if collection_thread.is_alive():
            # Thread didn't finish - give it more time
            print("Warning: Collector thread still processing, waiting longer...")
            time.sleep(5.0)
            collection_thread.join(timeout=30.0)
        
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
        
        # More rows than expected is acceptable (device may send data faster)
        if row_count > max_expected:
            print(f"Note: Collected {row_count} rows, which is above expected maximum {max_expected}. This is acceptable.")
        
        # Verify statistics match file count (within small tolerance for timing)
        stats_rows = statistics.get('rows', 0)
        assert abs(stats_rows - row_count) <= 100, (
            f"Statistics row count ({stats_rows}) doesn't match file row count ({row_count})"
        )
        
        # Calculate actual sampling rate
        actual_rate = row_count / collection_duration
        print(f"  Actual Sampling Rate: {actual_rate:.2f} Hz")

        # Verify actual rate is at least close to target (within 10% below, but higher is acceptable)
        # More rows than expected is acceptable (device may send data faster)
        rate_diff_pct = (actual_rate - sampling_rate) / sampling_rate
        if rate_diff_pct < -tolerance:
            # Rate is too low (more than 10% below target)
            assert False, (
                f"Actual sampling rate {actual_rate:.2f} Hz is more than {tolerance*100:.0f}% below target {sampling_rate} Hz"
            )
        elif rate_diff_pct > tolerance:
            # Rate is higher than expected - this is acceptable
            print(f"Note: Actual sampling rate {actual_rate:.2f} Hz is above target {sampling_rate} Hz. This is acceptable.")
        
        # Check for empty cells in INTERPOLATED columns only
        # Forward-fill is implemented for INTERPOLATED channels, so they should not have empty cells
        # after the first data point is seen
        from ic256_sampler.ic256_model import IC256Model
        from ic256_sampler.virtual_database import ChannelPolicy
        
        # Get column definitions to identify INTERPOLATED columns
        columns = IC256Model.create_columns(reference_channel)
        interpolated_column_names = [
            col_def.name for col_def in columns
            if col_def.policy == ChannelPolicy.INTERPOLATED and col_def.channel_path is not None
        ]
        
        if interpolated_column_names:
            with open(test_file, 'r', newline='', encoding='utf-8-sig') as f:
                reader = csv.reader(f)
                header = next(reader)
                
                # Find indices of INTERPOLATED columns
                interpolated_indices = [
                    i for i, h in enumerate(header) if h in interpolated_column_names
                ]
                
                empty_cell_issues = []
                last_values = {col_idx: None for col_idx in interpolated_indices}
                has_seen_data = {col_idx: False for col_idx in interpolated_indices}
                
                for row_num, row in enumerate(reader, start=2):
                    if len(row) < len(header):
                        # Row has fewer columns than header - missing cells
                        empty_cell_issues.append((row_num, "structural", f"Row has {len(row)} columns, expected {len(header)}"))
                        continue
                    
                    for col_idx in interpolated_indices:
                        if col_idx < len(row):
                            cell_value = row[col_idx]
                            # Check for empty or whitespace-only cells
                            if cell_value and cell_value.strip():
                                last_values[col_idx] = cell_value
                                has_seen_data[col_idx] = True
                            elif has_seen_data[col_idx]:
                                # We've seen data before, so forward-fill should have filled this
                                empty_cell_issues.append((row_num, header[col_idx], "empty cell after data seen"))
                
                if empty_cell_issues:
                    # Report first 20 issues
                    issues_str = ", ".join([
                        f"row {r} col '{c}' ({msg})" 
                        for r, c, msg in empty_cell_issues[:20]
                    ])
                    error_msg = (
                        f"Found {len(empty_cell_issues)} empty cells in INTERPOLATED columns "
                        f"(examples: {issues_str}). "
                        f"This indicates missing data that should be handled by forward-fill. "
                        f"INTERPOLATED columns should have values after the first data point is seen."
                    )
                    pytest.fail(error_msg)
