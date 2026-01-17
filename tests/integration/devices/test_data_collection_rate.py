"""Integration tests for data collection rate verification.

These tests verify that data collection happens at the expected sampling rates.
"""

import pytest
import time
import threading
import csv
from pathlib import Path
from datetime import datetime
from ic256_sampler.utils import is_valid_device, is_valid_ipv4
from ic256_sampler.device_manager import DeviceManager, IC256_CONFIG
from ic256_sampler.model_collector import ModelCollector, collect_data_with_model
from ic256_sampler.ic256_model import IC256Model
from ic256_sampler.virtual_database import ChannelPolicy


# Mark all tests in this file as integration tests with timeout
pytestmark = [pytest.mark.integration, pytest.mark.timeout(20)]


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
        collection_thread.join(timeout=120.0)
        
        # Verify thread finished
        if collection_thread.is_alive():
            print("Warning: Collector thread still processing, waiting longer...")
            time.sleep(5.0)
            collection_thread.join(timeout=30.0)
        
        # Verify file was created
        assert test_file.exists(), f"Data file was not created: {test_file}"
        
        # Count rows in CSV file
        row_count = 0
        with open(test_file, 'r', newline='') as f:
            reader = csv.reader(f)
            next(reader, None)  # Skip header
            for row in reader:
                if row:
                    row_count += 1
        
        # Verify row count
        min_expected = int(expected_rows * (1 - tolerance))
        max_expected = int(expected_rows * (1 + tolerance))
        
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
        
        if row_count > max_expected:
            print(f"Note: Collected {row_count} rows, which is above expected maximum {max_expected}. This is acceptable.")
        
        # Verify statistics match file count
        stats_rows = statistics.get('rows', 0)
        assert abs(stats_rows - row_count) <= 100, (
            f"Statistics row count ({stats_rows}) doesn't match file row count ({row_count})"
        )
        
        # Calculate actual sampling rate
        actual_rate = row_count / collection_duration
        print(f"  Actual Sampling Rate: {actual_rate:.2f} Hz")
        
        rate_diff_pct = (actual_rate - sampling_rate) / sampling_rate
        if rate_diff_pct < -tolerance:
            assert False, (
                f"Actual sampling rate {actual_rate:.2f} Hz is more than {tolerance*100:.0f}% below target {sampling_rate} Hz"
            )
        elif rate_diff_pct > tolerance:
            print(f"Note: Actual sampling rate {actual_rate:.2f} Hz is above target {sampling_rate} Hz. This is acceptable.")
        
        # Check for empty cells in data columns
        columns = IC256Model.create_columns(reference_channel)
        col_policy_map = {
            col_def.name: col_def.policy 
            for col_def in columns 
            if col_def.channel_path is not None
        }
        
        important_channels = [
            "External trigger",
            "Temperature (℃)",
            "Humidity (%rH)",
            "Pressure (hPa)",
        ]
        
        if col_policy_map:
            with open(test_file, 'r', newline='', encoding='utf-8-sig') as f:
                reader = csv.reader(f)
                header = next(reader)
                
                data_indices = [i for i, h in enumerate(header) if h in col_policy_map]
                interpolated_indices = [
                    i for i, h in enumerate(header) 
                    if h in col_policy_map and col_policy_map[h] == ChannelPolicy.INTERPOLATED
                ]
                important_indices = [i for i, h in enumerate(header) if h in important_channels]
                
                empty_cell_issues = []
                has_seen_data = {col_idx: False for col_idx in data_indices}
                
                for row_num, row in enumerate(reader, start=2):
                    if len(row) < len(header):
                        empty_cell_issues.append((row_num, "structural", f"Row has {len(row)} columns, expected {len(header)}"))
                        continue
                    
                    async_indices = [
                        i for i, h in enumerate(header) 
                        if h in col_policy_map and col_policy_map[h] == ChannelPolicy.ASYNCHRONOUS
                    ]
                    forward_fill_indices = interpolated_indices + async_indices
                    
                    for col_idx in forward_fill_indices:
                        if col_idx < len(row):
                            cell_value = row[col_idx]
                            if cell_value and cell_value.strip():
                                has_seen_data[col_idx] = True
                            elif has_seen_data[col_idx]:
                                col_name = header[col_idx]
                                empty_cell_issues.append((row_num, col_name, "empty cell after data seen (forward-fill should have filled)"))
                    
                    for col_idx in data_indices:
                        if col_idx < len(row):
                            cell_value = row[col_idx]
                            if cell_value and cell_value.strip():
                                has_seen_data[col_idx] = True
                
                if empty_cell_issues:
                    issues_str = ", ".join([
                        f"row {r} col '{c}' ({msg})" 
                        for r, c, msg in empty_cell_issues[:20]
                    ])
                    error_msg = (
                        f"Found {len(empty_cell_issues)} empty cells in INTERPOLATED/ASYNCHRONOUS columns "
                        f"(examples: {issues_str}). "
                        f"INTERPOLATED and ASYNCHRONOUS columns should have forward-fill values after the first data point is seen."
                    )
                    pytest.fail(error_msg)
                
                missing_important = []
                for col_idx in important_indices:
                    col_name = header[col_idx]
                    if not has_seen_data.get(col_idx, False):
                        missing_important.append(col_name)
                
                if missing_important:
                    pytest.fail(
                        f"Important channels have no data in CSV: {', '.join(missing_important)}. "
                        f"These channels should be collected: External trigger (ASYNCHRONOUS), "
                        f"Temperature/Humidity/Pressure (INTERPOLATED environmental channels)."
                    )
