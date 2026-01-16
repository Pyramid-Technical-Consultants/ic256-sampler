"""Integration tests for ModelCollector to compare with simple_capture behavior."""

import pytest
import time
import threading
import csv
from pathlib import Path
from datetime import datetime
from ic256_sampler.utils import is_valid_device, is_valid_ipv4
from ic256_sampler.igx_client import IGXWebsocketClient
from ic256_sampler.device_manager import DeviceManager, IC256_CONFIG
from ic256_sampler.model_collector import ModelCollector, collect_data_with_model
from ic256_sampler.ic256_model import IC256Model
from ic256_sampler.simple_capture import capture_to_database
from ic256_sampler.io_database import IODatabase


# Mark all tests as integration tests
pytestmark = pytest.mark.integration


class TestModelCollectorVsSimpleCapture:
    """Compare ModelCollector behavior with simple_capture to find bugs."""

    def test_compare_data_collection_rates(self, ic256_ip, tmp_path):
        """Compare data collection rates between simple_capture and ModelCollector.
        
        This test:
        1. Uses simple_capture to collect data for 2 seconds
        2. Uses ModelCollector to collect data for 2 seconds
        3. Compares the data rates and row counts
        """
        # Skip if IP is invalid
        if not is_valid_ipv4(ic256_ip):
            pytest.skip(f"Invalid IP address in config: {ic256_ip}")
        
        # Verify device is reachable
        if not is_valid_device(ic256_ip, "IC256"):
            pytest.skip(f"IC256 device at {ic256_ip} is not reachable or not responding")
        
        sampling_rate = 3000  # Use 3000 Hz for this test
        collection_duration = 2.0  # 2 seconds
        
        # Test 1: Use simple_capture
        print("\n=== Testing simple_capture ===")
        client1 = IGXWebsocketClient(ic256_ip)
        model = IC256Model()
        reference_channel = model.get_reference_channel()
        
        # Get all channel paths from model
        field_to_path = model.get_field_to_path_mapping()
        channel_paths = list(field_to_path.values())
        
        simple_db = capture_to_database(
            client=client1,
            channel_paths=channel_paths,
            duration=collection_duration,
        )
        client1.close()
        
        simple_stats = simple_db.get_statistics()
        simple_total_points = simple_stats['total_data_points']
        simple_ref_count = simple_db.get_channel_count(reference_channel)
        
        print(f"Simple capture results:")
        print(f"  Total data points: {simple_total_points}")
        print(f"  Reference channel points: {simple_ref_count}")
        print(f"  Reference channel rate: {simple_ref_count / collection_duration:.2f} Hz")
        
        # Test 2: Use ModelCollector
        print("\n=== Testing ModelCollector ===")
        test_file = tmp_path / f"model_collector_test_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        stop_event = threading.Event()
        
        # Create DeviceManager
        device_manager = DeviceManager()
        device_manager.stop_event = stop_event
        
        # Add device
        if not device_manager.add_device(IC256_CONFIG, ic256_ip, sampling_rate):
            pytest.skip(f"Failed to add IC256 device at {ic256_ip}")
        
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
        
        # Start collection
        collection_thread = threading.Thread(
            target=collect_data_with_model,
            args=(collector, stop_event),
            daemon=True
        )
        collection_thread.start()
        
        # Wait for startup
        time.sleep(0.5)
        
        # Collect for duration
        time.sleep(collection_duration)
        
        # Give a moment for final data to be collected
        time.sleep(0.1)
        
        # Stop data collection from devices
        stop_event.set()
        # Give DeviceManager threads time to finish their current iteration
        time.sleep(0.2)
        device_manager.stop()
        
        # Wait for collector thread to finish processing all data
        # The collector will continue processing until all data is written
        # For 3000 Hz at 2 seconds = 6000 rows, allow plenty of time
        collection_thread.join(timeout=60.0)  # Longer timeout for final processing
        
        # Verify thread finished
        if collection_thread.is_alive():
            # Thread didn't finish - this might be OK if it's still processing
            # but we should check the CSV to see if data was written
            print("Warning: Collector thread did not finish in time, but checking CSV...")
            time.sleep(2.0)  # Give it a bit more time
            collection_thread.join(timeout=10.0)  # One more try
        
        # Get ModelCollector database stats
        model_db = device_manager.get_io_database()
        model_stats = model_db.get_statistics()
        model_total_points = model_stats['total_data_points']
        model_ref_count = model_db.get_channel_count(reference_channel)
        
        # Count CSV rows
        csv_row_count = 0
        if test_file.exists():
            with open(test_file, 'r', newline='') as f:
                reader = csv.reader(f)
                next(reader, None)  # Skip header
                for row in reader:
                    if row:
                        csv_row_count += 1
        
        print(f"ModelCollector results:")
        print(f"  Total data points in IO database: {model_total_points}")
        print(f"  Reference channel points: {model_ref_count}")
        print(f"  Reference channel rate: {model_ref_count / collection_duration:.2f} Hz")
        print(f"  CSV rows written: {csv_row_count}")
        print(f"  Expected CSV rows: {int(sampling_rate * collection_duration)}")
        print(f"  CSV row rate: {csv_row_count / collection_duration:.2f} Hz")
        
        # Compare results
        print("\n=== Comparison ===")
        print(f"Simple capture reference rate: {simple_ref_count / collection_duration:.2f} Hz")
        print(f"ModelCollector reference rate: {model_ref_count / collection_duration:.2f} Hz")
        print(f"ModelCollector CSV rate: {csv_row_count / collection_duration:.2f} Hz")
        
        # The reference channel collection rates should be similar
        rate_diff = abs((simple_ref_count / collection_duration) - (model_ref_count / collection_duration))
        rate_diff_pct = (rate_diff / (simple_ref_count / collection_duration)) * 100 if simple_ref_count > 0 else 0
        
        print(f"Rate difference: {rate_diff:.2f} Hz ({rate_diff_pct:.1f}%)")
        
        # ModelCollector should collect at least 80% of what simple_capture collects
        # (accounting for startup delays)
        assert model_ref_count >= simple_ref_count * 0.8, \
            f"ModelCollector collected {model_ref_count} points vs simple_capture {simple_ref_count} points"
        
        # CSV rows should be close to sampling rate
        expected_csv_rows = int(sampling_rate * collection_duration)
        csv_tolerance = 0.2  # 20% tolerance
        assert csv_row_count >= expected_csv_rows * (1 - csv_tolerance), \
            f"CSV row count {csv_row_count} is below expected {expected_csv_rows * (1 - csv_tolerance)}"
        
        assert csv_row_count <= expected_csv_rows * (1 + csv_tolerance), \
            f"CSV row count {csv_row_count} is above expected {expected_csv_rows * (1 + csv_tolerance)}"
