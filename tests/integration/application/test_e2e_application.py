"""End-to-end integration tests for the full application workflow.

These tests verify the complete application flow:
1. Application initialization
2. Device connection
3. Data collection
4. CSV file creation with correct content

These tests require live device connections and will skip if devices are unavailable.
"""

import pytest
import time
import threading
import csv
from pathlib import Path
from tests.conftest import verify_csv_file, wait_for_condition


# Mark all tests in this file as integration tests with timeout
pytestmark = [pytest.mark.integration, pytest.mark.timeout(10)]


class TestEndToEndApplication:
    """End-to-end tests for the complete application workflow."""

    @pytest.mark.integration
    def test_application_full_workflow_ic256(self, require_ic256_device, app_with_mock_gui, tmp_path):
        """End-to-end test: Full application workflow with IC256 device.
        
        This test:
        1. Creates an Application instance
        2. Mocks the GUI (since we don't need actual GUI for testing)
        3. Connects to a real IC256 device
        4. Starts data collection
        5. Collects data for a short period
        6. Stops data collection
        7. Verifies CSV file is created with correct structure and data
        """
        app = app_with_mock_gui
        sampling_rate = 500
        collection_duration = 3.0
        
        app.window.sampling_entry.get.return_value = str(sampling_rate)
        
        app._ensure_connections()
        assert app.device_manager is not None
        assert "IC256" in app.device_manager.connections
        
        # Start data collection
        device_thread = threading.Thread(
            target=app._device_thread,
            name="e2e_device_thread",
            daemon=True
        )
        device_thread.start()
        time.sleep(1.0)
        
        assert app.collector is not None
        assert app.collector_thread.is_alive()
        assert app.device_manager._running is True
        
        # Wait for data collection
        for i in range(int(collection_duration) + 2):
            time.sleep(1.0)
            if i % 2 == 0:
                stats = app.device_manager.get_io_database().get_statistics()
                total_points = stats.get('total_data_points', 0)
                rows_written = app.collector.get_statistics().get('rows', 0)
                print(f"  After {i+1}s: {total_points} data points, {rows_written} CSV rows")
        
        total_points = app.device_manager.get_io_database().get_statistics().get('total_data_points', 0)
        assert total_points > 10, f"Data should be collected. Got {total_points} data points"
        
        rows_written = app.collector.get_statistics().get('rows', 0)
        assert rows_written > 0, f"CSV rows should be written. Got {rows_written} rows"
        
        # Stop data collection
        app.stop_collection()
        
        # Wait for CSV to be written with timeout
        def csv_ready():
            if not app.collector:
                return False
            rows_written = app.collector.get_statistics().get('rows', 0)
            thread_done = not app.collector_thread or not app.collector_thread.is_alive()
            return rows_written > 0 and thread_done
        
        wait_for_condition(csv_ready, timeout=10.0, description="CSV to be written")
        
        # Join collector thread with timeout
        if app.collector_thread and app.collector_thread.is_alive():
            app.collector_thread.join(timeout=5.0)
        
        # Verify CSV file was created
        csv_files = list(tmp_path.glob("IC256_42x35-*.csv"))
        assert len(csv_files) > 0, f"CSV file should be created in {tmp_path}"
        
        csv_file = csv_files[0]
        row_count, header = verify_csv_file(csv_file, min_rows=10)
        
        # Verify final statistics match
        final_rows = app.collector.get_statistics().get('rows', 0)
        assert final_rows == row_count, \
            f"Collector statistics ({final_rows}) should match CSV row count ({row_count})"
        
        print(f"\nE2E Test Results:")
        print(f"  CSV File: {csv_file}")
        print(f"  CSV Rows: {row_count}")
        print(f"  Data Points: {total_points}")

    @pytest.mark.integration
    def test_application_connects_and_collects_data(self, require_ic256_device, app_with_mock_gui):
        """Simplified end-to-end test: Verify application can connect and collect data."""
        app = app_with_mock_gui
        
        app._ensure_connections()
        assert app.device_manager is not None
        assert "IC256" in app.device_manager.connections
        
        # Start collection
        device_thread = threading.Thread(
            target=app._device_thread,
            name="e2e_device_thread",
            daemon=True
        )
        device_thread.start()
        time.sleep(1.0)
        
        assert app.collector is not None
        assert app.device_manager._running is True
        
        # Collect for a short time
        time.sleep(2.0)
        
        # Verify data is being collected
        stats = app.device_manager.get_io_database().get_statistics()
        total_points = stats.get('total_data_points', 0)
        assert total_points > 10, f"Should collect data. Got {total_points} points"
        
        # Stop collection
        app.stop_collection()
        
        # Wait for threads to stop
        wait_for_condition(
            lambda: not app.device_manager._running,
            timeout=5.0,
            description="device manager to stop"
        )
        
        assert app.device_manager._running is False

    @pytest.mark.integration
    def test_application_csv_file_format(self, require_ic256_device, app_with_mock_gui, tmp_path):
        """End-to-end test: Verify CSV file format is correct."""
        app = app_with_mock_gui
        sampling_rate = 500
        collection_duration = 2.0
        
        app.window.sampling_entry.get.return_value = str(sampling_rate)
        
        app._ensure_connections()
        device_thread = threading.Thread(
            target=app._device_thread,
            name="e2e_csv_test",
            daemon=True
        )
        device_thread.start()
        
        time.sleep(1.0 + collection_duration)
        app.stop_collection()
        
        # Wait for CSV file to be created
        wait_for_condition(
            lambda: len(list(tmp_path.glob("IC256_42x35-*.csv"))) > 0,
            timeout=5.0,
            description="CSV file to be created"
        )
        
        # Find CSV file
        csv_files = list(tmp_path.glob("IC256_42x35-*.csv"))
        assert len(csv_files) > 0, "CSV file should be created"
        
        csv_file = csv_files[0]
        
        # Verify CSV format
        with open(csv_file, 'r', newline='', encoding='utf-8-sig') as f:
            reader = csv.reader(f)
            
            # Skip metadata lines if present
            header = None
            for row in reader:
                if row and not row[0].startswith('#'):
                    header = row
                    break
            
            assert header is not None, "CSV should have a header row"
            assert len(header) > 5, f"CSV should have multiple columns. Got {len(header)}"
            
            # Verify all data rows have same number of columns as header
            row_count = 0
            for row in reader:
                if row and not row[0].startswith('#'):
                    assert len(row) == len(header), \
                        f"Row {row_count + 1} should have {len(header)} columns. Got {len(row)} columns."
                    row_count += 1
                    if row_count >= 100:  # Check first 100 rows
                        break
            
            assert row_count > 0, "CSV should have data rows"


# Simplified pipeline tests - keeping only the most important ones
class TestDataCollectionPipeline:
    """Integration tests to isolate where data collection stops."""
    
    @pytest.mark.integration
    def test_iodatabase_receives_data_from_device_manager(self, require_ic256_device):
        """Test that DeviceManager collects data into IODatabase."""
        from ic256_sampler.device_manager import DeviceManager, IC256_CONFIG
        import threading
        
        device_manager = DeviceManager()
        stop_event = threading.Event()
        device_manager.stop_event = stop_event
        
        assert device_manager.add_device(IC256_CONFIG, require_ic256_device, sampling_rate=500)
        device_manager.start()
        time.sleep(2.0)
        
        stats = device_manager.get_io_database().get_statistics()
        total_points = stats.get('total_data_points', 0)
        
        assert total_points > 100, \
            f"IODatabase should have collected data. Got {total_points} points."
        
        stop_event.set()
        device_manager.stop()
        
        # Verify data persisted
        final_stats = device_manager.get_io_database().get_statistics()
        assert final_stats.get('total_data_points', 0) == total_points

    @pytest.mark.integration
    def test_application_iodatabase_collects_data_after_clear(self, require_ic256_device, app_with_mock_gui):
        """Test that IODatabase collects data after clear_database() is called."""
        app = app_with_mock_gui
        app._ensure_connections()
        
        # Simulate what _device_thread does: clear database and start
        app.device_manager.stop_event = app.stop_event
        app.device_manager.stop()
        app.device_manager.clear_database()
        app.device_manager.start()
        
        time.sleep(2.0)
        
        io_stats = app.device_manager.get_io_database().get_statistics()
        total_points = io_stats.get('total_data_points', 0)
        
        assert total_points > 100, \
            f"IODatabase should collect data after clear. Got {total_points} points."
        
        app.stop_event.set()
        app.device_manager.stop()
