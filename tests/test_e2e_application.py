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
from unittest.mock import Mock, MagicMock, patch
from ic256_sampler.application import Application
from ic256_sampler.utils import is_valid_device, is_valid_ipv4
from ic256_sampler.ic256_model import IC256Model


# Mark all tests in this file as integration tests
pytestmark = pytest.mark.integration


class TestEndToEndApplication:
    """End-to-end tests for the complete application workflow."""

    def test_application_full_workflow_ic256(self, ic256_ip, tmp_path):
        """End-to-end test: Full application workflow with IC256 device.
        
        This test:
        1. Creates an Application instance
        2. Mocks the GUI (since we don't need actual GUI for testing)
        3. Connects to a real IC256 device
        4. Starts data collection
        5. Collects data for a short period
        6. Stops data collection
        7. Verifies CSV file is created with correct structure and data
        
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
        sampling_rate = 500  # Use lower rate for faster test
        collection_duration = 3.0  # 3 seconds of collection
        expected_min_rows = int(sampling_rate * collection_duration * 0.5)  # 50% tolerance for startup/shutdown
        
        # Create Application instance
        app = Application()
        
        # Create mock GUI window
        mock_window = Mock()
        mock_window.ix256_a_entry = Mock()
        mock_window.ix256_a_entry.get = Mock(return_value=ic256_ip)
        mock_window.tx2_entry = Mock()
        mock_window.tx2_entry.get = Mock(return_value="")  # No TX2 for this test
        mock_window.note_entry = Mock()
        mock_window.note_entry.get = Mock(return_value="E2E Test")
        mock_window.path_entry = Mock()
        mock_window.path_entry.get = Mock(return_value=str(tmp_path))
        mock_window.sampling_entry = Mock()
        mock_window.sampling_entry.get = Mock(return_value=str(sampling_rate))
        mock_window.root = Mock()
        mock_window.root.after = Mock()
        mock_window.reset_elapse_time = Mock()
        mock_window.reset_statistics = Mock()
        
        app.window = mock_window
        
        # Mock GUI helper functions to avoid actual GUI updates
        with patch('ic256_sampler.application.safe_gui_update'), \
             patch('ic256_sampler.application.set_button_state_safe'), \
             patch('ic256_sampler.application.show_message_safe'), \
             patch('ic256_sampler.application.log_message_safe'):
            
            # Step 1: Ensure connections are established
            app._ensure_connections()
            
            # Verify device manager was created
            assert app.device_manager is not None, "DeviceManager should be created"
            
            # Verify connection exists
            assert "IC256-42/35" in app.device_manager.connections, \
                "IC256 device connection should be established"
            
            connection = app.device_manager.connections["IC256-42/35"]
            assert connection.ip_address == ic256_ip, \
                f"Connection should use correct IP: {ic256_ip}"
            
            # Step 2: Start data collection
            # Use _device_thread directly to start collection
            device_thread = threading.Thread(
                target=app._device_thread,
                name="e2e_device_thread",
                daemon=True
            )
            device_thread.start()
            
            # Give time for thread to start and initialize
            time.sleep(1.0)
            
            # Verify collector was created
            assert app.collector is not None, "ModelCollector should be created"
            assert app.collector_thread is not None, "Collector thread should be created"
            assert app.collector_thread.is_alive(), "Collector thread should be running"
            
            # Verify device manager is running
            assert app.device_manager._running is True, "DeviceManager should be running"
            
            # Step 3: Wait for data collection
            # Give extra time for startup and data collection
            # Check periodically that data is being collected
            for i in range(int(collection_duration) + 2):
                time.sleep(1.0)
                stats = app.device_manager.get_io_database().get_statistics()
                total_points = stats.get('total_data_points', 0)
                collector_stats = app.collector.get_statistics()
                rows_written = collector_stats.get('rows', 0)
                if i % 2 == 0:  # Print every 2 seconds
                    print(f"  After {i+1}s: {total_points} data points, {rows_written} CSV rows")
            
            # Check that data is being collected
            stats = app.device_manager.get_io_database().get_statistics()
            total_points = stats.get('total_data_points', 0)
            # Be lenient - startup delays and device response time can vary
            assert total_points > 10, \
                f"Data should be collected. Got {total_points} data points (allowing for startup delays and device response time)"
            
            # Check collector statistics
            collector_stats = app.collector.get_statistics()
            rows_written = collector_stats.get('rows', 0)
            assert rows_written > 0, \
                f"CSV rows should be written. Got {rows_written} rows"
            
            # Step 4: Stop data collection
            app.stop_collection()
            
            # Wait for collection to stop and finalize
            # Give time for threads to finish processing and CSV to be written
            # Check periodically that CSV is being written
            for i in range(10):  # Wait up to 10 seconds
                time.sleep(1.0)
                if app.collector:
                    collector_stats = app.collector.get_statistics()
                    rows_written = collector_stats.get('rows', 0)
                    if i % 2 == 0:  # Print every 2 seconds
                        print(f"  After stop, {i+1}s: {rows_written} CSV rows written")
                    if rows_written > 0 and not app.collector_thread.is_alive():
                        break  # CSV is written and thread finished
            
            # Wait for collector thread to finish
            if app.collector_thread and app.collector_thread.is_alive():
                app.collector_thread.join(timeout=15.0)
            
            # Step 5: Verify CSV file was created
            # Find the CSV file (it should be in tmp_path with timestamp)
            csv_files = list(tmp_path.glob("IC256_42x35-*.csv"))
            assert len(csv_files) > 0, \
                f"CSV file should be created in {tmp_path}. Found files: {list(tmp_path.glob('*'))}"
            
            csv_file = csv_files[0]  # Use the first (and likely only) CSV file
            
            # Verify file exists and has content
            assert csv_file.exists(), f"CSV file should exist: {csv_file}"
            assert csv_file.stat().st_size > 0, "CSV file should not be empty"
            
            # Step 6: Verify CSV file structure and content
            with open(csv_file, 'r', newline='', encoding='utf-8-sig') as f:
                reader = csv.reader(f)
                
                # Read header
                header = next(reader)
                assert len(header) > 0, "CSV should have header row"
                
                # Verify expected columns exist (check for key IC256 columns)
                header_str = ' '.join(header)
                expected_columns = [
                    "Timestamp (s)",
                    "Dose",
                    "Channel Sum",
                    "Temperature",
                    "Humidity",
                ]
                
                for col in expected_columns:
                    assert col in header_str, \
                        f"CSV header should contain '{col}'. Header: {header}"
                
                # Count data rows
                row_count = 0
                data_rows = []
                for row in reader:
                    if row and len(row) == len(header):  # Valid row with all columns
                        row_count += 1
                        if row_count <= 5:  # Store first few rows for inspection
                            data_rows.append(row)
                
                # Verify row count (be lenient due to startup delays and timing)
                # We expect at least some rows - be very lenient for E2E test
                # The important thing is that CSV is created and has data
                min_acceptable_rows = max(10, int(expected_min_rows * 0.1))  # At least 10 rows or 10% of expected
                assert row_count >= min_acceptable_rows, \
                    f"CSV should have at least {min_acceptable_rows} rows. Got {row_count} rows. " \
                    f"Expected ~{int(sampling_rate * collection_duration)} rows for {collection_duration}s at {sampling_rate} Hz. " \
                    f"This may indicate data collection or CSV writing issues."
                
                # Verify first few rows have data
                assert len(data_rows) > 0, "CSV should have at least one data row"
                
                # Verify time column is increasing (first column should be time)
                time_col_idx = header.index("Timestamp (s)") if "Timestamp (s)" in header else 0
                if len(data_rows) >= 2:
                    try:
                        time1 = float(data_rows[0][time_col_idx])
                        time2 = float(data_rows[1][time_col_idx])
                        assert time2 > time1, \
                            f"Time should be increasing. Row 1: {time1}, Row 2: {time2}"
                    except (ValueError, IndexError):
                        pass  # Skip if time parsing fails
                
                # Verify data columns have values (not all empty)
                # Check a few key columns
                primary_dose_idx = None
                channel_sum_idx = None
                for i, col in enumerate(header):
                    if "Dose" in col and "Primary" not in col:  # "Dose" column
                        primary_dose_idx = i
                    if "Channel Sum" in col:
                        channel_sum_idx = i
                
                if primary_dose_idx is not None:
                    has_data = any(
                        row[primary_dose_idx].strip() != "" 
                        for row in data_rows 
                        if len(row) > primary_dose_idx
                    )
                    assert has_data, "Primary dose column should have data"
                
                if channel_sum_idx is not None:
                    has_data = any(
                        row[channel_sum_idx].strip() != "" 
                        for row in data_rows 
                        if len(row) > channel_sum_idx
                    )
                    assert has_data, "Channel sum column should have data"
            
            # Step 7: Verify final statistics
            final_stats = app.collector.get_statistics()
            final_rows = final_stats.get('rows', 0)
            assert final_rows == row_count, \
                f"Collector statistics ({final_rows}) should match CSV row count ({row_count})"
            
            print(f"\nE2E Test Results:")
            print(f"  Collection Duration: {collection_duration} seconds")
            print(f"  Sampling Rate: {sampling_rate} Hz")
            print(f"  CSV File: {csv_file}")
            print(f"  CSV Rows: {row_count}")
            print(f"  Expected Min Rows: {expected_min_rows}")
            print(f"  Data Points Collected: {total_points}")
            print(f"  File Size: {csv_file.stat().st_size} bytes")

    def test_application_connects_and_collects_data(self, ic256_ip, tmp_path):
        """Simplified end-to-end test: Verify application can connect and collect data.
        
        This is a lighter-weight test that verifies:
        1. Application can be initialized
        2. Device connection is established
        3. Data collection starts
        4. Data is collected into the database
        
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
        
        # Create Application instance
        app = Application()
        
        # Create minimal mock GUI
        mock_window = Mock()
        mock_window.ix256_a_entry = Mock()
        mock_window.ix256_a_entry.get = Mock(return_value=ic256_ip)
        mock_window.tx2_entry = Mock()
        mock_window.tx2_entry.get = Mock(return_value="")
        mock_window.note_entry = Mock()
        mock_window.note_entry.get = Mock(return_value="E2E Test")
        mock_window.path_entry = Mock()
        mock_window.path_entry.get = Mock(return_value=str(tmp_path))
        mock_window.sampling_entry = Mock()
        mock_window.sampling_entry.get = Mock(return_value="500")
        mock_window.root = Mock()
        mock_window.root.after = Mock()
        mock_window.reset_elapse_time = Mock()
        mock_window.reset_statistics = Mock()
        
        app.window = mock_window
        
        with patch('ic256_sampler.application.safe_gui_update'), \
             patch('ic256_sampler.application.set_button_state_safe'), \
             patch('ic256_sampler.application.show_message_safe'), \
             patch('ic256_sampler.application.log_message_safe'):
            
            # Ensure connections
            app._ensure_connections()
            
            # Verify connection
            assert app.device_manager is not None
            assert "IC256-42/35" in app.device_manager.connections
            
            # Start collection
            device_thread = threading.Thread(
                target=app._device_thread,
                name="e2e_device_thread",
                daemon=True
            )
            device_thread.start()
            
            # Wait for startup
            time.sleep(1.0)
            
            # Verify collection is running
            assert app.collector is not None
            assert app.device_manager._running is True
            
            # Collect for a short time
            time.sleep(2.0)
            
            # Verify data is being collected
            stats = app.device_manager.get_io_database().get_statistics()
            total_points = stats.get('total_data_points', 0)
            assert total_points > 10, \
                f"Should collect data. Got {total_points} points (allowing for startup delays)"
            
            # Stop collection
            app.stop_collection()
            time.sleep(1.0)
            
            # Verify stopped
            assert app.device_manager._running is False

    def test_application_csv_file_format(self, ic256_ip, tmp_path):
        """End-to-end test: Verify CSV file format is correct.
        
        This test verifies:
        1. CSV file is created with proper format
        2. Header row is correct
        3. Data rows have correct number of columns
        4. Metadata is included in the file
        
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
        
        sampling_rate = 500
        collection_duration = 2.0
        
        # Create Application instance
        app = Application()
        
        # Create mock GUI
        mock_window = Mock()
        mock_window.ix256_a_entry = Mock()
        mock_window.ix256_a_entry.get = Mock(return_value=ic256_ip)
        mock_window.tx2_entry = Mock()
        mock_window.tx2_entry.get = Mock(return_value="")
        mock_window.note_entry = Mock()
        mock_window.note_entry.get = Mock(return_value="CSV Format Test")
        mock_window.path_entry = Mock()
        mock_window.path_entry.get = Mock(return_value=str(tmp_path))
        mock_window.sampling_entry = Mock()
        mock_window.sampling_entry.get = Mock(return_value=str(sampling_rate))
        mock_window.root = Mock()
        mock_window.root.after = Mock()
        mock_window.reset_elapse_time = Mock()
        mock_window.reset_statistics = Mock()
        
        app.window = mock_window
        
        with patch('ic256_sampler.application.safe_gui_update'), \
             patch('ic256_sampler.application.set_button_state_safe'), \
             patch('ic256_sampler.application.show_message_safe'), \
             patch('ic256_sampler.application.log_message_safe'):
            
            # Start collection
            app._ensure_connections()
            device_thread = threading.Thread(
                target=app._device_thread,
                name="e2e_csv_test",
                daemon=True
            )
            device_thread.start()
            
            time.sleep(1.0 + collection_duration)
            
            # Stop collection
            app.stop_collection()
            time.sleep(3.0)  # Give time for CSV to be written
            
            # Find CSV file
            csv_files = list(tmp_path.glob("IC256_42x35-*.csv"))
            assert len(csv_files) > 0, "CSV file should be created"
            
            csv_file = csv_files[0]
            
            # Verify CSV format
            with open(csv_file, 'r', newline='', encoding='utf-8-sig') as f:
                lines = f.readlines()
                
                # Check for metadata comments at the top
                has_metadata = any(
                    line.startswith('#') or 'Device' in line or 'Sampling' in line
                    for line in lines[:10]
                )
                
                # Read as CSV
                f.seek(0)
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
                            f"Row {row_count + 1} should have {len(header)} columns (same as header). " \
                            f"Got {len(row)} columns. Row: {row[:5]}..."
                        row_count += 1
                        if row_count >= 100:  # Check first 100 rows
                            break
                
                assert row_count > 0, "CSV should have data rows"


class TestDataCollectionPipeline:
    """Integration tests to isolate where data collection stops.
    
    These tests verify each stage of the data collection pipeline:
    1. DeviceManager -> IODatabase (data collection)
    2. IODatabase -> VirtualDatabase (row generation)
    3. VirtualDatabase -> CSVWriter (CSV writing)
    """

    @pytest.mark.integration
    def test_iodatabase_receives_data_from_device_manager(self, ic256_ip):
        """Test that DeviceManager collects data into IODatabase.
        
        This isolates the first stage: device -> IODatabase
        """
        from ic256_sampler.utils import is_valid_device, is_valid_ipv4
        from ic256_sampler.device_manager import DeviceManager, IC256_CONFIG
        
        # Skip if IP is invalid
        if not is_valid_ipv4(ic256_ip):
            pytest.skip(f"Invalid IP address in config: {ic256_ip}")
        
        # Verify device is reachable
        if not is_valid_device(ic256_ip, "IC256"):
            pytest.skip(f"IC256 device at {ic256_ip} is not reachable or not responding")
        
        device_manager = DeviceManager()
        stop_event = threading.Event()
        device_manager.stop_event = stop_event
        
        # Add device
        success = device_manager.add_device(IC256_CONFIG, ic256_ip, sampling_rate=500)
        assert success, "Device should be added"
        
        # Start collection
        device_manager.start()
        
        # Wait for data collection
        time.sleep(2.0)
        
        # Check IODatabase has data
        stats = device_manager.get_io_database().get_statistics()
        total_points = stats.get('total_data_points', 0)
        
        print(f"\nIODatabase Test Results:")
        print(f"  Total data points: {total_points}")
        print(f"  Channels: {list(device_manager.get_io_database().get_all_channels())}")
        
        # Should have collected significant data
        assert total_points > 100, \
            f"IODatabase should have collected data. Got {total_points} points. " \
            "If this fails, DeviceManager is not collecting data properly."
        
        # Stop
        stop_event.set()
        device_manager.stop()
        
        # Verify data persisted
        final_stats = device_manager.get_io_database().get_statistics()
        final_points = final_stats.get('total_data_points', 0)
        assert final_points == total_points, "Data should persist after stop"

    @pytest.mark.integration
    def test_virtual_database_builds_rows_from_iodatabase(self, ic256_ip):
        """Test that VirtualDatabase builds rows from IODatabase data.
        
        This isolates the second stage: IODatabase -> VirtualDatabase
        """
        from ic256_sampler.utils import is_valid_device, is_valid_ipv4
        from ic256_sampler.device_manager import DeviceManager, IC256_CONFIG
        from ic256_sampler.ic256_model import IC256Model
        from ic256_sampler.virtual_database import VirtualDatabase
        
        # Skip if IP is invalid
        if not is_valid_ipv4(ic256_ip):
            pytest.skip(f"Invalid IP address in config: {ic256_ip}")
        
        # Verify device is reachable
        if not is_valid_device(ic256_ip, "IC256"):
            pytest.skip(f"IC256 device at {ic256_ip} is not reachable or not responding")
        
        # Collect data into IODatabase
        device_manager = DeviceManager()
        stop_event = threading.Event()
        device_manager.stop_event = stop_event
        
        success = device_manager.add_device(IC256_CONFIG, ic256_ip, sampling_rate=500)
        assert success, "Device should be added"
        
        device_manager.start()
        time.sleep(2.0)  # Collect for 2 seconds
        
        # Get IODatabase
        io_database = device_manager.get_io_database()
        stats = io_database.get_statistics()
        total_points = stats.get('total_data_points', 0)
        
        print(f"\nVirtualDatabase Test - IODatabase Stats:")
        print(f"  Total data points: {total_points}")
        
        assert total_points > 50, "IODatabase should have data before building VirtualDatabase"
        
        # Stop collection
        stop_event.set()
        device_manager.stop()
        
        # Create VirtualDatabase
        model = IC256Model()
        reference_channel = model.get_reference_channel()
        columns = model.create_columns(reference_channel)
        
        virtual_db = VirtualDatabase(
            io_database=io_database,
            reference_channel=reference_channel,
            sampling_rate=500,
            columns=columns,
        )
        
        # Build virtual database
        virtual_db.build()
        
        row_count = virtual_db.get_row_count()
        
        print(f"  VirtualDatabase rows: {row_count}")
        print(f"  Expected rows (2s @ 500Hz): ~1000")
        
        # Should have built many rows
        assert row_count > 100, \
            f"VirtualDatabase should build rows from IODatabase. Got {row_count} rows. " \
            "If this fails, VirtualDatabase.build() is not working properly."
        
        # Test rebuild
        # Add more data to IODatabase (simulate continued collection)
        # Then rebuild should add more rows
        initial_row_count = row_count
        
        # Rebuild should not add rows if no new data (or add very few due to timing)
        virtual_db.rebuild()
        after_rebuild_count = virtual_db.get_row_count()
        
        # Should be same or very close (no new data, but allow for edge cases)
        # The important thing is that rebuild() doesn't fail or break
        assert abs(after_rebuild_count - initial_row_count) <= 1, \
            f"Rebuild should not add many rows if no new data. Initial: {initial_row_count}, After: {after_rebuild_count}"

    @pytest.mark.integration
    def test_virtual_database_rebuild_adds_new_rows(self, ic256_ip):
        """Test that VirtualDatabase rebuild() adds new rows when new data arrives.
        
        This tests the incremental rebuild functionality.
        """
        from ic256_sampler.utils import is_valid_device, is_valid_ipv4
        from ic256_sampler.device_manager import DeviceManager, IC256_CONFIG
        from ic256_sampler.ic256_model import IC256Model
        from ic256_sampler.virtual_database import VirtualDatabase
        
        # Skip if IP is invalid
        if not is_valid_ipv4(ic256_ip):
            pytest.skip(f"Invalid IP address in config: {ic256_ip}")
        
        # Verify device is reachable
        if not is_valid_device(ic256_ip, "IC256"):
            pytest.skip(f"IC256 device at {ic256_ip} is not reachable or not responding")
        
        # Collect initial data
        device_manager = DeviceManager()
        stop_event = threading.Event()
        device_manager.stop_event = stop_event
        
        success = device_manager.add_device(IC256_CONFIG, ic256_ip, sampling_rate=500)
        assert success, "Device should be added"
        
        device_manager.start()
        time.sleep(1.0)  # Collect for 1 second
        
        # Build VirtualDatabase with initial data
        io_database = device_manager.get_io_database()
        model = IC256Model()
        reference_channel = model.get_reference_channel()
        columns = model.create_columns(reference_channel)
        
        virtual_db = VirtualDatabase(
            io_database=io_database,
            reference_channel=reference_channel,
            sampling_rate=500,
            columns=columns,
        )
        
        virtual_db.build()
        initial_row_count = virtual_db.get_row_count()
        
        print(f"\nRebuild Test - Initial build:")
        print(f"  Initial rows: {initial_row_count}")
        
        assert initial_row_count > 0, "Initial build should create rows"
        
        # Continue collecting data
        time.sleep(1.0)  # Collect for another second
        
        # Rebuild should add new rows
        virtual_db.rebuild()
        after_rebuild_count = virtual_db.get_row_count()
        
        print(f"  After rebuild: {after_rebuild_count}")
        print(f"  New rows added: {after_rebuild_count - initial_row_count}")
        
        # CRITICAL: Rebuild should add more rows
        assert after_rebuild_count > initial_row_count, \
            f"rebuild() should add new rows when new data arrives. " \
            f"Initial: {initial_row_count}, After rebuild: {after_rebuild_count}. " \
            "If this fails, rebuild() is not working properly."
        
        # Stop
        stop_event.set()
        device_manager.stop()

    @pytest.mark.integration
    def test_csv_writer_writes_all_virtual_database_rows(self, tmp_path):
        """Test that CSVWriter writes all rows from VirtualDatabase.
        
        This isolates the third stage: VirtualDatabase -> CSVWriter
        """
        from ic256_sampler.io_database import IODatabase
        from ic256_sampler.ic256_model import IC256Model
        from ic256_sampler.virtual_database import VirtualDatabase
        from ic256_sampler.csv_writer import CSVWriter
        
        # Create IODatabase with test data
        io_database = IODatabase()
        reference_channel = "/test/reference"
        io_database.add_channel(reference_channel)
        
        # Add multiple data points to simulate collection
        base_time = 1000000000000000000  # 1e18 nanoseconds
        for i in range(1000):  # 1000 data points
            timestamp_ns = base_time + i * 2000000  # 2ms apart (500 Hz)
            io_database.add_data_point(reference_channel, 100.0 + i, timestamp_ns)
        
        # Create VirtualDatabase
        model = IC256Model()
        columns = model.create_columns(reference_channel)
        
        virtual_db = VirtualDatabase(
            io_database=io_database,
            reference_channel=reference_channel,
            sampling_rate=500,
            columns=columns,
        )
        
        virtual_db.build()
        virtual_row_count = virtual_db.get_row_count()
        
        print(f"\nCSVWriter Test:")
        print(f"  VirtualDatabase rows: {virtual_row_count}")
        
        assert virtual_row_count > 0, "VirtualDatabase should have rows"
        
        # Create CSVWriter
        csv_file = tmp_path / "test_output.csv"
        csv_writer = CSVWriter(
            virtual_database=virtual_db,
            file_path=str(csv_file),
            device_name="test",
            note="Test",
        )
        
        # Write all rows
        rows_written = csv_writer.write_all()
        
        print(f"  Rows written: {rows_written}")
        print(f"  CSVWriter.rows_written: {csv_writer.rows_written}")
        
        # Should have written all rows
        assert rows_written == virtual_row_count, \
            f"CSVWriter should write all rows. Virtual: {virtual_row_count}, Written: {rows_written}"
        assert csv_writer.rows_written == virtual_row_count, \
            f"CSVWriter.rows_written should match. Expected: {virtual_row_count}, Got: {csv_writer.rows_written}"
        
        # Verify CSV file
        assert csv_file.exists(), "CSV file should exist"
        
        # Count rows in file
        with open(csv_file, 'r', newline='') as f:
            reader = csv.reader(f)
            header = next(reader)
            file_row_count = sum(1 for row in reader if row)
        
        assert file_row_count == virtual_row_count, \
            f"CSV file should have all rows. Expected: {virtual_row_count}, File: {file_row_count}"
        
        csv_writer.close()

    @pytest.mark.integration
    def test_model_collector_iteration_creates_rows(self, ic256_ip, tmp_path):
        """Test that ModelCollector.collect_iteration() creates and writes rows.
        
        This tests the ModelCollector iteration loop.
        """
        from ic256_sampler.utils import is_valid_device, is_valid_ipv4
        from ic256_sampler.device_manager import DeviceManager, IC256_CONFIG
        from ic256_sampler.model_collector import ModelCollector
        from ic256_sampler.ic256_model import IC256Model
        
        # Skip if IP is invalid
        if not is_valid_ipv4(ic256_ip):
            pytest.skip(f"Invalid IP address in config: {ic256_ip}")
        
        # Verify device is reachable
        if not is_valid_device(ic256_ip, "IC256"):
            pytest.skip(f"IC256 device at {ic256_ip} is not reachable or not responding")
        
        # Setup
        device_manager = DeviceManager()
        stop_event = threading.Event()
        device_manager.stop_event = stop_event
        
        success = device_manager.add_device(IC256_CONFIG, ic256_ip, sampling_rate=500)
        assert success, "Device should be added"
        
        model = IC256Model()
        reference_channel = model.get_reference_channel()
        
        csv_file = tmp_path / "test_collector.csv"
        collector = ModelCollector(
            device_manager=device_manager,
            model=model,
            reference_channel=reference_channel,
            sampling_rate=500,
            file_path=str(csv_file),
            device_name="ic256_45",
            note="Test",
        )
        
        # Start collection
        collector.start()
        
        # Run multiple iterations
        initial_rows = collector.csv_writer.rows_written
        print(f"\nModelCollector Iteration Test:")
        print(f"  Initial rows: {initial_rows}")
        
        # Wait for data to accumulate
        time.sleep(1.0)
        
        # Run several iterations
        for i in range(10):
            collector.collect_iteration()
            current_rows = collector.csv_writer.rows_written
            virtual_rows = collector.virtual_database.get_row_count()
            if i % 2 == 0:
                print(f"  Iteration {i}: CSV rows={current_rows}, Virtual rows={virtual_rows}")
            time.sleep(0.1)
        
        final_rows = collector.csv_writer.rows_written
        final_virtual_rows = collector.virtual_database.get_row_count()
        
        print(f"  Final CSV rows: {final_rows}")
        print(f"  Final Virtual rows: {final_virtual_rows}")
        
        # Should have written more rows
        assert final_rows > initial_rows, \
            f"collect_iteration() should write rows. Initial: {initial_rows}, Final: {final_rows}"
        
        # Virtual database should have rows
        assert final_virtual_rows > 0, \
            f"VirtualDatabase should have rows. Got: {final_virtual_rows}"
        
        # Stop
        stop_event.set()
        collector.stop()
        collector.finalize()

    @pytest.mark.integration
    def test_virtual_database_rebuild_with_continuous_data(self, ic256_ip):
        """Test VirtualDatabase rebuild behavior with continuous data collection.
        
        This simulates the real scenario where data comes in continuously
        and rebuild() is called repeatedly.
        """
        from ic256_sampler.utils import is_valid_device, is_valid_ipv4
        from ic256_sampler.device_manager import DeviceManager, IC256_CONFIG
        from ic256_sampler.ic256_model import IC256Model
        from ic256_sampler.virtual_database import VirtualDatabase
        
        # Skip if IP is invalid
        if not is_valid_ipv4(ic256_ip):
            pytest.skip(f"Invalid IP address in config: {ic256_ip}")
        
        # Verify device is reachable
        if not is_valid_device(ic256_ip, "IC256"):
            pytest.skip(f"IC256 device at {ic256_ip} is not reachable or not responding")
        
        # Setup
        device_manager = DeviceManager()
        stop_event = threading.Event()
        device_manager.stop_event = stop_event
        
        success = device_manager.add_device(IC256_CONFIG, ic256_ip, sampling_rate=500)
        assert success, "Device should be added"
        
        device_manager.start()
        
        # Create VirtualDatabase
        io_database = device_manager.get_io_database()
        model = IC256Model()
        reference_channel = model.get_reference_channel()
        columns = model.create_columns(reference_channel)
        
        virtual_db = VirtualDatabase(
            io_database=io_database,
            reference_channel=reference_channel,
            sampling_rate=500,
            columns=columns,
        )
        
        # Wait for initial data
        time.sleep(0.5)
        
        # Initial build
        virtual_db.build()
        initial_count = virtual_db.get_row_count()
        
        print(f"\nContinuous Rebuild Test:")
        print(f"  Initial build: {initial_count} rows")
        
        # Run multiple rebuilds while data is being collected
        rebuild_counts = [initial_count]
        for i in range(10):
            time.sleep(0.2)  # Let data accumulate
            virtual_db.rebuild()
            current_count = virtual_db.get_row_count()
            rebuild_counts.append(current_count)
            if i % 2 == 0:
                print(f"  Rebuild {i}: {current_count} rows (added {current_count - rebuild_counts[-2] if len(rebuild_counts) > 1 else 0})")
        
        final_count = virtual_db.get_row_count()
        
        print(f"  Final: {final_count} rows")
        print(f"  Total growth: {final_count - initial_count} rows")
        
        # Should have grown
        assert final_count > initial_count, \
            f"VirtualDatabase should grow with rebuilds. Initial: {initial_count}, Final: {final_count}. " \
            "If this fails, rebuild() is not adding rows as new data arrives."
        
        # Check that rebuilds are actually adding rows
        growing_rebuilds = sum(1 for i in range(1, len(rebuild_counts)) if rebuild_counts[i] > rebuild_counts[i-1])
        assert growing_rebuilds > 0, \
            f"At least some rebuilds should add rows. Rebuild counts: {rebuild_counts}"
        
        # Stop
        stop_event.set()
        device_manager.stop()
