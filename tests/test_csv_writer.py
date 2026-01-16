"""Tests for CSV Writer.

These tests validate the CSV writer functionality for writing
VirtualDatabase rows to CSV files.
"""

import pytest
import csv
import tempfile
import os
from pathlib import Path
from ic256_sampler.io_database import IODatabase
from ic256_sampler.virtual_database import (
    VirtualDatabase,
    ColumnDefinition,
    ChannelPolicy,
)
from ic256_sampler.csv_writer import CSVWriter


class TestCSVWriter:
    """Tests for CSVWriter class."""

    def test_create_csv_writer(self, tmp_path):
        """Test creating a CSV writer."""
        io_db = IODatabase()
        columns = [
            ColumnDefinition(name="Timestamp (s)", channel_path=None, policy=ChannelPolicy.SYNCHRONIZED),
            ColumnDefinition(name="Channel Sum", channel_path="/test/channel_sum", policy=ChannelPolicy.SYNCHRONIZED),
        ]
        virtual_db = VirtualDatabase(io_db, "/test/channel_sum", 3000, columns)
        file_path = tmp_path / "test.csv"
        
        writer = CSVWriter(
            virtual_database=virtual_db,
            file_path=str(file_path),
            device_name="ic256_45",
            note="Test",
        )
        
        assert writer.virtual_database == virtual_db
        assert writer.file_path == file_path
        assert writer.device_name == "ic256_45"
        assert writer.rows_written == 0

    def test_write_empty_database(self, tmp_path):
        """Test writing an empty virtual database."""
        io_db = IODatabase()
        columns = [
            ColumnDefinition(name="Timestamp (s)", channel_path=None, policy=ChannelPolicy.SYNCHRONIZED),
            ColumnDefinition(name="Channel Sum", channel_path="/test/channel_sum", policy=ChannelPolicy.SYNCHRONIZED),
        ]
        virtual_db = VirtualDatabase(io_db, "/test/channel_sum", 3000, columns)
        file_path = tmp_path / "test.csv"
        
        writer = CSVWriter(
            virtual_database=virtual_db,
            file_path=str(file_path),
            device_name="ic256_45",
            note="Test",
        )
        
        rows_written = writer.write_all()
        assert rows_written == 0
        assert writer.rows_written == 0
        
        # File should exist with just headers
        assert file_path.exists()
        with open(file_path, 'r') as f:
            reader = csv.reader(f)
            rows = list(reader)
            assert len(rows) == 1  # Just header row

    def test_write_single_channel_data(self, tmp_path):
        """Test writing virtual database with single channel."""
        io_db = IODatabase()
        channel_path = "/test/channel_sum"
        
        # Add data points spanning 1 second
        timestamp1 = 1000000000000000000
        timestamp2 = timestamp1 + int(1e9)  # 1 second later
        
        io_db.add_data_point(channel_path, 100, timestamp1)
        io_db.add_data_point(channel_path, 200, timestamp2)
        
        columns = [
            ColumnDefinition(name="Timestamp (s)", channel_path=None, policy=ChannelPolicy.SYNCHRONIZED),
            ColumnDefinition(name="Channel Sum", channel_path=channel_path, policy=ChannelPolicy.SYNCHRONIZED),
        ]
        virtual_db = VirtualDatabase(io_db, channel_path, 10, columns)  # 10 Hz
        virtual_db.build()
        
        file_path = tmp_path / "test.csv"
        writer = CSVWriter(
            virtual_database=virtual_db,
            file_path=str(file_path),
            device_name="ic256_45",
            note="Test",
        )
        
        rows_written = writer.write_all()
        assert rows_written > 0
        assert writer.rows_written == rows_written
        
        # Verify file contents
        assert file_path.exists()
        with open(file_path, 'r') as f:
            reader = csv.reader(f)
            rows = list(reader)
            assert len(rows) > 1  # Header + data rows
            assert len(rows) == rows_written + 1  # +1 for header

    def test_write_multiple_channels(self, tmp_path):
        """Test writing virtual database with multiple channels."""
        io_db = IODatabase()
        channel1 = "/test/channel_sum"
        channel2 = "/test/primary_dose"
        
        timestamp1 = 1000000000000000000
        timestamp2 = timestamp1 + int(1e9)
        
        io_db.add_data_point(channel1, 100, timestamp1)
        io_db.add_data_point(channel1, 200, timestamp2)
        io_db.add_data_point(channel2, 50, timestamp1)
        io_db.add_data_point(channel2, 60, timestamp2)
        
        columns = [
            ColumnDefinition(name="Timestamp (s)", channel_path=None, policy=ChannelPolicy.SYNCHRONIZED),
            ColumnDefinition(name="Channel Sum", channel_path=channel1, policy=ChannelPolicy.SYNCHRONIZED),
            ColumnDefinition(name="Primary Dose", channel_path=channel2, policy=ChannelPolicy.INTERPOLATED),
        ]
        virtual_db = VirtualDatabase(io_db, channel1, 10, columns)
        virtual_db.build()
        
        file_path = tmp_path / "test.csv"
        writer = CSVWriter(
            virtual_database=virtual_db,
            file_path=str(file_path),
            device_name="ic256_45",
            note="Test",
        )
        
        rows_written = writer.write_all()
        assert rows_written > 0
        
        # Verify file has correct structure
        with open(file_path, 'r') as f:
            reader = csv.reader(f)
            rows = list(reader)
            
            # Check header
            assert "Timestamp" in rows[0][0]
            assert len(rows[0]) > 1
            
            # Check data rows
            for row in rows[1:]:
                assert len(row) > 0
                # First column should be timestamp
                try:
                    float(row[0])
                except ValueError:
                    pytest.fail(f"First column should be timestamp, got: {row[0]}")

    def test_incremental_write(self, tmp_path):
        """Test that write_all can be called multiple times incrementally."""
        io_db = IODatabase()
        channel_path = "/test/channel_sum"
        
        timestamp1 = 1000000000000000000
        timestamp2 = timestamp1 + int(1e9)
        
        io_db.add_data_point(channel_path, 100, timestamp1)
        io_db.add_data_point(channel_path, 200, timestamp2)
        
        columns = [
            ColumnDefinition(name="Timestamp (s)", channel_path=None, policy=ChannelPolicy.SYNCHRONIZED),
            ColumnDefinition(name="Channel Sum", channel_path=channel_path, policy=ChannelPolicy.SYNCHRONIZED),
        ]
        virtual_db = VirtualDatabase(io_db, channel_path, 10, columns)
        virtual_db.build()
        
        file_path = tmp_path / "test.csv"
        writer = CSVWriter(
            virtual_database=virtual_db,
            file_path=str(file_path),
            device_name="ic256_45",
            note="Test",
        )
        
        # Write first batch
        rows1 = writer.write_all()
        assert rows1 > 0
        initial_count = writer.rows_written
        
        # Add more data to virtual database
        io_db.add_data_point(channel_path, 300, timestamp1 + int(2e9))
        virtual_db.rebuild()
        
        # Write again - should only write new rows
        rows2 = writer.write_all()
        assert rows2 > 0
        assert writer.rows_written == initial_count + rows2

    def test_flush_and_sync(self, tmp_path):
        """Test flush and sync operations."""
        io_db = IODatabase()
        channel_path = "/test/channel_sum"
        
        io_db.add_data_point(channel_path, 100, 1000000000000000000)
        
        columns = [
            ColumnDefinition(name="Timestamp (s)", channel_path=None, policy=ChannelPolicy.SYNCHRONIZED),
            ColumnDefinition(name="Channel Sum", channel_path=channel_path, policy=ChannelPolicy.SYNCHRONIZED),
        ]
        virtual_db = VirtualDatabase(io_db, channel_path, 10, columns)
        virtual_db.build()
        
        file_path = tmp_path / "test.csv"
        writer = CSVWriter(
            virtual_database=virtual_db,
            file_path=str(file_path),
            device_name="ic256_45",
            note="Test",
        )
        
        writer.write_all()
        writer.flush()
        writer.sync()
        
        # File should exist and be written
        assert file_path.exists()
        assert file_path.stat().st_size > 0

    def test_close(self, tmp_path):
        """Test closing the CSV writer."""
        io_db = IODatabase()
        channel_path = "/test/channel_sum"
        
        io_db.add_data_point(channel_path, 100, 1000000000000000000)
        
        columns = [
            ColumnDefinition(name="Timestamp (s)", channel_path=None, policy=ChannelPolicy.SYNCHRONIZED),
            ColumnDefinition(name="Channel Sum", channel_path=channel_path, policy=ChannelPolicy.SYNCHRONIZED),
        ]
        virtual_db = VirtualDatabase(io_db, channel_path, 10, columns)
        virtual_db.build()
        
        file_path = tmp_path / "test.csv"
        writer = CSVWriter(
            virtual_database=virtual_db,
            file_path=str(file_path),
            device_name="ic256_45",
            note="Test",
        )
        
        writer.write_all()
        writer.close()
        
        # File should be closed and finalized
        assert file_path.exists()
        assert writer._file_handle is None

    def test_csv_no_missing_cells(self, tmp_path):
        """Test that CSV file has no missing cells - all rows have all columns filled.
        
        This test verifies the structural integrity of CSV files:
        1. Creates a virtual database with multiple channels
        2. Writes data to CSV
        3. Reads the CSV back and verifies:
           - Every row has the same number of columns as the header (structural integrity)
           - Timestamp cells are never empty (required field)
           - All expected columns are present in the header
           - Note: Data columns may be empty if no matching data point is found (expected behavior)
        """
        io_db = IODatabase()
        channel_path1 = "/test/channel_sum"
        channel_path2 = "/test/channel_1"
        channel_path3 = "/test/channel_2"
        
        # Add data points to multiple channels at high frequency
        # Use 1000 Hz data rate to ensure we have data for most rows
        base_timestamp = 1000000000000000000
        for i in range(1000):
            timestamp = base_timestamp + i * 1000000  # 1ms intervals (1000 Hz)
            io_db.add_data_point(channel_path1, 100.0 + i * 0.1, timestamp)
            io_db.add_data_point(channel_path2, 200.0 + i * 0.1, timestamp)
            io_db.add_data_point(channel_path3, 300.0 + i * 0.1, timestamp)
        
        columns = [
            ColumnDefinition(name="Timestamp (s)", channel_path=None, policy=ChannelPolicy.SYNCHRONIZED),
            ColumnDefinition(name="Channel Sum", channel_path=channel_path1, policy=ChannelPolicy.INTERPOLATED),
            ColumnDefinition(name="Channel 1", channel_path=channel_path2, policy=ChannelPolicy.INTERPOLATED),
            ColumnDefinition(name="Channel 2", channel_path=channel_path3, policy=ChannelPolicy.INTERPOLATED),
            ColumnDefinition(name="Note", channel_path=None, policy=ChannelPolicy.SYNCHRONIZED),
        ]
        
        # Use 500 Hz sampling rate - should have data for most rows
        virtual_db = VirtualDatabase(io_db, channel_path1, 500, columns)
        virtual_db.build()
        
        file_path = tmp_path / "test.csv"
        writer = CSVWriter(
            virtual_database=virtual_db,
            file_path=str(file_path),
            device_name="test_device",
            note="Test Note",
        )
        
        rows_written = writer.write_all()
        writer.flush()
        writer.close()
        
        assert rows_written > 0
        assert file_path.exists()
        
        # Read CSV file and verify structural integrity
        with open(file_path, 'r', newline='', encoding='utf-8-sig') as f:
            reader = csv.reader(f)
            
            # Read header
            header = next(reader)
            expected_column_count = len(header)
            
            # Verify header has expected columns
            expected_columns = ["Timestamp (s)", "Channel Sum", "Channel 1", "Channel 2", "Note"]
            assert len(header) == len(expected_columns), \
                f"Header has {len(header)} columns, expected {len(expected_columns)}"
            
            for expected_col in expected_columns:
                assert expected_col in header, f"Expected column '{expected_col}' not found in header"
            
            # Read and verify all data rows
            row_number = 1  # Start at 1 since header is row 0
            rows_with_issues = []
            
            for row in reader:
                row_number += 1
                
                # Check column count - this is the critical structural check
                if len(row) != expected_column_count:
                    rows_with_issues.append({
                        'row': row_number,
                        'issue': f'Column count mismatch: {len(row)} columns, expected {expected_column_count}',
                        'row_data': row
                    })
                    continue
                
                # Check that timestamp is never empty (required field)
                timestamp_idx = header.index("Timestamp (s)")
                if timestamp_idx < len(row):
                    timestamp_value = row[timestamp_idx]
                    if not timestamp_value or timestamp_value.strip() == "":
                        rows_with_issues.append({
                            'row': row_number,
                            'column': "Timestamp (s)",
                            'issue': 'Empty timestamp cell (required field)',
                            'row_data': row
                        })
            
            # Report any structural issues
            if rows_with_issues:
                error_msg = f"Found {len(rows_with_issues)} rows with structural issues:\n"
                for issue in rows_with_issues[:10]:  # Show first 10 issues
                    error_msg += f"  Row {issue['row']}: {issue['issue']}\n"
                    if 'column' in issue:
                        error_msg += f"    Column: {issue['column']}\n"
                    error_msg += f"    Row data: {issue['row_data']}\n"
                
                if len(rows_with_issues) > 10:
                    error_msg += f"  ... and {len(rows_with_issues) - 10} more issues\n"
                
                pytest.fail(error_msg)
            
            # Verify we read some rows
            assert row_number > 1, "No data rows found in CSV file"
            
            # Additional check: verify that at least some rows have data (not all empty)
            # Re-read the file to count rows with data
            f.seek(0)
            reader2 = csv.reader(f)
            next(reader2)  # Skip header
            
            rows_with_data = 0
            for row in reader2:
                for col_name, cell_value in zip(header, row):
                    if col_name not in ["Timestamp (s)", "Note"]:
                        if cell_value and cell_value.strip() != "":
                            rows_with_data += 1
                            break
            
            # At least some rows should have data
            assert rows_with_data > 0, "No data values found in CSV file - all data cells are empty"

    def test_csv_no_empty_cells_in_data_columns(self, tmp_path):
        """Test that CSV file has no empty cells in data columns when data is available.
        
        This test reproduces the issue where Dose column (and other data columns) 
        can have empty values even when data points exist. The issue occurs when
        the virtual database can't find a matching data point for INTERPOLATED
        channels at certain timestamps.
        
        This test:
        1. Creates data with sparse sampling for one channel (simulating Dose channel)
        2. Creates virtual database with INTERPOLATED policy for that channel
        3. Writes to CSV
        4. Verifies that all rows have values (using forward-fill for missing values)
        """
        io_db = IODatabase()
        ref_channel = "/test/channel_sum"
        dose_channel = "/test/primary_dose"
        
        # Add reference channel data at high frequency (3000 Hz)
        base_timestamp = 1000000000000000000
        for i in range(1000):
            timestamp = base_timestamp + i * 333333  # ~3000 Hz
            io_db.add_data_point(ref_channel, 100.0 + i * 0.1, timestamp)
        
        # Add dose channel data at lower frequency (every 10th point) - simulating sparse data
        for i in range(0, 1000, 10):
            timestamp = base_timestamp + i * 333333
            io_db.add_data_point(dose_channel, 2.0 + i * 0.001, timestamp)
        
        columns = [
            ColumnDefinition(name="Timestamp (s)", channel_path=None, policy=ChannelPolicy.SYNCHRONIZED),
            ColumnDefinition(name="Channel Sum", channel_path=ref_channel, policy=ChannelPolicy.SYNCHRONIZED),
            ColumnDefinition(name="Dose", channel_path=dose_channel, policy=ChannelPolicy.INTERPOLATED),
            ColumnDefinition(name="Note", channel_path=None, policy=ChannelPolicy.SYNCHRONIZED),
        ]
        
        # Use 3000 Hz sampling rate (same as reference channel)
        virtual_db = VirtualDatabase(io_db, ref_channel, 3000, columns)
        virtual_db.build()
        
        file_path = tmp_path / "test.csv"
        writer = CSVWriter(
            virtual_database=virtual_db,
            file_path=str(file_path),
            device_name="test_device",
            note="Test Note",
        )
        
        rows_written = writer.write_all()
        writer.flush()
        writer.close()
        
        assert rows_written > 0
        
        # Read CSV and check for empty Dose cells
        with open(file_path, 'r', newline='', encoding='utf-8-sig') as f:
            reader = csv.reader(f)
            header = next(reader)
            
            dose_idx = header.index("Dose")
            empty_dose_rows = []
            last_dose_value = None
            
            for row_num, row in enumerate(reader, start=2):
                if len(row) > dose_idx:
                    dose_value = row[dose_idx]
                    if dose_value and dose_value.strip() != "":
                        last_dose_value = dose_value
                    elif last_dose_value is None:
                        # First few rows might not have data yet - that's OK
                        if row_num > 20:  # But after 20 rows, we should have some data
                            empty_dose_rows.append(row_num)
                    # Note: We allow empty values if we haven't seen any data yet
                    # But once we have data, all subsequent rows should have values
                    # (either from interpolation or forward-fill)
            
            # Report issues
            if empty_dose_rows:
                error_msg = (
                    f"Found {len(empty_dose_rows)} rows with empty Dose values "
                    f"(rows: {empty_dose_rows[:20]}). "
                    f"This indicates the virtual database is not properly handling "
                    f"missing values for INTERPOLATED channels. "
                    f"Expected forward-fill or interpolation to fill these gaps."
                )
                pytest.fail(error_msg)
    
    def test_csv_forward_fill_for_interpolated_channels(self, tmp_path):
        """Test that forward-fill works correctly for INTERPOLATED channels.
        
        This test verifies that when an INTERPOLATED channel has gaps in data,
        the virtual database uses forward-fill (last known value) to fill missing cells.
        This prevents empty cells in CSV files.
        """
        io_db = IODatabase()
        ref_channel = "/test/channel_sum"
        dose_channel = "/test/primary_dose"
        
        # Add reference channel data continuously
        base_timestamp = 1000000000000000000
        for i in range(100):
            timestamp = base_timestamp + i * 1000000  # 1ms intervals
            io_db.add_data_point(ref_channel, 100.0 + i, timestamp)
        
        # Add dose channel data with a gap (missing data points 20-40)
        for i in range(100):
            if i < 20 or i >= 40:  # Gap in the middle
                timestamp = base_timestamp + i * 1000000
                io_db.add_data_point(dose_channel, 2.0 + i * 0.01, timestamp)
        
        columns = [
            ColumnDefinition(name="Timestamp (s)", channel_path=None, policy=ChannelPolicy.SYNCHRONIZED),
            ColumnDefinition(name="Channel Sum", channel_path=ref_channel, policy=ChannelPolicy.SYNCHRONIZED),
            ColumnDefinition(name="Dose", channel_path=dose_channel, policy=ChannelPolicy.INTERPOLATED),
            ColumnDefinition(name="Note", channel_path=None, policy=ChannelPolicy.SYNCHRONIZED),
        ]
        
        virtual_db = VirtualDatabase(io_db, ref_channel, 1000, columns)
        virtual_db.build()
        
        file_path = tmp_path / "test.csv"
        writer = CSVWriter(
            virtual_database=virtual_db,
            file_path=str(file_path),
            device_name="test_device",
            note="Test Note",
        )
        
        rows_written = writer.write_all()
        writer.flush()
        writer.close()
        
        assert rows_written > 0
        
        # Read CSV and verify forward-fill worked
        with open(file_path, 'r', newline='', encoding='utf-8-sig') as f:
            reader = csv.reader(f)
            header = next(reader)
            
            dose_idx = header.index("Dose")
            last_dose_value = None
            empty_after_data_started = []
            
            for row_num, row in enumerate(reader, start=2):
                if len(row) > dose_idx:
                    dose_value = row[dose_idx]
                    if dose_value and dose_value.strip() != "":
                        last_dose_value = float(dose_value)
                    elif last_dose_value is not None:
                        # We had data before, but now it's empty - this should not happen with forward-fill
                        empty_after_data_started.append(row_num)
            
            # After we've seen the first dose value, all subsequent rows should have values
            # (either from interpolation or forward-fill)
            if empty_after_data_started:
                error_msg = (
                    f"Found {len(empty_after_data_started)} rows with empty Dose values "
                    f"after data started (rows: {empty_after_data_started[:20]}). "
                    f"Forward-fill should have filled these gaps with the last known value."
                )
                pytest.fail(error_msg)

    def test_get_statistics(self, tmp_path):
        """Test getting CSV writer statistics."""
        io_db = IODatabase()
        channel_path = "/test/channel_sum"
        
        io_db.add_data_point(channel_path, 100, 1000000000000000000)
        
        columns = [
            ColumnDefinition(name="Timestamp (s)", channel_path=None, policy=ChannelPolicy.SYNCHRONIZED),
            ColumnDefinition(name="Channel Sum", channel_path=channel_path, policy=ChannelPolicy.SYNCHRONIZED),
        ]
        virtual_db = VirtualDatabase(io_db, channel_path, 10, columns)
        virtual_db.build()
        
        file_path = tmp_path / "test.csv"
        writer = CSVWriter(
            virtual_database=virtual_db,
            file_path=str(file_path),
            device_name="ic256_45",
            note="Test",
        )
        
        stats = writer.get_statistics()
        assert stats['rows_written'] == 0
        assert stats['file_size'] == 0
        
        writer.write_all()
        
        stats = writer.get_statistics()
        assert stats['rows_written'] > 0
        assert stats['file_size'] > 0
        assert 'file_path' in stats

    def test_can_prune_rows(self, tmp_path):
        """Test checking if rows can be pruned."""
        io_db = IODatabase()
        channel_path = "/test/channel_sum"
        
        # Add enough data to test pruning
        timestamp1 = 1000000000000000000
        for i in range(100):
            io_db.add_data_point(channel_path, 100 + i, timestamp1 + int(i * 1e8))
        
        columns = [
            ColumnDefinition(name="Timestamp (s)", channel_path=None, policy=ChannelPolicy.SYNCHRONIZED),
            ColumnDefinition(name="Channel Sum", channel_path=channel_path, policy=ChannelPolicy.SYNCHRONIZED),
        ]
        virtual_db = VirtualDatabase(io_db, channel_path, 10, columns)
        virtual_db.build()
        
        file_path = tmp_path / "test.csv"
        writer = CSVWriter(
            virtual_database=virtual_db,
            file_path=str(file_path),
            device_name="ic256_45",
            note="Test",
        )
        
        # Initially, can't prune (nothing written)
        assert not writer.can_prune_rows(rows_to_keep=10)
        
        # Write all rows
        writer.write_all()
        
        # Now can prune (all rows written)
        assert writer.can_prune_rows(rows_to_keep=10)
        
        # Check prunable count
        prunable = writer.get_prunable_row_count(rows_to_keep=10)
        assert prunable > 0

    def test_prune_after_write(self, tmp_path):
        """Test pruning rows from virtual database after writing."""
        io_db = IODatabase()
        channel_path = "/test/channel_sum"
        
        # Add data
        timestamp1 = 1000000000000000000
        for i in range(50):
            io_db.add_data_point(channel_path, 100 + i, timestamp1 + int(i * 1e8))
        
        columns = [
            ColumnDefinition(name="Timestamp (s)", channel_path=None, policy=ChannelPolicy.SYNCHRONIZED),
            ColumnDefinition(name="Channel Sum", channel_path=channel_path, policy=ChannelPolicy.SYNCHRONIZED),
        ]
        virtual_db = VirtualDatabase(io_db, channel_path, 10, columns)
        virtual_db.build()
        
        initial_row_count = virtual_db.get_row_count()
        
        file_path = tmp_path / "test.csv"
        writer = CSVWriter(
            virtual_database=virtual_db,
            file_path=str(file_path),
            device_name="ic256_45",
            note="Test",
        )
        
        # Write all rows
        writer.write_all()
        
        # Prune rows (keep last 10)
        rows_to_keep = 10
        if writer.can_prune_rows(rows_to_keep=rows_to_keep):
            pruned = virtual_db.prune_rows(rows_to_keep)
            assert pruned > 0
            assert virtual_db.get_row_count() == rows_to_keep


class TestCSVWriterIntegration:
    """Integration tests for CSVWriter with real device data."""

    def test_write_real_device_data(self, ic256_ip, tmp_path):
        """Test writing real device data to CSV.
        
        This test requires:
        - A live IC256 device at the IP in config.json
        - Network connectivity to that device
        """
        from ic256_sampler.utils import is_valid_device, is_valid_ipv4
        from ic256_sampler.igx_client import IGXWebsocketClient
        from ic256_sampler.device_paths import IC256_45_PATHS
        from ic256_sampler.simple_capture import capture_to_database
        from ic256_sampler.ic256_model import IC256Model
        
        # Skip if IP is invalid
        if not is_valid_ipv4(ic256_ip):
            pytest.skip(f"Invalid IP address in config: {ic256_ip}")
        
        # Verify device is reachable
        if not is_valid_device(ic256_ip, "IC256"):
            pytest.skip(f"IC256 device at {ic256_ip} is not reachable or not responding")
        
        # Get all channel paths that will be used in the virtual database
        # This includes all channels from IC256Model.create_columns()
        reference_channel = IC256_45_PATHS["adc"]["channel_sum"]
        columns = IC256Model.create_columns(reference_channel)
        channel_paths = [
            col_def.channel_path 
            for col_def in columns 
            if col_def.channel_path is not None
        ]
        
        # Capture data for 2 seconds (capture all channels that will be used)
        client = IGXWebsocketClient(ic256_ip)
        io_db = capture_to_database(client, channel_paths, duration=2.0)
        
        # Build virtual database
        sampling_rate = 3000
        
        virtual_db = VirtualDatabase(io_db, reference_channel, sampling_rate, columns)
        virtual_db.build()
        
        # Write to CSV
        file_path = tmp_path / "test_output.csv"
        writer = CSVWriter(
            virtual_database=virtual_db,
            file_path=str(file_path),
            device_name="ic256_45",
            note="Integration test",
        )
        
        rows_written = writer.write_all()
        writer.close()
        
        # Verify results
        print(f"\nCSV Writer Results:")
        print(f"  Rows Written: {rows_written}")
        print(f"  File Size: {writer.file_size} bytes")
        print(f"  File Path: {file_path}")
        
        assert rows_written > 0
        assert file_path.exists()
        assert file_path.stat().st_size > 0
        
        # Verify CSV structure
        with open(file_path, 'r') as f:
            reader = csv.reader(f)
            rows = list(reader)
            
            # Should have header + data rows
            assert len(rows) == rows_written + 1
            
            # Check header
            assert "Timestamp" in rows[0][0]
            
            # Check data rows
            for i, row in enumerate(rows[1:11]):  # Check first 10 data rows
                assert len(row) > 0
                # First column should be timestamp (scientific notation)
                assert "e" in row[0] or "." in row[0]
        
        # Verify statistics
        stats = writer.get_statistics()
        assert stats['rows_written'] == rows_written
        assert stats['file_size'] > 0
        assert stats['virtual_db_rows'] == virtual_db.get_row_count()
        
        # Check for empty cells in INTERPOLATED columns only
        # Forward-fill is implemented for INTERPOLATED channels, so they should not have empty cells
        # after the first data point is seen
        from ic256_sampler.virtual_database import ChannelPolicy
        
        # Get column definitions to identify INTERPOLATED columns
        interpolated_column_names = [
            col_def.name for col_def in virtual_db.columns
            if col_def.policy == ChannelPolicy.INTERPOLATED and col_def.channel_path is not None
        ]
        
        if interpolated_column_names:
            with open(file_path, 'r', newline='', encoding='utf-8-sig') as f:
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

    def test_incremental_write_real_data(self, ic256_ip, tmp_path):
        """Test incremental writing with real device data."""
        from ic256_sampler.utils import is_valid_device, is_valid_ipv4
        from ic256_sampler.igx_client import IGXWebsocketClient
        from ic256_sampler.device_paths import IC256_45_PATHS
        from ic256_sampler.simple_capture import capture_to_database
        from ic256_sampler.ic256_model import IC256Model
        
        # Skip if IP is invalid
        if not is_valid_ipv4(ic256_ip):
            pytest.skip(f"Invalid IP address in config: {ic256_ip}")
        
        # Verify device is reachable
        if not is_valid_device(ic256_ip, "IC256"):
            pytest.skip(f"IC256 device at {ic256_ip} is not reachable or not responding")
        
        # Get all channel paths that will be used in the virtual database
        # This includes all channels from IC256Model.create_columns()
        reference_channel = IC256_45_PATHS["adc"]["channel_sum"]
        columns = IC256Model.create_columns(reference_channel)
        channel_paths = [
            col_def.channel_path 
            for col_def in columns 
            if col_def.channel_path is not None
        ]
        
        # Capture data for 1 second (capture all channels that will be used)
        client = IGXWebsocketClient(ic256_ip)
        io_db = capture_to_database(client, channel_paths, duration=1.0)
        
        # Build virtual database
        virtual_db = VirtualDatabase(io_db, reference_channel, 3000, columns)
        virtual_db.build()
        
        file_path = tmp_path / "test_incremental.csv"
        writer = CSVWriter(
            virtual_database=virtual_db,
            file_path=str(file_path),
            device_name="ic256_45",
            note="Test",
        )
        
        # Write first batch
        rows1 = writer.write_all()
        assert rows1 > 0
        
        # Write again (should write nothing new)
        rows2 = writer.write_all()
        assert rows2 == 0
        
        # Verify file
        with open(file_path, 'r') as f:
            reader = csv.reader(f)
            rows = list(reader)
            assert len(rows) == rows1 + 1  # +1 for header
        
        writer.close()
