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
        
        # Capture data for 2 seconds
        channel_paths = [
            IC256_45_PATHS["adc"]["channel_sum"],
            IC256_45_PATHS["adc"]["primary_dose"],
        ]
        
        client = IGXWebsocketClient(ic256_ip)
        io_db = capture_to_database(client, channel_paths, duration=2.0)
        
        # Build virtual database
        reference_channel = IC256_45_PATHS["adc"]["channel_sum"]
        sampling_rate = 3000
        columns = IC256Model.create_columns(reference_channel)
        
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
        
        # Capture data
        channel_paths = [IC256_45_PATHS["adc"]["channel_sum"]]
        client = IGXWebsocketClient(ic256_ip)
        io_db = capture_to_database(client, channel_paths, duration=1.0)
        
        # Build virtual database
        reference_channel = channel_paths[0]
        columns = IC256Model.create_columns(reference_channel)
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
