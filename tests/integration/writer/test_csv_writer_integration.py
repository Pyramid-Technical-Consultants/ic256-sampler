"""Integration tests for CSV Writer with real device data.

Test Methodology:
-----------------
These tests validate the end-to-end CSV writing workflow with real device data:
1. Connect to a live IC256 device
2. Capture real data from all required channels
3. Build a virtual database from the captured data
4. Write the data to CSV
5. Validate the output file structure and content

These tests focus on integration behavior rather than implementation details.
Unit tests cover detailed edge cases (empty cells, forward-fill, etc.).

Requirements:
- A live IC256 device at the IP in config.json
- Network connectivity to that device
"""

import pytest
import csv
from ic256_sampler.csv_writer import CSVWriter
from ic256_sampler.virtual_database import VirtualDatabase


# Mark all tests in this file as integration tests with timeout
pytestmark = [pytest.mark.integration, pytest.mark.timeout(15)]


class TestCSVWriterIntegration:
    """Integration tests for CSVWriter with real device data."""

    @staticmethod
    def _setup_device_and_capture(ic256_ip, duration=2.0):
        """Helper method to set up device connection and capture data.
        
        Args:
            ic256_ip: IP address of the IC256 device
            duration: Duration to capture data in seconds
            
        Returns:
            Tuple of (virtual_db, columns) ready for CSV writing
        """
        from ic256_sampler.utils import is_valid_device, is_valid_ipv4
        from ic256_sampler.igx_client import IGXWebsocketClient
        from ic256_sampler.device_paths import IC256_45_PATHS
        from ic256_sampler.simple_capture import capture_to_database
        from ic256_sampler.ic256_model import IC256Model
        
        # Validate device connectivity
        if not is_valid_ipv4(ic256_ip):
            pytest.skip(f"Invalid IP address in config: {ic256_ip}")
        
        if not is_valid_device(ic256_ip, "IC256"):
            pytest.skip(f"IC256 device at {ic256_ip} is not reachable or not responding")
        
        # Set up channels and columns
        reference_channel = IC256_45_PATHS["adc"]["channel_sum"]
        columns = IC256Model.create_columns(reference_channel)
        channel_paths = [
            col_def.channel_path 
            for col_def in columns 
            if col_def.channel_path is not None
        ]
        
        # Capture data from device
        client = IGXWebsocketClient(ic256_ip)
        io_db = capture_to_database(client, channel_paths, duration=duration)
        
        # Build virtual database
        sampling_rate = 3000
        virtual_db = VirtualDatabase(io_db, reference_channel, sampling_rate, columns)
        virtual_db.build()
        
        return virtual_db, columns

    @staticmethod
    def _validate_csv_structure(file_path, expected_rows):
        """Validate basic CSV file structure.
        
        Args:
            file_path: Path to the CSV file
            expected_rows: Expected number of data rows (excluding header)
        """
        assert file_path.exists(), "CSV file should exist"
        assert file_path.stat().st_size > 0, "CSV file should not be empty"
        
        with open(file_path, 'r', newline='', encoding='utf-8-sig') as f:
            reader = csv.reader(f)
            rows = list(reader)
            
            # Check we have header + expected data rows
            assert len(rows) == expected_rows + 1, \
                f"Expected {expected_rows + 1} rows (header + data), got {len(rows)}"
            
            # Validate header
            header = rows[0]
            assert len(header) > 0, "Header should not be empty"
            assert "Timestamp" in header[0], "First column should be Timestamp"
            
            # Validate data rows have consistent structure
            for i, row in enumerate(rows[1:], start=1):
                assert len(row) == len(header), \
                    f"Row {i} has {len(row)} columns, expected {len(header)} (matching header)"
                assert len(row) > 0, f"Row {i} should not be empty"
                # Timestamp should be present and parseable
                assert row[0], f"Row {i} timestamp should not be empty"
                try:
                    float(row[0])
                except ValueError:
                    pytest.fail(f"Row {i} timestamp '{row[0]}' is not a valid number")

    @pytest.mark.integration
    def test_write_real_device_data(self, ic256_ip, tmp_path):
        """Test writing real device data to CSV.
        
        This test validates the complete workflow:
        1. Captures 2 seconds of real device data
        2. Builds a virtual database
        3. Writes all data to CSV
        4. Validates file structure and basic content
        
        Focus: End-to-end integration, not implementation details.
        """
        # Set up and capture data
        virtual_db, columns = self._setup_device_and_capture(ic256_ip, duration=2.0)
        
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
        
        # Basic validation
        assert rows_written > 0, "Should write at least some data rows"
        
        # Validate file structure
        self._validate_csv_structure(file_path, rows_written)
        
        # Validate statistics match
        stats = writer.get_statistics()
        assert stats['rows_written'] == rows_written
        assert stats['file_size'] > 0
        assert stats['virtual_db_rows'] == virtual_db.get_row_count()

    @pytest.mark.integration
    def test_incremental_write_real_data(self, ic256_ip, tmp_path):
        """Test incremental writing with real device data.
        
        This test validates that:
        1. write_all() can be called multiple times
        2. Subsequent calls don't duplicate data
        3. File structure remains consistent
        
        Focus: Incremental write behavior, not data content details.
        """
        # Set up and capture data
        virtual_db, columns = self._setup_device_and_capture(ic256_ip, duration=1.0)
        
        # Create writer
        file_path = tmp_path / "test_incremental.csv"
        writer = CSVWriter(
            virtual_database=virtual_db,
            file_path=str(file_path),
            device_name="ic256_45",
            note="Test",
        )
        
        # First write - should write all data
        rows1 = writer.write_all()
        assert rows1 > 0, "First write should write data"
        
        # Second write - should write nothing (already written)
        rows2 = writer.write_all()
        assert rows2 == 0, "Second write should not duplicate data"
        
        # Validate file structure
        self._validate_csv_structure(file_path, rows1)
        
        writer.close()
