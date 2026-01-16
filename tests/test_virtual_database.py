"""Tests for Virtual Database.

These tests validate the virtual database functionality for creating
synthetic table rows from IO database data.
"""

import pytest
from ic256_sampler.io_database import IODatabase
from ic256_sampler.virtual_database import VirtualDatabase, VirtualRow


class TestVirtualDatabase:
    """Tests for VirtualDatabase class."""

    def test_create_virtual_database(self):
        """Test creating a virtual database."""
        io_db = IODatabase()
        virtual_db = VirtualDatabase(
            io_database=io_db,
            reference_channel="/test/channel_sum",
            sampling_rate=3000,
        )
        assert virtual_db.io_database == io_db
        assert virtual_db.reference_channel == "/test/channel_sum"
        assert virtual_db.sampling_rate == 3000
        assert len(virtual_db.rows) == 0

    def test_build_empty_database(self):
        """Test building virtual database from empty IO database."""
        io_db = IODatabase()
        virtual_db = VirtualDatabase(
            io_database=io_db,
            reference_channel="/test/channel_sum",
            sampling_rate=3000,
        )
        virtual_db.build()
        assert virtual_db.get_row_count() == 0

    def test_build_single_channel(self):
        """Test building virtual database from single channel."""
        io_db = IODatabase()
        channel_path = "/test/channel_sum"
        
        # Add data points spanning 1 second
        timestamp1 = 1000000000000000000
        timestamp2 = timestamp1 + int(1e9)  # 1 second later
        
        io_db.add_data_point(channel_path, 100, timestamp1)
        io_db.add_data_point(channel_path, 200, timestamp2)
        
        virtual_db = VirtualDatabase(
            io_database=io_db,
            reference_channel=channel_path,
            sampling_rate=10,  # 10 Hz = 10 rows per second
        )
        virtual_db.build()
        
        # Should have ~10 rows (1 second * 10 Hz)
        rows = virtual_db.get_rows()
        assert len(rows) >= 9  # Allow some tolerance
        assert len(rows) <= 11
        
        # Check first and last rows
        assert rows[0].timestamp >= 0.0
        assert rows[-1].timestamp <= 1.0

    def test_build_multiple_channels(self):
        """Test building virtual database from multiple channels."""
        io_db = IODatabase()
        channel1 = "/test/channel_sum"
        channel2 = "/test/primary_dose"
        
        # Add data points spanning 1 second
        timestamp1 = 1000000000000000000
        timestamp2 = timestamp1 + int(1e9)  # 1 second later
        
        io_db.add_data_point(channel1, 100, timestamp1)
        io_db.add_data_point(channel1, 200, timestamp2)
        io_db.add_data_point(channel2, 50, timestamp1)
        io_db.add_data_point(channel2, 60, timestamp2)
        
        virtual_db = VirtualDatabase(
            io_database=io_db,
            reference_channel=channel1,
            sampling_rate=10,  # 10 Hz
        )
        virtual_db.build()
        
        rows = virtual_db.get_rows()
        assert len(rows) > 0
        
        # Check that rows contain data from both channels
        for row in rows:
            assert channel1 in row.data
            assert channel2 in row.data

    def test_get_row_at_index(self):
        """Test getting a row by index."""
        io_db = IODatabase()
        channel_path = "/test/channel_sum"
        
        timestamp1 = 1000000000000000000
        timestamp2 = timestamp1 + int(1e9)
        
        io_db.add_data_point(channel_path, 100, timestamp1)
        io_db.add_data_point(channel_path, 200, timestamp2)
        
        virtual_db = VirtualDatabase(
            io_database=io_db,
            reference_channel=channel_path,
            sampling_rate=10,
        )
        virtual_db.build()
        
        # Get first row
        row0 = virtual_db.get_row_at_index(0)
        assert row0 is not None
        assert row0.timestamp >= 0.0
        
        # Get last row
        last_idx = virtual_db.get_row_count() - 1
        last_row = virtual_db.get_row_at_index(last_idx)
        assert last_row is not None
        
        # Get out of range
        assert virtual_db.get_row_at_index(99999) is None

    def test_get_row_at_time(self):
        """Test getting a row at a specific time."""
        io_db = IODatabase()
        channel_path = "/test/channel_sum"
        
        timestamp1 = 1000000000000000000
        timestamp2 = timestamp1 + int(1e9)
        
        io_db.add_data_point(channel_path, 100, timestamp1)
        io_db.add_data_point(channel_path, 200, timestamp2)
        
        virtual_db = VirtualDatabase(
            io_database=io_db,
            reference_channel=channel_path,
            sampling_rate=10,
        )
        virtual_db.build()
        
        # Get row at 0.5 seconds
        row = virtual_db.get_row_at_time(0.5, tolerance=0.1)
        assert row is not None
        assert abs(row.timestamp - 0.5) <= 0.1

    def test_get_statistics(self):
        """Test getting virtual database statistics."""
        io_db = IODatabase()
        channel_path = "/test/channel_sum"
        
        timestamp1 = 1000000000000000000
        timestamp2 = timestamp1 + int(2e9)  # 2 seconds later
        
        io_db.add_data_point(channel_path, 100, timestamp1)
        io_db.add_data_point(channel_path, 200, timestamp2)
        
        sampling_rate = 10
        virtual_db = VirtualDatabase(
            io_database=io_db,
            reference_channel=channel_path,
            sampling_rate=sampling_rate,
        )
        virtual_db.build()
        
        stats = virtual_db.get_statistics()
        
        assert stats['row_count'] > 0
        assert stats['time_span'] > 0
        assert stats['sampling_rate'] == sampling_rate
        assert stats['expected_rows'] == int(stats['time_span'] * sampling_rate)
        assert stats['actual_rows'] == stats['row_count']
        assert stats['coverage'] > 0.0

    def test_clear(self):
        """Test clearing the virtual database."""
        io_db = IODatabase()
        channel_path = "/test/channel_sum"
        
        timestamp1 = 1000000000000000000
        io_db.add_data_point(channel_path, 100, timestamp1)
        
        virtual_db = VirtualDatabase(
            io_database=io_db,
            reference_channel=channel_path,
            sampling_rate=10,
        )
        virtual_db.build()
        
        assert virtual_db.get_row_count() > 0
        assert virtual_db._built
        
        virtual_db.clear()
        assert len(virtual_db.rows) == 0
        assert not virtual_db._built
        
        # After clear, get_row_count will rebuild, so check rows directly
        assert len(virtual_db.rows) == 0


class TestVirtualDatabaseIntegration:
    """Integration tests for VirtualDatabase with real device data."""

    def test_build_from_real_capture(self, ic256_ip):
        """Test building virtual database from real device capture.
        
        This test requires:
        - A live IC256 device at the IP in config.json
        - Network connectivity to that device
        """
        from ic256_sampler.utils import is_valid_device, is_valid_ipv4
        from ic256_sampler.igx_client import IGXWebsocketClient
        from ic256_sampler.device_paths import IC256_45_PATHS
        from ic256_sampler.simple_capture import capture_to_database
        
        # Skip if IP is invalid
        if not is_valid_ipv4(ic256_ip):
            pytest.skip(f"Invalid IP address in config: {ic256_ip}")
        
        # Verify device is reachable
        if not is_valid_device(ic256_ip, "IC256"):
            pytest.skip(f"IC256 device at {ic256_ip} is not reachable or not responding")
        
        # Get channel paths
        channel_paths = [
            IC256_45_PATHS["adc"]["channel_sum"],
            IC256_45_PATHS["adc"]["primary_dose"],
        ]
        
        # Capture data for 2 seconds
        client = IGXWebsocketClient(ic256_ip)
        io_db = capture_to_database(client, channel_paths, duration=2.0)
        
        # Build virtual database using channel_sum as reference
        reference_channel = IC256_45_PATHS["adc"]["channel_sum"]
        sampling_rate = 3000
        
        virtual_db = VirtualDatabase(
            io_database=io_db,
            reference_channel=reference_channel,
            sampling_rate=sampling_rate,
        )
        virtual_db.build()
        
        # Verify virtual database
        stats = virtual_db.get_statistics()
        print(f"\nVirtual Database Statistics:")
        print(f"  Time Span: {stats['time_span']:.3f} seconds")
        print(f"  Sampling Rate: {stats['sampling_rate']} Hz")
        print(f"  Expected Rows: {stats['expected_rows']}")
        print(f"  Actual Rows: {stats['actual_rows']}")
        print(f"  Coverage: {stats['coverage']:.2%}")
        
        # Verify we got rows
        assert stats['row_count'] > 0, "Should have created rows"
        assert stats['time_span'] > 0, "Should have time span"
        
        # Verify row count is close to expected (within 10% tolerance)
        expected = stats['expected_rows']
        actual = stats['actual_rows']
        tolerance = 0.10
        
        assert actual >= int(expected * (1 - tolerance)), \
            f"Row count {actual} is below minimum expected {int(expected * (1 - tolerance))}"
        assert actual <= int(expected * (1 + tolerance)), \
            f"Row count {actual} is above maximum expected {int(expected * (1 + tolerance))}"
        
        # Verify rows have data
        rows = virtual_db.get_rows()
        assert len(rows) > 0
        
        # Check that rows contain data from reference channel
        for row in rows[:10]:  # Check first 10 rows
            assert reference_channel in row.data
            # Reference channel should always have data (it's the timing source)
            assert row.data[reference_channel] is not None

    def test_virtual_database_row_timing(self, ic256_ip):
        """Test that virtual database rows are at correct intervals.
        
        This test validates that rows are spaced correctly at the sampling rate.
        """
        from ic256_sampler.utils import is_valid_device, is_valid_ipv4
        from ic256_sampler.igx_client import IGXWebsocketClient
        from ic256_sampler.device_paths import IC256_45_PATHS
        from ic256_sampler.simple_capture import capture_to_database
        
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
        sampling_rate = 3000
        virtual_db = VirtualDatabase(
            io_database=io_db,
            reference_channel=channel_paths[0],
            sampling_rate=sampling_rate,
        )
        virtual_db.build()
        
        # Verify row spacing
        rows = virtual_db.get_rows()
        assert len(rows) > 0
        
        row_interval = 1.0 / sampling_rate
        for i in range(1, min(100, len(rows))):  # Check first 100 rows
            time_diff = rows[i].timestamp - rows[i-1].timestamp
            # Allow small tolerance for floating point
            assert abs(time_diff - row_interval) < row_interval * 0.01, \
                f"Row {i} spacing {time_diff} should be {row_interval}"
