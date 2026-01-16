"""Tests for IO Database data structure.

These tests validate the IO database structure for storing and querying
data from multiple channels.
"""

import pytest
import time
from ic256_sampler.io_database import IODatabase, ChannelData, DataPoint


class TestIODatabase:
    """Tests for IODatabase class."""

    def test_create_empty_database(self):
        """Test creating an empty database."""
        db = IODatabase()
        assert len(db.channels) == 0
        assert db.global_first_timestamp is None
        assert db.session_start_time > 0

    def test_add_channel(self):
        """Test adding a channel to the database."""
        db = IODatabase()
        channel_path = "/test/channel"
        
        channel_data = db.add_channel(channel_path)
        assert channel_path in db.channels
        assert channel_data.channel_path == channel_path
        assert channel_data.count == 0

    def test_add_data_point(self):
        """Test adding data points to a channel."""
        db = IODatabase()
        channel_path = "/test/channel"
        
        # Add first data point
        timestamp1 = 1000000000000000000  # 1e18 nanoseconds
        db.add_data_point(channel_path, 123, timestamp1)
        
        channel_data = db.get_channel(channel_path)
        assert channel_data is not None
        assert channel_data.count == 1
        assert channel_data.first_timestamp == timestamp1
        assert channel_data.last_timestamp == timestamp1
        assert db.global_first_timestamp == timestamp1
        
        # Add second data point
        timestamp2 = timestamp1 + int(1e9)  # 1 second later
        db.add_data_point(channel_path, 456, timestamp2)
        
        assert channel_data.count == 2
        assert channel_data.last_timestamp == timestamp2
        assert len(channel_data.data_points) == 2

    def test_multiple_channels(self):
        """Test adding data to multiple channels."""
        db = IODatabase()
        
        channel1 = "/test/channel1"
        channel2 = "/test/channel2"
        
        timestamp1 = 1000000000000000000
        timestamp2 = timestamp1 + int(1e9)
        
        db.add_data_point(channel1, 100, timestamp1)
        db.add_data_point(channel2, 200, timestamp1)
        db.add_data_point(channel1, 101, timestamp2)
        db.add_data_point(channel2, 201, timestamp2)
        
        assert db.get_channel_count(channel1) == 2
        assert db.get_channel_count(channel2) == 2
        assert db.get_total_count() == 4

    def test_get_point_at_time(self):
        """Test getting data point at a specific elapsed time."""
        db = IODatabase()
        channel_path = "/test/channel"
        
        timestamp1 = 1000000000000000000
        timestamp2 = timestamp1 + int(1e9)  # 1 second later
        timestamp3 = timestamp1 + int(2e9)  # 2 seconds later
        
        db.add_data_point(channel_path, 100, timestamp1)
        db.add_data_point(channel_path, 200, timestamp2)
        db.add_data_point(channel_path, 300, timestamp3)
        
        # Get point at 1 second elapsed time
        point = db.get_channel(channel_path).get_point_at_time(1.0, tolerance=0.1)
        assert point is not None
        assert point.value == 200
        assert abs(point.elapsed_time - 1.0) < 0.1

    def test_get_points_in_range(self):
        """Test getting data points within a time range."""
        db = IODatabase()
        channel_path = "/test/channel"
        
        timestamp1 = 1000000000000000000
        timestamp2 = timestamp1 + int(1e9)  # 1 second
        timestamp3 = timestamp1 + int(2e9)  # 2 seconds
        timestamp4 = timestamp1 + int(3e9)  # 3 seconds
        
        db.add_data_point(channel_path, 100, timestamp1)
        db.add_data_point(channel_path, 200, timestamp2)
        db.add_data_point(channel_path, 300, timestamp3)
        db.add_data_point(channel_path, 400, timestamp4)
        
        # Get points between 1 and 2 seconds
        points = db.get_channel(channel_path).get_points_in_range(1.0, 2.0)
        assert len(points) == 2
        assert points[0].value == 200
        assert points[1].value == 300

    def test_get_data_at_time_all_channels(self):
        """Test getting data from all channels at a specific time."""
        db = IODatabase()
        
        channel1 = "/test/channel1"
        channel2 = "/test/channel2"
        
        timestamp1 = 1000000000000000000
        timestamp2 = timestamp1 + int(1e9)  # 1 second later
        
        db.add_data_point(channel1, 100, timestamp1)
        db.add_data_point(channel2, 200, timestamp1)
        db.add_data_point(channel1, 101, timestamp2)
        db.add_data_point(channel2, 201, timestamp2)
        
        # Get data at 1 second elapsed time
        data = db.get_data_at_time(1.0, tolerance=0.1)
        assert channel1 in data
        assert channel2 in data
        assert data[channel1] is not None
        assert data[channel2] is not None
        assert data[channel1].value == 101
        assert data[channel2].value == 201

    def test_get_statistics(self):
        """Test getting database statistics."""
        db = IODatabase()
        
        channel1 = "/test/channel1"
        channel2 = "/test/channel2"
        
        timestamp1 = 1000000000000000000
        timestamp2 = timestamp1 + int(1e9)  # 1 second later
        
        db.add_data_point(channel1, 100, timestamp1)
        db.add_data_point(channel2, 200, timestamp1)
        db.add_data_point(channel1, 101, timestamp2)
        
        stats = db.get_statistics()
        
        assert stats['total_channels'] == 2
        assert stats['total_data_points'] == 3
        assert stats['global_first_timestamp'] == timestamp1
        assert channel1 in stats['channels']
        assert channel2 in stats['channels']
        assert stats['channels'][channel1]['count'] == 2
        assert stats['channels'][channel2]['count'] == 1

    def test_clear_database(self):
        """Test clearing the database."""
        db = IODatabase()
        
        db.add_data_point("/test/channel", 100, 1000000000000000000)
        assert db.get_total_count() == 1
        
        db.clear()
        assert len(db.channels) == 0
        assert db.get_total_count() == 0
        assert db.global_first_timestamp is None

    def test_elapsed_time_calculation(self):
        """Test that elapsed time is calculated correctly."""
        db = IODatabase()
        channel_path = "/test/channel"
        
        timestamp1 = 1000000000000000000
        timestamp2 = timestamp1 + int(1e9)  # 1 second later
        timestamp3 = timestamp1 + int(2e9)  # 2 seconds later
        
        db.add_data_point(channel_path, 100, timestamp1)
        db.add_data_point(channel_path, 200, timestamp2)
        db.add_data_point(channel_path, 300, timestamp3)
        
        points = list(db.get_channel(channel_path).data_points)
        assert abs(points[0].elapsed_time - 0.0) < 0.001
        assert abs(points[1].elapsed_time - 1.0) < 0.001
        assert abs(points[2].elapsed_time - 2.0) < 0.001


class TestIODatabaseIntegration:
    """Integration tests for IO database with real device."""

    def test_capture_multiple_channels_to_database(self, ic256_ip):
        """Test capturing multiple channels into IO database.
        
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
        
        # Create client and database
        client = IGXWebsocketClient(ic256_ip)
        database = IODatabase()
        
        # Capture data for 2 seconds
        duration = 2.0
        database = capture_to_database(client, channel_paths, duration, database)
        
        # Verify database has data
        stats = database.get_statistics()
        assert stats['total_channels'] == 2
        assert stats['total_data_points'] > 0
        
        # Verify each channel has data
        for channel_path in channel_paths:
            count = database.get_channel_count(channel_path)
            assert count > 0, f"Channel {channel_path} should have data"
            
            channel_data = database.get_channel(channel_path)
            assert channel_data.first_timestamp is not None
            assert channel_data.last_timestamp is not None
            assert channel_data.last_timestamp >= channel_data.first_timestamp
        
        print(f"\nMulti-Channel Capture Results:")
        print(f"  Duration: {duration} seconds")
        print(f"  Total Channels: {stats['total_channels']}")
        print(f"  Total Data Points: {stats['total_data_points']}")
        for channel_path, channel_stats in stats['channels'].items():
            print(f"  {channel_path}:")
            print(f"    Count: {channel_stats['count']}")
            print(f"    Rate: {channel_stats['rate']:.2f} points/second")
            print(f"    Time Span: {channel_stats['time_span']:.3f} seconds")
