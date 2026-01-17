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


class TestIODatabaseEdgeCases:
    """Comprehensive edge case tests for IODatabase."""
    
    def test_many_points_same_timestamp(self):
        """Test IODatabase with many points at the same timestamp."""
        db = IODatabase()
        channel_path = "/test/channel"
        
        base_timestamp = 1000000000000000000
        
        # Add 1000 points all at the same timestamp
        for i in range(1000):
            db.add_data_point(channel_path, i, base_timestamp)
        
        channel_data = db.get_channel(channel_path)
        assert channel_data.count == 1000
        assert channel_data.first_timestamp == base_timestamp
        assert channel_data.last_timestamp == base_timestamp
        
        # All points should have elapsed_time = 0.0
        points = list(channel_data.data_points)
        for point in points:
            assert abs(point.elapsed_time - 0.0) < 1e-9, \
                f"Point should have elapsed_time = 0.0, got {point.elapsed_time}"
    
    def test_points_very_close_timestamps(self):
        """Test IODatabase with points at very close timestamps (nanoseconds apart)."""
        db = IODatabase()
        channel_path = "/test/channel"
        
        base_timestamp = 1000000000000000000
        
        # Add points 1 nanosecond apart
        for i in range(100):
            db.add_data_point(channel_path, i, base_timestamp + i)
        
        channel_data = db.get_channel(channel_path)
        assert channel_data.count == 100
        
        # All points should have elapsed_time very close to 0.0
        points = list(channel_data.data_points)
        for point in points:
            # Elapsed time should be in nanoseconds (very small)
            assert point.elapsed_time < 1e-6, \
                f"Point should have elapsed_time < 1e-6, got {point.elapsed_time}"
    
    def test_rapid_data_arrival(self):
        """Test IODatabase with rapid data arrival (simulating device burst)."""
        db = IODatabase()
        channel_path = "/test/channel"
        
        base_timestamp = 1000000000000000000
        
        # Simulate rapid arrival: 100 points in 1ms
        for i in range(100):
            timestamp = base_timestamp + int(i * 0.00001 * 1e9)  # 0.01ms apart
            db.add_data_point(channel_path, i, timestamp)
        
        channel_data = db.get_channel(channel_path)
        assert channel_data.count == 100
        
        # Time span should be very small (< 1ms)
        stats = db.get_statistics()
        channel_stats = stats['channels'][channel_path]
        assert channel_stats['time_span'] < 0.001, \
            f"Time span should be < 1ms, got {channel_stats['time_span']}"
    
    def test_mixed_timestamp_patterns(self):
        """Test IODatabase with mixed timestamp patterns (bursts and continuous)."""
        db = IODatabase()
        channel_path = "/test/channel"
        
        base_timestamp = 1000000000000000000
        
        # Burst 1: 10 points at t=0
        for i in range(10):
            db.add_data_point(channel_path, i, base_timestamp)
        
        # Continuous: 50 points over 1 second
        for i in range(50):
            db.add_data_point(channel_path, 10 + i, base_timestamp + int(i * 0.02 * 1e9))
        
        # Burst 2: 20 points at t=1s
        for i in range(20):
            db.add_data_point(channel_path, 60 + i, base_timestamp + int(1e9))
        
        channel_data = db.get_channel(channel_path)
        assert channel_data.count == 80
        
        # Verify time span
        stats = db.get_statistics()
        channel_stats = stats['channels'][channel_path]
        assert channel_stats['time_span'] >= 1.0, \
            f"Time span should be >= 1s, got {channel_stats['time_span']}"
    
    def test_elapsed_time_calculation_consistency(self):
        """Test that elapsed_time is calculated consistently across channels."""
        db = IODatabase()
        channel1 = "/test/channel1"
        channel2 = "/test/channel2"
        
        base_timestamp = 1000000000000000000
        
        # Add points to both channels at same timestamps
        for i in range(10):
            timestamp = base_timestamp + int(i * 0.1 * 1e9)  # 0.1s apart
            db.add_data_point(channel1, i, timestamp)
            db.add_data_point(channel2, i * 10, timestamp)
        
        # Both channels should have same elapsed_time values
        ch1_data = db.get_channel(channel1)
        ch2_data = db.get_channel(channel2)
        
        ch1_points = list(ch1_data.data_points)
        ch2_points = list(ch2_data.data_points)
        
        assert len(ch1_points) == len(ch2_points)
        
        for p1, p2 in zip(ch1_points, ch2_points):
            assert abs(p1.elapsed_time - p2.elapsed_time) < 1e-9, \
                f"Elapsed times should match. Ch1: {p1.elapsed_time}, Ch2: {p2.elapsed_time}"
    
    def test_clear_and_rebuild(self):
        """Test clearing IODatabase and rebuilding with new data."""
        db = IODatabase()
        channel_path = "/test/channel"
        
        base_timestamp = 1000000000000000000
        
        # Add initial data
        for i in range(10):
            db.add_data_point(channel_path, i, base_timestamp + int(i * 0.1 * 1e9))
        
        assert db.get_total_count() == 10
        
        # Clear
        db.clear()
        assert db.get_total_count() == 0
        assert db.global_first_timestamp is None
        
        # Add new data
        new_base = base_timestamp + int(10e9)  # 10 seconds later
        for i in range(20):
            db.add_data_point(channel_path, i, new_base + int(i * 0.1 * 1e9))
        
        assert db.get_total_count() == 20
        
        # New data should have elapsed_time starting from 0.0 again
        channel_data = db.get_channel(channel_path)
        first_point = channel_data.data_points[0]
        assert abs(first_point.elapsed_time - 0.0) < 1e-9, \
            f"First point after clear should have elapsed_time = 0.0, got {first_point.elapsed_time}"
    
    def test_large_dataset_performance(self):
        """Test IODatabase performance with large dataset."""
        db = IODatabase()
        channel_path = "/test/channel"
        
        base_timestamp = 1000000000000000000
        
        # Add 10,000 points
        import time
        start_time = time.time()
        for i in range(10000):
            db.add_data_point(channel_path, i, base_timestamp + int(i * 0.001 * 1e9))
        add_time = time.time() - start_time
        
        # Should complete in reasonable time (< 1 second)
        assert add_time < 1.0, f"Adding 10k points took {add_time:.2f}s, should be < 1s"
        
        # Query performance
        start_time = time.time()
        points = db.get_channel(channel_path).get_points_in_range(1.0, 2.0)
        query_time = time.time() - start_time
        
        # Should query quickly (< 0.1 second)
        assert query_time < 0.1, f"Query took {query_time:.2f}s, should be < 0.1s"
        
        assert len(points) > 0
    
    def test_concurrent_channel_addition(self):
        """Test adding data to multiple channels concurrently (simulated)."""
        db = IODatabase()
        channels = [f"/test/channel{i}" for i in range(10)]
        
        base_timestamp = 1000000000000000000
        
        # Add data to all channels
        for i in range(100):
            for channel in channels:
                db.add_data_point(channel, i, base_timestamp + int(i * 0.01 * 1e9))
        
        # All channels should have data
        for channel in channels:
            assert db.get_channel_count(channel) == 100
        
        # Total count should be correct
        assert db.get_total_count() == 1000  # 10 channels * 100 points
    
    def test_get_statistics_with_empty_channels(self):
        """Test get_statistics() with empty channels."""
        db = IODatabase()
        
        # Add channels but no data
        db.add_channel("/test/channel1")
        db.add_channel("/test/channel2")
        
        stats = db.get_statistics()
        assert stats['total_channels'] == 2
        assert stats['total_data_points'] == 0
    
    def test_get_data_at_time_with_gaps(self):
        """Test get_data_at_time() when there are gaps in data."""
        db = IODatabase()
        channel_path = "/test/channel"
        
        base_timestamp = 1000000000000000000
        
        # Add points with gaps
        db.add_data_point(channel_path, 100, base_timestamp)  # t=0
        db.add_data_point(channel_path, 200, base_timestamp + int(1e9))  # t=1s
        db.add_data_point(channel_path, 300, base_timestamp + int(3e9))  # t=3s (gap from 1s to 3s)
        
        # Query at t=2s (in the gap)
        data = db.get_data_at_time(2.0, tolerance=0.5)
        # Should return None or closest point
        assert channel_path in data
        # May return None if no point within tolerance, or closest point
        assert data[channel_path] is None or data[channel_path].value in [200, 300]


class TestIODatabaseMissingCoverage:
    """Tests for IODatabase methods with missing coverage."""
    
    def test_channel_data_add_point_with_reference_timestamp(self):
        """Test ChannelData.add_point with explicit reference_timestamp.
        
        Note: For the first point, reference_timestamp is ignored and first_timestamp
        is used as the reference. This test verifies behavior for subsequent points.
        """
        channel_data = ChannelData(channel_path="/test/channel")
        
        base_timestamp = 1000000000000000000
        
        # Add first point (reference_timestamp parameter is ignored, uses timestamp itself)
        channel_data.add_point(100, base_timestamp, reference_timestamp=base_timestamp - int(10e9))
        first_point = channel_data.data_points[0]
        # First point always has elapsed_time = 0.0
        assert abs(first_point.elapsed_time - 0.0) < 1e-9
        
        # Add second point with explicit reference timestamp
        reference = base_timestamp - int(2e9)  # 2 seconds before base
        timestamp2 = base_timestamp + int(1e9)  # 1 second after base
        channel_data.add_point(200, timestamp2, reference_timestamp=reference)
        
        # Second point's elapsed time should be calculated from reference
        second_point = channel_data.data_points[1]
        expected_elapsed = (timestamp2 - reference) / 1e9  # Should be 3.0 seconds
        assert abs(second_point.elapsed_time - expected_elapsed) < 1e-9
    
    def test_channel_data_add_point_reference_after_first(self):
        """Test ChannelData.add_point when reference_timestamp is set after first point."""
        channel_data = ChannelData(channel_path="/test/channel")
        
        base_timestamp = 1000000000000000000
        
        # Add first point (sets first_timestamp)
        channel_data.add_point(100, base_timestamp)
        
        # Add second point with explicit reference (different from first_timestamp)
        reference = base_timestamp + int(1e9)  # 1 second after first
        timestamp2 = base_timestamp + int(2e9)  # 2 seconds after first
        channel_data.add_point(200, timestamp2, reference_timestamp=reference)
        
        # Second point's elapsed time should be calculated from reference
        points = list(channel_data.data_points)
        assert len(points) == 2
        # First point: elapsed_time = 0 (relative to itself)
        assert abs(points[0].elapsed_time - 0.0) < 1e-9
        # Second point: elapsed_time = (timestamp2 - reference) / 1e9 = 1.0
        assert abs(points[1].elapsed_time - 1.0) < 1e-9
    
    def test_get_points_in_range_large_dataset(self):
        """Test get_points_in_range with large dataset (uses binary search)."""
        channel_data = ChannelData(channel_path="/test/channel")
        
        base_timestamp = 1000000000000000000
        # Add 200 points to trigger binary search path (threshold is 100)
        for i in range(200):
            timestamp = base_timestamp + int(i * 0.01 * 1e9)  # 0.01s apart
            channel_data.add_point(i, timestamp)
        
        # Query range in the middle
        points = channel_data.get_points_in_range(0.5, 1.5)
        
        # Should return points between 0.5s and 1.5s (50-150 points)
        assert len(points) == 101  # 50 to 150 inclusive
        assert points[0].value == 50
        assert points[-1].value == 150
    
    def test_get_point_at_time_large_dataset(self):
        """Test get_point_at_time with large dataset (uses binary search)."""
        channel_data = ChannelData(channel_path="/test/channel")
        
        base_timestamp = 1000000000000000000
        # Add 100 points to trigger binary search path (threshold is 50)
        for i in range(100):
            timestamp = base_timestamp + int(i * 0.01 * 1e9)  # 0.01s apart
            channel_data.add_point(i, timestamp)
        
        # Query point at 0.5s elapsed time
        point = channel_data.get_point_at_time(0.5, tolerance=0.01)
        
        assert point is not None
        assert point.value == 50
        assert abs(point.elapsed_time - 0.5) < 0.01
    
    def test_get_point_at_time_large_dataset_outside_tolerance(self):
        """Test get_point_at_time with large dataset when no point within tolerance."""
        channel_data = ChannelData(channel_path="/test/channel")
        
        base_timestamp = 1000000000000000000
        # Add 100 points
        for i in range(100):
            timestamp = base_timestamp + int(i * 0.01 * 1e9)
            channel_data.add_point(i, timestamp)
        
        # Query point far outside range with small tolerance
        point = channel_data.get_point_at_time(10.0, tolerance=0.001)
        
        # Should return None (no point within tolerance)
        assert point is None
    
    def test_channel_data_get_statistics_empty(self):
        """Test ChannelData.get_statistics with empty channel."""
        channel_data = ChannelData(channel_path="/test/channel")
        
        stats = channel_data.get_statistics()
        
        assert stats['count'] == 0
        assert stats['first_timestamp'] is None
        assert stats['last_timestamp'] is None
    
    def test_get_data_in_range(self):
        """Test IODatabase.get_data_in_range method."""
        db = IODatabase()
        
        channel1 = "/test/channel1"
        channel2 = "/test/channel2"
        
        base_timestamp = 1000000000000000000
        
        # Add points to both channels
        for i in range(10):
            timestamp = base_timestamp + int(i * 0.1 * 1e9)  # 0.1s apart
            db.add_data_point(channel1, i * 10, timestamp)
            db.add_data_point(channel2, i * 20, timestamp)
        
        # Get data in range 0.5s to 1.5s
        data = db.get_data_in_range(0.5, 1.5)
        
        # Should return data for both channels
        assert channel1 in data
        assert channel2 in data
        assert len(data[channel1]) > 0
        assert len(data[channel2]) > 0
        
        # All points should be in the specified range
        for channel_path, points in data.items():
            for point in points:
                assert 0.5 <= point.elapsed_time <= 1.5
    
    def test_get_data_in_range_empty_range(self):
        """Test get_data_in_range with range that has no data."""
        db = IODatabase()
        channel_path = "/test/channel"
        
        base_timestamp = 1000000000000000000
        
        # Add points only at 0-1 seconds
        for i in range(10):
            timestamp = base_timestamp + int(i * 0.1 * 1e9)
            db.add_data_point(channel_path, i, timestamp)
        
        # Query range 10-11 seconds (no data)
        data = db.get_data_in_range(10.0, 11.0)
        
        # Should return empty list for the channel
        assert channel_path in data
        assert len(data[channel_path]) == 0
    
    def test_get_data_in_range_partial_coverage(self):
        """Test get_data_in_range when only some channels have data in range."""
        db = IODatabase()
        
        channel1 = "/test/channel1"
        channel2 = "/test/channel2"
        
        base_timestamp = 1000000000000000000
        
        # Channel1: data at 0-1 seconds
        for i in range(10):
            timestamp = base_timestamp + int(i * 0.1 * 1e9)
            db.add_data_point(channel1, i, timestamp)
        
        # Channel2: data at 2-3 seconds
        for i in range(10):
            timestamp = base_timestamp + int((2 + i * 0.1) * 1e9)
            db.add_data_point(channel2, i, timestamp)
        
        # Query range 0.5-1.5 seconds (only channel1 has data)
        data = db.get_data_in_range(0.5, 1.5)
        
        assert channel1 in data
        assert channel2 in data
        assert len(data[channel1]) > 0
        assert len(data[channel2]) == 0  # No data in range
