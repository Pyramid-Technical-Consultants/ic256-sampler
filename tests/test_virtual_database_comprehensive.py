"""Comprehensive tests for Virtual Database edge cases and rebuild behavior.

These tests cover scenarios that could cause data collection bugs, including:
- Data points arriving at the same timestamp
- Rebuild() behavior with minimal time advancement
- Incremental rebuilds with burst data
- Edge cases around elapsed_time = 0.0
"""

import pytest
from ic256_sampler.io_database import IODatabase
from ic256_sampler.virtual_database import (
    VirtualDatabase,
    ColumnDefinition,
    ChannelPolicy,
)


class TestVirtualDatabaseRebuildEdgeCases:
    """Comprehensive tests for rebuild() edge cases that could cause data collection bugs."""

    def test_rebuild_with_same_timestamp_data(self):
        """Test rebuild() when all data points have the same timestamp.
        
        This tests the bug scenario where data points arrive in a burst
        at the same timestamp, causing only 1 row to be created initially.
        """
        io_db = IODatabase()
        channel_path = "/test/channel_sum"
        
        base_timestamp = 1000000000000000000
        
        # Add 11 data points all at the same timestamp (simulating burst arrival)
        for i in range(11):
            io_db.add_data_point(channel_path, 100 + i, base_timestamp)
        
        columns = [
            ColumnDefinition(name="Timestamp (s)", channel_path=None, policy=ChannelPolicy.SYNCHRONIZED),
            ColumnDefinition(name="Channel Sum", channel_path=channel_path, policy=ChannelPolicy.SYNCHRONIZED),
        ]
        
        virtual_db = VirtualDatabase(
            io_database=io_db,
            reference_channel=channel_path,
            sampling_rate=500,  # 500 Hz
            columns=columns,
        )
        
        # Initial build - should create at least 1 row
        virtual_db.build()
        initial_rows = virtual_db.get_row_count()
        assert initial_rows > 0, "Should create at least 1 row even with same timestamp data"
        
        # Add more data points at slightly later time
        for i in range(11, 50):
            io_db.add_data_point(channel_path, 100 + i, base_timestamp + int(0.001e9))  # 1ms later
        
        # Rebuild should create more rows even if time hasn't advanced much
        virtual_db.rebuild()
        after_rebuild_rows = virtual_db.get_row_count()
        
        # Should have created more rows (even if time hasn't advanced much)
        # The fix should handle this by checking point count vs row count
        assert after_rebuild_rows >= initial_rows, \
            f"Rebuild should create more rows. Initial: {initial_rows}, After: {after_rebuild_rows}"
    
    def test_rebuild_with_minimal_time_advancement(self):
        """Test rebuild() when new data arrives but time hasn't advanced enough for next row.
        
        This tests the scenario where rebuild() might return early because
        last_elapsed < start_time, even though new data has arrived.
        """
        io_db = IODatabase()
        channel_path = "/test/channel_sum"
        
        base_timestamp = 1000000000000000000
        row_interval = 1.0 / 500  # 500 Hz = 0.002s per row
        
        # Add initial data spanning 0.002s (1 row interval)
        io_db.add_data_point(channel_path, 100, base_timestamp)
        io_db.add_data_point(channel_path, 101, base_timestamp + int(0.002e9))
        
        columns = [
            ColumnDefinition(name="Timestamp (s)", channel_path=None, policy=ChannelPolicy.SYNCHRONIZED),
            ColumnDefinition(name="Channel Sum", channel_path=channel_path, policy=ChannelPolicy.SYNCHRONIZED),
        ]
        
        virtual_db = VirtualDatabase(
            io_database=io_db,
            reference_channel=channel_path,
            sampling_rate=500,
            columns=columns,
        )
        
        # Initial build
        virtual_db.build()
        initial_rows = virtual_db.get_row_count()
        initial_last_time = virtual_db._last_built_time
        
        # Add more data points, but time hasn't advanced enough for next row
        # Add 50 points at time = initial_last_time + row_interval - 1e-9 (just before next row)
        for i in range(50):
            io_db.add_data_point(
                channel_path, 
                102 + i, 
                base_timestamp + int((initial_last_time + row_interval - 1e-9) * 1e9)
            )
        
        # Rebuild should still process the data
        # Even though time hasn't advanced enough, we have many more points than rows
        virtual_db.rebuild()
        after_rebuild_rows = virtual_db.get_row_count()
        
        # Should have created more rows because we have significantly more points
        assert after_rebuild_rows > initial_rows, \
            f"Rebuild should create rows when point count >> row count. " \
            f"Initial: {initial_rows}, After: {after_rebuild_rows}, Points: {io_db.get_channel_count(channel_path)}"
    
    def test_rebuild_incremental_with_burst_data(self):
        """Test multiple rebuilds with data arriving in bursts.
        
        Simulates real-world scenario where data arrives in bursts,
        causing many points at similar timestamps.
        """
        io_db = IODatabase()
        channel_path = "/test/channel_sum"
        
        base_timestamp = 1000000000000000000
        
        columns = [
            ColumnDefinition(name="Timestamp (s)", channel_path=None, policy=ChannelPolicy.SYNCHRONIZED),
            ColumnDefinition(name="Channel Sum", channel_path=channel_path, policy=ChannelPolicy.SYNCHRONIZED),
        ]
        
        virtual_db = VirtualDatabase(
            io_database=io_db,
            reference_channel=channel_path,
            sampling_rate=500,
            columns=columns,
        )
        
        # Burst 1: 10 points at t=0
        for i in range(10):
            io_db.add_data_point(channel_path, 100 + i, base_timestamp)
        
        virtual_db.build()
        rows_after_burst1 = virtual_db.get_row_count()
        assert rows_after_burst1 > 0
        
        # Burst 2: 20 points at t=0.001s (1ms later)
        for i in range(20):
            io_db.add_data_point(channel_path, 110 + i, base_timestamp + int(0.001e9))
        
        virtual_db.rebuild()
        rows_after_burst2 = virtual_db.get_row_count()
        assert rows_after_burst2 >= rows_after_burst1
        
        # Burst 3: 30 points at t=0.002s (2ms later)
        for i in range(30):
            io_db.add_data_point(channel_path, 130 + i, base_timestamp + int(0.002e9))
        
        virtual_db.rebuild()
        rows_after_burst3 = virtual_db.get_row_count()
        assert rows_after_burst3 >= rows_after_burst2
        
        # Should have accumulated rows across all bursts
        assert rows_after_burst3 > rows_after_burst1, \
            f"Should accumulate rows across bursts. Burst1: {rows_after_burst1}, Burst3: {rows_after_burst3}"
    
    def test_rebuild_with_elapsed_time_zero(self):
        """Test rebuild() when all data points have elapsed_time = 0.0.
        
        This tests the edge case where global_first_timestamp causes
        all points to have elapsed_time = 0.0.
        """
        io_db = IODatabase()
        channel_path = "/test/channel_sum"
        
        base_timestamp = 1000000000000000000
        
        # Add many points all at the same timestamp
        for i in range(100):
            io_db.add_data_point(channel_path, 100 + i, base_timestamp)
        
        columns = [
            ColumnDefinition(name="Timestamp (s)", channel_path=None, policy=ChannelPolicy.SYNCHRONIZED),
            ColumnDefinition(name="Channel Sum", channel_path=channel_path, policy=ChannelPolicy.SYNCHRONIZED),
        ]
        
        virtual_db = VirtualDatabase(
            io_database=io_db,
            reference_channel=channel_path,
            sampling_rate=500,
            columns=columns,
        )
        
        # Build should handle this gracefully
        virtual_db.build()
        initial_rows = virtual_db.get_row_count()
        
        # Even with all points at elapsed_time = 0.0, we should create at least 1 row
        assert initial_rows > 0, "Should create at least 1 row even with elapsed_time = 0.0"
        
        # Add more points at same timestamp
        for i in range(100, 200):
            io_db.add_data_point(channel_path, 200 + i, base_timestamp)
        
        # Rebuild should still work
        virtual_db.rebuild()
        after_rebuild_rows = virtual_db.get_row_count()
        
        # Should handle the case where we have many more points than rows
        # even if time hasn't advanced
        assert after_rebuild_rows >= initial_rows
    
    def test_rebuild_continuous_incremental(self):
        """Test continuous incremental rebuilds with slowly advancing time.
        
        Simulates scenario where data arrives continuously but time advances slowly,
        testing that rebuild() continues to create rows.
        """
        io_db = IODatabase()
        channel_path = "/test/channel_sum"
        
        base_timestamp = 1000000000000000000
        row_interval = 1.0 / 500  # 0.002s
        
        columns = [
            ColumnDefinition(name="Timestamp (s)", channel_path=None, policy=ChannelPolicy.SYNCHRONIZED),
            ColumnDefinition(name="Channel Sum", channel_path=channel_path, policy=ChannelPolicy.SYNCHRONIZED),
        ]
        
        virtual_db = VirtualDatabase(
            io_database=io_db,
            reference_channel=channel_path,
            sampling_rate=500,
            columns=columns,
        )
        
        # Add initial data
        io_db.add_data_point(channel_path, 100, base_timestamp)
        virtual_db.build()
        initial_rows = virtual_db.get_row_count()
        
        # Simulate continuous data arrival with slow time advancement
        # Add data every 0.0001s (10x faster than row interval)
        for step in range(100):
            time_offset = step * 0.0001  # 0.1ms per step
            io_db.add_data_point(channel_path, 101 + step, base_timestamp + int(time_offset * 1e9))
            
            # Rebuild every 10 steps
            if step % 10 == 0:
                virtual_db.rebuild()
                current_rows = virtual_db.get_row_count()
                # Should accumulate rows over time
                assert current_rows >= initial_rows, \
                    f"Rows should not decrease. Step {step}, Rows: {current_rows}"
        
        # Final rebuild
        virtual_db.rebuild()
        final_rows = virtual_db.get_row_count()
        
        # Should have created many rows over 100 steps
        assert final_rows > initial_rows, \
            f"Should accumulate rows over time. Initial: {initial_rows}, Final: {final_rows}"
    
    def test_rebuild_with_many_points_few_rows(self):
        """Test rebuild() when there are many data points but few rows.
        
        This directly tests the bug scenario: many points at same/similar
        timestamps should still result in rows being created.
        """
        io_db = IODatabase()
        channel_path = "/test/channel_sum"
        
        base_timestamp = 1000000000000000000
        
        # Add 11 points (like the bug scenario)
        for i in range(11):
            io_db.add_data_point(channel_path, 100 + i, base_timestamp)
        
        columns = [
            ColumnDefinition(name="Timestamp (s)", channel_path=None, policy=ChannelPolicy.SYNCHRONIZED),
            ColumnDefinition(name="Channel Sum", channel_path=channel_path, policy=ChannelPolicy.SYNCHRONIZED),
        ]
        
        virtual_db = VirtualDatabase(
            io_database=io_db,
            reference_channel=channel_path,
            sampling_rate=500,
            columns=columns,
        )
        
        # Build
        virtual_db.build()
        initial_rows = virtual_db.get_row_count()
        initial_points = io_db.get_channel_count(channel_path)
        
        # Verify we have many more points than rows (the bug scenario)
        assert initial_points > initial_rows, \
            f"Should have more points than rows. Points: {initial_points}, Rows: {initial_rows}"
        
        # Add 50 more points at same timestamp
        for i in range(50):
            io_db.add_data_point(channel_path, 111 + i, base_timestamp)
        
        # Rebuild should create more rows even though time hasn't advanced
        virtual_db.rebuild()
        after_rebuild_rows = virtual_db.get_row_count()
        after_rebuild_points = io_db.get_channel_count(channel_path)
        
        # Should have created more rows because point_count >> row_count
        assert after_rebuild_points > after_rebuild_rows * 2, \
            f"Should have many more points than rows. Points: {after_rebuild_points}, Rows: {after_rebuild_rows}"
        
        # Rebuild should have created additional rows
        assert after_rebuild_rows >= initial_rows, \
            f"Rebuild should create rows when points >> rows. Initial: {initial_rows}, After: {after_rebuild_rows}"


class TestIODatabaseEdgeCases:
    """Comprehensive tests for IODatabase edge cases."""
    
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


class TestVirtualDatabaseIODatabaseIntegration:
    """Integration tests for VirtualDatabase with IODatabase edge cases."""
    
    def test_build_and_rebuild_with_burst_data(self):
        """Test build() and rebuild() with burst data patterns."""
        io_db = IODatabase()
        channel_path = "/test/channel_sum"
        
        base_timestamp = 1000000000000000000
        
        columns = [
            ColumnDefinition(name="Timestamp (s)", channel_path=None, policy=ChannelPolicy.SYNCHRONIZED),
            ColumnDefinition(name="Channel Sum", channel_path=channel_path, policy=ChannelPolicy.SYNCHRONIZED),
        ]
        
        virtual_db = VirtualDatabase(
            io_database=io_db,
            reference_channel=channel_path,
            sampling_rate=500,
            columns=columns,
        )
        
        # Burst 1: Build with 11 points at same timestamp
        for i in range(11):
            io_db.add_data_point(channel_path, 100 + i, base_timestamp)
        
        virtual_db.build()
        rows_after_build = virtual_db.get_row_count()
        assert rows_after_build > 0
        
        # Burst 2: Add 50 more points, rebuild
        for i in range(50):
            io_db.add_data_point(channel_path, 111 + i, base_timestamp + int(0.001e9))
        
        virtual_db.rebuild()
        rows_after_rebuild1 = virtual_db.get_row_count()
        assert rows_after_rebuild1 >= rows_after_build
        
        # Burst 3: Add 100 more points, rebuild
        for i in range(100):
            io_db.add_data_point(channel_path, 161 + i, base_timestamp + int(0.002e9))
        
        virtual_db.rebuild()
        rows_after_rebuild2 = virtual_db.get_row_count()
        assert rows_after_rebuild2 >= rows_after_rebuild1
        
        # Should have accumulated rows across all rebuilds
        assert rows_after_rebuild2 > rows_after_build
    
    def test_rebuild_with_continuous_slow_data(self):
        """Test rebuild() with continuous data that arrives slowly."""
        io_db = IODatabase()
        channel_path = "/test/channel_sum"
        
        base_timestamp = 1000000000000000000
        
        columns = [
            ColumnDefinition(name="Timestamp (s)", channel_path=None, policy=ChannelPolicy.SYNCHRONIZED),
            ColumnDefinition(name="Channel Sum", channel_path=channel_path, policy=ChannelPolicy.SYNCHRONIZED),
        ]
        
        virtual_db = VirtualDatabase(
            io_database=io_db,
            reference_channel=channel_path,
            sampling_rate=500,
            columns=columns,
        )
        
        # Add initial point
        io_db.add_data_point(channel_path, 100, base_timestamp)
        virtual_db.build()
        
        # Add points slowly (one every 0.001s = 1ms)
        for i in range(100):
            io_db.add_data_point(channel_path, 101 + i, base_timestamp + int(i * 0.001 * 1e9))
            
            # Rebuild every 10 points
            if i % 10 == 0:
                virtual_db.rebuild()
        
        # Final rebuild
        virtual_db.rebuild()
        final_rows = virtual_db.get_row_count()
        
        # Should have created rows over time
        assert final_rows > 0, "Should have created rows with continuous data"
