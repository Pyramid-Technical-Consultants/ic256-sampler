"""Test incremental rebuild functionality to verify it doesn't skip rows."""

import pytest
from ic256_sampler.io_database import IODatabase
from ic256_sampler.virtual_database import VirtualDatabase, ColumnDefinition, ChannelPolicy


def test_incremental_rebuild_no_skipped_rows():
    """Test that incremental rebuild doesn't skip rows."""
    io_db = IODatabase()
    channel_path = "/test/channel_sum"
    sampling_rate = 10  # 10 Hz = 0.1 second intervals
    
    # Add data points over 2 seconds
    base_timestamp = 1000000000000000000
    for i in range(21):  # 0 to 20 = 2 seconds at 0.1s intervals
        timestamp = base_timestamp + int(i * 0.1 * 1e9)
        io_db.add_data_point(channel_path, 100 + i, timestamp)
    
    columns = [
        ColumnDefinition(name="Timestamp (s)", channel_path=None, policy=ChannelPolicy.SYNCHRONIZED),
        ColumnDefinition(name="Channel Sum", channel_path=channel_path, policy=ChannelPolicy.INTERPOLATED),
    ]
    
    virtual_db = VirtualDatabase(
        io_database=io_db,
        reference_channel=channel_path,
        sampling_rate=sampling_rate,
        columns=columns,
    )
    
    # First build - should build rows for first second
    # Simulate: build first second, then add more data
    virtual_db.build()
    initial_count = len(virtual_db.rows)
    initial_last_time = virtual_db._last_built_time
    
    # Verify we got rows
    assert initial_count > 0, "Should have built some rows"
    assert initial_last_time is not None, "Should have set last built time"
    
    # Add more data points (simulating continued collection)
    for i in range(21, 31):  # Add 1 more second of data
        timestamp = base_timestamp + int(i * 0.1 * 1e9)
        io_db.add_data_point(channel_path, 100 + i, timestamp)
    
    # Rebuild incrementally
    virtual_db.rebuild()
    new_count = len(virtual_db.rows)
    new_last_time = virtual_db._last_built_time
    
    # Verify we got more rows
    assert new_count > initial_count, "Should have built more rows incrementally"
    assert new_last_time > initial_last_time, "Last built time should have advanced"
    
    # Verify no gaps in row timestamps
    row_times = [row.timestamp for row in virtual_db.rows]
    row_interval = 1.0 / sampling_rate
    
    for i in range(1, len(row_times)):
        expected_diff = row_interval
        actual_diff = row_times[i] - row_times[i-1]
        # Allow small floating point error
        assert abs(actual_diff - expected_diff) < 1e-9, \
            f"Gap between rows {i-1} and {i}: expected {expected_diff}, got {actual_diff}"
    
    print(f"\nIncremental Rebuild Test:")
    print(f"  Initial rows: {initial_count}")
    print(f"  Final rows: {new_count}")
    print(f"  Rows added: {new_count - initial_count}")
    print(f"  Initial last time: {initial_last_time}")
    print(f"  Final last time: {new_last_time}")
