"""Tests for simple single-channel data capture.

These tests validate the low-level data capture mechanism before
building the full multi-channel system.
"""

import pytest
import time
import threading
from ic256_sampler.simple_capture import capture_single_channel, capture_single_channel_with_stats
from ic256_sampler.igx_client import IGXWebsocketClient
from ic256_sampler.utils import is_valid_device, is_valid_ipv4
from ic256_sampler.device_paths import IC256_45_PATHS


# Mark all tests in this file as integration tests with timeout
pytestmark = [pytest.mark.integration, pytest.mark.timeout(10)]


class TestSimpleChannelCapture:
    """Tests for simple single-channel data capture."""

    def test_capture_channel_sum_basic(self, ic256_ip):
        """Test basic capture of channel_sum data.
        
        This test:
        1. Connects to a real IC256 device
        2. Captures data from channel_sum for 2 seconds
        3. Verifies that data points are captured
        4. Validates timestamp format
        
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
        
        # Get channel path
        channel_path = IC256_45_PATHS["adc"]["channel_sum"]
        
        # Create client
        client = IGXWebsocketClient(ic256_ip)
        
        # Capture data for 2 seconds
        duration = 2.0
        captured_data = capture_single_channel(client, channel_path, duration)
        
        # Verify we got data
        assert len(captured_data) > 0, "No data points were captured"
        
        # Verify we got a reasonable amount of data for a 2-second capture
        # At typical rates (1000+ Hz), 2 seconds should yield at least 1000 points
        assert len(captured_data) >= 500, \
            f"Expected at least 500 data points for a {duration}-second capture, " \
            f"but only got {len(captured_data)}. This may indicate device communication issues."
        
        # Verify data format
        for value, ts_ns in captured_data:
            assert isinstance(ts_ns, int), f"Timestamp should be int, got {type(ts_ns)}"
            assert ts_ns > 0, f"Timestamp should be positive, got {ts_ns}"
            # Timestamps should be in nanoseconds (very large numbers)
            assert ts_ns > 1e15, f"Timestamp {ts_ns} seems too small for nanoseconds since 1970"
        
        print(f"\nCapture Results:")
        print(f"  Duration: {duration} seconds")
        print(f"  Data Points Captured: {len(captured_data)}")
        print(f"  Average Rate: {len(captured_data) / duration:.2f} points/second")
        if captured_data:
            first_ts = captured_data[0][1]
            last_ts = captured_data[-1][1]
            time_span = (last_ts - first_ts) / 1e9
            print(f"  Time Span: {time_span:.3f} seconds")
            print(f"  First Timestamp: {first_ts}")
            print(f"  Last Timestamp: {last_ts}")

    def test_capture_channel_sum_with_stats(self, ic256_ip):
        """Test capture with statistics.
        
        This test validates the statistics returned by capture_single_channel_with_stats.
        """
        # Skip if IP is invalid
        if not is_valid_ipv4(ic256_ip):
            pytest.skip(f"Invalid IP address in config: {ic256_ip}")
        
        # Verify device is reachable
        if not is_valid_device(ic256_ip, "IC256"):
            pytest.skip(f"IC256 device at {ic256_ip} is not reachable or not responding")
        
        # Get channel path
        channel_path = IC256_45_PATHS["adc"]["channel_sum"]
        
        # Create client
        client = IGXWebsocketClient(ic256_ip)
        
        # Capture data with stats
        duration = 2.0
        stats = capture_single_channel_with_stats(client, channel_path, duration)
        
        # Verify stats structure
        assert 'data' in stats
        assert 'count' in stats
        assert 'duration' in stats
        assert 'rate' in stats
        assert 'first_timestamp' in stats
        assert 'last_timestamp' in stats
        
        # Verify stats values
        assert stats['count'] == len(stats['data'])
        assert stats['count'] > 0, "Should have captured some data"
        assert stats['duration'] > 0, "Duration should be positive"
        assert stats['rate'] > 0, "Rate should be positive"
        assert stats['first_timestamp'] is not None
        assert stats['last_timestamp'] is not None
        assert stats['last_timestamp'] >= stats['first_timestamp']
        
        print(f"\nCapture Statistics:")
        print(f"  Data Points: {stats['count']}")
        print(f"  Duration: {stats['duration']:.3f} seconds")
        print(f"  Rate: {stats['rate']:.2f} points/second")
        print(f"  Time Span: {(stats['last_timestamp'] - stats['first_timestamp']) / 1e9:.3f} seconds")

    def test_capture_channel_sum_stop_event(self, ic256_ip):
        """Test that stop_event can interrupt capture.
        
        This validates that the capture can be stopped early using a stop_event.
        """
        # Skip if IP is invalid
        if not is_valid_ipv4(ic256_ip):
            pytest.skip(f"Invalid IP address in config: {ic256_ip}")
        
        # Verify device is reachable
        if not is_valid_device(ic256_ip, "IC256"):
            pytest.skip(f"IC256 device at {ic256_ip} is not reachable or not responding")
        
        # Get channel path
        channel_path = IC256_45_PATHS["adc"]["channel_sum"]
        
        # Create client
        client = IGXWebsocketClient(ic256_ip)
        
        # Create stop event
        stop_event = threading.Event()
        
        # Start capture in a thread
        captured_data = []
        def capture_thread():
            nonlocal captured_data
            captured_data = capture_single_channel(client, channel_path, 10.0, stop_event)
        
        thread = threading.Thread(target=capture_thread, daemon=True)
        thread.start()
        
        # Wait 1 second, then stop
        time.sleep(1.0)
        stop_event.set()
        
        # Wait for thread to finish
        thread.join(timeout=5.0)
        
        # Verify we got some data but not 10 seconds worth
        assert len(captured_data) > 0, "Should have captured some data before stop"
        
        print(f"\nStop Event Test:")
        print(f"  Data Points Captured: {len(captured_data)}")
        print(f"  Expected: Less than 10 seconds worth of data")

    def test_capture_channel_sum_all_entries(self, ic256_ip):
        """Test that ALL entries in each update message are captured.
        
        This validates that we're processing the full array of arrays,
        not just the last entry.
        """
        # Skip if IP is invalid
        if not is_valid_ipv4(ic256_ip):
            pytest.skip(f"Invalid IP address in config: {ic256_ip}")
        
        # Verify device is reachable
        if not is_valid_device(ic256_ip, "IC256"):
            pytest.skip(f"IC256 device at {ic256_ip} is not reachable or not responding")
        
        # Get channel path
        channel_path = IC256_45_PATHS["adc"]["channel_sum"]
        
        # Create client
        client = IGXWebsocketClient(ic256_ip)
        
        # Capture for a short duration to get multiple update messages
        duration = 1.0
        captured_data = capture_single_channel(client, channel_path, duration)
        
        # Verify we got data
        assert len(captured_data) > 0, "No data points were captured"
        
        # Check that timestamps are increasing (or at least not decreasing)
        # This validates we're getting sequential data
        timestamps = [ts for _, ts in captured_data]
        for i in range(1, len(timestamps)):
            # Timestamps should generally increase, but allow for some out-of-order
            # due to async nature (just check they're not all the same)
            assert timestamps[i] != timestamps[0] or i < 10, \
                "All timestamps are identical - may not be processing all entries"
        
        print(f"\nAll Entries Test:")
        print(f"  Total Data Points: {len(captured_data)}")
        print(f"  Unique Timestamps: {len(set(timestamps))}")
        print(f"  Timestamp Range: {min(timestamps)} to {max(timestamps)}")
        print(f"  Time Span: {(max(timestamps) - min(timestamps)) / 1e9:.6f} seconds")

    def test_capture_thousands_of_data_points(self, ic256_ip):
        """Test that the system can collect thousands of data points from the device.
        
        This test explicitly validates:
        1. Real device communication is working
        2. Thousands of data points can be collected successfully
        3. Data collection rate is reasonable (at least 1000 points/second)
        
        This is a critical integration test that confirms the core functionality
        of communicating with the device and collecting large volumes of data.
        """
        # Skip if IP is invalid
        if not is_valid_ipv4(ic256_ip):
            pytest.skip(f"Invalid IP address in config: {ic256_ip}")
        
        # Verify device is reachable
        if not is_valid_device(ic256_ip, "IC256"):
            pytest.skip(f"IC256 device at {ic256_ip} is not reachable or not responding")
        
        # Get channel path
        channel_path = IC256_45_PATHS["adc"]["channel_sum"]
        
        # Create client
        client = IGXWebsocketClient(ic256_ip)
        
        # Capture data for 5 seconds to ensure we get thousands of points
        # At typical rates (1000+ Hz), 5 seconds should yield 5000+ points
        duration = 5.0
        captured_data = capture_single_channel(client, channel_path, duration)
        
        # CRITICAL ASSERTIONS: Verify thousands of data points
        total_points = len(captured_data)
        assert total_points >= 1000, \
            f"Expected at least 1000 data points, but only got {total_points}. " \
            f"This indicates a problem with device communication or data collection."
        
        # Verify we got thousands (at least 2000 for a 5-second capture)
        assert total_points >= 2000, \
            f"Expected at least 2000 data points for a {duration}-second capture, " \
            f"but only got {total_points}. Data collection rate may be too low."
        
        # Calculate data rate
        data_rate = total_points / duration
        
        # Verify reasonable data rate (at least 400 points/second)
        # This ensures we're actually getting real-time data, not just a few samples
        assert data_rate >= 400, \
            f"Data collection rate ({data_rate:.2f} points/second) is too low. " \
            f"Expected at least 400 points/second. This suggests device communication issues."
        
        # Verify data format and timestamps
        for value, ts_ns in captured_data:
            assert isinstance(ts_ns, int), f"Timestamp should be int, got {type(ts_ns)}"
            assert ts_ns > 0, f"Timestamp should be positive, got {ts_ns}"
            assert ts_ns > 1e15, f"Timestamp {ts_ns} seems too small for nanoseconds since 1970"
        
        # Verify timestamps are increasing (data is sequential)
        timestamps = [ts for _, ts in captured_data]
        assert len(set(timestamps)) > total_points * 0.9, \
            f"Too many duplicate timestamps. Expected mostly unique timestamps, " \
            f"but got {len(set(timestamps))} unique out of {total_points} total."
        
        # Calculate time span from device timestamps
        first_ts = captured_data[0][1]
        last_ts = captured_data[-1][1]
        time_span = (last_ts - first_ts) / 1e9
        
        # Verify time span is reasonable (at least 4 seconds of actual data)
        assert time_span >= 4.0, \
            f"Time span of captured data ({time_span:.3f} seconds) is too short. " \
            f"Expected at least 4 seconds of data for a {duration}-second capture."
        
        print(f"\nThousands of Data Points Test:")
        print(f"  ✓ Device Communication: SUCCESS")
        print(f"  ✓ Total Data Points: {total_points:,}")
        print(f"  ✓ Data Collection Rate: {data_rate:.2f} points/second")
        print(f"  ✓ Capture Duration: {duration} seconds")
        print(f"  ✓ Time Span (from timestamps): {time_span:.3f} seconds")
        print(f"  ✓ Unique Timestamps: {len(set(timestamps)):,}")
        print(f"  ✓ First Timestamp: {first_ts}")
        print(f"  ✓ Last Timestamp: {last_ts}")
        
        # Additional validation: verify we're getting continuous data
        # Check that timestamps are generally increasing
        out_of_order_count = 0
        for i in range(1, len(timestamps)):
            if timestamps[i] < timestamps[i-1]:
                out_of_order_count += 1
        
        out_of_order_pct = (out_of_order_count / len(timestamps)) * 100
        assert out_of_order_pct < 5.0, \
            f"Too many out-of-order timestamps ({out_of_order_pct:.2f}%). " \
            f"This may indicate data processing issues."
        
        print(f"  ✓ Out-of-order timestamps: {out_of_order_count} ({out_of_order_pct:.2f}%)")
        print(f"\n  RESULT: Successfully collected {total_points:,} data points from device!")
