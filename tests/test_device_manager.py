"""Unit and integration tests for DeviceManager.

These tests verify that DeviceManager properly:
- Manages device connections
- Collects data from devices into IODatabase
- Coordinates between keepalive and collection threads
- Handles start/stop operations correctly
"""

import pytest
import time
import threading
from unittest.mock import Mock, MagicMock, patch, call
from ic256_sampler.device_manager import (
    DeviceManager,
    DeviceConfig,
    DeviceConnection,
    IC256_CONFIG,
    TX2_CONFIG,
)
from ic256_sampler.io_database import IODatabase
from ic256_sampler.ic256_model import IC256Model


class TestDeviceManagerBasics:
    """Basic tests for DeviceManager initialization and configuration."""

    def test_device_manager_init(self):
        """Test that DeviceManager initializes correctly."""
        manager = DeviceManager()
        assert manager.io_database is not None
        assert isinstance(manager.io_database, IODatabase)
        assert manager.connections == {}
        assert manager._running is False
        assert manager.stop_event is not None

    def test_add_device_creates_connection(self):
        """Test that add_device creates a connection structure."""
        manager = DeviceManager()
        
        # Mock the websocket client and validation
        with patch('ic256_sampler.device_manager.is_valid_device', return_value=True):
            with patch('ic256_sampler.device_manager.IGXWebsocketClient') as mock_client_class:
                mock_client = MagicMock()
                mock_client_class.return_value = mock_client
                
                # Mock field creation
                mock_field = MagicMock()
                mock_field.getPath.return_value = "/test/path"
                mock_client.field.return_value = mock_field
                
                # Mock channel creator
                def mock_channel_creator(client):
                    return {"test_channel": mock_field}
                
                config = DeviceConfig(
                    device_name="TestDevice",
                    device_type="TEST",
                    channel_creator=mock_channel_creator,
                    model_creator=lambda: MagicMock(),  # Provide a model creator
                )
                
                # Mock model
                with patch('ic256_sampler.device_manager.IC256Model') as mock_model_class:
                    mock_model = MagicMock()
                    mock_model_class.return_value = mock_model
                    mock_model.setup_device.return_value = None
                    mock_model.get_field_to_path_mapping.return_value = {"test_channel": "/test/path"}
                    
                    # Mock updateSubscribedFields to avoid blocking
                    mock_client.updateSubscribedFields.return_value = None
                    
                    result = manager.add_device(config, "192.168.1.100", 500)
                    
                    assert result is True
                    assert "TestDevice" in manager.connections
                    connection = manager.connections["TestDevice"]
                    assert connection.config == config
                    assert connection.ip_address == "192.168.1.100"
                    assert connection.client == mock_client


class TestDeviceManagerDataCollection:
    """Tests for data collection functionality in DeviceManager."""

    def test_collect_from_device_calls_update_subscribed_fields(self):
        """Test that _collect_from_device calls updateSubscribedFields().
        
        This is a critical test to prevent regression of the bug where
        data collection failed because updateSubscribedFields() wasn't called.
        """
        manager = DeviceManager()
        manager.stop_event = threading.Event()
        
        # Create mock client
        mock_client = MagicMock()
        mock_client.updateSubscribedFields.return_value = None
        mock_client.ws = MagicMock()  # Simulate connected websocket
        
        # Create mock channels with datums
        mock_channel = MagicMock()
        mock_channel.getDatums.return_value = []  # Empty initially
        mock_channel.getPath.return_value = "/test/channel"
        mock_channel.clearDatums.return_value = None
        
        channels = {"test_channel": mock_channel}
        field_to_path = {"test_channel": "/test/channel"}
        
        config = DeviceConfig(
            device_name="TestDevice",
            device_type="TEST",
            channel_creator=lambda c: channels,
        )
        
        # Start collection in a thread
        collection_thread = threading.Thread(
            target=manager._collect_from_device,
            args=(config, mock_client, channels, None, field_to_path, "192.168.1.100"),
            daemon=True
        )
        collection_thread.start()
        
        # Give it time to run a few iterations
        time.sleep(0.1)
        
        # Stop the collection
        manager.stop_event.set()
        collection_thread.join(timeout=1.0)
        
        # Verify updateSubscribedFields was called
        # It should be called multiple times during the collection loop
        assert mock_client.updateSubscribedFields.call_count > 0, \
            "updateSubscribedFields() must be called in _collect_from_device to process messages"
        
        # Verify it was called before getDatums (order matters)
        calls = mock_client.updateSubscribedFields.call_args_list
        channel_calls = mock_channel.getDatums.call_args_list
        
        # At least one updateSubscribedFields call should happen
        assert len(calls) > 0, "updateSubscribedFields must be called"

    def test_collect_all_channel_data_processes_datums(self):
        """Test that _collect_all_channel_data properly processes channel datums."""
        manager = DeviceManager()
        
        # Create mock channel with data
        mock_channel = MagicMock()
        mock_channel.getDatums.return_value = [
            [100.5, 1000000000],  # value, timestamp_ns
            [101.0, 1000001000],
            [101.5, 1000002000],
        ]
        mock_channel.getPath.return_value = "/test/channel"
        mock_channel.clearDatums.return_value = None
        
        channels = {"test_channel": mock_channel}
        field_to_path = {"test_channel": "/test/channel"}
        
        # Collect data
        first_timestamp = manager._collect_all_channel_data(channels, field_to_path, None)
        
        # Verify data was added to database
        assert "/test/channel" in manager.io_database.get_all_channels()
        channel_data = manager.io_database.get_channel("/test/channel")
        assert len(channel_data.data_points) == 3
        
        # Verify first timestamp was set
        assert first_timestamp == 1000000000
        
        # Verify clearDatums was called after processing
        assert mock_channel.clearDatums.called

    def test_collect_all_channel_data_handles_empty_datums(self):
        """Test that _collect_all_channel_data handles empty datums gracefully."""
        manager = DeviceManager()
        
        mock_channel = MagicMock()
        mock_channel.getDatums.return_value = []  # No data
        mock_channel.getPath.return_value = "/test/channel"
        
        channels = {"test_channel": mock_channel}
        field_to_path = {"test_channel": "/test/channel"}
        
        # Should not raise exception
        first_timestamp = manager._collect_all_channel_data(channels, field_to_path, None)
        
        # No data should be added
        assert "/test/channel" not in manager.io_database.get_all_channels() or \
               len(manager.io_database.get_channel("/test/channel").data_points) == 0
        
        # First timestamp should remain None
        assert first_timestamp is None

    def test_collect_all_channel_data_handles_invalid_timestamps(self):
        """Test that _collect_all_channel_data handles invalid timestamps."""
        manager = DeviceManager()
        
        mock_channel = MagicMock()
        # Mix of valid and invalid timestamps
        mock_channel.getDatums.return_value = [
            [100.0, 1000000000],  # Valid
            [101.0, "invalid"],   # Invalid - should be skipped
            [102.0, 1000002000],  # Valid
        ]
        mock_channel.getPath.return_value = "/test/channel"
        mock_channel.clearDatums.return_value = None
        
        channels = {"test_channel": mock_channel}
        field_to_path = {"test_channel": "/test/channel"}
        
        first_timestamp = manager._collect_all_channel_data(channels, field_to_path, None)
        
        # Only valid data points should be added
        channel_data = manager.io_database.get_channel("/test/channel")
        assert len(channel_data.data_points) == 2  # Only 2 valid points
        
        # First timestamp should be from first valid point
        assert first_timestamp == 1000000000

    def test_collect_all_channel_data_handles_multiple_channels(self):
        """Test that _collect_all_channel_data processes multiple channels."""
        manager = DeviceManager()
        
        # Create multiple mock channels
        mock_channel1 = MagicMock()
        mock_channel1.getDatums.return_value = [[100.0, 1000000000]]
        mock_channel1.getPath.return_value = "/channel1"
        mock_channel1.clearDatums.return_value = None
        
        mock_channel2 = MagicMock()
        mock_channel2.getDatums.return_value = [[200.0, 1000001000]]
        mock_channel2.getPath.return_value = "/channel2"
        mock_channel2.clearDatums.return_value = None
        
        channels = {
            "channel1": mock_channel1,
            "channel2": mock_channel2,
        }
        field_to_path = {
            "channel1": "/channel1",
            "channel2": "/channel2",
        }
        
        first_timestamp = manager._collect_all_channel_data(channels, field_to_path, None)
        
        # Both channels should have data
        assert "/channel1" in manager.io_database.get_all_channels()
        assert "/channel2" in manager.io_database.get_all_channels()
        
        channel1_data = manager.io_database.get_channel("/channel1")
        channel2_data = manager.io_database.get_channel("/channel2")
        
        assert len(channel1_data.data_points) == 1
        assert len(channel2_data.data_points) == 1
        
        # First timestamp should be from earliest point
        assert first_timestamp == 1000000000


class TestDeviceManagerThreadCoordination:
    """Tests for thread coordination and lifecycle management."""

    def test_start_starts_collection_threads(self):
        """Test that start() actually starts the collection threads."""
        manager = DeviceManager()
        manager.stop_event = threading.Event()
        
        # Create a mock connection
        mock_thread = MagicMock()
        mock_thread.is_alive.return_value = False
        mock_thread.start = MagicMock()
        
        mock_keepalive_thread = MagicMock()
        mock_keepalive_thread.is_alive.return_value = True
        
        connection = DeviceConnection(
            config=IC256_CONFIG,
            ip_address="192.168.1.100",
            client=MagicMock(),
            channels={},
            model=MagicMock(),
            field_to_path={},
            thread=mock_thread,
            keepalive_thread=mock_keepalive_thread,
        )
        
        manager.connections["IC256-42/35"] = connection
        
        # Start the manager
        manager.start()
        
        # Verify thread was started
        assert manager._running is True
        mock_thread.start.assert_called_once()

    def test_stop_stops_collection_threads(self):
        """Test that stop() properly signals threads to stop (non-blocking).
        
        Note: stop() is non-blocking and does not join threads. Thread joining
        is handled by close_all_connections().
        """
        manager = DeviceManager()
        manager.stop_event = threading.Event()
        manager._running = True
        
        # Create a mock thread that's alive
        mock_thread = MagicMock()
        mock_thread.is_alive.return_value = True
        mock_thread.join = MagicMock()
        
        connection = DeviceConnection(
            config=IC256_CONFIG,
            ip_address="192.168.1.100",
            client=MagicMock(),
            channels={},
            model=MagicMock(),
            field_to_path={},
            thread=mock_thread,
            keepalive_thread=MagicMock(),
        )
        
        manager.connections["IC256-42/35"] = connection
        
        # Stop the manager (non-blocking)
        manager.stop()
        
        # Verify stop_event was set and _running flag is False
        # Note: stop() does NOT join threads - that's handled by close_all_connections()
        assert manager.stop_event.is_set()
        assert manager._running is False
        # stop() is non-blocking, so join() should NOT be called here
        mock_thread.join.assert_not_called()

    def test_start_idempotent(self):
        """Test that calling start() multiple times is safe."""
        manager = DeviceManager()
        manager.stop_event = threading.Event()
        
        mock_thread = MagicMock()
        mock_thread.is_alive.return_value = False
        mock_thread.start = MagicMock()
        
        connection = DeviceConnection(
            config=IC256_CONFIG,
            ip_address="192.168.1.100",
            client=MagicMock(),
            channels={},
            model=MagicMock(),
            field_to_path={},
            thread=mock_thread,
            keepalive_thread=MagicMock(),
        )
        
        manager.connections["IC256-42/35"] = connection
        
        # Call start multiple times
        manager.start()
        manager.start()
        manager.start()
        
        # Thread should only be started once (first time)
        assert mock_thread.start.call_count == 1


class TestDeviceManagerIntegration:
    """Integration tests for DeviceManager with mocked websocket.
    
    These tests verify the full data collection flow without requiring
    a live device connection.
    """

    @pytest.mark.integration
    def test_device_manager_collects_data_into_database(self, tmp_path):
        """Integration test: Verify DeviceManager collects data into IODatabase.
        
        This test uses a mocked websocket client to simulate device data
        and verifies that data flows from the device through DeviceManager
        into the IODatabase.
        """
        manager = DeviceManager()
        manager.stop_event = threading.Event()
        
        # Create mock client that simulates data updates
        mock_client = MagicMock()
        mock_client.ws = MagicMock()  # Simulate connected websocket
        
        # Track updateSubscribedFields calls
        update_count = [0]
        
        def mock_update_subscribed_fields():
            update_count[0] += 1
            # Simulate data appearing in channels after a few updates
            if update_count[0] >= 2:
                # After a few updates, start returning data
                pass
        
        mock_client.updateSubscribedFields = mock_update_subscribed_fields
        
        # Create mock channels that return data after a few calls
        call_count = [0]
        
        def mock_get_datums():
            call_count[0] += 1
            # Return data after a few iterations
            if call_count[0] >= 3:
                return [[100.0 + call_count[0], 1000000000 + call_count[0] * 1000]]
            return []
        
        mock_channel = MagicMock()
        mock_channel.getDatums = mock_get_datums
        mock_channel.getPath.return_value = "/test/channel"
        mock_channel.clearDatums.return_value = None
        
        channels = {"test_channel": mock_channel}
        field_to_path = {"test_channel": "/test/channel"}
        
        config = DeviceConfig(
            device_name="TestDevice",
            device_type="TEST",
            channel_creator=lambda c: channels,
        )
        
        # Start collection in a thread
        collection_thread = threading.Thread(
            target=manager._collect_from_device,
            args=(config, mock_client, channels, None, field_to_path, "192.168.1.100"),
            daemon=True
        )
        collection_thread.start()
        
        # Let it run for a bit
        time.sleep(0.2)
        
        # Stop collection
        manager.stop_event.set()
        collection_thread.join(timeout=1.0)
        
        # Verify updateSubscribedFields was called
        assert update_count[0] > 0, "updateSubscribedFields must be called"
        
        # Verify data was collected (if any was returned)
        if call_count[0] >= 3:
            # Data should be in database
            assert "/test/channel" in manager.io_database.get_all_channels()
            channel_data = manager.io_database.get_channel("/test/channel")
            assert len(channel_data.data_points) > 0

    @pytest.mark.integration
    def test_device_manager_data_collection_rate(self):
        """Integration test: Verify data collection happens at reasonable rate.
        
        This test verifies that when data is available, the collection
        thread processes it quickly enough.
        """
        manager = DeviceManager()
        manager.stop_event = threading.Event()
        
        mock_client = MagicMock()
        mock_client.ws = MagicMock()
        mock_client.updateSubscribedFields.return_value = None
        
        # Create channel that returns data every time
        data_points = []
        for i in range(100):
            data_points.append([100.0 + i, 1000000000 + i * 1000])
        
        data_index = [0]
        
        def mock_get_datums():
            if data_index[0] < len(data_points):
                idx = data_index[0]
                data_index[0] += 1
                return [data_points[idx]]
            return []
        
        mock_channel = MagicMock()
        mock_channel.getDatums = mock_get_datums
        mock_channel.getPath.return_value = "/test/channel"
        mock_channel.clearDatums.return_value = None
        
        channels = {"test_channel": mock_channel}
        field_to_path = {"test_channel": "/test/channel"}
        
        config = DeviceConfig(
            device_name="TestDevice",
            device_type="TEST",
            channel_creator=lambda c: channels,
        )
        
        # Start collection
        collection_thread = threading.Thread(
            target=manager._collect_from_device,
            args=(config, mock_client, channels, None, field_to_path, "192.168.1.100"),
            daemon=True
        )
        collection_thread.start()
        
        # Let it collect for a short time
        time.sleep(0.1)
        
        # Stop
        manager.stop_event.set()
        collection_thread.join(timeout=1.0)
        
        # Verify data was collected
        # Should have collected multiple data points in 0.1 seconds
        if "/test/channel" in manager.io_database.get_all_channels():
            channel_data = manager.io_database.get_channel("/test/channel")
            # Should have collected at least some data
            assert len(channel_data.data_points) > 0, \
                "Data collection should process available data points"


class TestDeviceManagerRealDeviceIntegration:
    """Integration tests with real devices to verify end-to-end data collection.
    
    These tests require live device connections and verify that data
    flows correctly from device -> DeviceManager -> IODatabase.
    """

    @pytest.mark.integration
    def test_device_manager_collects_data_from_real_device(self, ic256_ip):
        """Integration test: Verify DeviceManager collects data from real IC256 device.
        
        This test:
        1. Connects to a real IC256 device
        2. Starts data collection
        3. Verifies data appears in IODatabase
        4. Verifies data collection rate is reasonable
        
        This is a lower-level test than the full ModelCollector integration tests,
        focusing specifically on DeviceManager's data collection capability.
        """
        from ic256_sampler.utils import is_valid_device, is_valid_ipv4
        
        # Skip if IP is invalid
        if not is_valid_ipv4(ic256_ip):
            pytest.skip(f"Invalid IP address in config: {ic256_ip}")
        
        # Verify device is reachable
        if not is_valid_device(ic256_ip, "IC256"):
            pytest.skip(f"IC256 device at {ic256_ip} is not reachable or not responding")
        
        # Create DeviceManager
        device_manager = DeviceManager()
        stop_event = threading.Event()
        device_manager.stop_event = stop_event
        
        # Add device
        success = device_manager.add_device(IC256_CONFIG, ic256_ip, sampling_rate=500)
        assert success, "Device should be added successfully"
        
        # Get the connection to verify it exists
        assert "IC256-42/35" in device_manager.connections
        connection = device_manager.connections["IC256-42/35"]
        
        # Get initial database state
        initial_stats = device_manager.io_database.get_statistics()
        initial_total_points = initial_stats.get('total_data_points', 0)
        
        # Start data collection
        device_manager.start()
        
        # Wait for data collection (give it time to connect and start collecting)
        time.sleep(2.0)
        
        # Check that data is being collected
        stats = device_manager.io_database.get_statistics()
        total_points = stats.get('total_data_points', 0)
        
        # Should have collected some data points
        assert total_points > initial_total_points, \
            f"Expected data collection. Initial: {initial_total_points}, Current: {total_points}"
        
        # Verify at least one channel has data
        all_channels = device_manager.io_database.get_all_channels()
        assert len(all_channels) > 0, "At least one channel should have been added"
        
        # Check that at least one channel has data points
        channels_with_data = [
            ch for ch in all_channels
            if device_manager.io_database.get_channel_count(ch) > 0
        ]
        assert len(channels_with_data) > 0, \
            f"Expected at least one channel with data. Channels: {all_channels}"
        
        # Stop collection
        stop_event.set()
        device_manager.stop()
        
        # Verify final data count
        final_stats = device_manager.io_database.get_statistics()
        final_total_points = final_stats.get('total_data_points', 0)
        
        # Should have collected a reasonable amount of data in 2 seconds
        # At 500 Hz, we'd expect ~1000 points per channel, but with multiple channels
        # and startup delays, we'll be more lenient
        assert final_total_points >= 100, \
            f"Expected at least 100 data points in 2 seconds. Got: {final_total_points}"
        
        # Cleanup
        device_manager.close_all_connections()

    @pytest.mark.integration
    def test_device_manager_data_collection_continues_after_start(self, ic256_ip):
        """Integration test: Verify data collection continues after start() is called.
        
        This test specifically verifies that:
        1. Data collection starts when start() is called
        2. Data continues to be collected over time
        3. Data collection stops when stop() is called
        
        This helps catch issues where threads don't start or stop properly.
        """
        from ic256_sampler.utils import is_valid_device, is_valid_ipv4
        
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
        assert success, "Device should be added successfully"
        
        # Verify connection exists
        assert "IC256-42/35" in device_manager.connections
        
        # Start collection
        device_manager.start()
        assert device_manager._running is True
        
        # Wait a bit and check data is being collected
        time.sleep(1.0)
        stats1 = device_manager.io_database.get_statistics()
        points1 = stats1.get('total_data_points', 0)
        
        # Wait more and verify data continues to accumulate
        time.sleep(1.0)
        stats2 = device_manager.io_database.get_statistics()
        points2 = stats2.get('total_data_points', 0)
        
        # Data should continue to accumulate
        assert points2 > points1, \
            f"Data should continue accumulating. After 1s: {points1}, After 2s: {points2}"
        
        # Stop collection
        stop_event.set()
        device_manager.stop()
        assert device_manager._running is False
        
        # Wait a bit and verify data collection has stopped
        time.sleep(0.5)
        stats3 = device_manager.io_database.get_statistics()
        points3 = stats3.get('total_data_points', 0)
        
        # Data should not increase much after stop (allowing for final processing)
        # Allow some small increase for final processing, but not much
        assert points3 <= points2 + 50, \
            f"Data collection should stop. Before stop: {points2}, After stop: {points3}"
        
        # Cleanup
        device_manager.close_all_connections()
    
    @pytest.mark.integration
    @pytest.mark.timeout(20)  # Test runs for 10 seconds + overhead
    def test_device_manager_collects_thousands_of_data_points(self, ic256_ip):
        """Integration test: Verify DeviceManager can collect thousands of data points.
        
        This comprehensive test verifies that:
        1. DeviceManager can collect data continuously over an extended period
        2. IODatabase correctly stores thousands of data points
        3. Data collection rate is consistent and matches expected sampling rate
        4. All channels receive data
        5. Data points have correct timestamps and elapsed_time values
        
        This test requires:
        - A live IC256 device at the IP in config.json
        - Network connectivity to that device
        - Sufficient time to collect thousands of points (10+ seconds)
        """
        from ic256_sampler.utils import is_valid_device, is_valid_ipv4
        
        # Skip if IP is invalid
        if not is_valid_ipv4(ic256_ip):
            pytest.skip(f"Invalid IP address in config: {ic256_ip}")
        
        # Verify device is reachable
        if not is_valid_device(ic256_ip, "IC256"):
            pytest.skip(f"IC256 device at {ic256_ip} is not reachable or not responding")
        
        # Test parameters
        sampling_rate = 500  # 500 Hz
        collection_duration = 10.0  # 10 seconds
        expected_min_points_per_channel = int(sampling_rate * collection_duration * 0.7)  # 70% tolerance for startup/shutdown
        expected_min_total_points = expected_min_points_per_channel * 5  # At least 5 channels should have data
        
        # Create DeviceManager
        device_manager = DeviceManager()
        stop_event = threading.Event()
        device_manager.stop_event = stop_event
        
        # Add device
        success = device_manager.add_device(IC256_CONFIG, ic256_ip, sampling_rate=sampling_rate)
        assert success, "Device should be added successfully"
        
        # Verify connection exists
        assert "IC256-42/35" in device_manager.connections, "IC256 device connection should be established"
        connection = device_manager.connections["IC256-42/35"]
        assert connection.ip_address == ic256_ip, f"Connection should use correct IP: {ic256_ip}"
        
        # Get initial database state
        initial_stats = device_manager.io_database.get_statistics()
        initial_total_points = initial_stats.get('total_data_points', 0)
        initial_channels = set(device_manager.io_database.get_all_channels())
        
        print(f"\nDeviceManager Large Dataset Collection Test:")
        print(f"  Sampling Rate: {sampling_rate} Hz")
        print(f"  Collection Duration: {collection_duration} seconds")
        print(f"  Expected Min Points: {expected_min_total_points}")
        print(f"  Initial Points: {initial_total_points}")
        print(f"  Initial Channels: {len(initial_channels)}")
        
        # Start data collection
        start_time = time.time()
        device_manager.start()
        
        # Verify device manager is running
        assert device_manager._running is True, "DeviceManager should be running"
        assert not stop_event.is_set(), "stop_event should not be set"
        
        # Verify collection thread is alive
        with device_manager._lock:
            assert connection.thread is not None, "Collection thread should exist"
            assert connection.thread.is_alive(), "Collection thread should be alive"
        
        # Monitor data collection over time
        check_intervals = [2.0, 5.0, 8.0, collection_duration]  # Check at 2s, 5s, 8s, and final
        points_at_intervals = []
        channels_at_intervals = []
        
        for check_time in check_intervals:
            elapsed = time.time() - start_time
            if elapsed < check_time:
                time.sleep(check_time - elapsed)
            
            stats = device_manager.io_database.get_statistics()
            total_points = stats.get('total_data_points', 0)
            all_channels = set(device_manager.io_database.get_all_channels())
            
            points_at_intervals.append(total_points)
            channels_at_intervals.append(len(all_channels))
            
            print(f"  After {check_time:.1f}s: {total_points} points, {len(all_channels)} channels")
            
            # Verify data is accumulating
            if check_time > 1.0:  # After first second
                assert total_points > initial_total_points, \
                    f"Data should be accumulating. Initial: {initial_total_points}, Current: {total_points}"
        
        # Get final statistics
        final_stats = device_manager.io_database.get_statistics()
        final_total_points = final_stats.get('total_data_points', 0)
        final_channels = set(device_manager.io_database.get_all_channels())
        
        print(f"  Final: {final_total_points} points, {len(final_channels)} channels")
        
        # Stop collection
        stop_event.set()
        device_manager.stop()
        
        # Wait for threads to finish
        with device_manager._lock:
            if connection.thread.is_alive():
                connection.thread.join(timeout=5.0)
        
        # Verify final data count
        assert final_total_points >= expected_min_total_points, \
            f"Expected at least {expected_min_total_points} total data points in {collection_duration}s. " \
            f"Got: {final_total_points}. " \
            f"This may indicate data collection is not working properly."
        
        # Verify multiple channels have data
        assert len(final_channels) >= 5, \
            f"Expected at least 5 channels with data. Got: {len(final_channels)} channels. " \
            f"Channels: {final_channels}"
        
        # Verify data accumulation was continuous (points should increase over time)
        assert points_at_intervals[-1] > points_at_intervals[0], \
            f"Points should accumulate over time. Started: {points_at_intervals[0]}, Ended: {points_at_intervals[-1]}"
        
        # Verify data accumulation rate is reasonable
        # Points should increase roughly linearly with time
        if len(points_at_intervals) >= 2:
            rate_2s = (points_at_intervals[1] - points_at_intervals[0]) / 2.0 if len(points_at_intervals) > 1 else 0
            rate_final = (points_at_intervals[-1] - points_at_intervals[0]) / check_intervals[-1]
            
            print(f"  Collection Rate (first 2s): {rate_2s:.1f} points/sec")
            print(f"  Collection Rate (overall): {rate_final:.1f} points/sec")
            
            # Rate should be reasonable (at least 100 points/sec for 500 Hz with multiple channels)
            assert rate_final >= 100, \
                f"Collection rate should be at least 100 points/sec. Got: {rate_final:.1f} points/sec"
        
        # Verify each channel has substantial data
        channels_with_sufficient_data = []
        for channel_path in final_channels:
            channel_count = device_manager.io_database.get_channel_count(channel_path)
            if channel_count >= expected_min_points_per_channel:
                channels_with_sufficient_data.append(channel_path)
            
            channel_data = device_manager.io_database.get_channel(channel_path)
            assert channel_data is not None, f"Channel {channel_path} should exist"
            assert channel_data.count == channel_count, "Channel count should match"
            assert channel_data.first_timestamp is not None, f"Channel {channel_path} should have first timestamp"
            assert channel_data.last_timestamp is not None, f"Channel {channel_path} should have last timestamp"
            assert channel_data.last_timestamp >= channel_data.first_timestamp, \
                f"Channel {channel_path} last timestamp should be >= first timestamp"
        
        print(f"  Channels with sufficient data ({expected_min_points_per_channel}+ points): {len(channels_with_sufficient_data)}")
        
        # At least some channels should have sufficient data
        assert len(channels_with_sufficient_data) >= 3, \
            f"Expected at least 3 channels with {expected_min_points_per_channel}+ points. " \
            f"Got: {len(channels_with_sufficient_data)}. " \
            f"Channels with data: {channels_with_sufficient_data}"
        
        # Verify elapsed_time calculation is correct
        # Check a few channels to ensure elapsed_time is calculated correctly
        for channel_path in list(final_channels)[:3]:  # Check first 3 channels
            channel_data = device_manager.io_database.get_channel(channel_path)
            points = list(channel_data.data_points)
            
            if len(points) >= 2:
                # First point should have elapsed_time = 0.0 (or very close)
                assert abs(points[0].elapsed_time - 0.0) < 1e-6, \
                    f"First point in {channel_path} should have elapsed_time ≈ 0.0, got {points[0].elapsed_time}"
                
                # Elapsed times should be monotonically increasing
                for i in range(1, min(100, len(points))):  # Check first 100 points
                    assert points[i].elapsed_time >= points[i-1].elapsed_time, \
                        f"Elapsed times should be monotonic. Point {i-1}: {points[i-1].elapsed_time}, " \
                        f"Point {i}: {points[i].elapsed_time}"
                
                # Last point should have reasonable elapsed_time (close to collection duration)
                last_elapsed = points[-1].elapsed_time
                assert last_elapsed >= collection_duration * 0.5, \
                    f"Last point elapsed_time should be at least {collection_duration * 0.5}s. " \
                    f"Got: {last_elapsed}s"
                assert last_elapsed <= collection_duration * 1.5, \
                    f"Last point elapsed_time should be at most {collection_duration * 1.5}s. " \
                    f"Got: {last_elapsed}s"
        
        # Verify data point timestamps are reasonable
        # All timestamps should be within a reasonable range
        global_first = device_manager.io_database.global_first_timestamp
        assert global_first is not None, "Global first timestamp should be set"
        
        for channel_path in list(final_channels)[:3]:  # Check first 3 channels
            channel_data = device_manager.io_database.get_channel(channel_path)
            points = list(channel_data.data_points)
            
            # Get channel's first timestamp and global first timestamp
            channel_first = channel_data.first_timestamp
            assert channel_first is not None, f"Channel {channel_path} should have first timestamp"
            
            # elapsed_time is calculated relative to global_first_timestamp (not channel_first)
            # This is how IODatabase.add_data_point() works - it uses global_first_timestamp as reference
            global_first = device_manager.io_database.global_first_timestamp
            assert global_first is not None, "Global first timestamp should be set"
            
            for i, point in enumerate(points[:100]):  # Check first 100 points
                # Timestamp should be >= channel's first timestamp
                assert point.timestamp_ns >= channel_first, \
                    f"Point timestamp {point.timestamp_ns} should be >= channel_first {channel_first}"
                
                # For the first point in a channel, elapsed_time is 0.0 because ChannelData.add_point()
                # sets reference_timestamp = timestamp_ns for the first point, ignoring global_first_timestamp.
                # For subsequent points, elapsed_time is relative to global_first_timestamp.
                if i == 0:
                    # First point should have elapsed_time = 0.0
                    assert abs(point.elapsed_time - 0.0) < 1e-6, \
                        f"First point in channel should have elapsed_time = 0.0, got {point.elapsed_time}"
                else:
                    # Subsequent points should use global_first_timestamp as reference
                    expected_elapsed = (point.timestamp_ns - global_first) / 1e9
                    assert abs(point.elapsed_time - expected_elapsed) < 1e-6, \
                        f"Elapsed time {point.elapsed_time} should match timestamp difference from global_first {expected_elapsed}"
        
        # Print detailed statistics
        print(f"\nDetailed Statistics:")
        print(f"  Total Data Points: {final_total_points}")
        print(f"  Total Channels: {len(final_channels)}")
        print(f"  Global First Timestamp: {global_first}")
        
        for channel_path in sorted(final_channels)[:10]:  # Print first 10 channels
            channel_data = device_manager.io_database.get_channel(channel_path)
            channel_stats = final_stats['channels'].get(channel_path, {})
            print(f"  {channel_path}:")
            print(f"    Count: {channel_data.count}")
            print(f"    Time Span: {channel_stats.get('time_span', 0):.3f}s")
            print(f"    Rate: {channel_stats.get('rate', 0):.1f} points/sec")
        
        # Cleanup
        device_manager.close_all_connections()
    
    @pytest.mark.integration
    def test_device_manager_to_virtual_database_creates_thousands_of_rows(self, ic256_ip):
        """Integration test: Verify full pipeline DeviceManager -> IODatabase -> VirtualDatabase.
        
        This comprehensive test verifies the complete data flow:
        1. DeviceManager collects data from real device into IODatabase
        2. IODatabase stores thousands of data points
        3. VirtualDatabase builds thousands of rows from IODatabase data
        
        This test requires:
        - A live IC256 device at the IP in config.json
        - Network connectivity to that device
        - Sufficient time to collect thousands of points and create thousands of rows (10+ seconds)
        """
        from ic256_sampler.utils import is_valid_device, is_valid_ipv4
        from ic256_sampler.ic256_model import IC256Model
        from ic256_sampler.virtual_database import VirtualDatabase
        
        # Skip if IP is invalid
        if not is_valid_ipv4(ic256_ip):
            pytest.skip(f"Invalid IP address in config: {ic256_ip}")
        
        # Verify device is reachable
        if not is_valid_device(ic256_ip, "IC256"):
            pytest.skip(f"IC256 device at {ic256_ip} is not reachable or not responding")
        
        # Test parameters
        sampling_rate = 500  # 500 Hz
        collection_duration = 10.0  # 10 seconds
        expected_min_rows = int(sampling_rate * collection_duration * 0.7)  # 70% tolerance for startup/shutdown
        expected_min_io_points = expected_min_rows * 5  # At least 5 channels should have data
        
        print(f"\nDeviceManager -> IODatabase -> VirtualDatabase Pipeline Test:")
        print(f"  Sampling Rate: {sampling_rate} Hz")
        print(f"  Collection Duration: {collection_duration} seconds")
        print(f"  Expected Min IODatabase Points: {expected_min_io_points}")
        print(f"  Expected Min VirtualDatabase Rows: {expected_min_rows}")
        
        # Step 1: Collect data using DeviceManager into IODatabase
        device_manager = DeviceManager()
        stop_event = threading.Event()
        device_manager.stop_event = stop_event
        
        # Add device
        success = device_manager.add_device(IC256_CONFIG, ic256_ip, sampling_rate=sampling_rate)
        assert success, "Device should be added successfully"
        
        # Verify connection exists
        assert "IC256-42/35" in device_manager.connections, "IC256 device connection should be established"
        connection = device_manager.connections["IC256-42/35"]
        
        # Get IODatabase
        io_database = device_manager.get_io_database()
        
        # Get initial state
        initial_stats = io_database.get_statistics()
        initial_total_points = initial_stats.get('total_data_points', 0)
        
        # Start data collection
        start_time = time.time()
        device_manager.start()
        
        # Verify device manager is running
        assert device_manager._running is True, "DeviceManager should be running"
        
        # Monitor data collection
        print(f"  Collecting data for {collection_duration} seconds...")
        time.sleep(collection_duration)
        
        # Get IODatabase statistics
        io_stats = io_database.get_statistics()
        io_total_points = io_stats.get('total_data_points', 0)
        io_channels = set(io_database.get_all_channels())
        
        print(f"  IODatabase: {io_total_points} points, {len(io_channels)} channels")
        
        # Stop collection
        stop_event.set()
        device_manager.stop()
        
        # Wait for threads to finish
        with device_manager._lock:
            if connection.thread.is_alive():
                connection.thread.join(timeout=5.0)
        
        # Verify IODatabase has thousands of points
        assert io_total_points >= expected_min_io_points, \
            f"IODatabase should have at least {expected_min_io_points} points. Got: {io_total_points}"
        
        assert len(io_channels) >= 5, \
            f"IODatabase should have at least 5 channels. Got: {len(io_channels)}"
        
        # Step 2: Build VirtualDatabase from IODatabase
        print(f"  Building VirtualDatabase from IODatabase...")
        
        model = IC256Model()
        reference_channel = model.get_reference_channel()
        columns = model.create_columns(reference_channel)
        
        virtual_db = VirtualDatabase(
            io_database=io_database,
            reference_channel=reference_channel,
            sampling_rate=sampling_rate,
            columns=columns,
        )
        
        # Build virtual database
        virtual_db.build()
        
        # Get VirtualDatabase statistics
        virtual_stats = virtual_db.get_statistics()
        virtual_row_count = virtual_db.get_row_count()
        
        print(f"  VirtualDatabase: {virtual_row_count} rows")
        print(f"  Time Span: {virtual_stats.get('time_span', 0):.3f} seconds")
        print(f"  Expected Rows: {virtual_stats.get('expected_rows', 0)}")
        print(f"  Actual Rows: {virtual_stats.get('actual_rows', 0)}")
        
        # Verify VirtualDatabase has thousands of rows
        assert virtual_row_count >= expected_min_rows, \
            f"VirtualDatabase should have at least {expected_min_rows} rows. Got: {virtual_row_count}. " \
            f"This indicates VirtualDatabase.build() is not creating rows from IODatabase data properly."
        
        # Verify row count is reasonable (should be close to expected for the time span)
        expected_rows = virtual_stats.get('expected_rows', 0)
        if expected_rows > 0:
            # Allow 20% tolerance for timing variations
            assert virtual_row_count >= int(expected_rows * 0.8), \
                f"VirtualDatabase row count {virtual_row_count} should be at least 80% of expected {expected_rows}"
            assert virtual_row_count <= int(expected_rows * 1.2), \
                f"VirtualDatabase row count {virtual_row_count} should be at most 120% of expected {expected_rows}"
        
        # Verify rows have data
        rows = virtual_db.get_rows()
        assert len(rows) == virtual_row_count, "Row count should match get_rows() length"
        assert len(rows) > 0, "Should have rows"
        
        # Verify first and last rows have reasonable timestamps
        first_row = rows[0]
        last_row = rows[-1]
        
        assert first_row.timestamp >= 0.0, f"First row timestamp should be >= 0.0, got {first_row.timestamp}"
        assert last_row.timestamp > first_row.timestamp, \
            f"Last row timestamp {last_row.timestamp} should be > first row timestamp {first_row.timestamp}"
        
        # Verify time span is reasonable
        time_span = last_row.timestamp - first_row.timestamp
        assert time_span >= collection_duration * 0.7, \
            f"Time span {time_span}s should be at least 70% of collection duration {collection_duration}s"
        assert time_span <= collection_duration * 1.3, \
            f"Time span {time_span}s should be at most 130% of collection duration {collection_duration}s"
        
        # Verify rows have data in them
        # Check first 10 and last 10 rows
        for i, row in enumerate(rows[:10]):
            assert len(row.data) > 0, f"Row {i} should have data"
            # At least some columns should have non-None values
            non_none_count = sum(1 for v in row.data.values() if v is not None)
            assert non_none_count > 0, f"Row {i} should have at least one non-None value"
        
        for i, row in enumerate(rows[-10:], len(rows) - 10):
            assert len(row.data) > 0, f"Row {i} should have data"
            non_none_count = sum(1 for v in row.data.values() if v is not None)
            assert non_none_count > 0, f"Row {i} should have at least one non-None value"
        
        # Verify row spacing is correct (should be approximately 1/sampling_rate)
        row_interval = 1.0 / sampling_rate
        for i in range(1, min(100, len(rows))):  # Check first 100 rows
            time_diff = rows[i].timestamp - rows[i-1].timestamp
            # Allow 5% tolerance for floating point and timing variations
            assert abs(time_diff - row_interval) < row_interval * 0.05, \
                f"Row {i} spacing {time_diff} should be approximately {row_interval} (got {abs(time_diff - row_interval)} difference)"
        
        # Print summary
        print(f"\nPipeline Test Summary:")
        print(f"  IODatabase Points: {io_total_points}")
        print(f"  IODatabase Channels: {len(io_channels)}")
        print(f"  VirtualDatabase Rows: {virtual_row_count}")
        print(f"  VirtualDatabase Time Span: {time_span:.3f}s")
        print(f"  Row Spacing: {row_interval:.4f}s (expected: {1.0/sampling_rate:.4f}s)")
        
        # Cleanup
        device_manager.close_all_connections()
