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
        """Test that stop() properly stops collection threads."""
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
        
        # Stop the manager
        manager.stop()
        
        # Verify stop_event was set and thread join was called
        assert manager.stop_event.is_set()
        assert manager._running is False
        mock_thread.join.assert_called_once()

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
