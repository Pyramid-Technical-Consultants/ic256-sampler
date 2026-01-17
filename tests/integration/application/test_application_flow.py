"""Tests to isolate data collection issues in Application._device_thread flow.

These tests specifically test scenarios that might cause data collection to stop
prematurely, such as:
- stop() and clear_database() before start()
- Connection errors during setup_device()
- Thread recreation after stop()
"""

import pytest
import time
import threading
from ic256_sampler.device_manager import DeviceManager, IC256_CONFIG
from ic256_sampler.model_collector import ModelCollector
from ic256_sampler.ic256_model import IC256Model
from tests.conftest import wait_for_condition

# Mark all tests in this file as integration tests with timeout
pytestmark = [pytest.mark.integration, pytest.mark.timeout(10)]


class TestApplicationDataCollectionFlow:
    """Tests for Application data collection flow that might reveal blind spots."""
    
    @pytest.mark.integration
    def test_device_thread_after_stop_and_clear(self, require_ic256_device, app_with_mock_gui):
        """Test that _device_thread works correctly after stop() and clear_database().
        
        This tests the exact flow that happens in _device_thread:
        1. stop() is called
        2. clear_database() is called
        3. setup_device() is called
        4. New threads are created
        5. start() is called
        6. Data collection should work
        """
        app = app_with_mock_gui
        app._ensure_connections()
        
        device_manager = app.device_manager
        connection = device_manager.connections["IC256-42/35"]
        
        # Simulate the exact flow from _device_thread
        device_manager.stop_event = app.stop_event
        device_manager.stop()
        
        if connection.thread.is_alive():
            connection.thread.join(timeout=2.0)
        
        device_manager.clear_database()
        assert device_manager.get_io_database().get_statistics().get('total_data_points', 0) == 0
        
        # Setup device with error recovery
        try:
            connection.model.setup_device(connection.client, 500)
        except (ConnectionAbortedError, ConnectionResetError, OSError):
            connection.client.reconnect()
            connection.model.setup_device(connection.client, 500)
        
        # Re-subscribe to data channels
        connection.client.sendSubscribeFields({
            field: True for field in connection.channels.values()
        })
        
        # Create new thread
        if connection.thread.is_alive():
            connection.thread.join(timeout=1.0)
        
        config = connection.config
        new_thread = threading.Thread(
            target=device_manager._collect_from_device,
            name=f"{config.device_type.lower()}_device_{connection.ip_address}",
            daemon=True,
            args=(config, connection.client, connection.channels, connection.model, 
                  connection.field_to_path, connection.ip_address),
        )
        connection.thread = new_thread
        device_manager.start()
        
        assert device_manager._running is True
        assert new_thread.is_alive()
        
        # Collect data for a few seconds
        time.sleep(3.0)
        
        total_points = device_manager.get_io_database().get_statistics().get('total_data_points', 0)
        assert total_points > 500, f"Should collect data after stop/clear/start. Got {total_points} points."
        
        app.stop_event.set()
        device_manager.stop()
    
    @pytest.mark.integration
    def test_device_thread_with_connection_error_recovery(self, require_ic256_device, app_with_mock_gui):
        """Test that _device_thread recovers from connection errors during setup."""
        app = app_with_mock_gui
        app._ensure_connections()
        
        device_manager = app.device_manager
        connection = device_manager.connections["IC256-42/35"]
        
        device_manager.stop_event = app.stop_event
        device_manager.stop()
        device_manager.clear_database()
        
        # Setup device with error recovery
        setup_success = False
        try:
            connection.model.setup_device(connection.client, 500)
            connection.client.sendSubscribeFields({
                field: True for field in connection.channels.values()
            })
            setup_success = True
        except (ConnectionAbortedError, ConnectionResetError, OSError):
            try:
                connection.client.reconnect()
                connection.model.setup_device(connection.client, 500)
                connection.client.sendSubscribeFields({
                    field: True for field in connection.channels.values()
                })
                setup_success = True
            except Exception:
                pass
        
        assert setup_success, "Setup should succeed after reconnect"
        
        # Create new thread and start
        if connection.thread.is_alive():
            connection.thread.join(timeout=1.0)
        
        config = connection.config
        new_thread = threading.Thread(
            target=device_manager._collect_from_device,
            name=f"{config.device_type.lower()}_device_{connection.ip_address}",
            daemon=True,
            args=(config, connection.client, connection.channels, connection.model,
                  connection.field_to_path, connection.ip_address),
        )
        connection.thread = new_thread
        device_manager.start()
        
        time.sleep(3.0)
        
        total_points = device_manager.get_io_database().get_statistics().get('total_data_points', 0)
        assert total_points > 500, f"Should collect data after connection error recovery. Got {total_points} points."
        
        app.stop_event.set()
        device_manager.stop()
    
    @pytest.mark.integration
    def test_collector_iteration_with_minimal_data(self, require_ic256_device):
        """Test that ModelCollector.collect_iteration() works with minimal initial data."""
        device_manager = DeviceManager()
        stop_event = threading.Event()
        device_manager.stop_event = stop_event
        
        assert device_manager.add_device(IC256_CONFIG, require_ic256_device, sampling_rate=500)
        device_manager.start()
        
        time.sleep(0.1)  # Very short collection
        initial_points = device_manager.get_io_database().get_statistics().get('total_data_points', 0)
        
        # Create ModelCollector
        model = IC256Model()
        collector = ModelCollector(
            device_manager=device_manager,
            model=model,
            reference_channel=model.get_reference_channel(),
            sampling_rate=500,
            file_path="/tmp/test_minimal.csv",
            device_name="ic256",
            note="Minimal Data Test",
        )
        
        collector.start()
        
        # Run iterations
        for i in range(100):
            collector.collect_iteration()
            time.sleep(0.001)
        
        time.sleep(2.0)  # Collect more data
        
        for i in range(200):
            collector.collect_iteration()
            time.sleep(0.001)
        
        final_io_points = device_manager.get_io_database().get_statistics().get('total_data_points', 0)
        final_virtual_rows = collector.virtual_database.get_row_count()
        final_csv_rows = collector.csv_writer.rows_written
        
        assert final_io_points > initial_points
        if final_io_points > 50:
            assert final_virtual_rows > 0
            assert final_csv_rows > 0
        
        stop_event.set()
        device_manager.stop()
        collector.finalize()
    
    @pytest.mark.integration
    def test_device_thread_full_flow_with_diagnostics(self, require_ic256_device, app_with_mock_gui):
        """Test full _device_thread flow with detailed diagnostics."""
        app = app_with_mock_gui
        app._ensure_connections()
        
        device_thread = threading.Thread(
            target=app._device_thread,
            name="diagnostics_device_thread",
            daemon=True
        )
        device_thread.start()
        
        # Wait for collection to start and collect some data
        wait_for_condition(
            lambda: app.collector is not None and app.collector.csv_writer.rows_written > 0,
            timeout=3.0,
            interval=0.1,
            description="collection to start and write data"
        )
        
        # Monitor data collection - check every second, but only for a few iterations
        # to stay within timeout
        for i in range(5):
            time.sleep(1.0)
            if app.device_manager:
                io_points = app.device_manager.get_io_database().get_statistics().get('total_data_points', 0)
                if app.collector:
                    virtual_rows = app.collector.virtual_database.get_row_count()
                    csv_rows = app.collector.csv_writer.rows_written
                    print(f"  {i+1}s: IO={io_points}, Virtual={virtual_rows}, CSV={csv_rows}")
        
        app.stop_collection()
        
        if app.collector:
            print(f"  Final: Virtual={app.collector.virtual_database.get_row_count()}, "
                  f"CSV={app.collector.csv_writer.rows_written}")
        if app.device_manager:
            print(f"  Final: IO={app.device_manager.get_io_database().get_statistics().get('total_data_points', 0)}")
