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
from unittest.mock import Mock, patch
from ic256_sampler.application import Application
from ic256_sampler.device_manager import DeviceManager, IC256_CONFIG
from ic256_sampler.utils import is_valid_device, is_valid_ipv4


class TestApplicationDataCollectionFlow:
    """Tests for Application data collection flow that might reveal blind spots."""
    
    @pytest.mark.integration
    def test_device_thread_after_stop_and_clear(self, ic256_ip):
        """Test that _device_thread works correctly after stop() and clear_database().
        
        This tests the exact flow that happens in _device_thread:
        1. stop() is called
        2. clear_database() is called
        3. setup_device() is called
        4. New threads are created
        5. start() is called
        6. Data collection should work
        
        This might reveal issues with thread recreation or connection state.
        """
        # Skip if IP is invalid
        if not is_valid_ipv4(ic256_ip):
            pytest.skip(f"Invalid IP address in config: {ic256_ip}")
        
        # Verify device is reachable
        if not is_valid_device(ic256_ip, "IC256"):
            pytest.skip(f"IC256 device at {ic256_ip} is not reachable or not responding")
        
        # Create Application
        app = Application()
        
        # Create mock GUI
        mock_window = Mock()
        mock_window.ix256_a_entry = Mock()
        mock_window.ix256_a_entry.get = Mock(return_value=ic256_ip)
        mock_window.tx2_entry = Mock()
        mock_window.tx2_entry.get = Mock(return_value="")
        mock_window.note_entry = Mock()
        mock_window.note_entry.get = Mock(return_value="Stop/Clear Test")
        mock_window.path_entry = Mock()
        mock_window.path_entry.get = Mock(return_value="/tmp/test")
        mock_window.sampling_entry = Mock()
        mock_window.sampling_entry.get = Mock(return_value="500")
        mock_window.root = Mock()
        mock_window.root.after = Mock()
        mock_window.reset_elapse_time = Mock()
        mock_window.reset_statistics = Mock()
        
        app.window = mock_window
        
        with patch('ic256_sampler.application.safe_gui_update'), \
             patch('ic256_sampler.application.set_button_state_safe'), \
             patch('ic256_sampler.application.show_message_safe'), \
             patch('ic256_sampler.application.log_message_safe'):
            
            # Step 1: Ensure connections exist
            app._ensure_connections()
            
            device_manager = app.device_manager
            assert device_manager is not None
            
            # Verify connection exists
            assert "IC256-42/35" in device_manager.connections
            connection = device_manager.connections["IC256-42/35"]
            
            # Step 2: Simulate the exact flow from _device_thread
            # Set stop_event
            device_manager.stop_event = app.stop_event
            
            # Stop (like _device_thread does)
            device_manager.stop()
            
            # Wait for threads to finish
            if connection.thread.is_alive():
                connection.thread.join(timeout=2.0)
            
            # Clear database (like _device_thread does)
            device_manager.clear_database()
            
            # Verify database is cleared
            stats = device_manager.get_io_database().get_statistics()
            assert stats.get('total_data_points', 0) == 0, "Database should be cleared"
            
            # Step 3: Setup device (like _device_thread does)
            # Check subscription state before setup
            print(f"  Before setup_device:")
            print(f"    SubscribedFields count: {len(connection.client.subscribedFields)}")
            
            try:
                connection.model.setup_device(connection.client, 500)
            except (ConnectionAbortedError, ConnectionResetError, OSError) as e:
                print(f"Connection error during setup: {e}")
                # Try reconnect
                try:
                    connection.client.reconnect()
                    connection.model.setup_device(connection.client, 500)
                    # Re-subscribe to channels after reconnect
                    connection.client.sendSubscribeFields({
                        field: True for field in connection.channels.values()
                    })
                except Exception as retry_error:
                    pytest.fail(f"Failed to setup device after reconnect: {retry_error}")
            
            # Check subscription state after setup
            print(f"  After setup_device:")
            print(f"    SubscribedFields count: {len(connection.client.subscribedFields)}")
            
            # CRITICAL: Re-subscribe to data channels after setup_device
            # setup_device() only handles frequency fields, but we need to ensure
            # data channels are still subscribed (they should be, but let's be explicit)
            connection.client.sendSubscribeFields({
                field: True for field in connection.channels.values()
            })
            
            print(f"  After re-subscribe:")
            print(f"    SubscribedFields count: {len(connection.client.subscribedFields)}")
            
            # Step 4: Create new thread (like _device_thread does)
            if connection.thread.is_alive():
                connection.thread.join(timeout=1.0)
            
            config = connection.config
            new_thread = threading.Thread(
                target=device_manager._collect_from_device,
                name=f"{config.device_type.lower()}_device_{connection.ip_address}",
                daemon=True,
                args=(config, connection.client, connection.channels, connection.model, connection.field_to_path, connection.ip_address),
            )
            connection.thread = new_thread
            
            # Step 5: Start (like ModelCollector.start() does)
            device_manager.start()
            
            # Verify thread is started
            assert device_manager._running is True
            assert new_thread.is_alive(), "New thread should be started"
            
            # Step 6: Check connection state before starting
            print(f"\nBefore start():")
            print(f"  Connection client ws: {connection.client.ws}")
            print(f"  Connection client ws.connected: {connection.client.ws.connected if connection.client.ws != '' else 'N/A'}")
            print(f"  Thread alive before start: {new_thread.is_alive()}")
            
            # Step 7: Collect data for a few seconds
            # Monitor data collection over time
            for i in range(3):
                time.sleep(1.0)
                stats = device_manager.get_io_database().get_statistics()
                total_points = stats.get('total_data_points', 0)
                print(f"  After {i+1}s: {total_points} points, thread alive: {new_thread.is_alive()}")
                
                # Check connection state
                if connection.client.ws != "":
                    print(f"    WS connected: {connection.client.ws.connected}")
            
            # Final check
            stats = device_manager.get_io_database().get_statistics()
            total_points = stats.get('total_data_points', 0)
            
            print(f"\nStop/Clear/Start Test Results:")
            print(f"  Total data points: {total_points}")
            print(f"  DeviceManager._running: {device_manager._running}")
            print(f"  Thread alive: {new_thread.is_alive()}")
            if connection.client.ws != "":
                print(f"  WS connected: {connection.client.ws.connected}")
            
            # Should have collected substantial data
            assert total_points > 500, \
                f"Should collect data after stop/clear/start. Got {total_points} points. " \
                "If this fails, there's an issue with the stop/clear/start flow."
            
            # Stop
            app.stop_event.set()
            device_manager.stop()
    
    @pytest.mark.integration
    def test_device_thread_with_connection_error_recovery(self, ic256_ip):
        """Test that _device_thread recovers from connection errors during setup.
        
        This tests the scenario where setup_device() fails with ConnectionAbortedError,
        then reconnects and continues.
        """
        # Skip if IP is invalid
        if not is_valid_ipv4(ic256_ip):
            pytest.skip(f"Invalid IP address in config: {ic256_ip}")
        
        # Verify device is reachable
        if not is_valid_device(ic256_ip, "IC256"):
            pytest.skip(f"IC256 device at {ic256_ip} is not reachable or not responding")
        
        # Create Application
        app = Application()
        
        # Create mock GUI
        mock_window = Mock()
        mock_window.ix256_a_entry = Mock()
        mock_window.ix256_a_entry.get = Mock(return_value=ic256_ip)
        mock_window.tx2_entry = Mock()
        mock_window.tx2_entry.get = Mock(return_value="")
        mock_window.note_entry = Mock()
        mock_window.note_entry.get = Mock(return_value="Connection Error Test")
        mock_window.path_entry = Mock()
        mock_window.path_entry.get = Mock(return_value="/tmp/test")
        mock_window.sampling_entry = Mock()
        mock_window.sampling_entry.get = Mock(return_value="500")
        mock_window.root = Mock()
        mock_window.root.after = Mock()
        mock_window.reset_elapse_time = Mock()
        mock_window.reset_statistics = Mock()
        
        app.window = mock_window
        
        with patch('ic256_sampler.application.safe_gui_update'), \
             patch('ic256_sampler.application.set_button_state_safe'), \
             patch('ic256_sampler.application.show_message_safe'), \
             patch('ic256_sampler.application.log_message_safe'):
            
            # Ensure connections exist
            app._ensure_connections()
            
            device_manager = app.device_manager
            connection = device_manager.connections["IC256-42/35"]
            
            # Simulate connection error scenario
            device_manager.stop_event = app.stop_event
            device_manager.stop()
            device_manager.clear_database()
            
            # Setup device (might fail, but should recover)
            setup_success = False
            try:
                connection.model.setup_device(connection.client, 500)
                # CRITICAL: Re-subscribe to data channels after setup_device()
                connection.client.sendSubscribeFields({
                    field: True for field in connection.channels.values()
                })
                setup_success = True
            except (ConnectionAbortedError, ConnectionResetError, OSError) as e:
                print(f"Connection error during setup (expected): {e}")
                # Reconnect and retry
                try:
                    connection.client.reconnect()
                    connection.model.setup_device(connection.client, 500)
                    # CRITICAL: Re-subscribe to data channels after reconnect
                    connection.client.sendSubscribeFields({
                        field: True for field in connection.channels.values()
                    })
                    setup_success = True
                except Exception as retry_error:
                    print(f"Reconnect failed: {retry_error}")
            
            assert setup_success, "Setup should succeed after reconnect"
            
            # Create new thread
            if connection.thread.is_alive():
                connection.thread.join(timeout=1.0)
            
            config = connection.config
            new_thread = threading.Thread(
                target=device_manager._collect_from_device,
                name=f"{config.device_type.lower()}_device_{connection.ip_address}",
                daemon=True,
                args=(config, connection.client, connection.channels, connection.model, connection.field_to_path, connection.ip_address),
            )
            connection.thread = new_thread
            
            # Start
            device_manager.start()
            
            # Collect data
            time.sleep(3.0)
            
            # Verify data collection
            stats = device_manager.get_io_database().get_statistics()
            total_points = stats.get('total_data_points', 0)
            
            print(f"\nConnection Error Recovery Test:")
            print(f"  Total data points: {total_points}")
            
            assert total_points > 500, \
                f"Should collect data after connection error recovery. Got {total_points} points."
            
            # Stop
            app.stop_event.set()
            device_manager.stop()
    
    @pytest.mark.integration
    def test_collector_iteration_with_minimal_data(self, ic256_ip):
        """Test that ModelCollector.collect_iteration() works with minimal initial data.
        
        This tests the scenario where only a few data points are collected initially,
        then more data arrives. This might reveal issues with rebuild() or CSV writing.
        """
        from ic256_sampler.model_collector import ModelCollector, collect_data_with_model
        from ic256_sampler.ic256_model import IC256Model
        
        # Skip if IP is invalid
        if not is_valid_ipv4(ic256_ip):
            pytest.skip(f"Invalid IP address in config: {ic256_ip}")
        
        # Verify device is reachable
        if not is_valid_device(ic256_ip, "IC256"):
            pytest.skip(f"IC256 device at {ic256_ip} is not reachable or not responding")
        
        # Create DeviceManager and collect minimal data
        device_manager = DeviceManager()
        stop_event = threading.Event()
        device_manager.stop_event = stop_event
        
        success = device_manager.add_device(IC256_CONFIG, ic256_ip, sampling_rate=500)
        assert success
        
        device_manager.start()
        
        # Collect just a few data points (simulating the 11 points scenario)
        time.sleep(0.1)  # Very short collection
        
        io_database = device_manager.get_io_database()
        stats = io_database.get_statistics()
        initial_points = stats.get('total_data_points', 0)
        
        print(f"\nMinimal Data Test:")
        print(f"  Initial points: {initial_points}")
        
        # Create ModelCollector
        model = IC256Model()
        reference_channel = model.get_reference_channel()
        
        collector = ModelCollector(
            device_manager=device_manager,
            model=model,
            reference_channel=reference_channel,
            sampling_rate=500,
            file_path="/tmp/test_minimal.csv",
            device_name="ic256",
            note="Minimal Data Test",
        )
        
        # Run a few iterations to see if rows are created
        collector.start()
        
        # Run several iterations
        for i in range(100):  # 100 iterations at 0.001s = 0.1s
            collector.collect_iteration()
            time.sleep(0.001)
            
            if i % 20 == 0:
                virtual_rows = collector.virtual_database.get_row_count()
                csv_rows = collector.csv_writer.rows_written
                io_points = device_manager.get_io_database().get_statistics().get('total_data_points', 0)
                print(f"  Iteration {i}: IO points={io_points}, Virtual rows={virtual_rows}, CSV rows={csv_rows}")
        
        # Continue collecting more data
        time.sleep(2.0)  # Collect more data
        
        # Run more iterations
        for i in range(200):
            collector.collect_iteration()
            time.sleep(0.001)
            
            if i % 50 == 0:
                virtual_rows = collector.virtual_database.get_row_count()
                csv_rows = collector.csv_writer.rows_written
                io_points = device_manager.get_io_database().get_statistics().get('total_data_points', 0)
                print(f"  Iteration {i+100}: IO points={io_points}, Virtual rows={virtual_rows}, CSV rows={csv_rows}")
        
        # Final check
        final_virtual_rows = collector.virtual_database.get_row_count()
        final_csv_rows = collector.csv_writer.rows_written
        final_io_points = device_manager.get_io_database().get_statistics().get('total_data_points', 0)
        
        print(f"  Final: IO points={final_io_points}, Virtual rows={final_virtual_rows}, CSV rows={final_csv_rows}")
        
        # Should have created rows as data accumulates
        assert final_io_points > initial_points, \
            f"IO points should increase. Initial: {initial_points}, Final: {final_io_points}"
        
        # If we have data, we should have rows
        if final_io_points > 50:
            assert final_virtual_rows > 0, \
                f"Should have virtual rows with {final_io_points} IO points. Got {final_virtual_rows} rows."
            assert final_csv_rows > 0, \
                f"Should have CSV rows with {final_io_points} IO points. Got {final_csv_rows} rows."
        
        # Stop
        stop_event.set()
        device_manager.stop()
        collector.finalize()
    
    @pytest.mark.integration
    def test_device_thread_full_flow_with_diagnostics(self, ic256_ip, tmp_path):
        """Test full _device_thread flow with detailed diagnostics.
        
        This test runs the actual _device_thread and monitors what happens
        at each step to identify where data collection stops.
        """
        # Skip if IP is invalid
        if not is_valid_ipv4(ic256_ip):
            pytest.skip(f"Invalid IP address in config: {ic256_ip}")
        
        # Verify device is reachable
        if not is_valid_device(ic256_ip, "IC256"):
            pytest.skip(f"IC256 device at {ic256_ip} is not reachable or not responding")
        
        # Create Application
        app = Application()
        
        # Create mock GUI
        mock_window = Mock()
        mock_window.ix256_a_entry = Mock()
        mock_window.ix256_a_entry.get = Mock(return_value=ic256_ip)
        mock_window.tx2_entry = Mock()
        mock_window.tx2_entry.get = Mock(return_value="")
        mock_window.note_entry = Mock()
        mock_window.note_entry.get = Mock(return_value="Diagnostics Test")
        mock_window.path_entry = Mock()
        mock_window.path_entry.get = Mock(return_value=str(tmp_path))
        mock_window.sampling_entry = Mock()
        mock_window.sampling_entry.get = Mock(return_value="500")
        mock_window.root = Mock()
        mock_window.root.after = Mock()
        mock_window.reset_elapse_time = Mock()
        mock_window.reset_statistics = Mock()
        
        app.window = mock_window
        
        with patch('ic256_sampler.application.safe_gui_update'), \
             patch('ic256_sampler.application.set_button_state_safe'), \
             patch('ic256_sampler.application.show_message_safe'), \
             patch('ic256_sampler.application.log_message_safe'):
            
            # Ensure connections
            app._ensure_connections()
            
            # Run _device_thread in a thread
            device_thread = threading.Thread(
                target=app._device_thread,
                name="diagnostics_device_thread",
                daemon=True
            )
            device_thread.start()
            
            # Wait for initialization
            time.sleep(2.0)
            
            # Monitor data collection with detailed diagnostics
            print(f"\nFull Flow Diagnostics:")
            for i in range(10):  # Monitor for 10 seconds
                time.sleep(1.0)
                
                if app.device_manager:
                    io_stats = app.device_manager.get_io_database().get_statistics()
                    io_points = io_stats.get('total_data_points', 0)
                    
                    if app.collector:
                        virtual_rows = app.collector.virtual_database.get_row_count()
                        csv_rows = app.collector.csv_writer.rows_written
                        
                        # Check VirtualDatabase state
                        vdb = app.collector.virtual_database
                        vdb_built = vdb._built
                        vdb_last_time = vdb._last_built_time
                        
                        # Check reference channel
                        ref_channel = vdb.reference_channel
                        ref_channel_data = app.device_manager.get_io_database().get_channel(ref_channel)
                        ref_count = ref_channel_data.count if ref_channel_data else 0
                        
                        print(f"  {i+1}s: IO={io_points}, Virtual={virtual_rows}, CSV={csv_rows}, "
                              f"Built={vdb_built}, LastTime={vdb_last_time}, RefCount={ref_count}")
                        
                        # Check if data is accumulating
                        if i > 0 and io_points == previous_io_points:
                            print(f"    WARNING: IO points not increasing! Stuck at {io_points}")
                            # Check thread status
                            with app.device_manager._lock:
                                for name, conn in app.device_manager.connections.items():
                                    thread_alive = conn.thread.is_alive() if conn.thread else False
                                    print(f"    {name} thread alive: {thread_alive}")
                    else:
                        print(f"  {i+1}s: IO={io_points}, Collector not created yet")
                
                previous_io_points = io_points if app.device_manager else 0
            
            # Stop
            app.stop_collection()
            
            # Final diagnostics
            if app.collector:
                final_virtual = app.collector.virtual_database.get_row_count()
                final_csv = app.collector.csv_writer.rows_written
                print(f"  Final: Virtual rows={final_virtual}, CSV rows={final_csv}")
            
            if app.device_manager:
                final_io = app.device_manager.get_io_database().get_statistics().get('total_data_points', 0)
                print(f"  Final: IO points={final_io}")
