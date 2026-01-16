"""Tests to verify websocket connections remain open between acquisitions.

This test verifies that websocket connections are NOT closed when stop()
is called, and only close when the program shuts down completely.
"""

import pytest
import time
import threading
from unittest.mock import Mock, patch
from ic256_sampler.application import Application
from ic256_sampler.device_manager import DeviceManager, IC256_CONFIG
from ic256_sampler.utils import is_valid_device, is_valid_ipv4


class TestWebsocketPersistence:
    """Tests for websocket connection persistence between acquisitions."""
    
    @pytest.mark.integration
    def test_websocket_remains_open_between_acquisitions(self, ic256_ip):
        """Test that websocket connections remain open between stop/start cycles.
        
        This test verifies:
        1. Connection is established on first acquisition
        2. Connection remains open after stop()
        3. Connection is reused on second acquisition (no reconnect)
        4. Connection only closes on program shutdown
        """
        # Skip if IP is invalid
        if not is_valid_ipv4(ic256_ip):
            pytest.skip(f"Invalid IP address in config: {ic256_ip}")
        
        # Verify device is reachable
        if not is_valid_device(ic256_ip, "IC256"):
            pytest.skip(f"IC256 device at {ic256_ip} is not reachable or not responding")
        
        app = Application()
        
        # Create mock GUI
        mock_window = Mock()
        mock_window.ix256_a_entry = Mock()
        mock_window.ix256_a_entry.get = Mock(return_value=ic256_ip)
        mock_window.tx2_entry = Mock()
        mock_window.tx2_entry.get = Mock(return_value="")
        mock_window.note_entry = Mock()
        mock_window.note_entry.get = Mock(return_value="Websocket Persistence Test")
        mock_window.path_entry = Mock()
        mock_window.path_entry.get = Mock(return_value="/tmp/test")
        mock_window.sampling_entry = Mock()
        mock_window.sampling_entry.get = Mock(return_value="500")
        mock_window.root = Mock()
        mock_window.root.after = Mock()
        mock_window.reset_elapse_time = Mock()
        mock_window.reset_statistics = Mock()
        
        app.window = mock_window
        
        # Track reconnect calls
        reconnect_calls = []
        original_reconnect = None
        
        def track_reconnect(self_ref):
            reconnect_calls.append(time.time())
            if original_reconnect:
                return original_reconnect(self_ref)
        
        with patch('ic256_sampler.application.safe_gui_update'), \
             patch('ic256_sampler.application.set_button_state_safe'), \
             patch('ic256_sampler.application.show_message_safe'), \
             patch('ic256_sampler.application.log_message_safe'):
            
            # First acquisition
            print("\n=== First Acquisition ===")
            app._ensure_connections()
            
            device_manager = app.device_manager
            assert device_manager is not None
            assert "IC256-42/35" in device_manager.connections
            
            connection = device_manager.connections["IC256-42/35"]
            original_ws = connection.client.ws
            original_reconnect = connection.client.reconnect
            
            # Patch reconnect to track calls
            connection.client.reconnect = lambda: track_reconnect(connection.client)
            
            # Verify connection is open
            assert original_ws != "", "Websocket should be created"
            assert original_ws.connected, "Websocket should be connected"
            
            # Start first acquisition
            device_thread = threading.Thread(
                target=app._device_thread,
                name="first_acquisition",
                daemon=True
            )
            device_thread.start()
            
            # Wait for setup
            time.sleep(2.0)
            
            # Verify connection is still the same (not reconnected)
            assert connection.client.ws == original_ws, "Websocket should not be recreated"
            assert len(reconnect_calls) == 0, f"Should not reconnect during first acquisition. Got {len(reconnect_calls)} reconnect calls"
            
            # Collect for a bit
            time.sleep(2.0)
            
            # Stop first acquisition
            app.stop_collection()
            time.sleep(1.0)
            
            # Verify connection is still open after stop
            assert connection.client.ws != "", "Websocket should still exist after stop"
            assert connection.client.ws.connected, "Websocket should still be connected after stop"
            assert connection.client.ws == original_ws, "Websocket should be the same instance after stop"
            assert len(reconnect_calls) == 0, f"Should not reconnect after stop. Got {len(reconnect_calls)} reconnect calls"
            
            # Second acquisition
            print("\n=== Second Acquisition ===")
            reconnect_calls.clear()  # Clear for second acquisition
            
            device_thread = threading.Thread(
                target=app._device_thread,
                name="second_acquisition",
                daemon=True
            )
            device_thread.start()
            
            # Wait for setup
            time.sleep(2.0)
            
            # Verify connection is still the same (not reconnected)
            assert connection.client.ws == original_ws, "Websocket should be the same instance after second start"
            # Allow for one reconnect if connection was actually closed (but it shouldn't be)
            assert len(reconnect_calls) == 0, \
                f"Should not reconnect during second acquisition. Connection should remain open. Got {len(reconnect_calls)} reconnect calls"
            
            # Stop second acquisition
            app.stop_collection()
            time.sleep(1.0)
            
            # Verify connection is still open
            assert connection.client.ws != "", "Websocket should still exist after second stop"
            assert connection.client.ws.connected, "Websocket should still be connected after second stop"
            assert connection.client.ws == original_ws, "Websocket should be the same instance after second stop"
            
            # Cleanup - close connections (simulating program shutdown)
            device_manager.close_all_connections()
            
            # Verify connection is now closed
            assert connection.client.ws == "" or not connection.client.ws.connected, \
                "Websocket should be closed after close_all_connections()"
    
    def test_stop_does_not_close_websocket(self):
        """Test that DeviceManager.stop() does not close websocket connections.
        
        This verifies that stop() only stops threads, not connections.
        """
        device_manager = DeviceManager()
        
        # Create a mock connection
        mock_client = Mock()
        mock_client.ws = Mock()
        mock_client.ws.connected = True
        mock_client.close = Mock()
        
        mock_connection = Mock()
        mock_connection.client = mock_client
        mock_connection.thread = Mock()
        mock_connection.thread.is_alive = Mock(return_value=False)
        
        device_manager.connections = {"test_device": mock_connection}
        device_manager._running = True
        
        # Call stop
        device_manager.stop()
        
        # Verify close() was NOT called
        mock_client.close.assert_not_called(), \
            "stop() should NOT close websocket connections"
        
        # Verify connection still exists
        assert "test_device" in device_manager.connections, \
            "Connection should still exist after stop()"
    
    def test_close_all_connections_closes_websockets(self):
        """Test that close_all_connections() actually closes websocket connections.
        
        This verifies that connections are only closed on program shutdown.
        """
        device_manager = DeviceManager()
        
        # Create mock connections
        mock_client1 = Mock()
        mock_client1.ws = Mock()
        mock_client1.ws.connected = True
        mock_client1.close = Mock()
        
        mock_client2 = Mock()
        mock_client2.ws = Mock()
        mock_client2.ws.connected = True
        mock_client2.close = Mock()
        
        mock_connection1 = Mock()
        mock_connection1.client = mock_client1
        mock_connection1.thread = Mock()
        mock_connection1.thread.is_alive = Mock(return_value=False)
        mock_connection1.keepalive_thread = Mock()
        mock_connection1.keepalive_thread.is_alive = Mock(return_value=False)
        
        mock_connection2 = Mock()
        mock_connection2.client = mock_client2
        mock_connection2.thread = Mock()
        mock_connection2.thread.is_alive = Mock(return_value=False)
        mock_connection2.keepalive_thread = Mock()
        mock_connection2.keepalive_thread.is_alive = Mock(return_value=False)
        
        device_manager.connections = {
            "device1": mock_connection1,
            "device2": mock_connection2
        }
        
        # Call close_all_connections
        device_manager.close_all_connections()
        
        # Verify close() was called on both clients
        assert mock_client1.close.call_count == 1, \
            "close_all_connections() should close all websocket connections"
        assert mock_client2.close.call_count == 1, \
            "close_all_connections() should close all websocket connections"
        
        # Verify connections are cleared
        assert len(device_manager.connections) == 0, \
            "close_all_connections() should clear all connections"
