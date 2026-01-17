"""Tests to verify websocket connections remain open between acquisitions.

This test verifies that websocket connections are NOT closed when stop()
is called, and only close when the program shuts down completely.
"""

import pytest
import time
import threading
from ic256_sampler.device_manager import DeviceManager


class TestWebsocketPersistence:
    """Tests for websocket connection persistence between acquisitions."""
    
    @pytest.mark.integration
    @pytest.mark.timeout(15)
    def test_websocket_remains_open_between_acquisitions(self, require_ic256_device, app_with_mock_gui):
        """Test that websocket connections remain open between stop/start cycles."""
        app = app_with_mock_gui
        
        # First acquisition
        app._ensure_connections()
        device_manager = app.device_manager
        connection = device_manager.connections["IC256-42/35"]
        original_ws = connection.client.ws
        
        # Track reconnect calls
        reconnect_calls = []
        original_reconnect = connection.client.reconnect
        
        def track_reconnect():
            reconnect_calls.append(time.time())
            return original_reconnect()
        
        connection.client.reconnect = track_reconnect
        
        assert original_ws != ""
        assert original_ws.connected
        
        device_thread = threading.Thread(
            target=app._device_thread,
            name="first_acquisition",
            daemon=True
        )
        device_thread.start()
        time.sleep(2.0)
        
        assert connection.client.ws == original_ws
        assert len(reconnect_calls) == 0
        
        time.sleep(2.0)
        app.stop_collection()
        time.sleep(1.0)
        
        assert connection.client.ws != ""
        assert connection.client.ws.connected
        assert connection.client.ws == original_ws
        assert len(reconnect_calls) == 0
        
        # Second acquisition
        reconnect_calls.clear()
        
        device_thread = threading.Thread(
            target=app._device_thread,
            name="second_acquisition",
            daemon=True
        )
        device_thread.start()
        time.sleep(2.0)
        
        assert connection.client.ws == original_ws
        assert len(reconnect_calls) == 0
        
        app.stop_collection()
        time.sleep(1.0)
        
        assert connection.client.ws != ""
        assert connection.client.ws.connected
        assert connection.client.ws == original_ws
        
        # Cleanup
        device_manager.close_all_connections()
        assert connection.client.ws == "" or not connection.client.ws.connected
    
    def test_stop_does_not_close_websocket(self):
        """Test that DeviceManager.stop() does not close websocket connections."""
        device_manager = DeviceManager()
        
        mock_client = type('MockClient', (), {
            'ws': type('MockWS', (), {'connected': True})(),
            'close': lambda: None
        })()
        
        mock_connection = type('MockConnection', (), {
            'client': mock_client,
            'thread': type('MockThread', (), {'is_alive': lambda: False})()
        })()
        
        device_manager.connections = {"test_device": mock_connection}
        device_manager._running = True
        
        close_called = False
        original_close = mock_client.close
        
        def track_close():
            nonlocal close_called
            close_called = True
            return original_close()
        
        mock_client.close = track_close
        
        device_manager.stop()
        
        assert not close_called, "stop() should NOT close websocket connections"
        assert "test_device" in device_manager.connections
    
    def test_close_all_connections_closes_websockets(self):
        """Test that close_all_connections() actually closes websocket connections."""
        device_manager = DeviceManager()
        
        close1_called = False
        close2_called = False
        
        mock_client1 = type('MockClient', (), {
            'ws': type('MockWS', (), {'connected': True})(),
            'close': lambda: setattr(mock_client1, '_close_called', True) or None
        })()
        
        mock_client2 = type('MockClient', (), {
            'ws': type('MockWS', (), {'connected': True})(),
            'close': lambda: setattr(mock_client2, '_close_called', True) or None
        })()
        
        mock_connection1 = type('MockConnection', (), {
            'client': mock_client1,
            'thread': type('MockThread', (), {'is_alive': lambda: False})(),
            'keepalive_thread': type('MockThread', (), {'is_alive': lambda: False})()
        })()
        
        mock_connection2 = type('MockConnection', (), {
            'client': mock_client2,
            'thread': type('MockThread', (), {'is_alive': lambda: False})(),
            'keepalive_thread': type('MockThread', (), {'is_alive': lambda: False})()
        })()
        
        device_manager.connections = {
            "device1": mock_connection1,
            "device2": mock_connection2
        }
        
        device_manager.close_all_connections()
        
        assert hasattr(mock_client1, '_close_called') and mock_client1._close_called
        assert hasattr(mock_client2, '_close_called') and mock_client2._close_called
        assert len(device_manager.connections) == 0
