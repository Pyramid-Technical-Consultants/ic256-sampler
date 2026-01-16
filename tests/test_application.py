"""Tests for Application class."""
import pytest
import threading
import time
from unittest.mock import Mock, MagicMock, patch, PropertyMock
from ic256_sampler.application import Application, DEFAULT_SAMPLING_RATE, MIN_SAMPLING_RATE, MAX_SAMPLING_RATE
from ic256_sampler.device_manager import DeviceManager, IC256_CONFIG, TX2_CONFIG


class TestApplication:
    """Tests for Application class."""
    
    def test_application_init(self):
        """Test Application initialization."""
        app = Application()
        assert app.window is None
        assert app.stop_event is not None
        assert isinstance(app.stop_event, threading.Event)
        assert app.device_statistics == {}
        assert app.collector is None
        assert app.device_manager is None
        assert app.collector_thread is None
        assert app._stopping is False
        assert app._cleanup_registered is False
    
    def test_get_gui_values_no_window(self):
        """Test _get_gui_values when window is None."""
        app = Application()
        result = app._get_gui_values()
        assert result == ("", "", "", "")
    
    def test_get_gui_values_with_window(self):
        """Test _get_gui_values when window exists."""
        app = Application()
        mock_window = Mock()
        mock_window.ix256_a_entry = Mock()
        mock_window.ix256_a_entry.get = Mock(return_value="192.168.1.100")
        mock_window.tx2_entry = Mock()
        mock_window.tx2_entry.get = Mock(return_value="192.168.1.101")
        mock_window.note_entry = Mock()
        mock_window.note_entry.get = Mock(return_value="Test Note")
        mock_window.path_entry = Mock()
        mock_window.path_entry.get = Mock(return_value="/test/path")
        
        app.window = mock_window
        result = app._get_gui_values()
        assert result == ("192.168.1.100", "192.168.1.101", "Test Note", "/test/path")
    
    def test_get_sampling_rate_no_window(self):
        """Test _get_sampling_rate when window is None."""
        app = Application()
        result = app._get_sampling_rate()
        assert result == DEFAULT_SAMPLING_RATE
    
    def test_get_sampling_rate_valid(self):
        """Test _get_sampling_rate with valid rate."""
        app = Application()
        mock_window = Mock()
        mock_window.sampling_entry = Mock()
        mock_window.sampling_entry.get = Mock(return_value="1000")
        app.window = mock_window
        
        result = app._get_sampling_rate()
        assert result == 1000
    
    def test_get_sampling_rate_invalid_too_low(self):
        """Test _get_sampling_rate with rate too low."""
        app = Application()
        mock_window = Mock()
        mock_window.sampling_entry = Mock()
        mock_window.sampling_entry.get = Mock(return_value="0")
        app.window = mock_window
        
        with patch('ic256_sampler.application.log_message_safe'):
            result = app._get_sampling_rate()
            assert result == DEFAULT_SAMPLING_RATE
    
    def test_get_sampling_rate_invalid_too_high(self):
        """Test _get_sampling_rate with rate too high."""
        app = Application()
        mock_window = Mock()
        mock_window.sampling_entry = Mock()
        mock_window.sampling_entry.get = Mock(return_value="10000")
        app.window = mock_window
        
        with patch('ic256_sampler.application.log_message_safe'):
            result = app._get_sampling_rate()
            assert result == DEFAULT_SAMPLING_RATE
    
    def test_get_sampling_rate_invalid_not_number(self):
        """Test _get_sampling_rate with non-numeric value."""
        app = Application()
        mock_window = Mock()
        mock_window.sampling_entry = Mock()
        mock_window.sampling_entry.get = Mock(return_value="abc")
        app.window = mock_window
        
        with patch('ic256_sampler.application.log_message_safe'):
            result = app._get_sampling_rate()
            assert result == DEFAULT_SAMPLING_RATE
    
    def test_ensure_connections_no_window(self):
        """Test _ensure_connections when window is None."""
        app = Application()
        app._ensure_connections()
        assert app.device_manager is None
    
    def test_ensure_connections_gui_not_ready(self):
        """Test _ensure_connections when GUI widgets don't exist yet."""
        app = Application()
        # Create a simple object without the attribute to properly test hasattr
        class SimpleWindow:
            pass
        simple_window = SimpleWindow()
        app.window = simple_window
        
        app._ensure_connections()
        # Should return early before creating device manager
        assert app.device_manager is None
    
    def test_ensure_connections_creates_device_manager(self):
        """Test _ensure_connections creates device manager when needed."""
        app = Application()
        mock_window = Mock()
        mock_window.ix256_a_entry = Mock()
        mock_window.ix256_a_entry.get = Mock(return_value="192.168.1.100")
        mock_window.tx2_entry = Mock()
        mock_window.tx2_entry.get = Mock(return_value="")
        mock_window.note_entry = Mock()
        mock_window.note_entry.get = Mock(return_value="")
        mock_window.path_entry = Mock()
        mock_window.path_entry.get = Mock(return_value="")
        mock_window.sampling_entry = Mock()
        mock_window.sampling_entry.get = Mock(return_value="500")
        app.window = mock_window
        
        with patch('ic256_sampler.application.is_valid_device', return_value=True), \
             patch('ic256_sampler.application.IGXWebsocketClient') as mock_client_class, \
             patch('ic256_sampler.application.log_message_safe'):
            mock_client = Mock()
            mock_client_class.return_value = mock_client
            mock_client.field = Mock(return_value="field1")
            
            app._ensure_connections()
            assert app.device_manager is not None
            assert isinstance(app.device_manager, DeviceManager)
    
    def test_cleanup_no_resources(self):
        """Test cleanup when no resources are initialized."""
        app = Application()
        # Should not raise any exceptions
        app.cleanup()
    
    def test_cleanup_with_device_manager(self):
        """Test cleanup with device manager."""
        app = Application()
        mock_device_manager = Mock()
        app.device_manager = mock_device_manager
        app.stop_event.set()  # Already stopped
        
        app.cleanup()
        mock_device_manager.close_all_connections.assert_called_once()
    
    def test_cleanup_with_collector(self):
        """Test cleanup with collector."""
        app = Application()
        mock_collector = Mock()
        app.collector = mock_collector
        # Don't set stop_event - simulate active collection
        app.stop_event.clear()
        
        app.cleanup()
        mock_collector.stop.assert_called_once()
        mock_collector.finalize.assert_called_once()
    
    def test_cleanup_stops_collection(self):
        """Test cleanup stops active collection."""
        app = Application()
        mock_device_manager = Mock()
        mock_collector = Mock()
        mock_thread = Mock()
        mock_thread.is_alive = Mock(return_value=False)
        
        app.device_manager = mock_device_manager
        app.collector = mock_collector
        app.collector_thread = mock_thread
        
        # stop_event not set (collection is active)
        app.cleanup()
        
        assert app.stop_event.is_set()
        assert app._stopping is True
        mock_device_manager.stop.assert_called_once()
        mock_collector.stop.assert_called_once()
        mock_collector.finalize.assert_called_once()
        mock_device_manager.close_all_connections.assert_called_once()
    
    def test_register_cleanup(self):
        """Test _register_cleanup registers handlers."""
        app = Application()
        
        with patch('ic256_sampler.application.atexit') as mock_atexit, \
             patch('ic256_sampler.application.signal') as mock_signal:
            mock_signal.SIGINT = 'SIGINT'
            mock_signal.signal = Mock()
            
            app._register_cleanup()
            
            assert app._cleanup_registered is True
            mock_atexit.register.assert_called_once_with(app.cleanup)
            mock_signal.signal.assert_called_once()
    
    def test_register_cleanup_idempotent(self):
        """Test _register_cleanup is idempotent."""
        app = Application()
        
        with patch('ic256_sampler.application.atexit') as mock_atexit, \
             patch('ic256_sampler.application.signal') as mock_signal:
            mock_signal.SIGINT = 'SIGINT'
            mock_signal.signal = Mock()
            
            app._register_cleanup()
            app._register_cleanup()  # Call again
            
            # Should only register once
            assert mock_atexit.register.call_count == 1
            assert mock_signal.signal.call_count == 1
    
    def test_log_callback(self):
        """Test _log_callback."""
        app = Application()
        mock_window = Mock()
        app.window = mock_window
        
        with patch('ic256_sampler.application.log_message_safe') as mock_log:
            app._log_callback("Test message", "INFO")
            mock_log.assert_called_once_with(mock_window, "Test message", "INFO")
    
    def test_start_collection_no_window(self):
        """Test start_collection when window is None."""
        app = Application()
        app.start_collection()  # Should not raise
    
    def test_start_collection_with_window(self):
        """Test start_collection creates background thread."""
        app = Application()
        mock_window = Mock()
        app.window = mock_window
        
        with patch('ic256_sampler.application.set_button_state_safe') as mock_set_button, \
             patch('ic256_sampler.application.show_message_safe') as mock_show_message, \
             patch('threading.Thread') as mock_thread_class:
            mock_thread = Mock()
            mock_thread_class.return_value = mock_thread
            
            app.start_collection()
            
            mock_set_button.assert_called_once_with(mock_window, "start_button", "disabled")
            mock_show_message.assert_called_once()
            mock_thread_class.assert_called_once()
            mock_thread.start.assert_called_once()
    
    def test_stop_collection_no_window(self):
        """Test stop_collection when window is None."""
        app = Application()
        app.stop_collection()  # Should not raise
    
    def test_stop_collection_with_resources(self):
        """Test stop_collection stops resources."""
        app = Application()
        mock_window = Mock()
        mock_window.root = Mock()
        mock_window.root.after = Mock()
        app.window = mock_window
        
        mock_device_manager = Mock()
        mock_collector = Mock()
        mock_thread = Mock()
        mock_thread.is_alive = Mock(return_value=False)
        
        app.device_manager = mock_device_manager
        app.collector = mock_collector
        app.collector_thread = mock_thread
        
        with patch('ic256_sampler.application.show_message_safe') as mock_show, \
             patch('ic256_sampler.application.log_message_safe') as mock_log:
            app.stop_collection()
            
            assert app.stop_event.is_set()
            assert app._stopping is True
            mock_device_manager.stop.assert_called_once()
            mock_collector.stop.assert_called_once()
            mock_show.assert_called()
            mock_log.assert_called()
            # _check_collector_thread_finished should be called via root.after
            mock_window.root.after.assert_called()
    
    def test_setup_devices_creates_thread(self):
        """Test setup_devices creates background thread."""
        app = Application()
        
        with patch('threading.Thread') as mock_thread_class:
            mock_thread = Mock()
            mock_thread_class.return_value = mock_thread
            
            app.setup_devices()
            
            mock_thread_class.assert_called_once()
            mock_thread.start.assert_called_once()
    
    def test_ensure_connections_reuses_existing(self):
        """Test _ensure_connections reuses existing connection with same IP."""
        app = Application()
        mock_window = Mock()
        mock_window.ix256_a_entry = Mock()
        mock_window.ix256_a_entry.get = Mock(return_value="192.168.1.100")
        mock_window.tx2_entry = Mock()
        mock_window.tx2_entry.get = Mock(return_value="")
        mock_window.note_entry = Mock()
        mock_window.note_entry.get = Mock(return_value="")
        mock_window.path_entry = Mock()
        mock_window.path_entry.get = Mock(return_value="")
        mock_window.sampling_entry = Mock()
        mock_window.sampling_entry.get = Mock(return_value="500")
        app.window = mock_window
        
        # Create device manager with existing connection
        app.device_manager = DeviceManager()
        mock_connection = Mock()
        mock_connection.ip_address = "192.168.1.100"
        mock_connection.thread = Mock()
        mock_connection.thread.is_alive = Mock(return_value=False)
        app.device_manager.connections[IC256_CONFIG.device_name] = mock_connection
        
        with patch('ic256_sampler.application.log_message_safe'):
            app._ensure_connections()
            
            # Should not create new connection, should reuse existing
            assert IC256_CONFIG.device_name in app.device_manager.connections
            assert app.device_manager.connections[IC256_CONFIG.device_name] == mock_connection
    
    def test_ensure_connections_updates_on_ip_change(self):
        """Test _ensure_connections updates connection when IP changes."""
        app = Application()
        mock_window = Mock()
        mock_window.ix256_a_entry = Mock()
        mock_window.ix256_a_entry.get = Mock(return_value="192.168.1.200")  # New IP
        mock_window.tx2_entry = Mock()
        mock_window.tx2_entry.get = Mock(return_value="")
        mock_window.note_entry = Mock()
        mock_window.note_entry.get = Mock(return_value="")
        mock_window.path_entry = Mock()
        mock_window.path_entry.get = Mock(return_value="")
        mock_window.sampling_entry = Mock()
        mock_window.sampling_entry.get = Mock(return_value="500")
        app.window = mock_window
        
        # Create device manager with existing connection (old IP)
        app.device_manager = DeviceManager()
        mock_old_connection = Mock()
        mock_old_connection.ip_address = "192.168.1.100"  # Old IP
        mock_old_connection.thread = Mock()
        mock_old_connection.thread.is_alive = Mock(return_value=False)
        mock_old_connection.client = Mock()
        app.device_manager.connections[IC256_CONFIG.device_name] = mock_old_connection
        
        with patch('ic256_sampler.application.is_valid_device', return_value=True), \
             patch('ic256_sampler.application.IGXWebsocketClient') as mock_client_class, \
             patch('ic256_sampler.application.log_message_safe'):
            mock_client = Mock()
            mock_client_class.return_value = mock_client
            mock_client.field = Mock(return_value="field1")
            
            app._ensure_connections()
            
            # Old connection should be closed
            mock_old_connection.client.close.assert_called_once()
            # New connection should be created
            mock_client_class.assert_called_once_with("192.168.1.200")
