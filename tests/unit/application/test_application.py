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
        
        with patch('ic256_sampler.device_manager.is_valid_device', return_value=True), \
             patch('ic256_sampler.device_manager.IGXWebsocketClient') as mock_client_class, \
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
        mock_thread.is_alive = Mock(return_value=True)  # Thread still alive, so _stopping stays True
        
        app.device_manager = mock_device_manager
        app.collector = mock_collector
        app.collector_thread = mock_thread
        
        with patch('ic256_sampler.application.show_message_safe') as mock_show, \
             patch('ic256_sampler.application.log_message_safe') as mock_log:
            app.stop_collection()
            
            assert app.stop_event.is_set()
            # _stopping is set in stop_collection (line 573)
            # It stays True because thread is alive, so _check_collector_thread_finished schedules a callback
            assert app._stopping is True, f"_stopping should be True but is {app._stopping}"
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
        
        with patch('ic256_sampler.device_manager.is_valid_device', return_value=True), \
             patch('ic256_sampler.application.log_message_safe'), \
             patch.object(app.device_manager, 'add_device') as mock_add_device:
            app._ensure_connections()
            
            # Old connection should be closed
            mock_old_connection.client.close.assert_called_once()
            # Old connection should be removed from connections dict
            assert IC256_CONFIG.device_name not in app.device_manager.connections
            # New connection should be created via add_device
            mock_add_device.assert_called_once()
            # Verify add_device was called with correct parameters
            call_args = mock_add_device.call_args
            assert call_args[0][0] == IC256_CONFIG
            assert call_args[0][1] == "192.168.1.200"
    
    def test_multiple_acquisitions(self):
        """Test that the application can handle multiple acquisitions sequentially.
        
        This test simulates:
        1. First acquisition: start -> stop -> complete
        2. Second acquisition: start -> stop -> complete
        Verifies that state is properly reset between acquisitions.
        """
        app = Application()
        mock_window = Mock()
        mock_window.ix256_a_entry = Mock()
        mock_window.ix256_a_entry.get = Mock(return_value="192.168.1.100")
        mock_window.tx2_entry = Mock()
        mock_window.tx2_entry.get = Mock(return_value="")
        mock_window.note_entry = Mock()
        mock_window.note_entry.get = Mock(return_value="Test Note")
        mock_window.path_entry = Mock()
        mock_window.path_entry.get = Mock(return_value="/test/path")
        mock_window.sampling_entry = Mock()
        mock_window.sampling_entry.get = Mock(return_value="500")
        mock_window.root = Mock()
        mock_window.root.after = Mock()
        mock_window.reset_elapse_time = Mock()
        mock_window.reset_statistics = Mock()
        app.window = mock_window
        
        # Setup device manager with a connection
        app.device_manager = DeviceManager()
        mock_connection = Mock()
        mock_connection.ip_address = "192.168.1.100"
        mock_connection.thread = Mock()
        mock_connection.thread.is_alive = Mock(return_value=False)
        mock_connection.model = Mock()
        mock_connection.model.setup_device = Mock()
        mock_connection.config = IC256_CONFIG
        mock_connection.client = Mock()
        mock_connection.channels = {}
        mock_connection.field_to_path = {}
        app.device_manager.connections[IC256_CONFIG.device_name] = mock_connection
        
        # Mock ModelCollector - create separate instances for each acquisition
        mock_collectors = [Mock(), Mock(), Mock()]
        for collector in mock_collectors:
            collector.stop = Mock()
            collector.finalize = Mock()
            collector.statistics = {}
        
        def model_collector_side_effect(*args, **kwargs):
            # Return a new collector instance for each call
            idx = len([c for c in mock_collectors if hasattr(c, '_used')])
            if idx < len(mock_collectors):
                mock_collectors[idx]._used = True
                return mock_collectors[idx]
            return Mock()
        
        with patch('ic256_sampler.application.safe_gui_update') as mock_safe_update, \
             patch('ic256_sampler.application.set_button_state_safe') as mock_set_button, \
             patch('ic256_sampler.application.show_message_safe') as mock_show, \
             patch('ic256_sampler.application.log_message_safe') as mock_log, \
             patch('ic256_sampler.file_path_generator.get_timestamp_strings', side_effect=[("20240101", "120000"), ("20240101", "120100"), ("20240101", "120200")]), \
             patch('ic256_sampler.application.ModelCollector', side_effect=model_collector_side_effect), \
             patch('ic256_sampler.model_collector.collect_data_with_model') as mock_collect_data, \
             patch('threading.Thread') as mock_thread_class:
            
            # Create mock threads - need separate instances for each acquisition
            mock_time_threads = [Mock(), Mock(), Mock()]
            mock_stats_threads = [Mock(), Mock(), Mock()]
            mock_collector_threads = [Mock(), Mock(), Mock()]
            
            # Configure is_alive behavior for collector threads
            mock_collector_threads[0].is_alive = Mock(side_effect=[True, False])  # First acquisition
            mock_collector_threads[1].is_alive = Mock(side_effect=[True, False])  # Second acquisition
            mock_collector_threads[2].is_alive = Mock(side_effect=[True, False])  # Third acquisition
            
            thread_call_count = {'elapse_time': 0, 'statistics_update': 0, 'model_collector': 0}
            
            def thread_side_effect(*args, **kwargs):
                name = kwargs.get('name', '')
                if 'elapse_time' in name:
                    idx = thread_call_count['elapse_time']
                    thread_call_count['elapse_time'] += 1
                    return mock_time_threads[idx] if idx < len(mock_time_threads) else Mock()
                elif 'statistics_update' in name:
                    idx = thread_call_count['statistics_update']
                    thread_call_count['statistics_update'] += 1
                    return mock_stats_threads[idx] if idx < len(mock_stats_threads) else Mock()
                elif 'model_collector' in name:
                    idx = thread_call_count['model_collector']
                    thread_call_count['model_collector'] += 1
                    return mock_collector_threads[idx] if idx < len(mock_collector_threads) else Mock()
                return Mock()
            
            mock_thread_class.side_effect = thread_side_effect
            
            # FIRST ACQUISITION
            # Start first acquisition
            app._device_thread()
            
            # Verify first acquisition started correctly
            assert app.stop_event.is_set() == False, "stop_event should be cleared for new acquisition"
            assert app.collector is not None, "collector should be created"
            assert app.collector_thread is not None, "collector_thread should be created"
            assert app.device_statistics != {}, "device_statistics should be initialized"
            
            first_collector = app.collector
            first_collector_thread = app.collector_thread
            
            # Stop first acquisition
            app.stop_collection()
            
            # Verify stop was called
            assert app.stop_event.is_set(), "stop_event should be set after stop_collection"
            assert app._stopping is True, "_stopping should be True after stop_collection"
            # The collector's stop method should be called (it's stored in app.collector)
            if app.collector:
                app.collector.stop.assert_called()
            
            # Simulate collector thread finishing (call _check_collector_thread_finished with thread dead)
            app._check_collector_thread_finished()
            
            # Note: finalize() is only called in cleanup(), not in _finalize_stop()
            # So we don't expect it to be called here during normal stop
            assert app._stopping is False, "_stopping should be False after thread finishes"
            
            # Reset mocks for second acquisition (no need, we have separate collectors)
            
            # SECOND ACQUISITION
            # Start second acquisition
            app._device_thread()
            
            # Verify second acquisition started correctly
            assert app.stop_event.is_set() == False, "stop_event should be cleared for second acquisition"
            assert app.collector is not None, "collector should be created for second acquisition"
            assert app.collector_thread is not None, "collector_thread should be created for second acquisition"
            assert app.collector_thread != first_collector_thread, "collector_thread should be a new instance"
            assert app.collector != first_collector, "collector should be a new instance"
            assert app.device_statistics != {}, "device_statistics should be re-initialized"
            
            # Verify device manager database was cleared (called in _device_thread)
            # We can't easily assert this without patching, but the key test is that
            # the second acquisition works, which proves state was properly reset
            
            # Stop second acquisition
            app.stop_collection()
            
            # Verify stop was called again
            assert app.stop_event.is_set(), "stop_event should be set after second stop_collection"
            assert app._stopping is True, "_stopping should be True after second stop_collection"
            # The collector's stop method should be called (it's stored in app.collector)
            if app.collector:
                app.collector.stop.assert_called()
            
            # Simulate collector thread finishing
            app._check_collector_thread_finished()
            
            # Note: finalize() is only called in cleanup(), not in _finalize_stop()
            assert app._stopping is False, "_stopping should be False after second thread finishes"
            
            # Verify that we can start a third acquisition (proving state is properly reset)
            # This is the key test - ensuring the second acquisition doesn't break the state
            app._device_thread()
            
            # Verify third acquisition started correctly
            assert app.stop_event.is_set() == False, "stop_event should be cleared for third acquisition"
            assert app.collector is not None, "collector should be created for third acquisition"
            assert app.collector_thread is not None, "collector_thread should be created for third acquisition"

    def test_device_manager_stop_called_before_new_acquisition(self):
        """CRITICAL REGRESSION TEST: Verify device_manager.stop() is called before starting new acquisition.
        
        This test prevents the regression where data collection stops after one row because
        device_manager._running was still True from a previous acquisition, causing start() to return early.
        """
        app = Application()
        mock_window = Mock()
        mock_window.ix256_a_entry = Mock()
        mock_window.ix256_a_entry.get = Mock(return_value="192.168.1.100")
        mock_window.tx2_entry = Mock()
        mock_window.tx2_entry.get = Mock(return_value="")
        mock_window.note_entry = Mock()
        mock_window.note_entry.get = Mock(return_value="Test Note")
        mock_window.path_entry = Mock()
        mock_window.path_entry.get = Mock(return_value="/test/path")
        mock_window.sampling_entry = Mock()
        mock_window.sampling_entry.get = Mock(return_value="500")
        mock_window.root = Mock()
        mock_window.root.after = Mock()
        mock_window.reset_elapse_time = Mock()
        mock_window.reset_statistics = Mock()
        app.window = mock_window
        
        # Setup device manager with a connection
        app.device_manager = DeviceManager()
        mock_connection = Mock()
        mock_connection.ip_address = "192.168.1.100"
        mock_connection.thread = Mock()
        mock_connection.thread.is_alive = Mock(return_value=False)
        mock_connection.model = Mock()
        mock_connection.model.setup_device = Mock()
        mock_connection.config = IC256_CONFIG
        mock_connection.client = Mock()
        mock_connection.channels = {}
        mock_connection.field_to_path = {}
        app.device_manager.connections[IC256_CONFIG.device_name] = mock_connection
        
        # Simulate a previous acquisition that left _running = True
        app.device_manager._running = True
        
        with patch('ic256_sampler.application.safe_gui_update'), \
             patch('ic256_sampler.application.set_button_state_safe'), \
             patch('ic256_sampler.application.show_message_safe'), \
             patch('ic256_sampler.application.log_message_safe'), \
             patch('ic256_sampler.file_path_generator.get_timestamp_strings', return_value=("20240101", "120000")), \
             patch('ic256_sampler.application.ModelCollector') as mock_collector_class, \
             patch('ic256_sampler.model_collector.collect_data_with_model'), \
             patch('threading.Thread'):
            
            # Track if stop() was called
            stop_called = [False]
            original_stop = app.device_manager.stop
            
            def track_stop():
                stop_called[0] = True
                original_stop()
            
            app.device_manager.stop = track_stop
            
            # Start new acquisition
            app._device_thread()
            
            # CRITICAL: Verify stop() was called to reset _running
            assert stop_called[0], "device_manager.stop() MUST be called before starting new acquisition to reset _running state"
            assert app.device_manager._running is False, "_running should be False after stop() is called"

    def test_device_manager_start_actually_starts_threads(self):
        """CRITICAL REGRESSION TEST: Verify that device_manager.start() actually starts threads.
        
        This test ensures that when start() is called with _running=False, threads are actually started,
        preventing the regression where _running=True causes start() to return early.
        """
        device_manager = DeviceManager()
        device_manager._running = False  # Ensure it's False
        device_manager.stop_event = threading.Event()
        device_manager.stop_event.clear()
        
        # Create a mock connection with a thread
        mock_thread = Mock()
        mock_thread.is_alive = Mock(return_value=False)
        mock_thread.start = Mock()
        
        from ic256_sampler.device_manager import DeviceConnection
        mock_connection = DeviceConnection(
            config=IC256_CONFIG,
            ip_address="192.168.1.100",
            client=Mock(),
            channels={},
            model=Mock(),
            field_to_path={},
            thread=mock_thread,
            keepalive_thread=Mock(),
        )
        
        device_manager.connections[IC256_CONFIG.device_name] = mock_connection
        
        # Call start() - should start the thread because _running is False
        device_manager.start()
        
        # CRITICAL: Verify thread was started
        assert device_manager._running is True, "_running should be True after start()"
        mock_thread.start.assert_called_once(), \
            "Thread MUST be started when _running is False. " \
            "If _running is True, start() returns early and threads won't start, " \
            "causing data collection to stop after one row."

    def test_device_manager_running_state_reset_between_acquisitions(self):
        """CRITICAL REGRESSION TEST: Verify _running state is properly reset between acquisitions.
        
        This test ensures that _running is False when starting a new acquisition,
        preventing the bug where start() returns early because _running is still True.
        """
        app = Application()
        mock_window = Mock()
        mock_window.ix256_a_entry = Mock()
        mock_window.ix256_a_entry.get = Mock(return_value="192.168.1.100")
        mock_window.tx2_entry = Mock()
        mock_window.tx2_entry.get = Mock(return_value="")
        mock_window.note_entry = Mock()
        mock_window.note_entry.get = Mock(return_value="Test Note")
        mock_window.path_entry = Mock()
        mock_window.path_entry.get = Mock(return_value="/test/path")
        mock_window.sampling_entry = Mock()
        mock_window.sampling_entry.get = Mock(return_value="500")
        mock_window.root = Mock()
        mock_window.root.after = Mock()
        mock_window.reset_elapse_time = Mock()
        mock_window.reset_statistics = Mock()
        app.window = mock_window
        
        # Setup device manager
        app.device_manager = DeviceManager()
        mock_connection = Mock()
        mock_connection.ip_address = "192.168.1.100"
        mock_connection.thread = Mock()
        mock_connection.thread.is_alive = Mock(return_value=False)
        mock_connection.model = Mock()
        mock_connection.model.setup_device = Mock()
        mock_connection.config = IC256_CONFIG
        mock_connection.client = Mock()
        mock_connection.channels = {}
        mock_connection.field_to_path = {}
        app.device_manager.connections[IC256_CONFIG.device_name] = mock_connection
        
        # Simulate first acquisition ending with _running = True
        app.device_manager._running = True
        app.device_manager.stop_event.set()
        
        with patch('ic256_sampler.application.safe_gui_update'), \
             patch('ic256_sampler.application.set_button_state_safe'), \
             patch('ic256_sampler.application.show_message_safe'), \
             patch('ic256_sampler.application.log_message_safe'), \
             patch('ic256_sampler.file_path_generator.get_timestamp_strings', return_value=("20240101", "120000")), \
             patch('ic256_sampler.application.ModelCollector'), \
             patch('ic256_sampler.model_collector.collect_data_with_model'), \
             patch('threading.Thread'):
            
            # Start second acquisition
            app._device_thread()
            
            # CRITICAL: After _device_thread, _running should be False (reset by stop())
            # This is the key regression test - _running must be False so start() will actually start threads
            assert app.device_manager._running is False, \
                "_running MUST be False after _device_thread() completes setup. " \
                "If True, device_manager.start() will return early and threads won't start, " \
                "causing data collection to stop after one row."

    def test_device_manager_start_not_blocked_by_running_flag(self):
        """CRITICAL REGRESSION TEST: Verify start() is not blocked when _running is False.
        
        This test directly verifies that device_manager.start() will start threads
        when _running is False, preventing the regression.
        """
        device_manager = DeviceManager()
        device_manager._running = False  # Ensure it's False
        device_manager.stop_event = threading.Event()
        device_manager.stop_event.clear()
        
        # Create a mock connection with a thread
        mock_thread = Mock()
        mock_thread.is_alive = Mock(return_value=False)
        mock_thread.start = Mock()
        
        from ic256_sampler.device_manager import DeviceConnection
        mock_connection = DeviceConnection(
            config=IC256_CONFIG,
            ip_address="192.168.1.100",
            client=Mock(),
            channels={},
            model=Mock(),
            field_to_path={},
            thread=mock_thread,
            keepalive_thread=Mock(),
        )
        
        device_manager.connections[IC256_CONFIG.device_name] = mock_connection
        
        # Call start() - should start the thread
        device_manager.start()
        
        # Verify thread was started
        assert device_manager._running is True, "_running should be True after start()"
        mock_thread.start.assert_called_once(), "Thread should be started when _running is False"
        
        # Now test the regression case: if _running is True, start() should return early
        device_manager._running = True
        mock_thread.start.reset_mock()
        
        # Call start() again - should return early without starting thread
        device_manager.start()
        
        # Thread should NOT be started again (because _running was already True)
        mock_thread.start.assert_not_called(), \
            "Thread should NOT be started if _running is already True (start() returns early)"

    def test_application_stops_device_manager_before_new_acquisition(self):
        """Integration test: Verify application properly stops device_manager before new acquisition.
        
        This test simulates the real scenario where a user:
        1. Starts an acquisition
        2. Stops it
        3. Starts a new acquisition
        
        Verifies that device_manager.stop() is called to reset state.
        """
        app = Application()
        mock_window = Mock()
        mock_window.ix256_a_entry = Mock()
        mock_window.ix256_a_entry.get = Mock(return_value="192.168.1.100")
        mock_window.tx2_entry = Mock()
        mock_window.tx2_entry.get = Mock(return_value="")
        mock_window.note_entry = Mock()
        mock_window.note_entry.get = Mock(return_value="Test Note")
        mock_window.path_entry = Mock()
        mock_window.path_entry.get = Mock(return_value="/test/path")
        mock_window.sampling_entry = Mock()
        mock_window.sampling_entry.get = Mock(return_value="500")
        mock_window.root = Mock()
        mock_window.root.after = Mock()
        mock_window.reset_elapse_time = Mock()
        mock_window.reset_statistics = Mock()
        app.window = mock_window
        
        # Setup device manager
        app.device_manager = DeviceManager()
        mock_connection = Mock()
        mock_connection.ip_address = "192.168.1.100"
        mock_connection.thread = Mock()
        mock_connection.thread.is_alive = Mock(return_value=False)
        mock_connection.model = Mock()
        mock_connection.model.setup_device = Mock()
        mock_connection.config = IC256_CONFIG
        mock_connection.client = Mock()
        mock_connection.channels = {}
        mock_connection.field_to_path = {}
        app.device_manager.connections[IC256_CONFIG.device_name] = mock_connection
        
        # Track stop() calls
        stop_call_count = [0]
        original_stop = app.device_manager.stop
        
        def track_stop():
            stop_call_count[0] += 1
            original_stop()
        
        app.device_manager.stop = track_stop
        
        with patch('ic256_sampler.application.safe_gui_update'), \
             patch('ic256_sampler.application.set_button_state_safe'), \
             patch('ic256_sampler.application.show_message_safe'), \
             patch('ic256_sampler.application.log_message_safe'), \
             patch('ic256_sampler.file_path_generator.get_timestamp_strings', return_value=("20240101", "120000")), \
             patch('ic256_sampler.application.ModelCollector'), \
             patch('ic256_sampler.model_collector.collect_data_with_model'), \
             patch('threading.Thread'):
            
            # Simulate first acquisition
            app.device_manager._running = True  # Simulate it was running
            app._device_thread()
            
            # Verify stop() was called
            assert stop_call_count[0] >= 1, \
                "device_manager.stop() should be called in _device_thread() to reset state before new acquisition"
            
            # Verify _running was reset
            assert app.device_manager._running is False, \
                "_running should be False after stop() is called, allowing start() to work in next acquisition"