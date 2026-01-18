"""Tests for Application class."""
import pytest
import threading
import time
from unittest.mock import Mock, MagicMock, patch, PropertyMock
from typing import Optional
from ic256_sampler.application import Application, DEFAULT_SAMPLING_RATE, MIN_SAMPLING_RATE, MAX_SAMPLING_RATE
from ic256_sampler.device_manager import DeviceManager, IC256_CONFIG, TX2_CONFIG


# Test helper functions
def create_mock_window(
    ic256_ip: str = "192.168.1.100",
    tx2_ip: str = "",
    note: str = "Test Note",
    save_path: str = "/test/path",
    sampling_rate: str = "500"
) -> Mock:
    """Create a mock GUI window with configurable values.
    
    Args:
        ic256_ip: IC256 IP address
        tx2_ip: TX2 IP address
        note: Note string
        save_path: Save path
        sampling_rate: Sampling rate as string
        
    Returns:
        Mock window object
    """
    mock_window = Mock()
    mock_window.ix256_a_entry = Mock()
    mock_window.ix256_a_entry.get = Mock(return_value=ic256_ip)
    mock_window.tx2_entry = Mock()
    mock_window.tx2_entry.get = Mock(return_value=tx2_ip)
    mock_window.note_entry = Mock()
    mock_window.note_entry.get = Mock(return_value=note)
    mock_window.path_entry = Mock()
    mock_window.path_entry.get = Mock(return_value=save_path)
    mock_window.sampling_entry = Mock()
    mock_window.sampling_entry.get = Mock(return_value=sampling_rate)
    mock_window.get_note_value = Mock(return_value=note)
    mock_window.root = Mock()
    mock_window.root.after = Mock()
    mock_window.reset_elapse_time = Mock()
    mock_window.reset_statistics = Mock()
    mock_window.update_statistics = Mock()
    return mock_window


def create_app_with_window(
    ic256_ip: str = "192.168.1.100",
    tx2_ip: str = "",
    note: str = "Test Note",
    save_path: str = "/test/path",
    sampling_rate: str = "500"
) -> Application:
    """Create an Application instance with a mock window.
    
    Args:
        ic256_ip: IC256 IP address
        tx2_ip: TX2 IP address
        note: Note string
        save_path: Save path
        sampling_rate: Sampling rate as string
        
    Returns:
        Application instance with mock window
    """
    app = Application()
    app.window = create_mock_window(ic256_ip, tx2_ip, note, save_path, sampling_rate)
    return app


def create_mock_device_manager_with_connection(
    device_name: str = IC256_CONFIG.device_name,
    ip_address: str = "192.168.1.100"
) -> tuple[DeviceManager, Mock]:
    """Create a DeviceManager with a mock connection.
    
    Args:
        device_name: Device name
        ip_address: IP address
        
    Returns:
        Tuple of (DeviceManager, mock_connection)
    """
    device_manager = DeviceManager()
    mock_connection = Mock()
    mock_connection.ip_address = ip_address
    mock_connection.thread = Mock()
    mock_connection.thread.is_alive = Mock(return_value=False)
    mock_connection.model = Mock()
    mock_connection.model.setup_device = Mock()
    mock_connection.config = IC256_CONFIG if device_name == IC256_CONFIG.device_name else TX2_CONFIG
    mock_connection.client = Mock()
    mock_connection.channels = {}
    mock_connection.field_to_path = {}
    device_manager.connections[device_name] = mock_connection
    return device_manager, mock_connection


@pytest.fixture
def common_patches():
    """Fixture providing common patches for application tests."""
    return patch.multiple(
        'ic256_sampler.application',
        safe_gui_update=Mock(),
        set_button_state_safe=Mock(),
        show_message_safe=Mock(),
        log_message_safe=Mock()
    )


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
        app = create_app_with_window(
            ic256_ip="192.168.1.100",
            tx2_ip="192.168.1.101",
            note="Test Note",
            save_path="/test/path"
        )
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
        app = create_app_with_window(ic256_ip="192.168.1.100", note="", save_path="")
        
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
        app = create_app_with_window()
        
        with patch('ic256_sampler.application.log_message_safe') as mock_log:
            app._log_callback("Test message", "INFO")
            mock_log.assert_called_once_with(app.window, "Test message", "INFO")
    
    def test_start_collection_no_window(self):
        """Test start_collection when window is None."""
        app = Application()
        app.start_collection()  # Should not raise
    
    def test_start_collection_with_window(self):
        """Test start_collection creates background thread."""
        app = create_app_with_window()
        
        with patch('ic256_sampler.application.set_button_state_safe') as mock_set_button, \
             patch('ic256_sampler.application.show_message_safe') as mock_show_message, \
             patch('threading.Thread') as mock_thread_class:
            mock_thread = Mock()
            mock_thread_class.return_value = mock_thread
            
            app.start_collection()
            
            mock_set_button.assert_called_once_with(app.window, "start_button", "disabled")
            mock_show_message.assert_called_once()
            mock_thread_class.assert_called_once()
            mock_thread.start.assert_called_once()
    
    def test_stop_collection_no_window(self):
        """Test stop_collection when window is None."""
        app = Application()
        app.stop_collection()  # Should not raise
    
    def test_stop_collection_with_resources(self):
        """Test stop_collection stops resources."""
        app = create_app_with_window()
        
        # Create a proper DeviceManager mock with connections dict
        mock_device_manager = DeviceManager()
        mock_collector = Mock()
        mock_thread = Mock()
        # Thread is not alive, so _wait_for_threads_blocking will complete and set _stopping = False
        mock_thread.is_alive = Mock(return_value=False)
        
        app.device_manager = mock_device_manager
        app.collector = mock_collector
        app.collector_thread = mock_thread
        
        with patch('ic256_sampler.application.show_message_safe') as mock_show, \
             patch('ic256_sampler.application.log_message_safe') as mock_log:
            app.stop_collection()
            
            assert app.stop_event.is_set()
            # _stopping is set to True during stop_collection, but _wait_for_threads_blocking()
            # resets it to False at the end (line 604) after all threads finish
            # Since mock thread is not alive, _wait_for_threads_blocking completes and sets _stopping = False
            assert app._stopping is False, f"_stopping should be False after stop_collection completes (threads finished), but is {app._stopping}"
            # DeviceManager.stop() is called via _safe_stop_resource
            # We can't easily assert it was called since it's wrapped, but we can verify state
            mock_collector.stop.assert_called_once()
            mock_show.assert_called()
            mock_log.assert_called()
    
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
        app = create_app_with_window(ic256_ip="192.168.1.200", note="", save_path="")
        
        # Create device manager with existing connection (old IP)
        app.device_manager, mock_old_connection = create_mock_device_manager_with_connection(
            device_name=IC256_CONFIG.device_name,
            ip_address="192.168.1.100"  # Old IP
        )
        
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
        app = create_app_with_window(
            ic256_ip="192.168.1.100",
            note="Test Note",
            save_path="/test/path"
        )
        
        # Setup device manager with a connection
        app.device_manager, mock_connection = create_mock_device_manager_with_connection(
            device_name=IC256_CONFIG.device_name,
            ip_address="192.168.1.100"
        )
        
        # Mock ModelCollector - create separate instances for each acquisition
        mock_collectors = [Mock(), Mock(), Mock()]
        collector_call_count = [0]  # Use list to allow modification in closure
        
        for collector in mock_collectors:
            collector.stop = Mock()
            collector.finalize = Mock()
            collector.statistics = {}
        
        def model_collector_side_effect(*args, **kwargs):
            # Initialize device_statistics (this is what the real create_for_collection does)
            # Args: device_manager, devices_added, sampling_rate, save_folder, note, device_statistics, log_callback
            if len(args) >= 6:
                device_statistics = args[5]
                devices_added = args[1]
            else:
                device_statistics = kwargs.get('device_statistics', {})
                devices_added = kwargs.get('devices_added', [])
            
            if device_statistics is not None:
                device_statistics.clear()
                if devices_added:
                    device_statistics.update({
                        device: {"rows": 0, "file_size": 0, "file_path": ""} 
                        for device in devices_added
                    })
            
            # Return a new collector instance for each call
            idx = collector_call_count[0]
            collector_call_count[0] += 1
            if idx < len(mock_collectors):
                # Set statistics reference on collector
                if device_statistics and devices_added:
                    mock_collectors[idx].statistics = device_statistics.get(devices_added[0], {})
                return mock_collectors[idx]
            # If we run out, return a new Mock
            new_collector = Mock()
            new_collector.stop = Mock()
            new_collector.finalize = Mock()
            new_collector.statistics = device_statistics.get(devices_added[0], {}) if device_statistics and devices_added else {}
            return new_collector
        
        with patch('ic256_sampler.application.safe_gui_update') as mock_safe_update, \
             patch('ic256_sampler.application.set_button_state_safe') as mock_set_button, \
             patch('ic256_sampler.application.show_message_safe') as mock_show, \
             patch('ic256_sampler.application.log_message_safe') as mock_log, \
             patch('ic256_sampler.file_path_generator.get_timestamp_strings', return_value=("20240101", "120000")), \
             patch('ic256_sampler.device_manager.get_timestamp_strings', return_value=("20240101", "120000")), \
             patch('ic256_sampler.model_collector.ModelCollector.get_devices_added', return_value=[IC256_CONFIG.device_name]), \
             patch('ic256_sampler.model_collector.ModelCollector.create_for_collection', side_effect=model_collector_side_effect), \
             patch('ic256_sampler.model_collector.collect_data_with_model') as mock_collect_data, \
             patch('threading.Thread') as mock_thread_class:
            
            # Create mock threads - need separate instances for each acquisition
            mock_time_threads = [Mock(), Mock(), Mock()]
            mock_stats_threads = [Mock(), Mock(), Mock()]
            mock_collector_threads = [Mock(), Mock(), Mock()]
            
            # Configure is_alive behavior for collector threads
            # When stop_collection() is called, the thread should appear finished (False)
            # This allows _check_collector_thread_finished() to reset _stopping immediately
            def make_is_alive(idx):
                call_count = [0]  # Use list to allow modification in closure
                def is_alive():
                    call_count[0] += 1
                    # Return False immediately - thread appears finished when stop is called
                    # This allows synchronous cleanup in stop_collection()
                    return False
                return is_alive
            
            mock_collector_threads[0].is_alive = Mock(side_effect=make_is_alive(0))
            mock_collector_threads[1].is_alive = Mock(side_effect=make_is_alive(1))
            mock_collector_threads[2].is_alive = Mock(side_effect=make_is_alive(2))
            
            thread_call_count = {'elapse_time': 0, 'statistics_update': 0, 'model_collector': 0}
            
            def thread_side_effect(*args, **kwargs):
                name = kwargs.get('name', '')
                if 'elapse_time' in name:
                    idx = thread_call_count['elapse_time']
                    thread_call_count['elapse_time'] += 1
                    if idx < len(mock_time_threads):
                        return mock_time_threads[idx]
                    # Return new mock if we run out
                    new_thread = Mock()
                    new_thread.is_alive = Mock(return_value=False)
                    new_thread.start = Mock()
                    return new_thread
                elif 'statistics_update' in name:
                    idx = thread_call_count['statistics_update']
                    thread_call_count['statistics_update'] += 1
                    if idx < len(mock_stats_threads):
                        return mock_stats_threads[idx]
                    # Return new mock if we run out
                    new_thread = Mock()
                    new_thread.is_alive = Mock(return_value=False)
                    new_thread.start = Mock()
                    return new_thread
                elif 'model_collector' in name:
                    idx = thread_call_count['model_collector']
                    thread_call_count['model_collector'] += 1
                    if idx < len(mock_collector_threads):
                        return mock_collector_threads[idx]
                    # Return new mock if we run out
                    new_thread = Mock()
                    new_thread.is_alive = Mock(return_value=False)
                    new_thread.start = Mock()
                    return new_thread
                # Return new mock for any other thread
                new_thread = Mock()
                new_thread.is_alive = Mock(return_value=False)
                new_thread.start = Mock()
                return new_thread
            
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
            # Note: _stopping is set to True during stop_collection()
            # When a window exists, stop_collection() uses async callbacks, so _stopping
            # may not be reset immediately. We'll verify it gets reset when starting the next acquisition.
            # The collector's stop method should be called (it's stored in app.collector)
            if app.collector:
                app.collector.stop.assert_called()
            
            # Manually trigger the cleanup to reset _stopping (simulating async callback)
            # This ensures state is ready for the next acquisition
            # Note: _stopping may not be reset immediately due to async behavior, but
            # the important thing is that the next acquisition can start correctly
            app._check_collector_thread_finished()
            
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
            # The collector's stop method should be called (it's stored in app.collector)
            if app.collector:
                app.collector.stop.assert_called()
            
            # Simulate collector thread finishing
            app._check_collector_thread_finished()
            
            # Note: _stopping may not be reset immediately due to async behavior, but
            # the important thing is that the next acquisition can start correctly
            
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
             patch('ic256_sampler.model_collector.ModelCollector.get_devices_added', return_value=[IC256_CONFIG.device_name]), \
             patch('ic256_sampler.model_collector.ModelCollector.create_for_collection', return_value=Mock()), \
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