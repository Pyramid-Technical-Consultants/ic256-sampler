"""Tests to verify stop operations are non-blocking and don't freeze the GUI.

This test verifies that stop_collection() and device_manager.stop() don't
block the calling thread, especially with large files.
"""

import pytest
import time
import threading
from unittest.mock import Mock, patch, MagicMock
from ic256_sampler.application import Application
from ic256_sampler.device_manager import DeviceManager


class TestStopNonBlocking:
    """Tests to verify stop operations are non-blocking."""
    
    def test_device_manager_stop_is_non_blocking(self):
        """Test that DeviceManager.stop() doesn't block for long periods.
        
        This verifies that stop() doesn't wait for threads to finish,
        which would block the GUI thread.
        """
        device_manager = DeviceManager()
        
        # Create mock connections with threads that take time to finish
        mock_thread1 = Mock()
        mock_thread1.is_alive = Mock(return_value=True)  # Thread is still alive
        mock_thread1.join = Mock()  # Track if join is called
        
        mock_thread2 = Mock()
        mock_thread2.is_alive = Mock(return_value=True)  # Thread is still alive
        mock_thread2.join = Mock()  # Track if join is called
        
        mock_connection1 = Mock()
        mock_connection1.thread = mock_thread1
        
        mock_connection2 = Mock()
        mock_connection2.thread = mock_thread2
        
        device_manager.connections = {
            "device1": mock_connection1,
            "device2": mock_connection2
        }
        device_manager._running = True
        
        # Call stop and measure time
        start_time = time.time()
        device_manager.stop()
        elapsed_time = time.time() - start_time
        
        # stop() should return immediately (non-blocking)
        # It should not call thread.join() which would block
        assert elapsed_time < 0.1, f"stop() should return immediately, took {elapsed_time:.3f}s"
        
        # Verify stop_event was set and _running was set to False
        assert device_manager.stop_event.is_set(), "stop_event should be set"
        assert device_manager._running is False, "_running should be False"
        
        # Verify thread.join() was NOT called (non-blocking)
        # The old implementation would call join(), but the new one doesn't
        mock_thread1.join.assert_not_called(), "stop() should not call thread.join() (non-blocking)"
        mock_thread2.join.assert_not_called(), "stop() should not call thread.join() (non-blocking)"
    
    def test_stop_collection_is_non_blocking(self):
        """Test that stop_collection() doesn't block the GUI thread.
        
        This verifies that stop_collection() returns quickly and uses
        non-blocking callbacks to check thread completion.
        """
        app = Application()
        
        # Create mock GUI
        mock_window = Mock()
        mock_window.root = Mock()
        mock_window.root.after = Mock()
        app.window = mock_window
        
        # Create mock device_manager and collector
        mock_device_manager = Mock()
        mock_collector = Mock()
        mock_collector_thread = Mock()
        mock_collector_thread.is_alive = Mock(return_value=True)  # Thread still alive
        
        app.device_manager = mock_device_manager
        app.collector = mock_collector
        app.collector_thread = mock_collector_thread
        
        # Call stop_collection and measure time
        start_time = time.time()
        app.stop_collection()
        elapsed_time = time.time() - start_time
        
        # stop_collection() should return immediately (non-blocking)
        assert elapsed_time < 0.1, f"stop_collection() should return immediately, took {elapsed_time:.3f}s"
        
        # Verify stop_event was set
        assert app.stop_event.is_set(), "stop_event should be set"
        assert app._stopping is True, "_stopping should be True"
        
        # Verify device_manager.stop() was called
        mock_device_manager.stop.assert_called_once()
        
        # Verify collector.stop() was called
        mock_collector.stop.assert_called_once()
        
        # Verify non-blocking callback was scheduled (not blocking join)
        mock_window.root.after.assert_called()
        # Should be called with _check_collector_thread_finished
        call_args = mock_window.root.after.call_args
        assert call_args is not None, "root.after() should be called for non-blocking check"
    
    def test_stop_collection_with_large_file_simulation(self):
        """Test that stop_collection() remains non-blocking even with large files.
        
        This verifies that with the fix, device_manager.stop() is non-blocking
        and stop_collection() returns immediately even when threads are still
        processing large amounts of data.
        """
        app = Application()
        
        # Create mock GUI
        mock_window = Mock()
        mock_window.root = Mock()
        mock_window.root.after = Mock()
        app.window = mock_window
        
        # Create mock device_manager - with the fix, stop() should be fast
        # (it no longer calls thread.join() which would block)
        mock_device_manager = Mock()
        # stop() should return immediately (non-blocking)
        mock_device_manager.stop = Mock()  # Fast, non-blocking
        
        mock_collector = Mock()
        mock_collector_thread = Mock()
        mock_collector_thread.is_alive = Mock(return_value=True)  # Thread still processing
        
        app.device_manager = mock_device_manager
        app.collector = mock_collector
        app.collector_thread = mock_collector_thread
        
        # Call stop_collection and measure time
        start_time = time.time()
        app.stop_collection()
        elapsed_time = time.time() - start_time
        
        # With the fix, stop_collection() should return immediately
        # even if threads are still processing large files
        assert elapsed_time < 0.1, \
            f"stop_collection() should return quickly even with large files, took {elapsed_time:.3f}s"
        
        # Verify device_manager.stop() was called (non-blocking)
        mock_device_manager.stop.assert_called_once()
        
        # Verify non-blocking callback was scheduled
        mock_window.root.after.assert_called()
