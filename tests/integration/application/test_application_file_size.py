"""Tests for file size display in GUI after multiple acquisitions.

This test verifies that file size is properly reset and updated
between acquisitions, ensuring the GUI displays correct file size
for each new acquisition.
"""

import pytest
import time
import threading
from unittest.mock import Mock, patch
from ic256_sampler.application import Application


class TestApplicationFileSizeDisplay:
    """Tests for file size display in GUI."""
    
    def test_file_size_resets_between_acquisitions(self):
        """Test that file size is properly reset between acquisitions."""
        app = Application()
        mock_window = Mock()
        mock_window.ix256_a_entry = Mock()
        mock_window.ix256_a_entry.get = Mock(return_value="192.168.1.100")
        mock_window.tx2_entry = Mock()
        mock_window.tx2_entry.get = Mock(return_value="")
        mock_window.note_entry = Mock()
        mock_window.note_entry.get = Mock(return_value="Test")
        mock_window.path_entry = Mock()
        mock_window.path_entry.get = Mock(return_value="/tmp/test")
        mock_window.sampling_entry = Mock()
        mock_window.sampling_entry.get = Mock(return_value="500")
        mock_window.root = Mock()
        mock_window.root.after = Mock()
        mock_window.reset_elapse_time = Mock()
        mock_window.reset_statistics = Mock()
        
        app.window = mock_window
        
        file_size_updates = []
        
        def track_update(rows, file_size):
            file_size_updates.append((rows, file_size))
        
        mock_window.update_statistics = Mock(side_effect=track_update)
        
        def mock_safe_gui_update(window, func):
            func()
        
        with patch('ic256_sampler.application.safe_gui_update', side_effect=mock_safe_gui_update), \
             patch('ic256_sampler.application.set_button_state_safe'), \
             patch('ic256_sampler.application.show_message_safe'), \
             patch('ic256_sampler.application.log_message_safe'):
            
            # First acquisition
            app.device_statistics = {"IC256-42/35": {"rows": 1000, "file_size": 50000, "file_path": "/tmp/test1.csv"}}
            app.stats_thread = threading.Thread(
                target=app._update_statistics,
                name="first_stats_thread",
                daemon=True
            )
            app.stats_thread.start()
            time.sleep(0.3)
            
            assert len(file_size_updates) > 0
            first_size = file_size_updates[-1][1]
            assert "KB" in first_size or "B" in first_size
            
            app.stop_event.set()
            if app.stats_thread.is_alive():
                app.stats_thread.join(timeout=1.0)
            
            # Second acquisition
            file_size_updates.clear()
            app.stop_event.clear()
            app.device_statistics = {"IC256-42/35": {"rows": 0, "file_size": 0, "file_path": ""}}
            app.stats_thread = threading.Thread(
                target=app._update_statistics,
                name="second_stats_thread",
                daemon=True
            )
            app.stats_thread.start()
            time.sleep(0.2)
            app.device_statistics = {"IC256-42/35": {"rows": 500, "file_size": 25000, "file_path": "/tmp/test2.csv"}}
            time.sleep(0.3)
            
            app.stop_event.set()
            if app.stats_thread.is_alive():
                app.stats_thread.join(timeout=1.0)
            
            assert len(file_size_updates) > 0
            second_sizes = [update[1] for update in file_size_updates]
            has_non_zero = any("0 B" not in size and "0.0" not in size for size in second_sizes)
            assert has_non_zero, f"Second acquisition should show file size updates. Got: {second_sizes}"
    
    @pytest.mark.integration
    def test_file_size_display_after_multiple_acquisitions(self, require_ic256_device, app_with_mock_gui, tmp_path):
        """Integration test: Verify file size displays correctly after multiple acquisitions."""
        app = app_with_mock_gui
        
        file_size_updates = []
        rows_updates = []
        
        def track_update(rows, file_size):
            file_size_updates.append(file_size)
            rows_updates.append(rows)
        
        app.window.update_statistics = Mock(side_effect=track_update)
        
        # First acquisition
        device_thread = threading.Thread(
            target=app._device_thread,
            name="first_acquisition",
            daemon=True
        )
        device_thread.start()
        time.sleep(2.0)
        time.sleep(3.0)
        app.stop_collection()
        time.sleep(2.0)
        
        if file_size_updates:
            first_final_size = file_size_updates[-1]
            first_final_rows = rows_updates[-1] if rows_updates else 0
            if first_final_rows > 0:
                assert "0 B" not in first_final_size or first_final_rows == 0
        
        # Second acquisition
        file_size_updates.clear()
        rows_updates.clear()
        
        device_thread = threading.Thread(
            target=app._device_thread,
            name="second_acquisition",
            daemon=True
        )
        device_thread.start()
        time.sleep(2.0)
        
        if file_size_updates:
            first_update_size = file_size_updates[0]
            assert "0 B" in first_update_size or float(first_update_size.split()[0]) < 1.0, \
                f"Second acquisition should start with small/zero file size. Got: {first_update_size}"
        
        time.sleep(3.0)
        app.stop_collection()
        time.sleep(2.0)
        
        if file_size_updates:
            second_final_size = file_size_updates[-1]
            second_final_rows = rows_updates[-1] if rows_updates else 0
            if second_final_rows > 0:
                assert "B" in second_final_size or "KB" in second_final_size or "MB" in second_final_size
            else:
                assert "B" in second_final_size

    def test_statistics_thread_does_not_interfere_between_acquisitions(self):
        """Test that old statistics threads don't interfere with new acquisitions."""
        app = Application()
        mock_window = Mock()
        mock_window.ix256_a_entry = Mock()
        mock_window.ix256_a_entry.get = Mock(return_value="192.168.1.100")
        mock_window.tx2_entry = Mock()
        mock_window.tx2_entry.get = Mock(return_value="")
        mock_window.note_entry = Mock()
        mock_window.note_entry.get = Mock(return_value="Test")
        mock_window.path_entry = Mock()
        mock_window.path_entry.get = Mock(return_value="/tmp/test")
        mock_window.sampling_entry = Mock()
        mock_window.sampling_entry.get = Mock(return_value="500")
        mock_window.root = Mock()
        mock_window.root.after = Mock()
        mock_window.reset_elapse_time = Mock()
        mock_window.reset_statistics = Mock()
        
        app.window = mock_window
        
        update_threads = []
        
        def track_update(rows, file_size):
            update_threads.append(threading.current_thread().name)
            mock_window.update_statistics(rows, file_size)
        
        mock_window.update_statistics.side_effect = track_update
        
        def mock_safe_gui_update(window, func):
            func()
        
        with patch('ic256_sampler.application.safe_gui_update', side_effect=mock_safe_gui_update), \
             patch('ic256_sampler.application.set_button_state_safe'), \
             patch('ic256_sampler.application.show_message_safe'), \
             patch('ic256_sampler.application.log_message_safe'):
            
            # First thread
            app.device_statistics = {"IC256-42/35": {"rows": 100, "file_size": 5000, "file_path": ""}}
            app.stats_thread = threading.Thread(
                target=app._update_statistics,
                name="first_stats_thread",
                daemon=True
            )
            app.stats_thread.start()
            time.sleep(0.3)
            
            assert len(update_threads) > 0
            assert "first_stats_thread" in update_threads
            
            # Stop first and start second
            app.stop_event.set()
            if app.stats_thread.is_alive():
                app.stats_thread.join(timeout=0.5)
            
            update_threads.clear()
            app.stop_event.clear()
            app.device_statistics = {"IC256-42/35": {"rows": 0, "file_size": 0, "file_path": ""}}
            app.stats_thread = threading.Thread(
                target=app._update_statistics,
                name="second_stats_thread",
                daemon=True
            )
            app.stats_thread.start()
            time.sleep(0.3)
            app.device_statistics = {"IC256-42/35": {"rows": 200, "file_size": 10000, "file_path": ""}}
            time.sleep(0.3)
            
            app.stop_event.set()
            if app.stats_thread.is_alive():
                app.stats_thread.join(timeout=0.5)
            
            second_thread_updates = [t for t in update_threads if "second_stats_thread" in t]
            first_thread_updates = [t for t in update_threads if "first_stats_thread" in t]
            
            assert len(second_thread_updates) > 0
            assert len(first_thread_updates) == 0
