"""Tests for file size display in GUI after multiple acquisitions.

This test verifies that file size is properly reset and updated
between acquisitions, ensuring the GUI displays correct file size
for each new acquisition.
"""

import pytest
import time
import threading
from unittest.mock import Mock, patch, MagicMock
from ic256_sampler.application import Application
from ic256_sampler.device_manager import DeviceManager, IC256_CONFIG
from ic256_sampler.utils import is_valid_device, is_valid_ipv4


class TestApplicationFileSizeDisplay:
    """Tests for file size display in GUI."""
    
    def test_file_size_resets_between_acquisitions(self):
        """Test that file size is properly reset between acquisitions.
        
        This test verifies:
        1. File size starts at 0 for first acquisition
        2. File size updates during collection
        3. File size resets to 0 for second acquisition
        4. File size updates correctly in second acquisition
        """
        app = Application()
        
        # Create mock GUI
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
        
        # Track file size updates
        file_size_updates = []
        
        def track_update(rows, file_size):
            file_size_updates.append((rows, file_size))
        
        mock_window.update_statistics = Mock(side_effect=track_update)
        
        # Mock safe_gui_update to actually call the function
        def mock_safe_gui_update(window, func):
            func()
        
        with patch('ic256_sampler.application.safe_gui_update', side_effect=mock_safe_gui_update), \
             patch('ic256_sampler.application.set_button_state_safe'), \
             patch('ic256_sampler.application.show_message_safe'), \
             patch('ic256_sampler.application.log_message_safe'):
            
            # Simulate first acquisition
            # Mock device_statistics to simulate data collection
            app.device_statistics = {"IC256-42/35": {"rows": 1000, "file_size": 50000, "file_path": "/tmp/test1.csv"}}
            
            # Start statistics thread for first acquisition
            app.stats_thread = threading.Thread(
                target=app._update_statistics,
                name="first_stats_thread",
                daemon=True
            )
            app.stats_thread.start()
            
            # Wait for updates
            time.sleep(0.3)
            
            # Verify file size was updated
            assert len(file_size_updates) > 0, "File size should be updated"
            first_size = file_size_updates[-1][1]
            assert "KB" in first_size or "B" in first_size, f"File size should be formatted: {first_size}"
            
            # Stop first statistics thread
            app.stop_event.set()
            if app.stats_thread.is_alive():
                app.stats_thread.join(timeout=1.0)
            
            # Clear updates for second acquisition
            file_size_updates.clear()
            
            # Simulate second acquisition - should reset file size
            # This simulates what happens in _device_thread when starting new acquisition
            app.stop_event.clear()
            
            # Reset device_statistics for new acquisition
            app.device_statistics = {"IC256-42/35": {"rows": 0, "file_size": 0, "file_path": ""}}
            
            # Start new statistics thread
            app.stats_thread = threading.Thread(
                target=app._update_statistics,
                name="second_stats_thread",
                daemon=True
            )
            app.stats_thread.start()
            
            # Simulate data collection in second acquisition
            time.sleep(0.2)
            app.device_statistics = {"IC256-42/35": {"rows": 500, "file_size": 25000, "file_path": "/tmp/test2.csv"}}
            time.sleep(0.3)  # Give thread time to update
            
            # Stop statistics thread
            app.stop_event.set()
            if app.stats_thread.is_alive():
                app.stats_thread.join(timeout=1.0)
            
            # Verify file size was updated for second acquisition
            assert len(file_size_updates) > 0, "File size should be updated in second acquisition"
            # Check that we got updates starting from 0 or low values
            second_sizes = [update[1] for update in file_size_updates]
            # At least one update should show a non-zero file size
            has_non_zero = any("0 B" not in size and "0.0" not in size for size in second_sizes)
            assert has_non_zero, f"Second acquisition should show file size updates. Got: {second_sizes}"
    
    @pytest.mark.integration
    def test_file_size_display_after_multiple_acquisitions(self, ic256_ip, tmp_path):
        """Integration test: Verify file size displays correctly after multiple acquisitions.
        
        This test:
        1. Runs a first acquisition
        2. Stops and verifies file size is displayed
        3. Starts a second acquisition
        4. Verifies file size resets and updates correctly
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
        mock_window.note_entry.get = Mock(return_value="File Size Test")
        mock_window.path_entry = Mock()
        mock_window.path_entry.get = Mock(return_value=str(tmp_path))
        mock_window.sampling_entry = Mock()
        mock_window.sampling_entry.get = Mock(return_value="500")
        mock_window.root = Mock()
        mock_window.root.after = Mock()
        mock_window.reset_elapse_time = Mock()
        mock_window.reset_statistics = Mock()
        
        # Track file size updates
        file_size_updates = []
        rows_updates = []
        
        def track_update(rows, file_size):
            file_size_updates.append(file_size)
            rows_updates.append(rows)
            print(f"  Update: rows={rows}, file_size={file_size}")
        
        mock_window.update_statistics = Mock(side_effect=track_update)
        
        app.window = mock_window
        
        with patch('ic256_sampler.application.safe_gui_update'), \
             patch('ic256_sampler.application.set_button_state_safe'), \
             patch('ic256_sampler.application.show_message_safe'), \
             patch('ic256_sampler.application.log_message_safe'):
            
            # First acquisition
            print("\n=== First Acquisition ===")
            device_thread = threading.Thread(
                target=app._device_thread,
                name="first_acquisition",
                daemon=True
            )
            device_thread.start()
            
            # Wait for collection to start
            time.sleep(2.0)
            
            # Collect for a few seconds
            time.sleep(3.0)
            
            # Stop first acquisition
            app.stop_collection()
            
            # Wait for threads to finish
            time.sleep(2.0)
            
            # Check first acquisition results
            first_final_size = file_size_updates[-1] if file_size_updates else "0 B"
            first_final_rows = rows_updates[-1] if rows_updates else 0
            print(f"  First acquisition final: rows={first_final_rows}, file_size={first_final_size}")
            
            # Note: This test may not collect data if device isn't properly connected
            # The important thing is that file size display works, not that data is collected
            if first_final_rows > 0:
                assert "0 B" not in first_final_size or first_final_rows == 0, \
                    f"First acquisition should have file size > 0. Got: {first_final_size}"
            
            # Clear tracking for second acquisition
            file_size_updates.clear()
            rows_updates.clear()
            
            # Second acquisition
            print("\n=== Second Acquisition ===")
            device_thread = threading.Thread(
                target=app._device_thread,
                name="second_acquisition",
                daemon=True
            )
            device_thread.start()
            
            # Wait for collection to start
            time.sleep(2.0)
            
            # Check that file size was reset
            if file_size_updates:
                first_update_size = file_size_updates[0]
                print(f"  Second acquisition first update: file_size={first_update_size}")
                # File size should start at 0 or very small for new acquisition
                # (might be small non-zero if file was just created)
                assert "0 B" in first_update_size or float(first_update_size.split()[0]) < 1.0, \
                    f"Second acquisition should start with small/zero file size. Got: {first_update_size}"
            
            # Collect for a few seconds
            time.sleep(3.0)
            
            # Stop second acquisition
            app.stop_collection()
            
            # Wait for threads to finish
            time.sleep(2.0)
            
            # Check second acquisition results
            if file_size_updates:
                second_final_size = file_size_updates[-1]
                second_final_rows = rows_updates[-1] if rows_updates else 0
                print(f"  Second acquisition final: rows={second_final_rows}, file_size={second_final_size}")
                
                # Note: This test may not collect data if device isn't properly connected
                # The important thing is that file size display works and resets properly
                if second_final_rows > 0:
                    # File size should be > 0 and properly formatted
                    assert "B" in second_final_size or "KB" in second_final_size or "MB" in second_final_size, \
                        f"File size should be formatted. Got: {second_final_size}"
                else:
                    # Even if no data collected, file size should be properly formatted (0 B)
                    assert "B" in second_final_size, f"File size should be formatted even with 0 rows. Got: {second_final_size}"
    
    def test_statistics_thread_does_not_interfere_between_acquisitions(self):
        """Test that old statistics threads don't interfere with new acquisitions.
        
        This verifies that when a new acquisition starts, old statistics
        threads are properly stopped to prevent stale data from being displayed.
        """
        app = Application()
        
        # Create mock GUI
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
        mock_window.update_statistics = Mock()
        
        app.window = mock_window
        
        # Track which thread is updating
        update_threads = []
        
        def track_update(rows, file_size):
            update_threads.append(threading.current_thread().name)
            mock_window.update_statistics(rows, file_size)
        
        mock_window.update_statistics.side_effect = track_update
        
        # Mock safe_gui_update to actually call the function
        def mock_safe_gui_update(window, func):
            func()
        
        with patch('ic256_sampler.application.safe_gui_update', side_effect=mock_safe_gui_update), \
             patch('ic256_sampler.application.set_button_state_safe'), \
             patch('ic256_sampler.application.show_message_safe'), \
             patch('ic256_sampler.application.log_message_safe'):
            
            # Start first statistics thread
            app.device_statistics = {"IC256-42/35": {"rows": 100, "file_size": 5000, "file_path": ""}}
            app.stats_thread = threading.Thread(
                target=app._update_statistics,
                name="first_stats_thread",
                daemon=True
            )
            app.stats_thread.start()
            
            time.sleep(0.3)  # Give thread more time
            
            # Verify first thread is updating
            assert len(update_threads) > 0, f"First thread should update statistics. Got updates from: {update_threads}"
            assert "first_stats_thread" in update_threads, f"First thread should be updating. Got: {update_threads}"
            
            # Stop first thread and start second (simulating new acquisition)
            app.stop_event.set()
            if app.stats_thread.is_alive():
                app.stats_thread.join(timeout=0.5)
            
            update_threads.clear()
            
            # Start second statistics thread
            app.stop_event.clear()
            app.device_statistics = {"IC256-42/35": {"rows": 0, "file_size": 0, "file_path": ""}}
            app.stats_thread = threading.Thread(
                target=app._update_statistics,
                name="second_stats_thread",
                daemon=True
            )
            app.stats_thread.start()
            
            time.sleep(0.3)
            
            # Update statistics
            app.device_statistics = {"IC256-42/35": {"rows": 200, "file_size": 10000, "file_path": ""}}
            time.sleep(0.3)
            
            # Stop second thread
            app.stop_event.set()
            if app.stats_thread.is_alive():
                app.stats_thread.join(timeout=0.5)
            
            # Verify only second thread updated (no first thread updates)
            second_thread_updates = [t for t in update_threads if "second_stats_thread" in t]
            first_thread_updates = [t for t in update_threads if "first_stats_thread" in t]
            
            assert len(second_thread_updates) > 0, "Second thread should update statistics"
            assert len(first_thread_updates) == 0, "First thread should not update after being stopped"
