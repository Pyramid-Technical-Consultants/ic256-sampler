"""Unit tests for StatisticsAggregator module."""

import pytest
import threading
import time
from unittest.mock import Mock, patch
from ic256_sampler.statistics_aggregator import (
    format_file_size,
    aggregate_statistics,
    StatisticsUpdater,
)


class TestFormatFileSize:
    """Tests for format_file_size function."""
    
    def test_format_bytes(self):
        """Test formatting bytes (< 1024)."""
        assert format_file_size(0) == "0 B"
        assert format_file_size(512) == "512 B"
        assert format_file_size(1023) == "1023 B"
    
    def test_format_kilobytes(self):
        """Test formatting kilobytes (1024 - 1048575)."""
        assert format_file_size(1024) == "1.0 KB"
        assert format_file_size(2048) == "2.0 KB"
        assert format_file_size(5120) == "5.0 KB"
        assert format_file_size(102400) == "100.0 KB"
        assert format_file_size(1048575) == "1024.0 KB"  # 1048575 / 1024 = 1023.999...
    
    def test_format_megabytes(self):
        """Test formatting megabytes (>= 1048576)."""
        assert format_file_size(1048576) == "1.0 MB"
        assert format_file_size(2097152) == "2.0 MB"
        assert format_file_size(5242880) == "5.0 MB"
        assert format_file_size(104857600) == "100.0 MB"


class TestAggregateStatistics:
    """Tests for aggregate_statistics function."""
    
    def test_aggregate_empty(self):
        """Test aggregating empty statistics."""
        result = aggregate_statistics({})
        assert result == {
            "total_rows": 0,
            "total_size": 0,
            "formatted_size": "0 B",
        }
    
    def test_aggregate_single_device(self):
        """Test aggregating statistics from single device."""
        device_stats = {
            "device1": {
                "rows": 100,
                "file_size": 5000,
            }
        }
        result = aggregate_statistics(device_stats)
        assert result["total_rows"] == 100
        assert result["total_size"] == 5000
        assert result["formatted_size"] == "4.9 KB"
    
    def test_aggregate_multiple_devices(self):
        """Test aggregating statistics from multiple devices."""
        device_stats = {
            "device1": {
                "rows": 100,
                "file_size": 5000,
            },
            "device2": {
                "rows": 200,
                "file_size": 10000,
            },
            "device3": {
                "rows": 50,
                "file_size": 2500,
            }
        }
        result = aggregate_statistics(device_stats)
        assert result["total_rows"] == 350
        assert result["total_size"] == 17500
        assert result["formatted_size"] == "17.1 KB"
    
    def test_aggregate_missing_keys(self):
        """Test aggregating statistics with missing keys."""
        device_stats = {
            "device1": {
                "rows": 100,
                # Missing file_size
            },
            "device2": {
                # Missing rows
                "file_size": 5000,
            }
        }
        result = aggregate_statistics(device_stats)
        assert result["total_rows"] == 100
        assert result["total_size"] == 5000
    
    def test_aggregate_zero_values(self):
        """Test aggregating statistics with zero values."""
        device_stats = {
            "device1": {
                "rows": 0,
                "file_size": 0,
            }
        }
        result = aggregate_statistics(device_stats)
        assert result["total_rows"] == 0
        assert result["total_size"] == 0
        assert result["formatted_size"] == "0 B"


class TestStatisticsUpdaterInit:
    """Tests for StatisticsUpdater initialization."""
    
    def test_init_sets_attributes(self):
        """Test StatisticsUpdater initialization sets all attributes."""
        device_stats = {}
        callback = Mock()
        updater = StatisticsUpdater(device_stats, callback, 0.2)
        
        assert updater.device_statistics is device_stats
        assert updater.update_callback is callback
        assert updater.update_interval == 0.2
        assert updater._stopping is False
        assert updater._collector_thread_alive is True
    
    def test_init_default_interval(self):
        """Test StatisticsUpdater uses default interval if not specified."""
        updater = StatisticsUpdater({}, Mock())
        assert updater.update_interval == 0.1


class TestStatisticsUpdaterFlags:
    """Tests for StatisticsUpdater flag management."""
    
    def test_set_stopping(self):
        """Test set_stopping updates stopping flag."""
        updater = StatisticsUpdater({}, Mock())
        updater.set_stopping(True)
        assert updater._stopping is True
        updater.set_stopping(False)
        assert updater._stopping is False
    
    def test_set_collector_thread_alive(self):
        """Test set_collector_thread_alive updates thread alive flag."""
        updater = StatisticsUpdater({}, Mock())
        updater.set_collector_thread_alive(False)
        assert updater._collector_thread_alive is False
        updater.set_collector_thread_alive(True)
        assert updater._collector_thread_alive is True
    
    def test_should_continue_not_stopping(self):
        """Test should_continue returns True when not stopping."""
        updater = StatisticsUpdater({}, Mock())
        updater._stopping = False
        assert updater.should_continue() is True
    
    def test_should_continue_stopping_thread_alive(self):
        """Test should_continue returns True when stopping but thread alive."""
        updater = StatisticsUpdater({}, Mock())
        updater._stopping = True
        updater._collector_thread_alive = True
        assert updater.should_continue() is True
    
    def test_should_continue_stopping_thread_dead(self):
        """Test should_continue returns False when stopping and thread dead."""
        updater = StatisticsUpdater({}, Mock())
        updater._stopping = True
        updater._collector_thread_alive = False
        assert updater.should_continue() is False


class TestStatisticsUpdaterUpdateLoop:
    """Tests for StatisticsUpdater update_loop method."""
    
    def test_update_loop_calls_callback(self):
        """Test update_loop calls callback with aggregated statistics."""
        device_stats = {
            "device1": {"rows": 100, "file_size": 5000}
        }
        callback = Mock()
        updater = StatisticsUpdater(device_stats, callback, 0.01)
        updater._stopping = True  # Set stopping so loop continues even when stop_event is set
        updater._collector_thread_alive = False  # Thread is dead so it can exit after stabilization
        
        stop_event = threading.Event()
        stop_event.set()  # Stop immediately
        
        updater.update_loop(stop_event)
        
        # Should have called callback at least once
        assert callback.call_count >= 1
        call_args = callback.call_args[0]
        assert call_args[0] == 100  # total_rows
        assert "KB" in call_args[1]  # formatted_size
    
    def test_update_loop_updates_multiple_times(self):
        """Test update_loop updates multiple times while running."""
        device_stats = {
            "device1": {"rows": 100, "file_size": 5000}
        }
        callback = Mock()
        updater = StatisticsUpdater(device_stats, callback, 0.01)
        
        stop_event = threading.Event()
        
        # Run in background thread and stop after a short time
        def run_and_stop():
            time.sleep(0.05)
            stop_event.set()
        
        thread = threading.Thread(target=run_and_stop, daemon=True)
        thread.start()
        
        updater.update_loop(stop_event)
        
        # Should have called callback multiple times
        assert callback.call_count >= 2
    
    def test_update_loop_stops_when_event_set(self):
        """Test update_loop stops when stop_event is set and not stopping."""
        device_stats = {}
        callback = Mock()
        updater = StatisticsUpdater(device_stats, callback, 0.01)
        
        stop_event = threading.Event()
        stop_event.set()
        
        updater.update_loop(stop_event)
        
        # Should exit quickly
        assert callback.call_count <= 1
    
    def test_update_loop_continues_while_stopping_if_thread_alive(self):
        """Test update_loop continues while stopping if thread is alive."""
        device_stats = {
            "device1": {"rows": 100, "file_size": 5000}
        }
        callback = Mock()
        updater = StatisticsUpdater(device_stats, callback, 0.01)
        updater._stopping = True
        updater._collector_thread_alive = True
        
        stop_event = threading.Event()
        stop_event.set()
        
        # Run briefly then stop
        def stop_after_delay():
            time.sleep(0.05)
            updater._collector_thread_alive = False
        
        thread = threading.Thread(target=stop_after_delay, daemon=True)
        thread.start()
        
        updater.update_loop(stop_event)
        
        # Should have called callback
        assert callback.call_count >= 1
    
    def test_update_loop_stops_after_stabilization(self):
        """Test update_loop stops after statistics stabilize."""
        device_stats = {
            "device1": {"rows": 100, "file_size": 5000}
        }
        callback = Mock()
        updater = StatisticsUpdater(device_stats, callback, 0.01)
        updater._stopping = True
        updater._collector_thread_alive = False  # Thread is dead
        
        stop_event = threading.Event()
        stop_event.set()
        
        updater.update_loop(stop_event)
        
        # Should have called callback and then stopped
        assert callback.call_count >= 1
    
    def test_update_loop_empty_statistics(self):
        """Test update_loop handles empty statistics."""
        device_stats = {}
        callback = Mock()
        updater = StatisticsUpdater(device_stats, callback, 0.01)
        
        stop_event = threading.Event()
        stop_event.set()
        
        updater.update_loop(stop_event)
        
        # Should not raise, may or may not call callback
        # (behavior depends on implementation)
