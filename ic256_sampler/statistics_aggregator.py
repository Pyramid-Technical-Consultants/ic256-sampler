"""Statistics aggregation utilities for data collection."""

import time
from typing import Dict, Any, Optional, Callable


def format_file_size(size_bytes: int) -> str:
    """Format file size in bytes to human-readable string.
    
    Args:
        size_bytes: File size in bytes
        
    Returns:
        Formatted string (e.g., "1.5 MB", "500 KB", "1024 B")
    """
    if size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.1f} KB"
    else:
        return f"{size_bytes / (1024 * 1024):.1f} MB"


def aggregate_statistics(device_statistics: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    """Aggregate statistics across all devices.
    
    Args:
        device_statistics: Dictionary mapping device names to their statistics
        
    Returns:
        Dictionary with aggregated statistics (total_rows, total_size)
    """
    total_rows = sum(stats.get("rows", 0) for stats in device_statistics.values())
    total_size = sum(stats.get("file_size", 0) for stats in device_statistics.values())
    
    return {
        "total_rows": total_rows,
        "total_size": total_size,
        "formatted_size": format_file_size(total_size),
    }


class StatisticsUpdater:
    """Handles periodic statistics updates for the GUI."""
    
    def __init__(
        self,
        device_statistics: Dict[str, Dict[str, Any]],
        update_callback: Callable[[int, str], None],
        update_interval: float = 0.1,
    ):
        """Initialize statistics updater.
        
        Args:
            device_statistics: Dictionary mapping device names to their statistics
            update_callback: Function to call with (rows, formatted_size)
            update_interval: Time between updates in seconds
        """
        self.device_statistics = device_statistics
        self.update_callback = update_callback
        self.update_interval = update_interval
        self._stopping = False
        self._collector_thread_alive = True
    
    def set_stopping(self, stopping: bool) -> None:
        """Set stopping flag.
        
        Args:
            stopping: True if collection is stopping
        """
        self._stopping = stopping
    
    def set_collector_thread_alive(self, alive: bool) -> None:
        """Set collector thread alive status.
        
        Args:
            alive: True if collector thread is alive
        """
        self._collector_thread_alive = alive
    
    def should_continue(self) -> bool:
        """Check if statistics updates should continue.
        
        Returns:
            True if updates should continue
        """
        return (
            not self._stopping or
            (self._stopping and self._collector_thread_alive)
        )
    
    def update_loop(self, stop_event) -> None:
        """Run statistics update loop.
        
        Continues updating even after stop_event is set, until collector thread finishes
        AND statistics have stabilized (no changes for a while).
        
        Args:
            stop_event: Threading event to signal stop
        """
        consecutive_no_change = 0
        previous_total_rows = 0
        previous_total_size = 0
        
        while True:
            should_continue = (
                not stop_event.is_set() or
                (self._stopping and self._collector_thread_alive)
            )
            
            if self.device_statistics:
                aggregated = aggregate_statistics(self.device_statistics)
                total_rows = aggregated["total_rows"]
                total_size = aggregated["total_size"]
                
                # Check if statistics have changed
                if self._stopping:
                    if (total_rows == previous_total_rows and 
                        total_size == previous_total_size):
                        consecutive_no_change += 1
                    else:
                        consecutive_no_change = 0
                        previous_total_rows = total_rows
                        previous_total_size = total_size
                    
                    # If collector thread finished and statistics haven't changed for 15 updates (1.5 seconds), we're done
                    if (not self._collector_thread_alive and 
                        consecutive_no_change >= 15):
                        # Final update before stopping
                        self.update_callback(total_rows, aggregated["formatted_size"])
                        break
                else:
                    # Not in stopping mode, update previous values
                    previous_total_rows = total_rows
                    previous_total_size = total_size
                
                # Update GUI
                self.update_callback(total_rows, aggregated["formatted_size"])
            
            if not should_continue:
                # Not in stopping mode and stop_event is set, exit
                break
            
            time.sleep(self.update_interval)
