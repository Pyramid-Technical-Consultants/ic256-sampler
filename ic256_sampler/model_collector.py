"""Model Collector - High-level data collection orchestrator.

This module provides the ModelCollector class which orchestrates the entire
data collection process using DeviceManager, VirtualDatabase, and CSVWriter.

The ModelCollector uses a DeviceManager to maintain network connections and
collects data from all devices into a shared IODatabase, then creates a
VirtualDatabase and CSVWriter for output.
"""

import time
import threading
from typing import Dict, List, Optional, Any
from pathlib import Path
from .device_manager import DeviceManager
from .io_database import IODatabase
from .virtual_database import VirtualDatabase, ColumnDefinition
from .csv_writer import CSVWriter


class ModelCollector:
    """High-level orchestrator for data collection using a DeviceManager.
    
    This class coordinates:
    1. DeviceManager: Manages multiple device connections and shared IODatabase
    2. VirtualDatabase: Synthetic row generation at sampling rate
    3. CSVWriter: Asynchronous file writing
    
    The collector works with any model that provides:
    - create_columns(reference_channel) -> List[ColumnDefinition]
    - get_reference_channel() -> str
    """
    
    def __init__(
        self,
        device_manager: DeviceManager,
        model: Any,  # Model class (e.g., IC256Model)
        reference_channel: str,
        sampling_rate: int,
        file_path: str,
        device_name: str,
        note: str,
    ):
        """Initialize the model collector.
        
        Args:
            device_manager: DeviceManager instance managing device connections
            model: Model instance (e.g., IC256Model) that provides create_columns()
            reference_channel: Channel path to use as timing reference
            sampling_rate: Sampling rate in Hz (rows per second)
            file_path: Full path to CSV output file
            device_name: Name of the device (for CSV metadata)
            note: Note string to include in CSV
        """
        self.device_manager = device_manager
        self.model = model
        self.reference_channel = reference_channel
        self.sampling_rate = sampling_rate
        self.file_path = file_path
        self.device_name = device_name
        self.note = note
        
        # Use shared IODatabase from DeviceManager
        self.io_database = device_manager.get_io_database()
        
        # Create column definitions from model
        self.columns = model.create_columns(reference_channel)
        
        # Create VirtualDatabase using shared IODatabase
        self.virtual_database = VirtualDatabase(
            io_database=self.io_database,
            reference_channel=reference_channel,
            sampling_rate=sampling_rate,
            columns=self.columns,
        )
        
        # Create CSVWriter
        self.csv_writer = CSVWriter(
            virtual_database=self.virtual_database,
            file_path=file_path,
            device_name=device_name,
            note=note,
        )
        
        # Statistics tracking
        self.statistics: Dict[str, Any] = {
            "rows": 0,
            "file_size": 0,
            "file_path": file_path,
        }
        
        # Thread safety
        self._lock = threading.Lock()
        self._running = False
    
    def start(self) -> None:
        """Start data collection.
        
        Subscribes to all channels with buffered data and initializes collection.
        """
        # Subscribe all channels with buffered data
        self.device_client.sendSubscribeFields({
            field: True for field in self.channels.values()
        })
        self.device_client.updateSubscribedFields()
        
        self._running = True
    
    def collect_iteration(self) -> None:
        """Perform one iteration of data collection.
        
        This method:
        1. Rebuilds VirtualDatabase with new data from shared IODatabase
        2. Writes new rows to CSV
        3. Updates statistics
        4. Prunes old rows if safe
        
        Note: Data collection from devices happens in DeviceManager threads.
        This method only processes the collected data.
        
        This should be called in a loop while collection is active.
        """
        if not self._running:
            return
        
        # STEP 1: Rebuild virtual database with new data from shared IODatabase
        # This creates rows at the sampling rate with all conversions applied
        self.virtual_database.rebuild()
        
        # STEP 2: Write new rows to CSV
        self.csv_writer.write_all()
        
        # STEP 3: Update statistics
        with self._lock:
            total_rows = self.csv_writer.rows_written
            if total_rows != self.statistics["rows"]:
                self.statistics["rows"] = total_rows
                if total_rows % 1000 == 0:  # Update file size less frequently
                    try:
                        self.statistics["file_size"] = self.csv_writer.file_size
                    except (OSError, AttributeError):
                        pass
        
        # STEP 4: Flush/sync periodically
        if self.csv_writer.rows_written % 1000 == 0:
            self.csv_writer.flush()
        if self.csv_writer.rows_written % 5000 == 0:
            self.csv_writer.sync()
        
        # STEP 5: Prune old rows from virtual database if safe
        if self.csv_writer.can_prune_rows(rows_to_keep=1000):
            prunable = self.csv_writer.get_prunable_row_count(rows_to_keep=1000)
            if prunable > 0:
                self.virtual_database.prune_rows(keep_last_n=1000)
    
    def stop(self) -> None:
        """Stop data collection and finalize output."""
        self._running = False
        
        # Final flush and close
        self.csv_writer.flush()
        self.csv_writer.sync()
        self.csv_writer.close()
        
        # Final statistics update
        with self._lock:
            try:
                self.statistics["file_size"] = self.csv_writer.file_size
            except (OSError, AttributeError):
                pass
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get current collection statistics.
        
        Returns:
            Dictionary with keys: rows, file_size, file_path
        """
        with self._lock:
            return self.statistics.copy()
    
    def _collect_all_channel_data(self) -> Optional[int]:
        """Collect ALL data from all channels and store in IODatabase.
        
        This is the core data collection function - it must be lossless.
        
        Returns:
            Updated first timestamp (or None if no new data)
        """
        updated_first_timestamp = self.first_timestamp
        
        # Process each channel - each may have data or not (partial updates are normal)
        for field_name, channel in self.channels.items():
            try:
                # Get array of arrays: [[value, timestamp], [value, timestamp], ...]
                data = channel.getDatums()
                
                if not data:
                    continue  # No data for this channel in this update - normal
                
                # Get channel path from field name mapping
                channel_path = self.field_to_path.get(field_name)
                if not channel_path:
                    # Fallback: try to get path from field object
                    try:
                        channel_path = channel.getPath()
                    except (AttributeError, TypeError):
                        # Last resort: use field name
                        channel_path = field_name
                
                # Ensure channel exists in database
                if channel_path not in self.io_database.get_all_channels():
                    self.io_database.add_channel(channel_path)
                
                # Process EVERY entry in the array
                for data_point in data:
                    if not isinstance(data_point, (list, tuple)) or len(data_point) < 2:
                        continue
                    
                    value = data_point[0]
                    ts_raw = data_point[1]
                    
                    # Convert timestamp to nanoseconds
                    try:
                        if isinstance(ts_raw, float):
                            if ts_raw < 1e12:  # Likely seconds
                                ts_ns = int(ts_raw * 1e9)
                            else:  # Already in nanoseconds
                                ts_ns = int(ts_raw)
                        elif isinstance(ts_raw, int):
                            ts_ns = ts_raw
                        else:
                            continue
                    except (ValueError, TypeError, OverflowError):
                        continue
                    
                    # Track first timestamp
                    if updated_first_timestamp is None:
                        updated_first_timestamp = ts_ns
                    
                    # Add to database (database handles elapsed time calculation)
                    self.io_database.add_data_point(channel_path, value, ts_ns)
                    
            except Exception as e:
                print(f"Error collecting data from {field_name}: {e}")
                continue
        
        return updated_first_timestamp




def collect_data_with_model(
    collector: ModelCollector,
    stop_event: threading.Event,
    update_interval: float = 0.001,
) -> None:
    """Run data collection loop with a ModelCollector.
    
    This is a convenience function that runs the collection loop until
    stop_event is set. Can be used as a thread target.
    
    Args:
        collector: ModelCollector instance
        stop_event: Threading event to signal stop
        update_interval: Time between collection iterations (seconds)
    """
    collector.start()
    
    try:
        while not stop_event.is_set():
            collector.collect_iteration()
            time.sleep(update_interval)
    finally:
        collector.stop()